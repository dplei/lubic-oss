//! 录制事件 → 发件箱 + 分段队列 → 向总控要凭证 → 上传 → 报文件名 → 总控确认后删本地文件。
//!
//! 场次、投稿都在总控；agent 只管自己机器上的分段。两样东西落盘：
//! - **状态文件**：分段队列（JSON 快照，每次变化整份重写，先写临时文件再改名）；
//! - **发件箱**（`<state>.outbox.json`）：要报给总控的事件，总控确认落库后才删。
//!
//! 分段上传成功后先进发件箱（`Uploaded`），总控确认后才删本地文件，所以文件名丢不了。
//! 上传一个分段期间到达的录制事件，先进发件箱、再追加进事件日志（`<state>.events.jsonl`），
//! 传完再并入快照；启动时重放日志。
//!
//! 开了边录边传的房间，分段一开录就另起一个跟随任务，跟着 `.part` 的增长逐块上传，收尾补最后一块
//! （`docs/design/tail-upload.md`）。跟随状态只在内存里：出任何错、或 agent 重启，这一段就在收尾后整段上传。

use async_trait::async_trait;
use lubic::recorder::Pace;
use lubic_events::{Event, PartClosed, PartOpened};
use lubic_proto::{AgentEvent, Envelope, RoomTask};
use lubic_upos::{Checkpoint, Growth, Ticket};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::sync::{Notify, watch};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// 凭证从签发起超过这么久还没开始传，就丢掉重新申请。实测至少 60 分钟有效。
pub const TICKET_FRESH: Duration = Duration::from_secs(50 * 60);
/// 边录边传的声明大小 = 切段阈值 + 这么多：录制只在关键帧处切段，最多超出一个 GOP
/// （实测 4Mbps、GOP 3 秒约 1.5MB；20Mbps、GOP 10 秒也只 25MB）。声明多出的部分不传，没有代价。
pub const TAIL_MARGIN: u64 = 64 * 1024 * 1024;

/// 开了边录边传的房间 → 声明大小。
pub fn tail_sizes(rooms: &[RoomTask]) -> HashMap<String, u64> {
    rooms
        .iter()
        .filter(|r| r.tail_upload)
        .filter_map(|r| Some((r.name.clone(), r.split_size_mb? * 1024 * 1024 + TAIL_MARGIN)))
        .collect()
}

/// 编排器能用的远端能力：总控（凭证、事件）与 UPOS（传字节）。真实实现见 `main.rs`，测试里换成假的。
#[async_trait]
pub trait Remote: Send + Sync + Clone + 'static {
    /// 向总控要一张上传凭证。`room` 让总控选对投稿账号。
    async fn ticket(
        &self,
        room: &str,
        part_id: &str,
        path: &Path,
        size: u64,
    ) -> Result<Ticket, String>;
    async fn upload(
        &self,
        path: &Path,
        ticket: &Ticket,
        checkpoint: &mut Checkpoint,
        on_progress: &mut (dyn for<'c> FnMut(&'c Checkpoint) + Send),
    ) -> Result<String, String>;
    /// 跟着文件增长上传（边录边传）。`declared` 是申请凭证时声明的大小；`checkpoint` 是同一张凭证上次停下的断点；
    /// `on_busy` 包住每批请求。
    async fn follow(
        &self,
        growth: watch::Receiver<Growth>,
        ticket: &Ticket,
        declared: u64,
        checkpoint: &mut Checkpoint,
        on_progress: &mut (dyn for<'c> FnMut(&'c Checkpoint) + Send),
        on_busy: &mut (dyn FnMut(bool) + Send),
    ) -> Result<String, String>;
    /// 把事件报给总控，返回已落库的最大 `seq`。
    async fn report(&self, events: &[Envelope]) -> Result<u64, String>;
}

/// 编排任务、上报任务与 `/v1/watch` 循环之间共享的一点东西。
#[derive(Default)]
pub struct Link {
    /// 叫醒编排任务：总控确认了事件，或总控恢复了。
    pub wake: Notify,
    /// 总控从失联恢复：编排任务据此清掉各分段的退避，立刻补传。
    pub back: AtomicBool,
    /// 待传（含正在传）的分段数与字节数，报给总控。
    pub queued_parts: AtomicU32,
    pub queued_bytes: AtomicU64,
    /// 正在传的分段：(part_id, 已传字节, 总字节)，报给总控。一次只传一段。
    pub uploading: Mutex<Option<(String, u64, u64)>>,
    /// 上传计数：`wall` 记忙碌时长，`bytes` 记已传字节。采样任务据此算上传速度。
    pub upload: Pace,
    /// 最近 10 分钟的上传速度（字节/秒，0 为未知），上传前估算耗时用。
    pub upload_bps: AtomicU64,
    /// 开了边录边传的房间 → 声明大小（[`tail_sizes`]），随房间清单更新。
    pub tails: Mutex<HashMap<String, u64>>,
    /// 总控要求暂停上传（`Assignments::hold_uploads`，「让出网络」）：置真时在传的整段上传与跟传立即停在断点上，
    /// 不取新分段；不算失败。置假时 `/v1/watch` 循环叫醒编排任务续传。
    pub hold: watch::Sender<bool>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct State {
    pub parts: Vec<Part>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Part {
    pub part_id: String,
    pub room: String,
    /// `Open` 时是正在写的 `.part`，之后是最终文件。
    pub path: PathBuf,
    pub size: u64,
    pub upload: Upload,
    /// 上传的退避，只在内存里，重启重新计数。
    #[serde(skip)]
    pub retry: Retry,
    /// 传过分块的凭证被丢弃、整段从头重传的次数（疑似凭证中途过期），只在内存里。
    #[serde(skip)]
    pub restarts: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Upload {
    /// 录制中（收到了 `PartOpened`，还没收到 `PartClosed`）。
    Open,
    Pending,
    /// 凭证、签发时刻（unix 毫秒）与断点：重启后用同一张凭证续传。
    Uploading {
        ticket: Ticket,
        issued_at: i64,
        checkpoint: Checkpoint,
        /// 因总控要求暂停上传（`Link::hold`）停下：恢复时凭证已过 [`TICKET_FRESH`] 就丢掉断点整段重传，
        /// 不先拿旧凭证失败一次。旧状态文件没有，读出为 false。
        #[serde(default)]
        held: bool,
    },
    /// 已传完，文件名在发件箱第 `seq` 条里；总控确认后删本地文件、移除这一项。
    Uploaded {
        seq: u64,
    },
}

impl Upload {
    fn uploadable(&self) -> bool {
        matches!(self, Upload::Pending | Upload::Uploading { .. })
    }
}

/// 失败次数与下次可重试的时刻：30 秒起、翻倍、10 分钟封顶。
#[derive(Debug, Default, Clone, Copy)]
pub struct Retry {
    failures: u32,
    at: Option<Instant>,
}

impl Retry {
    fn ready(&self, now: Instant) -> bool {
        self.at.is_none_or(|t| now >= t)
    }

    fn fail(&mut self) -> Duration {
        self.failures += 1;
        let wait = Duration::from_secs(30)
            .saturating_mul(1 << (self.failures - 1).min(5))
            .min(Duration::from_secs(600));
        self.at = Some(Instant::now() + wait);
        wait
    }
}

pub fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// 录制事件的 `at`：补发的分段带着实际收尾的时刻（见 `PartClosed::closed_at_ms`），其余用收到的时刻。
fn arrived_at(event: &Event) -> i64 {
    match event {
        Event::PartClosed(p) => p.closed_at_ms,
        _ => None,
    }
    .unwrap_or_else(now_ms)
}

/// 要报给总控的事件，按 `seq` 顺序；总控确认后删掉。每次变化整份重写。
pub struct Outbox {
    path: PathBuf,
    inner: Mutex<OutboxState>,
    /// 有新事件：叫醒上报任务。
    pub added: Notify,
}

#[derive(Default, Serialize, Deserialize)]
struct OutboxState {
    next_seq: u64,
    events: Vec<Envelope>,
}

impl Outbox {
    pub fn open(path: impl Into<PathBuf>) -> std::io::Result<Self> {
        let path = path.into();
        let inner = match std::fs::read(&path) {
            Ok(bytes) => serde_json::from_slice(&bytes)?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => OutboxState::default(),
            Err(e) => return Err(e),
        };
        Ok(Self {
            path,
            inner: Mutex::new(inner),
            added: Notify::new(),
        })
    }

    fn save(&self, inner: &OutboxState) {
        if let Err(e) = write_json(&self.path, inner) {
            error!(path = %self.path.display(), error = %e, "failed to save outbox");
        }
    }

    pub fn push(&self, event: AgentEvent) -> u64 {
        let mut inner = self.inner.lock().unwrap();
        inner.next_seq += 1;
        let seq = inner.next_seq;
        inner.events.push(Envelope { seq, event });
        self.save(&inner);
        drop(inner);
        self.added.notify_one();
        seq
    }

    /// 第 `seq` 条还没被总控确认。
    pub fn pending(&self, seq: u64) -> bool {
        self.inner
            .lock()
            .unwrap()
            .events
            .iter()
            .any(|e| e.seq == seq)
    }

    pub fn batch(&self, max: usize) -> Vec<Envelope> {
        let inner = self.inner.lock().unwrap();
        inner.events.iter().take(max).cloned().collect()
    }

    pub fn ack(&self, acked: u64) {
        let mut inner = self.inner.lock().unwrap();
        let before = inner.events.len();
        inner.events.retain(|e| e.seq > acked);
        if inner.events.len() != before {
            self.save(&inner);
        }
    }

    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().events.len()
    }
}

/// 上报任务：发件箱里有事件就成批报给总控，确认后删掉并叫醒编排任务（它去删已确认的本地文件）。
/// 总控失联时按 2 秒起、翻倍、30 秒封顶重试；恢复后让编排任务清掉各分段的退避。
pub async fn report_loop<R: Remote>(
    remote: R,
    outbox: Arc<Outbox>,
    link: Arc<Link>,
    shutdown: CancellationToken,
) {
    let mut failures = 0u32;
    loop {
        let batch = outbox.batch(200);
        if batch.is_empty() {
            tokio::select! {
                _ = outbox.added.notified() => continue,
                _ = shutdown.cancelled() => return,
            }
        }
        match remote.report(&batch).await {
            Ok(acked) => {
                // 总控恢复往往先由这里发现（`/v1/watch` 可能还挂在长轮询里）。
                if failures > 0 {
                    link.back.store(true, Ordering::Relaxed);
                }
                failures = 0;
                outbox.ack(acked);
                link.wake.notify_one();
            }
            Err(e) => {
                failures += 1;
                let wait = Duration::from_secs(2)
                    .saturating_mul(1 << (failures - 1).min(4))
                    .min(Duration::from_secs(30));
                if failures == 1 {
                    warn!(
                        error = e,
                        queued = outbox.len(),
                        "report failed, events stay queued"
                    );
                }
                tokio::select! {
                    _ = tokio::time::sleep(wait) => {}
                    _ = shutdown.cancelled() => return,
                }
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Step {
    Worked,
    Idle,
    /// 失败的那一个分段进入退避。
    Failed,
    /// 剩下的事都在退避，最早到这个时刻能再试。
    Wait(Instant),
    Interrupted,
}

pub struct Pipeline<R> {
    remote: R,
    path: PathBuf,
    pub state: State,
    outbox: Arc<Outbox>,
    link: Arc<Link>,
    line: String,
    /// 正在跟随上传的分段 → 给跟随任务报收尾。丢掉发送端，跟随任务就放弃。
    tails: HashMap<String, watch::Sender<Growth>>,
    tail_tx: UnboundedSender<TailDone>,
    tail_rx: UnboundedReceiver<TailDone>,
}

/// 跟随任务的结果：UPOS 文件名，或放弃的原因（这一段改为收尾后整段上传）。
pub struct TailDone {
    part_id: String,
    result: Result<String, String>,
}

impl<R: Remote> Pipeline<R> {
    /// 读入状态文件（不存在就从空开始）并重放事件日志，再按录制侧的规则收尾停在「录制中」的分段。
    /// 必须在录制启动前调用，否则会把本次正在写的 `.part` 当成遗留。
    pub fn open(
        remote: R,
        path: impl Into<PathBuf>,
        outbox: Arc<Outbox>,
        link: Arc<Link>,
        line: String,
    ) -> std::io::Result<Self> {
        let path = path.into();
        let state: State = match std::fs::read(&path) {
            Ok(bytes) => serde_json::from_slice(&bytes)?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => State::default(),
            Err(e) => return Err(e),
        };
        let (tail_tx, tail_rx) = unbounded_channel();
        let mut pipeline = Self {
            remote,
            path,
            state,
            outbox,
            link,
            line,
            tails: HashMap::new(),
            tail_tx,
            tail_rx,
        };
        let journal = journal_path(&pipeline.path);
        if let Ok(text) = std::fs::read_to_string(&journal) {
            for line in text.lines() {
                match serde_json::from_str(line) {
                    Ok(ev) => pipeline.apply(ev),
                    Err(e) => warn!(error = %e, "skipping unreadable event log line"),
                }
            }
        }
        let outbox = pipeline.outbox.clone();
        pipeline.state.parts.retain_mut(|p| {
            let keep = recover_open_part(p);
            if !keep {
                outbox.push(AgentEvent::Lost {
                    part_id: p.part_id.clone(),
                });
            }
            keep
        });
        pipeline.save()?;
        let _ = std::fs::remove_file(journal);
        Ok(pipeline)
    }

    fn save(&self) -> std::io::Result<()> {
        publish_queue(&self.link, &self.state);
        write_json(&self.path, &self.state)
    }

    fn save_or_log(&self) -> bool {
        match self.save() {
            Ok(()) => true,
            Err(e) => {
                error!(path = %self.path.display(), error = %e, "failed to save state");
                false
            }
        }
    }

    /// 录制侧事件刚到：先进发件箱，再改队列。
    pub fn arrive(&mut self, event: Event) {
        self.outbox.push(AgentEvent::Recorder {
            at: arrived_at(&event),
            event: event.clone(),
        });
        self.apply(event);
    }

    /// 只改队列（重放日志时也走这里，不重复进发件箱）。
    fn apply(&mut self, event: Event) {
        match event {
            Event::PartOpened(part) => self.part_opened(part),
            Event::PartClosed(part) => self.part_closed(part),
            // 录制侧删掉了空分段，总控从转发的事件里知道它是空段，不用另报 Lost。
            Event::PartDiscarded(part) => self.forget(&part.part_id),
            // 录制已结束，还挂着「录制中」的是被录制侧丢弃的空分段。
            Event::SessionEnded(owner) => self.drop_open(&owner.room),
        }
        self.save_or_log();
    }

    /// 丢掉房间里停在「录制中」的分段，并告诉总控它们没有内容。
    fn drop_open(&mut self, room: &str) {
        let Self { outbox, tails, .. } = self;
        self.state.parts.retain(|p| {
            let phantom = p.room == room && p.upload == Upload::Open;
            if phantom {
                tails.remove(&p.part_id);
                outbox.push(AgentEvent::Lost {
                    part_id: p.part_id.clone(),
                });
            }
            !phantom
        });
    }

    fn forget(&mut self, part_id: &str) {
        self.tails.remove(part_id);
        self.state.parts.retain(|p| p.part_id != part_id);
    }

    fn part(&mut self, part_id: &str) -> Option<&mut Part> {
        self.state.parts.iter_mut().find(|p| p.part_id == part_id)
    }

    fn part_opened(&mut self, ev: PartOpened) {
        if self.part(&ev.part_id).is_some() {
            return;
        }
        // 一个房间同时只写一个分段：新分段开录时上一个还在「录制中」，说明它被录制侧当空分段丢了。
        self.drop_open(&ev.owner.room);
        let declared = self.link.tails.lock().unwrap().get(&ev.owner.room).copied();
        if let Some(declared) = declared {
            self.start_tail(&ev.owner.room, &ev.part_id, &ev.path, declared);
        }
        self.state.parts.push(Part {
            part_id: ev.part_id,
            room: ev.owner.room,
            path: ev.path,
            size: 0,
            upload: Upload::Open,
            retry: Retry::default(),
            restarts: 0,
        });
    }

    fn start_tail(&mut self, room: &str, part_id: &str, path: &Path, declared: u64) {
        let (tx, rx) = watch::channel(Growth {
            path: path.to_path_buf(),
            closed: false,
        });
        info!(path = %path.display(), declared, "tail upload starts with the recording");
        tokio::spawn(tail(
            self.remote.clone(),
            self.link.clone(),
            room.to_string(),
            part_id.to_string(),
            declared,
            rx,
            self.tail_tx.clone(),
        ));
        self.tails.insert(part_id.to_string(), tx);
    }

    /// 跟随任务结束：成功就和整段上传成功一样进发件箱；失败就等收尾后整段上传。
    fn tail_done(&mut self, done: TailDone) {
        self.tails.remove(&done.part_id);
        let Some(part) = self.part(&done.part_id) else {
            return;
        };
        let shown = part.path.display().to_string();
        match done.result {
            Ok(filename) if part.upload == Upload::Pending => {
                let size = part.size;
                info!(path = shown, filename, "tail upload finished");
                let seq = self.outbox.push(AgentEvent::Uploaded {
                    part_id: done.part_id.clone(),
                    filename,
                    line: self.line.clone(),
                    size,
                });
                self.part(&done.part_id).unwrap().upload = Upload::Uploaded { seq };
                self.save_or_log();
            }
            Ok(_) => warn!(
                path = shown,
                "tail upload finished on an unclosed part, ignored"
            ),
            Err(e) => warn!(
                path = shown,
                error = e,
                "tail upload given up, the part will be uploaded whole once it closes"
            ),
        }
    }

    fn part_closed(&mut self, ev: PartClosed) {
        info!(room = ev.owner.room, path = %ev.path.display(), size = ev.size, "part queued");
        signal_closed(&self.tails, &ev);
        if let Some(part) = self.part(&ev.part_id) {
            if part.upload == Upload::Open {
                part.path = ev.path;
                part.size = ev.size;
                part.upload = Upload::Pending;
            }
            return;
        }
        self.state.parts.push(Part {
            part_id: ev.part_id,
            room: ev.owner.room,
            path: ev.path,
            size: ev.size,
            upload: Upload::Pending,
            retry: Retry::default(),
            restarts: 0,
        });
    }

    /// 删掉总控已确认的分段的本地文件，返回删了几个。
    fn clean_acked(&mut self) -> usize {
        let outbox = &self.outbox;
        let before = self.state.parts.len();
        self.state.parts.retain(|p| match p.upload {
            Upload::Uploaded { seq } if !outbox.pending(seq) => {
                if let Err(e) = std::fs::remove_file(&p.path)
                    && e.kind() != std::io::ErrorKind::NotFound
                {
                    warn!(path = %p.path.display(), error = %e, "failed to delete uploaded part");
                }
                info!(path = %p.path.display(), "controller has it, deleted local file");
                false
            }
            _ => true,
        });
        let n = before - self.state.parts.len();
        if n > 0 {
            self.save_or_log();
        }
        n
    }

    /// 做一件事：先删已确认的本地文件，再上传最早的待传分段；在退避中的跳过。
    /// `rx` 给了就在上传期间继续收事件（进发件箱、记进事件日志）。
    pub async fn step(
        &mut self,
        rx: Option<&mut UnboundedReceiver<Event>>,
        shutdown: &CancellationToken,
    ) -> Step {
        if self.link.back.swap(false, Ordering::Relaxed) {
            info!("controller is back, retrying parts now");
            for p in &mut self.state.parts {
                p.retry = Retry::default();
            }
        }
        while let Ok(done) = self.tail_rx.try_recv() {
            self.tail_done(done);
        }
        if self.clean_acked() > 0 {
            return Step::Worked;
        }
        if *self.link.hold.borrow() {
            return Step::Idle;
        }
        let now = Instant::now();
        let mut wait: Option<Instant> = None;
        for i in 0..self.state.parts.len() {
            let p = &self.state.parts[i];
            if !p.upload.uploadable() || self.tails.contains_key(&p.part_id) {
                continue;
            }
            if p.retry.ready(now) {
                return self.upload(i, rx, shutdown).await;
            }
            if let Some(at) = p.retry.at {
                wait = Some(wait.map_or(at, |w| w.min(at)));
            }
        }
        wait.map_or(Step::Idle, Step::Wait)
    }

    fn part_failed(&mut self, i: usize, what: &str, error: String) -> Step {
        let part = &mut self.state.parts[i];
        let wait = part.retry.fail();
        warn!(path = %part.path.display(), error, ?wait, failures = part.retry.failures, "{what} failed, backing off this part");
        Step::Failed
    }

    async fn upload(
        &mut self,
        i: usize,
        mut rx: Option<&mut UnboundedReceiver<Event>>,
        shutdown: &CancellationToken,
    ) -> Step {
        let part_id = self.state.parts[i].part_id.clone();
        let room = self.state.parts[i].room.clone();
        let path = self.state.parts[i].path.clone();
        let shown = path.display().to_string();
        let size = match std::fs::metadata(&path) {
            Ok(m) => m.len(),
            Err(e) => {
                warn!(path = shown, error = %e, "part file is gone, skipping it");
                self.outbox.push(AgentEvent::Lost {
                    part_id: part_id.clone(),
                });
                self.state.parts.remove(i);
                self.save_or_log();
                return Step::Worked;
            }
        };
        let held = match &self.state.parts[i].upload {
            Upload::Uploading {
                ticket,
                issued_at,
                checkpoint,
                held,
            } => {
                // 一块都没传的旧凭证可能已过期，重新申请几乎没有代价；已经传了一部分的继续用。
                // 暂停停下的往往停了几个小时，过期了就不赌，整段重传。
                let stale = now_ms() - issued_at > TICKET_FRESH.as_millis() as i64;
                if stale && checkpoint.done.is_empty() {
                    info!(
                        path = shown,
                        "ticket is stale and unused, requesting a new one"
                    );
                    None
                } else if stale && *held {
                    info!(
                        path = shown,
                        chunks = checkpoint.done.len(),
                        "ticket went stale while uploads were held, part restarts with a new one"
                    );
                    None
                } else {
                    Some((ticket.clone(), *issued_at, checkpoint.clone()))
                }
            }
            _ => None,
        };
        let (ticket, issued_at, mut checkpoint) = match held {
            Some(h) => h,
            None => match self.remote.ticket(&room, &part_id, &path, size).await {
                Ok(t) => (t, now_ms(), Checkpoint::default()),
                Err(e) if e.contains(lubic_proto::DISCARDED) => {
                    if let Err(e) = std::fs::remove_file(&path) {
                        warn!(path = shown, error = %e, "failed to delete discarded part");
                    }
                    info!(
                        path = shown,
                        "archive discarded on the controller, deleted local file"
                    );
                    self.outbox.push(AgentEvent::Lost { part_id });
                    self.state.parts.remove(i);
                    self.save_or_log();
                    return Step::Worked;
                }
                Err(e) => return self.part_failed(i, "ticket", e),
            },
        };
        let before = checkpoint.done.len();
        let chunk = ticket.chunk_size.max(1);
        let chunk_len = move |n: u64| chunk.min(size.saturating_sub(n * chunk));
        let remaining = size - checkpoint.done.iter().map(|&n| chunk_len(n)).sum::<u64>();
        let bps = self.link.upload_bps.load(Ordering::Relaxed);
        if bps > 0 && remaining / bps > TICKET_FRESH.as_secs() {
            warn!(
                path = shown,
                remaining,
                upload_bps = bps,
                eta_min = remaining / bps / 60,
                "upload may outlive its ticket"
            );
        }
        self.state.parts[i].upload = Upload::Uploading {
            ticket: ticket.clone(),
            issued_at,
            checkpoint: checkpoint.clone(),
            held: false,
        };
        self.save_or_log();

        info!(
            path = shown,
            size,
            resumed_chunks = before,
            "uploading part"
        );
        let started = std::time::Instant::now();
        let journal = journal_path(&self.path);
        let mut arrived = Vec::new();
        let link = self.link.clone();
        let _busy = link.upload.running();
        let mut seen = checkpoint.done.clone();
        let mut done = size - remaining;
        let mut paused = false;
        *link.uploading.lock().unwrap() = Some((part_id.clone(), done, size));
        let result = {
            let Self {
                remote,
                state,
                path: state_path,
                outbox,
                tails,
                ..
            } = self;
            let mut on_progress = |cp: &Checkpoint| {
                let new: u64 = cp.done.difference(&seen).map(|&n| chunk_len(n)).sum();
                link.upload.add(Duration::ZERO, new);
                done += new;
                *link.uploading.lock().unwrap() = Some((part_id.clone(), done, size));
                seen.clone_from(&cp.done);
                state.parts[i].upload = Upload::Uploading {
                    ticket: ticket.clone(),
                    issued_at,
                    checkpoint: cp.clone(),
                    held: false,
                };
                if let Err(e) = write_json(state_path, state) {
                    warn!(error = %e, "failed to save upload checkpoint");
                }
            };
            let upload = remote.upload(&path, &ticket, &mut checkpoint, &mut on_progress);
            tokio::pin!(upload);
            let mut held = link.hold.subscribe();
            let paused = &mut paused;
            loop {
                let next = async {
                    match rx.as_deref_mut() {
                        Some(rx) => rx.recv().await,
                        None => std::future::pending().await,
                    }
                };
                tokio::select! {
                    r = &mut upload => break Some(r),
                    ev = next => match ev {
                        Some(ev) => {
                            // 跟随任务等着收尾，不能等这一段传完才告诉它。
                            if let Event::PartClosed(closed) = &ev {
                                signal_closed(tails, closed);
                            }
                            outbox.push(AgentEvent::Recorder { at: arrived_at(&ev), event: ev.clone() });
                            append_journal(&journal, &ev);
                            arrived.push(ev);
                        }
                        None => rx = None,
                    },
                    _ = shutdown.cancelled() => break None,
                    // 停在断点上，不记失败；恢复后从断点续传（票据过期走上面的换新逻辑）。
                    _ = held.wait_for(|h| *h) => {
                        info!(path = shown, "uploads held by the controller, paused at the checkpoint");
                        *paused = true;
                        break None;
                    }
                }
            }
        };
        *link.uploading.lock().unwrap() = None;
        for ev in arrived {
            self.apply(ev);
        }
        let _ = std::fs::remove_file(&journal);
        let Some(result) = result else {
            if paused
                && let Some(Part {
                    upload: Upload::Uploading { held, .. },
                    ..
                }) = self.part(&part_id)
            {
                *held = true;
                self.save_or_log();
            }
            return Step::Interrupted;
        };
        // 刚并入的事件可能删掉了排在前面的空分段，下标要按 part_id 重新找。
        let Some(i) = self.state.parts.iter().position(|p| p.part_id == part_id) else {
            return Step::Worked;
        };
        match result {
            Ok(filename) => {
                info!(path = shown, filename, elapsed = ?started.elapsed(), "part uploaded");
                // 文件名先进发件箱再改状态：中间崩溃最多重传一次，不会丢文件名。
                let seq = self.outbox.push(AgentEvent::Uploaded {
                    part_id,
                    filename,
                    line: self.line.clone(),
                    size,
                });
                self.state.parts[i].upload = Upload::Uploaded { seq };
                self.save_or_log();
                Step::Worked
            }
            Err(e) => {
                let age_min = (now_ms() - issued_at) / 60_000;
                let chunks = checkpoint.done.len();
                // 这张凭证一块都没传上，多半是凭证失效：丢掉它，下次重新申请。
                if chunks == before {
                    self.state.parts[i].upload = Upload::Pending;
                    self.save_or_log();
                    // 之前传过分块却突然一块都传不上：疑似中途过期，整段从头重传。
                    // 日志留着实证凭证有效期（见 handoff.md「受限机上的凭证」）。
                    if chunks > 0 {
                        let part = &mut self.state.parts[i];
                        part.restarts += 1;
                        if part.restarts >= 2 {
                            error!(
                                path = shown,
                                ticket_age_min = age_min,
                                chunks,
                                restarts = part.restarts,
                                "ticket failed mid-upload again, part restarts from scratch"
                            );
                        } else {
                            warn!(
                                path = shown,
                                ticket_age_min = age_min,
                                chunks,
                                "ticket failed mid-upload, part restarts from scratch"
                            );
                        }
                    }
                }
                self.part_failed(
                    i,
                    &format!("upload (ticket {age_min} min old, {chunks} chunks done)"),
                    e,
                )
            }
        }
    }

    /// 常驻：收事件、做事，失败的那一项各自退避。`shutdown` 触发后收完已到的事件就返回；
    /// 正在传的分段停在断点上，下次续传。
    pub async fn run(mut self, mut rx: UnboundedReceiver<Event>, shutdown: CancellationToken) {
        let mut closed = false;
        let link = self.link.clone();
        loop {
            while let Ok(ev) = rx.try_recv() {
                self.arrive(ev);
            }
            if shutdown.is_cancelled() {
                return;
            }
            let wake = match self.step((!closed).then_some(&mut rx), &shutdown).await {
                Step::Worked | Step::Failed | Step::Interrupted => continue,
                Step::Idle => None,
                Step::Wait(t) => Some(t),
            };
            let wake = async {
                match wake {
                    Some(t) => tokio::time::sleep_until(t).await,
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                ev = rx.recv(), if !closed => match ev {
                    Some(ev) => self.arrive(ev),
                    None => closed = true,
                },
                _ = wake => {}
                _ = link.wake.notified() => {}
                _ = shutdown.cancelled() => {}
            }
        }
    }
}

fn signal_closed(tails: &HashMap<String, watch::Sender<Growth>>, ev: &PartClosed) {
    if let Some(tx) = tails.get(&ev.part_id) {
        tx.send_replace(Growth {
            path: ev.path.clone(),
            closed: true,
        });
    }
}

/// 跟随任务：开录就申请凭证（声明大小），跟着文件增长传，收尾补最后一块。结果交回编排任务。
async fn tail<R: Remote>(
    remote: R,
    link: Arc<Link>,
    room: String,
    part_id: String,
    declared: u64,
    growth: watch::Receiver<Growth>,
    done: UnboundedSender<TailDone>,
) {
    let result = follow_part(&remote, &link, &room, &part_id, declared, growth).await;
    let _ = done.send(TailDone { part_id, result });
    link.wake.notify_one();
}

async fn follow_part<R: Remote>(
    remote: &R,
    link: &Link,
    room: &str,
    part_id: &str,
    declared: u64,
    mut growth: watch::Receiver<Growth>,
) -> Result<String, String> {
    // 凭证用最终文件名申请：`upos_uri` 的扩展名跟着它走。
    let part = growth.borrow().path.clone();
    let name = part
        .to_str()
        .and_then(|p| p.strip_suffix(".part"))
        .map_or(part.clone(), PathBuf::from);
    let mut retry = Retry::default();
    let (ticket, issued) = loop {
        match remote.ticket(room, part_id, &name, declared).await {
            Ok(t) => break (t, Instant::now()),
            Err(e) => {
                let wait = retry.fail();
                warn!(path = %part.display(), error = e, ?wait, "tail ticket failed, retrying while recording");
                tokio::select! {
                    _ = tokio::time::sleep(wait) => {}
                    r = growth.changed() => if r.is_err() {
                        return Err("part abandoned".into());
                    },
                }
                if growth.borrow().closed {
                    return Err("part closed before a ticket was issued".into());
                }
            }
        }
    };
    let chunk = ticket.chunk_size.max(1);
    let watched = growth.clone();
    let mut seen = std::collections::BTreeSet::new();
    let mut on_progress = |cp: &Checkpoint| {
        // 只有收尾后的最后一块可能不满。
        let g = watched.borrow().clone();
        let size = std::fs::metadata(&g.path).map_or(u64::MAX, |m| m.len());
        let new: u64 = cp
            .done
            .difference(&seen)
            .map(|&n| chunk.min(size.saturating_sub(n * chunk)))
            .sum();
        seen.clone_from(&cp.done);
        link.upload.add(Duration::ZERO, new);
        debug!(path = %g.path.display(), chunks = seen.len(), "tail chunk uploaded");
    };
    // 超时时上传被中途丢弃，`on_busy(false)` 来不及调用，靠它补停计时。
    let busy = AtomicBool::new(false);
    let mut on_busy = |b: bool| {
        busy.store(b, Ordering::Relaxed);
        if b {
            link.upload.start()
        } else {
            link.upload.stop()
        }
    };
    let mut checkpoint = Checkpoint::default();
    let mut held = link.hold.subscribe();
    // 总控要求暂停上传：停下跟传，等恢复后带着断点接着传；等待时间照样算进凭证有效期。
    let upload = async {
        loop {
            while *held.borrow_and_update() {
                tokio::select! {
                    r = held.changed() => if r.is_err() {
                        return Err("agent shutting down".into());
                    },
                    r = growth.changed() => if r.is_err() {
                        return Err("part abandoned".into());
                    },
                }
            }
            tokio::select! {
                r = remote.follow(growth.clone(), &ticket, declared, &mut checkpoint, &mut on_progress, &mut on_busy) => break r,
                _ = held.wait_for(|h| *h) => {
                    info!(path = %part.display(), chunks = checkpoint.done.len(), "tail upload held by the controller");
                    if busy.swap(false, Ordering::Relaxed) {
                        link.upload.stop();
                    }
                }
            }
        }
    };
    let result = tokio::time::timeout_at(issued + TICKET_FRESH, upload).await;
    if busy.load(Ordering::Relaxed) {
        link.upload.stop();
    }
    result.unwrap_or_else(|_| {
        Err(format!(
            "ticket is {} min old and the part is not done",
            TICKET_FRESH.as_secs() / 60
        ))
    })
}

fn publish_queue(link: &Link, state: &State) {
    let queued = state.parts.iter().filter(|p| p.upload.uploadable());
    let (n, bytes) = queued.fold((0, 0), |(n, b), p| (n + 1, b + p.size));
    link.queued_parts.store(n, Ordering::Relaxed);
    link.queued_bytes.store(bytes, Ordering::Relaxed);
}

/// 启动时处理停在「录制中」的分段，返回是否留下：
/// 最终文件已在（改名后、事件落盘前崩溃）→ 按已关闭入队；只剩 `.part`（写到一半崩溃）→
/// 按录制侧规则收尾，空的删掉；都不在 → 丢掉这条记录。
fn recover_open_part(part: &mut Part) -> bool {
    if part.upload != Upload::Open {
        return true;
    }
    let shown = part.path.display().to_string();
    let finished = part
        .path
        .to_str()
        .and_then(|p| p.strip_suffix(".part"))
        .map(PathBuf::from)
        .filter(|p| p.exists());
    let recovered = match finished {
        Some(path) => Some(path),
        None if part.path.exists() => lubic::recorder::salvage_part(&part.path),
        None => None,
    };
    match recovered.and_then(|p| Some((std::fs::metadata(&p).ok()?.len(), p))) {
        Some((size, path)) => {
            info!(part = shown, path = %path.display(), size, "recovered leftover segment");
            part.path = path;
            part.size = size;
            part.upload = Upload::Pending;
            true
        }
        None => {
            info!(part = shown, "leftover segment had no media, dropped");
            false
        }
    }
}

fn journal_path(state: &Path) -> PathBuf {
    state.with_extension("events.jsonl")
}

fn append_journal(journal: &Path, ev: &Event) {
    let line = serde_json::to_string(ev).expect("event serializes");
    let written = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(journal)
        .and_then(|mut f| writeln!(f, "{line}"));
    if let Err(e) = written {
        error!(path = %journal.display(), error = %e, "failed to append event log");
    }
}

/// 整份重写：先写 `.tmp` 再改名，崩溃不会留下半个文件。
pub fn write_json(path: &Path, value: &impl Serialize) -> std::io::Result<()> {
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, serde_json::to_vec_pretty(value)?)?;
    std::fs::rename(tmp, path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use lubic_events::{CloseReason, Owner};
    use tokio::sync::mpsc::UnboundedSender;

    /// 发事件用的 sender、要发的事件、事件日志路径。
    type DuringUpload = (UnboundedSender<Event>, Vec<Event>, PathBuf);

    #[derive(Default)]
    struct Fake {
        calls: Mutex<Vec<String>>,
        /// 下一次上传传完一块就失败。
        fail_upload_once: Mutex<bool>,
        /// 上传一块都传不上（模拟凭证中途过期）。
        stuck: Mutex<bool>,
        /// 文件名主干在这里的分段，申请凭证总是失败。
        broken: Mutex<Vec<String>>,
        /// 下一次上传期间往 channel 里发这些事件，并记下事件日志里有几行。
        during_upload: Mutex<Option<DuringUpload>>,
        /// 总控失联：报事件失败。
        offline: Mutex<bool>,
        /// 稿件已在总控丢弃：申请凭证返回 410。
        discarded: Mutex<bool>,
        /// 跟随上传传完一块就失败。
        tail_fail: Mutex<bool>,
        /// 跟随上传卡在请求里不返回（慢到凭证过期）。
        tail_hang: Mutex<bool>,
        reported: Mutex<Vec<Envelope>>,
        /// 设了就在每块传完后记下它报的「正在传」。
        link: Mutex<Option<Arc<Link>>>,
        /// 整段上传或跟传传到这么多块时，像总控下发了暂停一样把 `link.hold` 置真，然后卡住不返回。
        hold_at: Mutex<Option<usize>>,
    }

    /// 传到 `hold_at` 块：置真暂停并卡住（等调用方因暂停丢掉这次上传）。
    async fn maybe_hold(f: &Fake, done: usize) {
        let hold = *f.hold_at.lock().unwrap() == Some(done);
        if hold {
            f.hold_at.lock().unwrap().take();
            let link = f.link.lock().unwrap().clone().unwrap();
            link.hold.send_replace(true);
            std::future::pending::<()>().await;
        }
    }

    /// 编排器要能把远端交给跟随任务（`'static`），测试里的假远端就漏掉。
    fn leak() -> &'static Fake {
        Box::leak(Box::default())
    }

    fn log(f: &Fake, s: String) {
        f.calls.lock().unwrap().push(s);
    }

    #[async_trait]
    impl Remote for &'static Fake {
        async fn ticket(&self, _: &str, _: &str, path: &Path, size: u64) -> Result<Ticket, String> {
            let stem = path.file_stem().unwrap().to_str().unwrap();
            log(self, format!("ticket {stem} {size}"));
            if *self.discarded.lock().unwrap() {
                return Err(format!("/v1/ticket: 410 Gone {}", lubic_proto::DISCARDED));
            }
            if *self.offline.lock().unwrap()
                || self.broken.lock().unwrap().iter().any(|b| b == stem)
            {
                return Err("rejected".into());
            }
            Ok(Ticket {
                endpoint: "//fake".into(),
                auth: "a".into(),
                upos_uri: format!("upos://b/n-{stem}.flv"),
                chunk_size: 1,
                biz_id: 1,
            })
        }

        async fn upload(
            &self,
            path: &Path,
            ticket: &Ticket,
            cp: &mut Checkpoint,
            on_progress: &mut (dyn for<'c> FnMut(&'c Checkpoint) + Send),
        ) -> Result<String, String> {
            let size = std::fs::metadata(path).unwrap().len();
            let during = self.during_upload.lock().unwrap().take();
            if let Some((tx, events, journal)) = during {
                for ev in events {
                    tx.send(ev).unwrap();
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
                let lines = std::fs::read_to_string(journal).unwrap_or_default();
                log(self, format!("journaled {}", lines.lines().count()));
            }
            log(
                self,
                format!("upload {} from {}", ticket.filename(), cp.done.len()),
            );
            if *self.stuck.lock().unwrap() {
                return Err("403 auth expired".into());
            }
            for i in 0..size {
                if cp.done.contains(&i) {
                    continue;
                }
                cp.done.insert(i);
                on_progress(cp);
                if let Some(link) = &*self.link.lock().unwrap() {
                    log(
                        self,
                        format!("uploading {:?}", link.uploading.lock().unwrap()),
                    );
                }
                maybe_hold(self, cp.done.len()).await;
                let mut fail = self.fail_upload_once.lock().unwrap();
                if *fail {
                    *fail = false;
                    return Err("network".into());
                }
            }
            Ok(ticket.filename())
        }

        /// 块大小 1 字节：没写完时文件有几字节就传几块，收尾后返回。
        async fn follow(
            &self,
            mut growth: watch::Receiver<Growth>,
            ticket: &Ticket,
            declared: u64,
            cp: &mut Checkpoint,
            on_progress: &mut (dyn for<'c> FnMut(&'c Checkpoint) + Send),
            on_busy: &mut (dyn FnMut(bool) + Send),
        ) -> Result<String, String> {
            let from = match cp.done.len() {
                0 => String::new(),
                n => format!(" from {n}"),
            };
            log(
                self,
                format!("follow {} {declared}{from}", ticket.filename()),
            );
            if *self.tail_hang.lock().unwrap() {
                on_busy(true);
                std::future::pending::<()>().await;
            }
            loop {
                let g = growth.borrow_and_update().clone();
                let size = std::fs::metadata(&g.path).map_or(0, |m| m.len());
                on_busy(true);
                for i in cp.done.len() as u64..size {
                    cp.done.insert(i);
                    on_progress(cp);
                    maybe_hold(self, cp.done.len()).await;
                    if *self.tail_fail.lock().unwrap() {
                        on_busy(false);
                        return Err("network".into());
                    }
                }
                on_busy(false);
                if g.closed {
                    return Ok(ticket.filename());
                }
                growth.changed().await.map_err(|_| "abandoned")?;
            }
        }

        async fn report(&self, events: &[Envelope]) -> Result<u64, String> {
            if *self.offline.lock().unwrap() {
                return Err("offline".into());
            }
            self.reported.lock().unwrap().extend(events.iter().cloned());
            Ok(events.last().map_or(0, |e| e.seq))
        }
    }

    fn closed(dir: &Path, name: &str, bytes: &[u8]) -> Event {
        let path = dir.join(format!("{name}.flv"));
        std::fs::write(&path, bytes).unwrap();
        Event::PartClosed(PartClosed {
            owner: owner(),
            part_id: name.into(),
            path,
            duration: Duration::from_secs(60),
            size: bytes.len() as u64,
            reason: CloseReason::TimedSplit,
            title: "t".into(),
            closed_at_ms: None,
        })
    }

    fn opened(dir: &Path, name: &str) -> (Event, PathBuf) {
        let part = dir.join(format!("{name}.flv.part"));
        let ev = Event::PartOpened(PartOpened {
            owner: owner(),
            part_id: name.into(),
            path: part.clone(),
            title: "t".into(),
            tz_offset_secs: None,
        });
        (ev, part)
    }

    fn owner() -> Owner {
        Owner {
            room: "r1".into(),
            session: "s1".into(),
        }
    }

    struct Env {
        dir: tempfile::TempDir,
        state: PathBuf,
        outbox: Arc<Outbox>,
        link: Arc<Link>,
    }

    impl Env {
        fn new() -> Self {
            let dir = tempfile::tempdir().unwrap();
            let state = dir.path().join("state.json");
            Self {
                outbox: Arc::new(Outbox::open(state.with_extension("outbox.json")).unwrap()),
                link: Arc::default(),
                state,
                dir,
            }
        }

        fn open(&self, fake: &'static Fake) -> Pipeline<&'static Fake> {
            Pipeline::open(
                fake,
                &self.state,
                self.outbox.clone(),
                self.link.clone(),
                "tx".into(),
            )
            .unwrap()
        }

        fn file(&self, name: &str) -> PathBuf {
            self.dir.path().join(name)
        }

        /// 模拟上报任务跑一轮。
        async fn report(&self, fake: &'static Fake) {
            if let Ok(acked) = fake.report(&self.outbox.batch(100)).await {
                self.outbox.ack(acked);
            }
        }

        fn outbox_kinds(&self) -> Vec<String> {
            self.outbox
                .batch(100)
                .iter()
                .map(|e| match &e.event {
                    AgentEvent::Recorder { event, .. } => match event {
                        Event::PartOpened(p) => format!("opened {}", p.part_id),
                        Event::PartClosed(p) => format!("closed {}", p.part_id),
                        Event::PartDiscarded(p) => format!("discarded {}", p.part_id),
                        Event::SessionEnded(_) => "ended".into(),
                    },
                    AgentEvent::Uploaded {
                        part_id, filename, ..
                    } => format!("uploaded {part_id} {filename}"),
                    AgentEvent::Lost { part_id } => format!("lost {part_id}"),
                    AgentEvent::Recording { room, .. } => format!("recording {room}"),
                })
                .collect()
        }
    }

    async fn until_idle<R: Remote>(p: &mut Pipeline<R>) -> Vec<Step> {
        let never = CancellationToken::new();
        let mut steps = Vec::new();
        loop {
            let step = p.step(None, &never).await;
            if matches!(step, Step::Idle | Step::Wait(_)) {
                return steps;
            }
            steps.push(step);
        }
    }

    #[tokio::test]
    async fn uploaded_part_is_deleted_only_after_controller_acks_its_filename() {
        let env = Env::new();
        let fake = leak();
        let mut p = env.open(fake);
        let (ev, _) = opened(env.dir.path(), "p1");
        p.arrive(ev);
        p.arrive(closed(env.dir.path(), "p1", b"abc"));
        assert_eq!(until_idle(&mut p).await, [Step::Worked]);
        assert_eq!(
            env.outbox_kinds(),
            ["opened p1", "closed p1", "uploaded p1 n-p1"]
        );
        // 总控还没确认：文件留着。
        assert!(env.file("p1.flv").exists());
        assert_eq!(p.state.parts[0].upload, Upload::Uploaded { seq: 3 });

        env.report(fake).await;
        assert_eq!(until_idle(&mut p).await, [Step::Worked]);
        assert!(!env.file("p1.flv").exists());
        assert!(p.state.parts.is_empty());
        let reported = fake.reported.lock().unwrap();
        assert!(matches!(
            &reported[2].event,
            AgentEvent::Uploaded { line, size: 3, .. } if line == "tx"
        ));
    }

    /// 故障试拉转正时一次补发多个暂存分段：总控按 `at − duration` 推开录时刻并据此排序，
    /// 所以补发的事件要带分段真实收尾的时刻，否则先短后长的两段会排反。
    #[tokio::test]
    async fn promoted_trial_parts_keep_their_recorded_order() {
        let env = Env::new();
        let fake = leak();
        let mut p = env.open(fake);
        let staging = env.dir.path().join(".trial");
        std::fs::create_dir_all(staging.join("r")).unwrap();
        let formal = lubic::recorder::RecordConfig {
            output: env.dir.path().join("rec/{name}").display().to_string(),
            filename: Some("x".into()),
            ..Default::default()
        };
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let gate = Arc::new(crate::trial::Gate::new(
            staging.clone(),
            formal,
            "r".into(),
            tx,
        ));
        let (staged, rec) = tokio::sync::mpsc::unbounded_channel();
        let forward = tokio::spawn({
            let gate = gate.clone();
            async move { gate.forward(rec).await }
        });
        // 短段 a 先收尾；长段 b 是 a 之后才开录的，300ms 后收尾。
        for (name, ms) in [("a", 10), ("b", 300)] {
            if name == "b" {
                tokio::time::sleep(Duration::from_millis(ms)).await;
            }
            let Event::PartClosed(mut part) = closed(&staging.join("r"), name, b"x") else {
                unreachable!()
            };
            part.duration = Duration::from_millis(ms);
            staged.send(Event::PartClosed(part)).unwrap();
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        gate.promote(false);
        drop(staged);
        forward.await.unwrap();
        while let Ok(ev) = rx.try_recv() {
            p.arrive(ev);
        }
        env.report(fake).await;
        let opened: Vec<_> = fake
            .reported
            .lock()
            .unwrap()
            .iter()
            .filter_map(|e| match &e.event {
                AgentEvent::Recorder {
                    event: Event::PartClosed(c),
                    at,
                } => Some((c.part_id.clone(), at - c.duration.as_millis() as i64)),
                _ => None,
            })
            .collect();
        let [(a, a_at), (b, b_at)] = &opened[..] else {
            panic!("{opened:?}");
        };
        assert_eq!((a.as_str(), b.as_str()), ("a", "b"));
        assert!(a_at < b_at, "开录顺序：{opened:?}");
    }

    #[tokio::test]
    async fn discarded_archive_deletes_the_part_without_uploading() {
        let env = Env::new();
        let fake = leak();
        *fake.discarded.lock().unwrap() = true;
        let mut p = env.open(fake);
        p.arrive(closed(env.dir.path(), "p1", b"abc"));
        assert_eq!(until_idle(&mut p).await, [Step::Worked]);
        assert!(!env.file("p1.flv").exists());
        assert!(p.state.parts.is_empty());
        assert_eq!(env.outbox_kinds(), ["closed p1", "lost p1"]);
    }

    #[tokio::test]
    async fn controller_offline_keeps_recording_queued_and_catches_up_when_back() {
        let env = Env::new();
        let fake = leak();
        *fake.offline.lock().unwrap() = true;
        let mut p = env.open(fake);
        p.arrive(closed(env.dir.path(), "p1", b"ab"));
        p.arrive(closed(env.dir.path(), "p2", b"cd"));
        let never = CancellationToken::new();
        // 拿不到凭证：每个分段各自退避，文件都留在本地，事件都在发件箱。
        assert_eq!(p.step(None, &never).await, Step::Failed);
        assert_eq!(p.step(None, &never).await, Step::Failed);
        assert!(matches!(p.step(None, &never).await, Step::Wait(_)));
        env.report(fake).await;
        assert_eq!(env.outbox.len(), 2);
        assert!(env.file("p1.flv").exists() && env.file("p2.flv").exists());

        // 总控恢复：清掉退避立刻补传，确认后删除。
        *fake.offline.lock().unwrap() = false;
        env.link.back.store(true, Ordering::Relaxed);
        assert_eq!(until_idle(&mut p).await, [Step::Worked, Step::Worked]);
        env.report(fake).await;
        until_idle(&mut p).await;
        assert!(!env.file("p1.flv").exists() && !env.file("p2.flv").exists());
        assert_eq!(env.outbox.len(), 0);
        assert_eq!(fake.reported.lock().unwrap().len(), 4);
    }

    /// 上传计量只算新完成的分块；传过分块的凭证突然一块都传不上（疑似中途过期）时，
    /// 丢掉凭证整段重传并记一次 restart。
    #[tokio::test]
    async fn upload_is_metered_and_mid_upload_ticket_loss_restarts_the_part() {
        let env = Env::new();
        let fake = leak();
        let never = CancellationToken::new();
        let mut p = env.open(fake);
        p.arrive(closed(env.dir.path(), "p1", b"abc"));
        *fake.fail_upload_once.lock().unwrap() = true;
        *fake.link.lock().unwrap() = Some(env.link.clone());
        assert_eq!(p.step(None, &never).await, Step::Failed);
        assert_eq!(env.link.upload.read().bytes, 1);
        assert!(
            fake.calls
                .lock()
                .unwrap()
                .contains(&r#"uploading Some(("p1", 1, 3))"#.to_string()),
            "{:?}",
            fake.calls.lock().unwrap()
        );
        assert_eq!(
            *env.link.uploading.lock().unwrap(),
            None,
            "失败退避中不算在传"
        );
        assert!(
            env.link.upload.read().wall > Duration::ZERO,
            "上传期间计忙碌时长"
        );
        assert!(matches!(p.state.parts[0].upload, Upload::Uploading { .. }));

        *fake.stuck.lock().unwrap() = true;
        p.state.parts[0].retry = Retry::default();
        assert_eq!(p.step(None, &never).await, Step::Failed);
        assert_eq!(p.state.parts[0].upload, Upload::Pending);
        assert_eq!(p.state.parts[0].restarts, 1);

        *fake.stuck.lock().unwrap() = false;
        p.state.parts[0].retry = Retry::default();
        assert_eq!(p.step(None, &never).await, Step::Worked);
        assert_eq!(env.link.upload.read().bytes, 4, "从头重传 3 块");
        assert_eq!(*env.link.uploading.lock().unwrap(), None);
        let tickets = fake
            .calls
            .lock()
            .unwrap()
            .iter()
            .filter(|c| c.starts_with("ticket"))
            .count();
        assert_eq!(tickets, 2);
    }

    #[tokio::test]
    async fn restart_resumes_unfinished_upload_and_refreshes_stale_unused_ticket() {
        let env = Env::new();
        let fake = leak();
        {
            let mut p = env.open(fake);
            p.arrive(closed(env.dir.path(), "p1", b"cde"));
            *fake.fail_upload_once.lock().unwrap() = true;
            assert_eq!(p.step(None, &CancellationToken::new()).await, Step::Failed);
            // 再放一个拿了凭证、一块没传、凭证已签发 51 分钟的分段。
            p.arrive(closed(env.dir.path(), "p2", b"f"));
            p.state.parts[1].upload = Upload::Uploading {
                ticket: fake.ticket("", "", Path::new("old.flv"), 1).await.unwrap(),
                issued_at: now_ms() - 51 * 60 * 1000,
                checkpoint: Checkpoint::default(),
                held: false,
            };
            p.save().unwrap();
        } // 进程「崩溃」

        let mut p = env.open(fake);
        assert_eq!(until_idle(&mut p).await, [Step::Worked, Step::Worked]);
        let calls = fake.calls.lock().unwrap().clone();
        assert_eq!(
            calls,
            [
                "ticket p1 3",
                "upload n-p1 from 0",
                "ticket old 1",
                // p1 用同一张凭证从第 1 块续传，没有重新申请。
                "upload n-p1 from 1",
                // p2 的旧凭证过期且没用过：重新申请。
                "ticket p2 1",
                "upload n-p2 from 0",
            ]
        );
    }

    #[tokio::test]
    async fn missing_file_is_reported_lost() {
        let env = Env::new();
        let fake = leak();
        let mut p = env.open(fake);
        let ev = closed(env.dir.path(), "p1", b"a");
        std::fs::remove_file(env.file("p1.flv")).unwrap();
        p.arrive(ev);
        until_idle(&mut p).await;
        assert!(p.state.parts.is_empty());
        assert_eq!(env.outbox_kinds(), ["closed p1", "lost p1"]);
    }

    #[tokio::test]
    async fn discarded_parts_leave_the_queue_without_lost() {
        let env = Env::new();
        let fake = leak();
        let mut p = env.open(fake);
        p.arrive(opened(env.dir.path(), "e1").0);
        let Event::PartClosed(empty) = closed(env.dir.path(), "e1", b"FLV") else {
            unreachable!()
        };
        p.arrive(Event::PartDiscarded(empty));
        p.arrive(Event::SessionEnded(owner()));
        assert!(p.state.parts.is_empty());
        assert_eq!(env.outbox_kinds(), ["opened e1", "discarded e1", "ended"]);
    }

    #[tokio::test]
    async fn discarded_empty_parts_are_reported_lost() {
        let env = Env::new();
        let fake = leak();
        let mut p = env.open(fake);
        // 录制侧丢弃的空分段只留下「录制中」：下一段开录或下播时报 Lost。
        p.arrive(opened(env.dir.path(), "e1").0);
        p.arrive(opened(env.dir.path(), "p2").0);
        p.arrive(Event::SessionEnded(owner()));
        assert!(p.state.parts.is_empty());
        assert_eq!(
            env.outbox_kinds(),
            ["opened e1", "opened p2", "lost e1", "ended", "lost p2"]
        );
    }

    #[tokio::test]
    async fn run_uploads_reports_and_deletes_then_stops_on_shutdown() {
        let env = Env::new();
        let fake = leak();
        let p = env.open(fake);
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(p.run(rx, shutdown.clone()));
        let reporter = tokio::spawn(report_loop(
            fake,
            env.outbox.clone(),
            env.link.clone(),
            shutdown.clone(),
        ));
        tx.send(closed(env.dir.path(), "p1", b"ab")).unwrap();
        for _ in 0..200 {
            if !env.file("p1.flv").exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        shutdown.cancel();
        task.await.unwrap();
        reporter.await.unwrap();
        assert!(!env.file("p1.flv").exists());
        assert_eq!(env.outbox.len(), 0);
    }

    #[tokio::test]
    async fn reporter_retries_while_offline_and_flags_controller_back() {
        let env = Env::new();
        let fake = leak();
        *fake.offline.lock().unwrap() = true;
        env.outbox.push(AgentEvent::Lost {
            part_id: "a".into(),
        });
        let shutdown = CancellationToken::new();
        let reporter = tokio::spawn(report_loop(
            fake,
            env.outbox.clone(),
            env.link.clone(),
            shutdown.clone(),
        ));
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(env.outbox.len(), 1);
        assert!(!env.link.back.load(Ordering::Relaxed));
        *fake.offline.lock().unwrap() = false;
        for _ in 0..400 {
            if env.outbox.len() == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(env.outbox.len(), 0);
        assert!(env.link.back.load(Ordering::Relaxed));
        shutdown.cancel();
        reporter.await.unwrap();
    }

    #[tokio::test]
    async fn failing_part_backs_off_alone_and_later_parts_still_upload() {
        let env = Env::new();
        let fake = leak();
        fake.broken.lock().unwrap().push("p1".into());
        let mut p = env.open(fake);
        p.arrive(closed(env.dir.path(), "p1", b"ab"));
        p.arrive(closed(env.dir.path(), "p2", b"cd"));
        let never = CancellationToken::new();
        assert_eq!(p.step(None, &never).await, Step::Failed);
        assert_eq!(p.step(None, &never).await, Step::Worked);
        assert!(matches!(p.state.parts[1].upload, Upload::Uploaded { .. }));
        let Step::Wait(at) = p.step(None, &never).await else {
            panic!("expected Wait");
        };
        assert!(at > Instant::now() + Duration::from_secs(25));
    }

    /// `len`（至少 43）字节、两帧 AAC 相隔 1 秒的最小 FLV：录制侧按「有没有满 1 秒的音视频」判断空分段，
    /// 随机字节会被丢掉。第二帧只写到正文前两字节，补零到 `len`。
    fn media_flv(len: usize) -> Vec<u8> {
        let mut out = vec![b'F', b'L', b'V', 1, 4, 0, 0, 0, 9, 0, 0, 0, 0];
        out.extend([8, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0xaf, 1, 0, 0, 0, 13]);
        out.extend([8, 0, 0, 2, 0, 0x03, 0xe8, 0, 0, 0, 0, 0xaf, 1]);
        out.resize(len, 0);
        out
    }

    #[tokio::test]
    async fn restart_recovers_segments_left_by_a_crash() {
        let env = Env::new();
        let fake = leak();
        let dir = env.dir.path();
        {
            let mut p = env.open(fake);
            // (b) 已改名为最终文件，PartClosed 没来得及落盘。
            let (ev, part) = opened(dir, "renamed");
            p.apply(ev);
            std::fs::write(part.with_extension(""), b"media").unwrap();
            // (a) 写到一半的 .part，有媒体。另起一个房间，免得被当成上一段的空分段。
            let (Event::PartOpened(mut ev), part) = opened(dir, "half") else {
                unreachable!()
            };
            ev.owner.room = "r2".into();
            p.apply(Event::PartOpened(ev));
            std::fs::write(&part, media_flv(60)).unwrap();
        } // 崩溃
        // 上传期间收到、只写进事件日志的 PartOpened 也要收回（房间 r3）。
        let (Event::PartOpened(mut ev), part) = opened(dir, "journaled") else {
            unreachable!()
        };
        ev.owner.room = "r3".into();
        std::fs::write(&part, media_flv(50)).unwrap();
        append_journal(&journal_path(&env.state), &Event::PartOpened(ev));
        // 只有 FLV 头的（房间 r4）。
        let (Event::PartOpened(mut ev), part) = opened(dir, "empty") else {
            unreachable!()
        };
        ev.owner.room = "r4".into();
        std::fs::write(&part, vec![1u8; 13]).unwrap();
        append_journal(&journal_path(&env.state), &Event::PartOpened(ev));

        let p = env.open(fake);
        assert!(!journal_path(&env.state).exists());
        let parts: Vec<_> = p
            .state
            .parts
            .iter()
            .map(|p| (p.part_id.as_str(), p.size, p.upload.clone()))
            .collect();
        assert_eq!(
            parts,
            [
                ("renamed", 5, Upload::Pending),
                ("half", 60, Upload::Pending),
                ("journaled", 50, Upload::Pending),
            ]
        );
        assert!(env.file("half.flv").exists());
        assert!(!env.file("empty.flv.part").exists());
        assert_eq!(env.outbox_kinds(), ["lost empty"]);
    }

    #[tokio::test]
    async fn events_during_upload_reach_outbox_and_journal_and_indices_survive() {
        let env = Env::new();
        let fake = leak();
        let mut p = env.open(fake);
        p.arrive(opened(env.dir.path(), "discarded").0);
        p.arrive(closed(env.dir.path(), "p1", b"ab"));
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        *fake.during_upload.lock().unwrap() = Some((
            tx,
            vec![
                closed(env.dir.path(), "p2", b"c"),
                Event::SessionEnded(owner()),
            ],
            journal_path(&env.state),
        ));
        let never = CancellationToken::new();
        assert_eq!(p.step(Some(&mut rx), &never).await, Step::Worked);
        assert!(
            fake.calls
                .lock()
                .unwrap()
                .contains(&"journaled 2".to_string())
        );
        assert!(!journal_path(&env.state).exists());
        let ids: Vec<_> = p.state.parts.iter().map(|p| p.part_id.as_str()).collect();
        assert_eq!(ids, ["p1", "p2"]);
        assert_eq!(
            env.outbox_kinds(),
            [
                "opened discarded",
                "closed p1",
                "closed p2",
                "ended",
                "lost discarded",
                "uploaded p1 n-p1"
            ]
        );
    }

    #[tokio::test]
    async fn outbox_survives_restart_and_acks_in_order() {
        let env = Env::new();
        env.outbox.push(AgentEvent::Lost {
            part_id: "a".into(),
        });
        env.outbox.push(AgentEvent::Lost {
            part_id: "b".into(),
        });
        env.outbox.ack(1);
        let again = Outbox::open(env.state.with_extension("outbox.json")).unwrap();
        assert_eq!(again.batch(10)[0].seq, 2);
        assert_eq!(
            again.push(AgentEvent::Lost {
                part_id: "c".into()
            }),
            3
        );
    }

    /// 反复做事，直到 `done` 成立（跟随任务在别的 task 里跑，要给它时间）。
    async fn settle<R: Remote>(p: &mut Pipeline<R>, done: impl Fn(&Pipeline<R>) -> bool) {
        let never = CancellationToken::new();
        for _ in 0..300 {
            p.step(None, &never).await;
            if done(p) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("did not settle: {:?}", p.state.parts);
    }

    fn tail_env(declared: u64) -> Env {
        let env = Env::new();
        env.link.tails.lock().unwrap().insert("r1".into(), declared);
        env
    }

    /// 收尾：`.part` 改名成最终文件，再发 `PartClosed`。
    fn close(env: &Env, name: &str) -> Event {
        let part = env.file(&format!("{name}.flv.part"));
        let bytes = std::fs::read(&part).unwrap();
        std::fs::remove_file(part).unwrap();
        closed(env.dir.path(), name, &bytes)
    }

    fn calls(fake: &Fake) -> Vec<String> {
        fake.calls.lock().unwrap().clone()
    }

    #[test]
    fn tail_sizes_only_for_rooms_with_a_size_split() {
        let task = |name: &str, tail, mb| RoomTask {
            name: name.into(),
            url: "u".into(),
            split_time: None,
            split_size_mb: mb,
            grace: None,
            bili_qn: None,
            douyin_quality: None,
            filename: String::new(),
            streamer: String::new(),
            tail_upload: tail,
        };
        let sizes = tail_sizes(&[
            task("a", true, Some(200)),
            task("b", false, Some(200)),
            task("c", true, None),
        ]);
        assert_eq!(
            sizes,
            [("a".to_string(), 200 * 1024 * 1024 + TAIL_MARGIN)].into()
        );
    }

    /// 开录就按声明大小、最终文件名要凭证；录制中跟着传；收尾后补完、进发件箱，确认后删除，从不整段上传。
    #[tokio::test]
    async fn tail_part_uploads_while_recording_and_is_deleted_once_acked() {
        let env = tail_env(100);
        let fake = leak();
        let mut p = env.open(fake);
        let (ev, part) = opened(env.dir.path(), "p1");
        p.arrive(ev);
        std::fs::write(&part, b"abc").unwrap();
        settle(&mut p, |_| env.link.upload.read().bytes == 3).await;
        assert_eq!(calls(fake), ["ticket p1 100", "follow n-p1 100"]);
        assert_eq!(
            p.state.parts[0].upload,
            Upload::Open,
            "录制中仍是 Open，崩溃收回不变"
        );
        assert!(
            env.link.upload.read().lag_ms.is_none(),
            "等文件长时不算忙碌"
        );

        std::fs::OpenOptions::new()
            .append(true)
            .open(&part)
            .unwrap()
            .write_all(b"de")
            .unwrap();
        p.arrive(close(&env, "p1"));
        settle(&mut p, |p| {
            matches!(p.state.parts[0].upload, Upload::Uploaded { .. })
        })
        .await;
        assert_eq!(env.link.upload.read().bytes, 5);
        assert!(
            calls(fake).iter().all(|c| !c.starts_with("upload")),
            "{:?}",
            calls(fake)
        );
        assert_eq!(env.outbox_kinds().last().unwrap(), "uploaded p1 n-p1");

        env.report(fake).await;
        settle(&mut p, |p| p.state.parts.is_empty()).await;
        assert!(!env.file("p1.flv").exists());
        let reported = fake.reported.lock().unwrap();
        assert!(matches!(
            reported.last().unwrap().event,
            AgentEvent::Uploaded { size: 5, .. }
        ));
    }

    /// 跟随上传出错：收尾前不碰它，收尾后用实际大小重新申请凭证、整段上传。
    #[tokio::test]
    async fn failed_tail_upload_falls_back_to_a_whole_upload_after_close() {
        let env = tail_env(100);
        let fake = leak();
        *fake.tail_fail.lock().unwrap() = true;
        let mut p = env.open(fake);
        let (ev, part) = opened(env.dir.path(), "p1");
        p.arrive(ev);
        std::fs::write(&part, b"abc").unwrap();
        settle(&mut p, |p| p.tails.is_empty()).await;
        assert_eq!(p.state.parts[0].upload, Upload::Open);

        p.arrive(close(&env, "p1"));
        settle(&mut p, |p| {
            matches!(p.state.parts[0].upload, Upload::Uploaded { .. })
        })
        .await;
        assert_eq!(
            calls(fake),
            [
                "ticket p1 100",
                "follow n-p1 100",
                "ticket p1 3",
                "upload n-p1 from 0"
            ]
        );
    }

    /// 录制期间一直要不到凭证：收尾时放弃跟随，整段上传。
    #[tokio::test]
    async fn closing_before_a_tail_ticket_falls_back_to_a_whole_upload() {
        let env = tail_env(100);
        let fake = leak();
        fake.broken.lock().unwrap().push("p1".into());
        let mut p = env.open(fake);
        let (ev, part) = opened(env.dir.path(), "p1");
        p.arrive(ev);
        std::fs::write(&part, b"ab").unwrap();
        settle(&mut p, |_| !calls(fake).is_empty()).await;
        fake.broken.lock().unwrap().clear();
        p.arrive(close(&env, "p1"));
        settle(&mut p, |p| {
            matches!(p.state.parts[0].upload, Upload::Uploaded { .. })
        })
        .await;
        assert_eq!(
            calls(fake),
            ["ticket p1 100", "ticket p1 2", "upload n-p1 from 0"]
        );
    }

    /// 凭证签发 50 分钟还没传完就放弃跟随（时钟是假的，自动快进）。
    #[tokio::test(start_paused = true)]
    async fn tail_upload_gives_up_when_its_ticket_gets_old() {
        let env = tail_env(100);
        let fake = leak();
        *fake.tail_hang.lock().unwrap() = true;
        let mut p = env.open(fake);
        p.arrive(opened(env.dir.path(), "p1").0);
        settle(&mut p, |_| calls(fake).len() == 2).await;
        assert!(
            env.link.upload.read().lag_ms.is_some(),
            "卡在请求里，算忙碌"
        );
        tokio::time::sleep(TICKET_FRESH - Duration::from_secs(1)).await;
        p.step(None, &CancellationToken::new()).await;
        assert_eq!(p.tails.len(), 1, "49 分钟时还在跟");
        tokio::time::sleep(Duration::from_secs(2)).await;
        settle(&mut p, |p| p.tails.is_empty()).await;
        assert_eq!(p.state.parts[0].upload, Upload::Open);
        assert!(env.link.upload.read().lag_ms.is_none(), "丢弃时补停计时");
    }

    /// 总控要求暂停上传（让出网络）：在传的整段停在断点上，不记失败、不报丢失、不取新分段；
    /// 恢复后用同一张凭证从断点续传完成。
    #[tokio::test]
    async fn held_upload_stops_at_its_checkpoint_and_resumes_from_it() {
        let env = Env::new();
        let fake = leak();
        let never = CancellationToken::new();
        let mut p = env.open(fake);
        p.arrive(closed(env.dir.path(), "p1", b"abcd"));
        p.arrive(closed(env.dir.path(), "p2", b"ef"));
        *fake.link.lock().unwrap() = Some(env.link.clone());
        *fake.hold_at.lock().unwrap() = Some(2);
        assert_eq!(p.step(None, &never).await, Step::Interrupted);
        let Upload::Uploading { checkpoint, .. } = &p.state.parts[0].upload else {
            panic!("{:?}", p.state.parts[0].upload);
        };
        assert_eq!(checkpoint.done.len(), 2, "断点保留");
        assert_eq!(p.state.parts[0].retry.failures, 0, "不算失败");
        assert_eq!(*env.link.uploading.lock().unwrap(), None);
        // 暂停期间不取新分段。
        assert_eq!(until_idle(&mut p).await, []);
        assert!(!env.outbox_kinds().iter().any(|k| k.starts_with("lost")));

        env.link.hold.send_replace(false);
        assert_eq!(until_idle(&mut p).await, [Step::Worked, Step::Worked]);
        let uploads: Vec<_> = calls(fake)
            .into_iter()
            .filter(|c| c.starts_with("upload ") || c.starts_with("ticket"))
            .collect();
        assert_eq!(
            uploads,
            [
                "ticket p1 4",
                "upload n-p1 from 0",
                "upload n-p1 from 2",
                "ticket p2 2",
                "upload n-p2 from 0"
            ]
        );
        assert_eq!(env.link.upload.read().bytes, 6, "没有重传");
    }

    /// 暂停了很久、恢复时凭证已超过 `TICKET_FRESH`：丢掉断点换新凭证整段重传，不先拿旧凭证失败一次，
    /// 不记失败、不算中途过期重传。没暂停过的照旧用传了一部分的旧凭证续传（受限机慢传）。
    #[tokio::test]
    async fn held_upload_with_a_stale_ticket_restarts_with_a_new_one() {
        let stale = || now_ms() - TICKET_FRESH.as_millis() as i64 - 60_000;
        let age = |p: &mut Pipeline<&'static Fake>| {
            let Upload::Uploading { issued_at, .. } = &mut p.state.parts[0].upload else {
                panic!("{:?}", p.state.parts[0].upload);
            };
            *issued_at = stale();
        };
        let uploads = |fake: &Fake| -> Vec<String> {
            calls(fake)
                .into_iter()
                .filter(|c| c.starts_with("upload ") || c.starts_with("ticket"))
                .collect()
        };
        let env = Env::new();
        let fake = leak();
        let mut p = env.open(fake);
        p.arrive(closed(env.dir.path(), "p1", b"abcd"));
        *fake.link.lock().unwrap() = Some(env.link.clone());
        *fake.hold_at.lock().unwrap() = Some(2);
        assert_eq!(
            p.step(None, &CancellationToken::new()).await,
            Step::Interrupted
        );
        age(&mut p);
        p.save_or_log();
        // 重启也记得是暂停停下的。
        drop(p);
        let mut p = env.open(fake);
        env.link.hold.send_replace(false);
        assert_eq!(until_idle(&mut p).await, [Step::Worked]);
        assert_eq!(
            uploads(fake),
            [
                "ticket p1 4",
                "upload n-p1 from 0",
                "ticket p1 4",
                "upload n-p1 from 0"
            ]
        );
        assert_eq!(
            (p.state.parts[0].restarts, p.state.parts[0].retry.failures),
            (0, 0)
        );

        // 没暂停过：传了一部分的旧凭证照旧续传。
        let env = Env::new();
        let fake = leak();
        let mut p = env.open(fake);
        p.arrive(closed(env.dir.path(), "p1", b"abcd"));
        *fake.fail_upload_once.lock().unwrap() = true;
        assert_eq!(p.step(None, &CancellationToken::new()).await, Step::Failed);
        age(&mut p);
        p.state.parts[0].retry = Retry::default();
        assert_eq!(until_idle(&mut p).await, [Step::Worked]);
        assert_eq!(
            uploads(fake),
            ["ticket p1 4", "upload n-p1 from 0", "upload n-p1 from 1"]
        );
    }

    /// 跟传同理：暂停时停下、不放弃，恢复后带着断点接着跟，收尾后完成，不整段重传。
    #[tokio::test]
    async fn held_tail_upload_resumes_from_its_checkpoint() {
        let env = tail_env(100);
        let fake = leak();
        *fake.link.lock().unwrap() = Some(env.link.clone());
        *fake.hold_at.lock().unwrap() = Some(1);
        let mut p = env.open(fake);
        let (ev, part) = opened(env.dir.path(), "p1");
        p.arrive(ev);
        std::fs::write(&part, b"abc").unwrap();
        settle(&mut p, |_| *env.link.hold.borrow()).await;
        // 跟随任务还在（没有放弃），停在 1 块上。
        for _ in 0..10 {
            p.step(None, &CancellationToken::new()).await;
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(p.tails.len(), 1);
        assert_eq!(env.link.upload.read().bytes, 1);
        assert!(env.link.upload.read().lag_ms.is_none(), "暂停时补停计时");

        env.link.hold.send_replace(false);
        p.arrive(close(&env, "p1"));
        settle(&mut p, |p| {
            matches!(p.state.parts[0].upload, Upload::Uploaded { .. })
        })
        .await;
        assert_eq!(
            calls(fake),
            ["ticket p1 100", "follow n-p1 100", "follow n-p1 100 from 1"]
        );
        assert_eq!(env.link.upload.read().bytes, 3);
    }

    /// 被录制侧丢掉的空分段：跟随任务跟着放弃，不留下什么。
    #[tokio::test]
    async fn discarded_tail_part_abandons_its_follower() {
        let env = tail_env(100);
        let fake = leak();
        let mut p = env.open(fake);
        p.arrive(opened(env.dir.path(), "e1").0);
        settle(&mut p, |_| calls(fake).len() == 2).await;
        p.arrive(Event::SessionEnded(owner()));
        settle(&mut p, |_| {
            env.outbox_kinds().contains(&"lost e1".to_string())
        })
        .await;
        assert!(p.tails.is_empty() && p.state.parts.is_empty());
    }
}
