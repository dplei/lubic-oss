//! agent：录总控派来的房间 → 每个分段向总控要凭证上传 → 报文件名 → 总控确认后删本地文件。
//!
//! 录制（`lubic`）和上传（`lubic-upos`）互不 import，只在这里经 `lubic-events` 的 channel 接起来。
//! 投稿账号的 cookie 只在总控；agent 至多有一份拉流用的 `record_cookies`。

mod monitor;
mod pipeline;
mod trial;

use async_trait::async_trait;
use chrono::FixedOffset;
use clap::Parser;
use lubic::downloader::live::{LivePlugin, builtin_plugins};
use lubic::recorder::{Pace, RecordConfig, watch_room};
use lubic_events::Event;
use lubic_proto::{
    Ack, AgentEvent, Assignments, Envelope, PROBE_PREFIX, Report, RoomTask, Status, TRIAL_PREFIX,
    TicketRequest, Watch,
};
use lubic_upos::{Checkpoint, Growth, Ticket};
use monitor::Monitor;
use pipeline::{Link, Outbox, Pipeline, Remote, report_loop, write_json};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use trial::Gate;

#[derive(Parser)]
#[command(version = if lubic_proto::COMMIT.is_empty() { "dev" } else { lubic_proto::COMMIT })]
struct Cli {
    /// 配置文件（TOML）。
    #[arg(short, long, default_value = "lubic-agent.toml")]
    config: PathBuf,
    /// 日志目录：除 stdout 外再按天写 `agent.YYYY-MM-DD.log`，保留 14 天。不配只输出到 stdout。
    #[arg(long)]
    log_dir: Option<PathBuf>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    /// 总控地址，如 `https://lubic.example.com`。
    controller: String,
    /// 总控配置里 `[agents.<本机名>]` 的 token，总控据此认出是哪台。
    token: String,
    /// 状态文件（JSON）：未传完的分段与断点。含上传凭证，别入库。
    /// 旁边还有 `.outbox.json`（待报事件）与 `.rooms.json`（最后一份房间清单）。
    state: PathBuf,
    /// UPOS 线路（preupload 的 upcdn），探测不可靠，显式指定。
    #[serde(default = "default_line")]
    line: String,
    /// 录制目录所在磁盘的使用率到这个比例就停录。
    #[serde(default = "default_threshold")]
    disk_threshold: f64,
    /// 录制目录，支持 `{name}`（房间名）；文件名模板由总控随房间下发。
    /// 旧写法（含 strftime 的完整文件模板，如 `/rec/{name}/{name}_%Y-%m-%dT%H_%M_%S`）照旧可用：
    /// 那时忽略总控下发的文件名模板，启动时警告一次。
    #[serde(default = "default_output")]
    output: String,
    /// 录 B 站用的 cookie 文件（拉高画质要登录）。最好是专录制的小号，不是投稿账号。
    record_cookies: Option<PathBuf>,
}

fn default_line() -> String {
    "tx".into()
}
fn default_threshold() -> f64 {
    0.85
}
fn default_output() -> String {
    "recordings/{name}".into()
}

/// 总控没发文件名模板（旧总控）时用的，与旧版默认的文件名相同。
const LEGACY_FILENAME: &str = "{streamer}_%Y-%m-%dT%H_%M_%S";

impl Config {
    /// `output` 还是带文件名的旧写法（含 strftime）。
    fn legacy_output(&self) -> bool {
        self.output.contains('%')
    }

    fn load(path: &Path) -> Result<Self, String> {
        let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
        toml::from_str(&text).map_err(|e| format!("{}: {e}", path.display()))
    }

    fn record_config(
        &self,
        room: &RoomTask,
        events: UnboundedSender<Event>,
        pace: Arc<Pace>,
        tz_offset: Option<FixedOffset>,
    ) -> RecordConfig {
        let filename = match room.filename.as_str() {
            "" => LEGACY_FILENAME,
            f => f,
        };
        let streamer = match room.streamer.as_str() {
            "" => &room.name,
            s => s,
        };
        let mut c = RecordConfig {
            output: self.output.clone(),
            filename: (!self.legacy_output()).then(|| filename.to_string()),
            streamer: streamer.to_string(),
            split_time: room.split_time,
            split_size: room.split_size_mb.map(|mb| mb * 1024 * 1024),
            events: Some(events),
            pace: Some(pace),
            tz_offset,
            ..Default::default()
        };
        if let Some(grace) = room.grace {
            c.offline_grace = grace;
        }
        if let Some(qn) = room.bili_qn {
            c.options.bilibili.qn = qn;
        }
        if let Some(q) = &room.douyin_quality {
            c.options.douyin.quality = q.clone();
        }
        c.credentials.bilibili_cookie_file = self.record_cookies.clone();
        c
    }

    /// 试拉写盘的目录：录制目录下的 `.probe/`，试拉一停就整个删掉。
    fn probe_dir(&self) -> PathBuf {
        record_dir(&self.output).join(".probe")
    }

    /// 试拉用的录制参数：同正常录制，但写进 [`Self::probe_dir`]、不发事件（不上传、不进稿件）。
    fn probe_config(
        &self,
        room: &RoomTask,
        events: UnboundedSender<Event>,
        pace: Arc<Pace>,
        tz_offset: Option<FixedOffset>,
    ) -> RecordConfig {
        let mut c = self.record_config(room, events, pace, tz_offset);
        c.output = self
            .probe_dir()
            .join("{name}")
            .to_string_lossy()
            .into_owned();
        c.filename = Some(c.filename.unwrap_or_else(|| LEGACY_FILENAME.into()));
        c.events = None;
        c
    }

    /// 故障试拉暂存录制的目录：录制目录下的 `.trial/`，与到期前试拉的 `.probe/` 分开（`handoff-health.md` §6.1）。
    fn trial_dir(&self) -> PathBuf {
        record_dir(&self.output).join(".trial")
    }

    /// 故障试拉用的录制参数：同正常录制，但转正前写进 [`Self::trial_dir`]（见 `RecordConfig::staging`）。
    fn trial_config(
        &self,
        room: &RoomTask,
        events: UnboundedSender<Event>,
        pace: Arc<Pace>,
        tz_offset: Option<FixedOffset>,
    ) -> RecordConfig {
        let mut c = self.record_config(room, events, pace, tz_offset);
        let dir = self.trial_dir().join("{name}");
        // 旧写法的 `output` 没有单独的文件名模板，暂存目录要自带文件名。
        let dir = match self.legacy_output() {
            true => dir.join(LEGACY_FILENAME),
            false => dir,
        };
        c.staging = Some(dir.to_string_lossy().into_owned());
        c
    }
}

/// 总控的 HTTP 客户端，兼做上传（上传只拿凭证传字节）。
#[derive(Clone)]
struct Controller {
    api: reqwest::Client,
    upload: reqwest::Client,
    base: String,
    token: String,
    line: String,
}

impl Controller {
    fn new(config: &Config) -> Self {
        Self {
            api: reqwest::Client::builder()
                .timeout(Duration::from_secs(60))
                .build()
                .expect("http client"),
            upload: reqwest::Client::new(),
            base: config.controller.trim_end_matches('/').to_string(),
            token: config.token.clone(),
            line: config.line.clone(),
        }
    }

    async fn post<T: DeserializeOwned>(
        &self,
        path: &str,
        body: &impl serde::Serialize,
        timeout: Duration,
    ) -> Result<T, String> {
        let res = self
            .api
            .post(format!("{}{path}", self.base))
            .bearer_auth(&self.token)
            .json(body)
            .timeout(timeout)
            .send()
            .await
            .map_err(|e| e.to_string())?;
        let status = res.status();
        if !status.is_success() {
            let text = res.text().await.unwrap_or_default();
            return Err(format!("{path}: {status} {text}"));
        }
        res.json().await.map_err(|e| format!("{path}: {e}"))
    }

    async fn watch(&self, watch: &Watch) -> Result<Assignments, String> {
        let timeout = lubic_proto::WATCH_HOLD + Duration::from_secs(15);
        self.post(lubic_proto::WATCH, watch, timeout).await
    }
}

#[async_trait]
impl Remote for Controller {
    async fn ticket(
        &self,
        room: &str,
        part_id: &str,
        path: &Path,
        size: u64,
    ) -> Result<Ticket, String> {
        let req = TicketRequest {
            room: room.into(),
            part_id: part_id.into(),
            file_name: path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("a.flv")
                .into(),
            size,
            line: self.line.clone(),
        };
        self.post(lubic_proto::TICKET, &req, Duration::from_secs(60))
            .await
    }

    async fn upload(
        &self,
        path: &Path,
        ticket: &Ticket,
        checkpoint: &mut Checkpoint,
        on_progress: &mut (dyn for<'c> FnMut(&'c Checkpoint) + Send),
    ) -> Result<String, String> {
        lubic_upos::upload(
            &self.upload,
            path,
            ticket,
            &Default::default(),
            checkpoint,
            |cp| on_progress(cp),
        )
        .await
        .map_err(|e| e.to_string())
    }

    async fn follow(
        &self,
        growth: watch::Receiver<Growth>,
        ticket: &Ticket,
        declared: u64,
        checkpoint: &mut Checkpoint,
        on_progress: &mut (dyn for<'c> FnMut(&'c Checkpoint) + Send),
        on_busy: &mut (dyn FnMut(bool) + Send),
    ) -> Result<String, String> {
        let options = lubic_upos::Options {
            declared_size: Some(declared),
            ..Default::default()
        };
        lubic_upos::follow(
            &self.upload,
            growth,
            ticket,
            &options,
            checkpoint,
            |cp| on_progress(cp),
            &mut *on_busy,
        )
        .await
        .map_err(|e| e.to_string())
    }

    async fn report(&self, events: &[Envelope]) -> Result<u64, String> {
        let report = Report {
            events: events.to_vec(),
        };
        let ack: Ack = self
            .post(lubic_proto::REPORT, &report, Duration::from_secs(60))
            .await?;
        Ok(ack.acked)
    }
}

/// 一个房间的录制任务；停下时收尾当前分段。
struct Recorder {
    task: RoomTask,
    /// 监测里的键：正常录制是房间名，试拉带前缀。
    key: String,
    pace: Arc<Pace>,
    handle: JoinHandle<()>,
    cancel: CancellationToken,
}

/// 起停录制要用的几样东西。
struct Rooms<'a> {
    config: &'a Config,
    events: &'a UnboundedSender<Event>,
    monitor: &'a Monitor,
    /// 平台插件，测试里换成本地的。
    plugins: Vec<Arc<dyn LivePlugin>>,
    /// 总控下发的统一时区偏移；`None` 时退回本机 `Local`。
    tz_offset: Option<FixedOffset>,
}

/// 总控派来试拉的房间在本机用的任务：名字带 [`PROBE_PREFIX`]，监测里与正常录制分开计数。
fn probe_task(task: &RoomTask) -> RoomTask {
    RoomTask {
        name: format!("{PROBE_PREFIX}{}", task.name),
        tail_upload: false,
        ..task.clone()
    }
}

/// 删掉试拉写的文件。目录不存在不算错。
fn clear_probe_dir(dir: &Path) {
    match std::fs::remove_dir_all(dir) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => {
            warn!(dir = %dir.display(), error = %e, "failed to remove probe files");
        }
        _ => {}
    }
}

/// 删掉两种试拉留下的文件（启动时调一次）。
fn clear_side_dirs(config: &Config) {
    clear_probe_dir(&config.probe_dir());
    clear_probe_dir(&config.trial_dir());
}

type MakeConfig =
    fn(&Config, &RoomTask, UnboundedSender<Event>, Arc<Pace>, Option<FixedOffset>) -> RecordConfig;

impl Recorder {
    fn start(rooms: &Rooms, task: &RoomTask) -> Option<Self> {
        let key = task.name.clone();
        Self::spawn(
            rooms,
            task,
            key,
            rooms.events.clone(),
            Config::record_config,
        )
    }

    /// 试拉 `task`（总控下发的原样）：只拉流、不上传，结果随监测报回（见 `Assignments::probe`）。
    fn start_probe(rooms: &Rooms, task: &RoomTask) -> Option<Self> {
        let task = probe_task(task);
        let key = task.name.clone();
        Self::spawn(
            rooms,
            &task,
            key,
            rooms.events.clone(),
            Config::probe_config,
        )
    }

    /// 起录制：`watch_room` 用 `task.name`（文件名、分段归属），监测用 `key`，事件发往 `events`。
    fn spawn(
        rooms: &Rooms,
        task: &RoomTask,
        key: String,
        events: UnboundedSender<Event>,
        make: MakeConfig,
    ) -> Option<Self> {
        let Rooms {
            config,
            monitor,
            plugins,
            ..
        } = rooms;
        let Some(plugin) = plugins.iter().find(|p| p.matches(&task.url)).cloned() else {
            error!(url = task.url, "no plugin matches this url, skipped");
            return None;
        };
        let pace = monitor.start_room(&key);
        let cancel = CancellationToken::new();
        let handle = tokio::spawn(watch_room(
            plugin,
            task.name.clone(),
            task.url.clone(),
            Arc::new(make(config, task, events, pace.clone(), rooms.tz_offset)),
            cancel.clone(),
        ));
        Some(Self {
            task: task.clone(),
            key,
            pace,
            handle,
            cancel,
        })
    }

    /// 停录：录制器在下一个关键帧收尾（最多等 10 秒）。
    async fn stop(self, monitor: &Monitor) {
        self.cancel.cancel();
        let _ = self.handle.await;
        monitor.stop_room(&self.key);
    }
}

/// 在跑的故障试拉：录制器的事件先过闸门（见 [`trial`]）。
struct Trial {
    recorder: Recorder,
    gate: Arc<Gate>,
}

impl Trial {
    /// `task` 是总控下发的原样，录制用真实房间名（分段归属对得上），监测键带 [`TRIAL_PREFIX`]。
    fn start(rooms: &Rooms, task: &RoomTask) -> Option<Self> {
        let formal = rooms.config.record_config(
            task,
            rooms.events.clone(),
            Default::default(),
            rooms.tz_offset,
        );
        let gate = Arc::new(Gate::new(
            rooms.config.trial_dir(),
            formal,
            task.name.clone(),
            rooms.events.clone(),
        ));
        let (tx, rx) = unbounded_channel();
        let key = format!("{TRIAL_PREFIX}{}", task.name);
        let recorder = Recorder::spawn(rooms, task, key, tx, Config::trial_config)?;
        let forward = gate.clone();
        tokio::spawn(async move { forward.forward(rx).await });
        Some(Self { recorder, gate })
    }
}

/// 本机唯一的试拉名额：到期前试拉（probe）或故障试拉（trial），故障试拉优先（`handoff-health.md` §4.2）。
#[derive(Default)]
struct Trials {
    probing: Option<Recorder>,
    trialing: Option<Trial>,
}

impl Trials {
    fn probe_room(&self) -> Option<&str> {
        self.probing.as_ref().map(probed)
    }

    fn trial_room(&self) -> Option<&str> {
        self.trialing
            .as_ref()
            .map(|t| t.recorder.task.name.as_str())
    }

    fn set_tz_offset(&self, tz: Option<FixedOffset>) {
        if let Some(r) = &self.probing {
            r.pace.set_tz_offset(tz);
        }
        if let Some(t) = &self.trialing {
            t.recorder.pace.set_tz_offset(tz);
            t.gate.set_tz_offset(tz);
        }
    }

    /// 让在跑的试拉与总控要的一致。停录时两种都不跑；同时下发时只跑故障试拉。
    async fn reconcile(
        &mut self,
        probe: Option<&RoomTask>,
        trial: Option<&RoomTask>,
        paused: bool,
        rooms: &Rooms<'_>,
    ) {
        let trial = trial.filter(|_| !paused);
        let probe = probe.filter(|_| !paused && trial.is_none());
        if self.probing.as_ref().map(|r| &r.task) != probe.map(probe_task).as_ref()
            && let Some(r) = self.probing.take()
        {
            info!(room = probed(&r), "probe stopped");
            r.stop(rooms.monitor).await;
            clear_probe_dir(&rooms.config.probe_dir());
        }
        if self.trialing.as_ref().map(|t| &t.recorder.task) != trial {
            if let Some(t) = self.trialing.take() {
                info!(
                    room = t.recorder.task.name,
                    "trial stopped, staged files removed"
                );
                t.recorder.stop(rooms.monitor).await;
                clear_probe_dir(&rooms.config.trial_dir());
            }
            if let Some(task) = trial {
                self.trialing = Trial::start(rooms, task);
                if self.trialing.is_some() {
                    info!(room = task.name, "trial started");
                }
            }
        }
        if self.probing.is_none()
            && let Some(task) = probe
        {
            self.probing = Recorder::start_probe(rooms, task);
            if self.probing.is_some() {
                info!(room = task.name, "probe started");
            }
        }
    }

    async fn stop(&mut self, rooms: &Rooms<'_>) {
        self.reconcile(None, None, true, rooms).await;
    }

    /// 正在故障试拉的房间出现在正式清单里（参数相同）就转正：同一条连接接着录，录制移进 `running`，
    /// 之后与正常录制一样（`handoff-health.md` §6.1）。停录时不转正，随后按停录删掉暂存。
    fn promote(
        &mut self,
        assigned: &[RoomTask],
        drop: &[String],
        running: &mut HashMap<String, Recorder>,
        paused: bool,
        rooms: &Rooms<'_>,
    ) {
        let Some(t) = self
            .trialing
            .take_if(|t| !paused && assigned.contains(&t.recorder.task))
        else {
            return;
        };
        let Trial { mut recorder, gate } = t;
        let room = recorder.task.name.clone();
        // 原机在试拉期间写出过视频，总控让丢弃切点前的暂存段；没说就全部保留（§6.2）。
        let drop = drop.contains(&room);
        gate.promote(drop);
        recorder.pace.promote();
        rooms.monitor.promote(&recorder.key, &room);
        recorder.key = room.clone();
        info!(
            room,
            staged = if drop { "dropped" } else { "kept" },
            "trial promoted, recording continues on the same connection"
        );
        running.insert(room, recorder);
    }
}

/// 让在跑的录制与期望清单一致：多的停掉，少的起来，参数变了的重启。
async fn reconcile(running: &mut HashMap<String, Recorder>, want: &[RoomTask], rooms: &Rooms<'_>) {
    let tasks = running.values().map(|r| &r.task);
    for name in stale(tasks, want) {
        info!(room = name, "room no longer assigned here, stopping");
        running.remove(&name).unwrap().stop(rooms.monitor).await;
    }
    for task in want {
        if !running.contains_key(&task.name)
            && let Some(r) = Recorder::start(rooms, task)
        {
            info!(room = task.name, "room assigned here, watching");
            running.insert(task.name.clone(), r);
        }
    }
}

/// 试拉录制对应的原房间名。
fn probed(r: &Recorder) -> &str {
    r.task
        .name
        .strip_prefix(PROBE_PREFIX)
        .unwrap_or(&r.task.name)
}

/// 在跑、但不在期望清单里（或参数变了）的房间。
fn stale<'a>(running: impl Iterator<Item = &'a RoomTask>, want: &[RoomTask]) -> Vec<String> {
    running
        .filter(|r| !want.contains(r))
        .map(|r| r.name.clone())
        .collect()
}

/// 录制目录所在的、已存在的最近一级目录（模板里第一个占位符之前的部分）。
fn record_dir(output: &str) -> PathBuf {
    let fixed = &output[..output.find(['{', '%']).unwrap_or(output.len())];
    let fixed = Path::new(fixed);
    let dir = if fixed.as_os_str().to_string_lossy().ends_with('/') {
        fixed
    } else {
        fixed.parent().unwrap_or(Path::new(""))
    };
    dir.ancestors()
        .find(|p| !p.as_os_str().is_empty() && p.exists())
        .unwrap_or(Path::new("."))
        .to_path_buf()
}

/// 磁盘使用率与普通用户可用字节，与 `df` 的 Use% 同口径：已用 / (已用 + 普通用户可用)。
fn disk_usage(path: &Path) -> std::io::Result<(f64, u64)> {
    use std::os::unix::ffi::OsStrExt;
    let c = std::ffi::CString::new(path.as_os_str().as_bytes())?;
    let mut s: libc::statvfs = unsafe { std::mem::zeroed() };
    if unsafe { libc::statvfs(c.as_ptr(), &mut s) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    #[allow(clippy::unnecessary_cast)] // 字段宽度随平台不同
    let (blocks, free, avail, frsize) = (
        s.f_blocks as f64,
        s.f_bfree as f64,
        s.f_bavail as f64,
        s.f_frsize as f64,
    );
    let used = blocks - free;
    let usage = if used + avail > 0.0 {
        used / (used + avail)
    } else {
        0.0
    };
    Ok((usage, (avail * frsize) as u64))
}

/// 同一份状态文件只允许一个进程用。锁随返回的文件句柄释放。
fn lock_state(state: &Path) -> Result<std::fs::File, String> {
    let path = state.with_extension("lock");
    let f = std::fs::File::create(&path).map_err(|e| format!("{}: {e}", path.display()))?;
    f.try_lock()
        .map_err(|_| format!("{} is in use by another lubic-agent", state.display()))?;
    Ok(f)
}

#[derive(Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
struct SavedRooms {
    rooms: Vec<RoomTask>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    tz_offset_secs: Option<i32>,
}

fn load_rooms(path: &Path) -> SavedRooms {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum StoredRooms {
        Current(SavedRooms),
        Legacy(Vec<RoomTask>),
    }
    std::fs::read(path)
        .ok()
        .and_then(|b| serde_json::from_slice::<StoredRooms>(&b).ok())
        .map(|stored| match stored {
            StoredRooms::Current(s) => s,
            StoredRooms::Legacy(rooms) => SavedRooms {
                rooms,
                tz_offset_secs: None,
            },
        })
        .unwrap_or_default()
}

async fn run(config: Config) -> Result<(), String> {
    let _lock = lock_state(&config.state)?;
    clear_side_dirs(&config);
    if config.legacy_output() {
        warn!(
            output = config.output,
            "output still includes a file name; the controller's filename template is ignored. \
             Set output to the directory only, e.g. /rec/{{name}}"
        );
    }
    let shown = |e: std::io::Error| format!("{}: {e}", config.state.display());
    let outbox = Arc::new(Outbox::open(config.state.with_extension("outbox.json")).map_err(shown)?);
    let link = Arc::new(Link::default());
    // 总控第一次回复前先不开始上传：可能正在让出网络（重启前收到过 `hold_uploads`）。
    // 第一次就连不上总控时放开，照离线时的做法传（见下面 `Err` 分支）。
    link.hold.send_replace(true);
    let mut replied = false;
    let controller = Controller::new(&config);
    let pipeline = Pipeline::open(
        controller.clone(),
        &config.state,
        outbox.clone(),
        link.clone(),
        config.line.clone(),
    )
    .map_err(shown)?;
    let (tx, rx) = unbounded_channel();
    let rx = lubic::danmaku::tee(rx, config.record_cookies.clone());
    let shutdown = CancellationToken::new();
    let uploader = tokio::spawn(pipeline.run(rx, shutdown.clone()));
    let reporter = tokio::spawn(report_loop(
        controller.clone(),
        outbox.clone(),
        link.clone(),
        shutdown.clone(),
    ));
    let monitor = Arc::new(Monitor::default());
    let sampler = tokio::spawn(sample_loop(
        monitor.clone(),
        outbox.clone(),
        link.clone(),
        shutdown.clone(),
    ));
    // 总控失联时照最后一份清单与时区继续录，重启也一样。
    let rooms_path = config.state.with_extension("rooms.json");
    let saved = load_rooms(&rooms_path);
    let mut assigned = saved.rooms;
    let mut tz_offset_secs = saved
        .tz_offset_secs
        .filter(|&s| FixedOffset::east_opt(s).is_some());
    let mut rooms = Rooms {
        config: &config,
        events: &tx,
        monitor: &monitor,
        plugins: builtin_plugins(),
        tz_offset: tz_offset_secs.and_then(FixedOffset::east_opt),
    };
    let mut cuts: Vec<(String, i64)> = Vec::new();
    let mut trial_drop: Vec<String> = Vec::new();
    // 总控要试拉的房间（原样）与在跑的试拉。
    let mut probe: Option<RoomTask> = None;
    let mut trial: Option<RoomTask> = None;
    let mut trials = Trials::default();
    let dir = record_dir(&config.output);
    let mut running: HashMap<String, Recorder> = HashMap::new();
    let mut paused = false;
    let mut failures = 0u32;
    let ctrl_c = lubic::stop_signal();
    tokio::pin!(ctrl_c);
    loop {
        let (usage, free) = disk_usage(&dir).unwrap_or_else(|e| {
            warn!(dir = %dir.display(), error = %e, "disk usage unavailable");
            (0.0, 0)
        });
        // ponytail: 无回差，水位附近会反复停录/开录（每次最多漏一个轮询周期）；嫌抖再加回差。
        let full = usage >= config.disk_threshold;
        if full != paused {
            if full {
                error!(
                    dir = %dir.display(),
                    usage = format!("{:.1}%", usage * 100.0),
                    threshold = format!("{:.0}%", config.disk_threshold * 100.0),
                    "disk almost full, recording stopped until uploads free space"
                );
            } else {
                info!(dir = %dir.display(), usage = format!("{:.1}%", usage * 100.0), "disk ok, recording resumes");
            }
            paused = full;
        }
        *link.tails.lock().unwrap() = pipeline::tail_sizes(&assigned);
        trials.promote(&assigned, &trial_drop, &mut running, paused, &rooms);
        reconcile(&mut running, if paused { &[] } else { &assigned }, &rooms).await;
        // 已在本机正式录的房间不再故障试拉。
        let want_trial = trial
            .as_ref()
            .filter(|t| !assigned.iter().any(|a| a.name == t.name));
        trials
            .reconcile(probe.as_ref(), want_trial, paused, &rooms)
            .await;
        for (room, ms) in &cuts {
            if let Some(r) = running.get(room) {
                r.pace.set_cut_live(*ms);
            }
        }
        let mut applied_cuts: Vec<_> = running
            .values()
            .filter(|r| r.pace.cut_live().is_some())
            .map(|r| r.task.name.clone())
            .collect();
        applied_cuts.sort();

        let mut watch = Watch {
            rooms: assigned.iter().map(|t| t.name.clone()).collect(),
            cuts: applied_cuts,
            probe: trials.probe_room().map(String::from),
            trial: trials.trial_room().map(String::from),
            hold_uploads: Some(*link.hold.borrow()),
            status: Status {
                disk_usage: usage,
                disk_free: free,
                paused,
                queued_parts: link.queued_parts.load(Ordering::Relaxed),
                queued_bytes: link.queued_bytes.load(Ordering::Relaxed),
                uploading: link.uploading.lock().unwrap().iter().cloned().collect(),
                outbox: outbox.len() as u32,
                commit: lubic_proto::COMMIT.into(),
                ..Default::default()
            },
        };
        monitor.fill(&mut watch.status);
        let result = tokio::select! {
            _ = &mut ctrl_c => break,
            r = controller.watch(&watch) => r,
            // 有房间新出现时间戳前跳：丢掉这次挂着的长轮询，带着最新状态立刻重发，总控才能马上判受限。
            _ = monitor.alarm.notified() => continue,
        };
        match result {
            Ok(a) => {
                if failures > 0 {
                    info!(failures, "controller is back");
                    link.back.store(true, Ordering::Relaxed);
                    link.wake.notify_one();
                }
                failures = 0;
                replied = true;
                for (room, ms) in &a.cuts {
                    if let Some(r) = running.get(room) {
                        r.pace.set_cut_live(*ms);
                    }
                }
                cuts = a.cuts;
                trial_drop = a.trial_drop;
                if link.hold.send_replace(a.hold_uploads) != a.hold_uploads {
                    if a.hold_uploads {
                        info!("controller asked to hold uploads (yielding network)");
                    } else {
                        info!("uploads resume");
                        link.wake.notify_one();
                    }
                }
                probe = a.probe;
                trial = a.trial;
                let next_tz_secs = match a.tz_offset_secs {
                    Some(s) if FixedOffset::east_opt(s).is_some() => Some(s),
                    Some(s) => {
                        warn!(
                            tz_offset_secs = s,
                            "ignoring out-of-range tz_offset_secs from controller"
                        );
                        tz_offset_secs
                    }
                    None => None,
                };
                let tz_changed = next_tz_secs != tz_offset_secs;
                if tz_changed {
                    tz_offset_secs = next_tz_secs;
                    rooms.tz_offset = tz_offset_secs.and_then(FixedOffset::east_opt);
                    for r in running.values() {
                        r.pace.set_tz_offset(rooms.tz_offset);
                    }
                    trials.set_tz_offset(rooms.tz_offset);
                    info!(?tz_offset_secs, "updated recording timezone offset");
                }
                let rooms_changed = a.rooms != assigned;
                if rooms_changed {
                    assigned = a.rooms;
                }
                if rooms_changed || tz_changed {
                    let saved = SavedRooms {
                        rooms: assigned.clone(),
                        tz_offset_secs,
                    };
                    if let Err(e) = write_json(&rooms_path, &saved) {
                        warn!(path = %rooms_path.display(), error = %e, "failed to save room list");
                    }
                }
            }
            Err(e) => {
                failures += 1;
                if !replied && link.hold.send_replace(false) {
                    info!(
                        "controller unreachable at startup, uploads start without its hold state"
                    );
                    link.wake.notify_one();
                }
                // 失联期间不试拉：结果报不上去，白占下行。
                probe = None;
                trial = None;
                if failures == 1 {
                    warn!(
                        error = e,
                        rooms = assigned.len(),
                        "controller unreachable, keep recording the last room list"
                    );
                }
                tokio::select! {
                    _ = &mut ctrl_c => break,
                    _ = tokio::time::sleep(Duration::from_secs(5)) => {}
                }
            }
        }
    }
    info!("shutting down, finalizing segments...");
    for (_, r) in running.drain() {
        r.stop(&monitor).await;
    }
    trials.stop(&rooms).await;
    shutdown.cancel();
    let _ = uploader.await;
    let _ = reporter.await;
    let _ = sampler.await;
    Ok(())
}

/// 采样任务：每秒检测是否有房间刚写出第一个关键帧并报给总控（换机交接要用）；
/// 每 [`monitor::SAMPLE_EVERY`] 采样一次窗口统计。
async fn sample_loop(
    monitor: Arc<Monitor>,
    outbox: Arc<Outbox>,
    link: Arc<Link>,
    shutdown: CancellationToken,
) {
    let mut check = tokio::time::interval(monitor::CHECK_EVERY);
    let mut sample = tokio::time::interval(monitor::SAMPLE_EVERY);
    loop {
        let do_sample = tokio::select! {
            _ = check.tick() => false,
            _ = sample.tick() => true,
            _ = shutdown.cancelled() => return,
        };
        let now = std::time::Instant::now();
        let started = if do_sample {
            monitor.sample(now, &link.upload)
        } else {
            monitor.check_started(now)
        };
        for s in started {
            info!(room = s.room, lag_ms = ?s.lag_ms, start_live_ms = ?s.start_live_ms, "first keyframe written");
            outbox.push(AgentEvent::Recording {
                room: s.room,
                lag_ms: s.lag_ms,
                start_live_ms: s.start_live_ms,
            });
        }
        if do_sample {
            let mut status = Status::default();
            monitor.fill(&mut status);
            link.upload_bps
                .store(status.upload_bps.unwrap_or(0.0) as u64, Ordering::Relaxed);
        }
    }
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    if let Err(e) = lubic::logging::init(cli.log_dir.as_deref(), "agent") {
        eprintln!("{e}");
        std::process::exit(1);
    }
    let result = match Config::load(&cli.config) {
        Err(e) => Err(e),
        Ok(config) => run(config).await,
    };
    if let Err(e) = result {
        error!("{e}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lubic::downloader::live::{LiveRequest, LiveResult, LiveStatus, LiveStream};

    fn config() -> Config {
        toml::from_str(
            r#"
            controller = "http://127.0.0.1:7070/"
            token = "t"
            state = "s.json"
            "#,
        )
        .unwrap()
    }

    fn task(name: &str, url: &str) -> RoomTask {
        RoomTask {
            name: name.into(),
            url: url.into(),
            split_time: Some(Duration::from_secs(120)),
            split_size_mb: None,
            grace: Some(Duration::from_secs(10)),
            bili_qn: Some(10000),
            douyin_quality: Some("hd".into()),
            filename: String::new(),
            streamer: String::new(),
            tail_upload: false,
        }
    }

    /// 总控下发的文件名模板与显示名就是 agent 录制用的；旧写法的 `output` 与旧总控各有退路。
    #[test]
    fn record_config_uses_the_filename_template_from_the_task() {
        let mut c = config();
        c.output = "/rec/{name}".into();
        let mut t = task("Secret", "u");
        t.filename = "{streamer} %H{colon}%M".into();
        t.streamer = "Secret（王者组排）".into();
        let tpl = |c: &Config, t: &RoomTask| {
            let rc = c.record_config(t, unbounded_channel().0, Default::default(), None);
            lubic::recorder::file_template(&rc, &t.name, "标题")
        };
        assert_eq!(tpl(&c, &t), "/rec/Secret/Secret（王者组排） %H:%M");
        // 旧总控：不发模板和显示名。
        let old = task("Secret", "u");
        assert_eq!(tpl(&c, &old), "/rec/Secret/Secret_%Y-%m-%dT%H_%M_%S");
        // 旧写法的 output：照旧，不用下发的模板。
        c.output = "/rec/{name}/{name}_%Y-%m-%dT%H_%M_%S".into();
        assert_eq!(tpl(&c, &t), "/rec/Secret/Secret_%Y-%m-%dT%H_%M_%S");
    }

    #[test]
    fn config_parses_with_defaults() {
        let c = config();
        assert_eq!((c.line.as_str(), c.disk_threshold), ("tx", 0.85));
        assert_eq!(Controller::new(&c).base, "http://127.0.0.1:7070");
        let tz = FixedOffset::east_opt(8 * 3600);
        let rc = c.record_config(
            &task("6", "u"),
            unbounded_channel().0,
            Default::default(),
            tz,
        );
        assert_eq!(rc.offline_grace, Duration::from_secs(10));
        assert_eq!(rc.split_time, Some(Duration::from_secs(120)));
        assert_eq!(rc.options.bilibili.qn, 10000);
        assert_eq!(rc.options.douyin.quality, "hd");
        assert_eq!(rc.tz_offset, tz);
    }

    /// 试拉写进录制目录下的 `.probe/`、不发事件；任务名带前缀，不边录边传。
    #[test]
    fn probe_config_writes_under_the_probe_dir_without_events() {
        let mut c = config();
        c.output = "/tmp/rec/{name}".into();
        let mut t = task("Secret", "u");
        t.tail_upload = true;
        let p = probe_task(&t);
        assert_eq!(p.name, "probe:Secret");
        assert!(!p.tail_upload);
        let tz = FixedOffset::east_opt(9 * 3600);
        let rc = c.probe_config(&p, unbounded_channel().0, Default::default(), tz);
        assert!(rc.events.is_none());
        assert_eq!(rc.output, "/tmp/.probe/{name}");
        assert_eq!(c.probe_dir(), PathBuf::from("/tmp/.probe"));
        assert_eq!(rc.filename.as_deref(), Some(LEGACY_FILENAME));
        assert_eq!(rc.tz_offset, tz);
    }

    /// 正常录制、legacy_output、试拉与故障试拉都继承总控下发的时区偏移。
    #[test]
    fn tz_offset_flows_into_legacy_and_trial_configs() {
        let mut c = config();
        c.output = "/tmp/rec/{name}/{name}_%Y-%m-%dT%H_%M_%S".into();
        assert!(c.legacy_output());
        let tz = FixedOffset::west_opt(5 * 3600);
        let t = task("r", "u");
        assert_eq!(
            c.record_config(&t, unbounded_channel().0, Default::default(), tz)
                .tz_offset,
            tz
        );
        assert_eq!(
            c.trial_config(&t, unbounded_channel().0, Default::default(), tz)
                .tz_offset,
            tz
        );
    }

    /// `.rooms.json` 读写兼容旧版纯数组 JSON 与新版带 `tz_offset_secs` 的对象。
    #[test]
    fn load_rooms_supports_legacy_array_and_saved_object() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("s.rooms.json");
        let t = task("r", "u");
        // 旧版纯数组 JSON
        write_json(&path, &[&t]).unwrap();
        let legacy = load_rooms(&path);
        assert_eq!(legacy.rooms, std::slice::from_ref(&t));
        assert_eq!(legacy.tz_offset_secs, None);
        // 新版对象（含 tz_offset_secs）
        let saved = SavedRooms {
            rooms: vec![t.clone()],
            tz_offset_secs: Some(28800),
        };
        write_json(&path, &saved).unwrap();
        assert_eq!(load_rooms(&path), saved);
    }

    #[test]
    fn clear_probe_dir_removes_files_and_ignores_missing() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join(".probe");
        clear_probe_dir(&dir);
        std::fs::create_dir_all(dir.join("probe:r")).unwrap();
        std::fs::write(dir.join("probe:r/a.flv"), b"x").unwrap();
        clear_probe_dir(&dir);
        assert!(!dir.exists());
    }

    /// 启动时清掉两种试拉留下的文件。
    #[test]
    fn clear_side_dirs_removes_probe_and_trial_files() {
        let tmp = tempfile::tempdir().unwrap();
        let mut c = config();
        c.output = tmp.path().join("{name}").display().to_string();
        for dir in [c.probe_dir(), c.trial_dir()] {
            std::fs::create_dir_all(dir.join("r")).unwrap();
            std::fs::write(dir.join("r/a.flv"), b"x").unwrap();
        }
        assert_ne!(
            c.probe_dir(),
            c.trial_dir(),
            "故障试拉不和到期前试拉混用目录"
        );
        clear_side_dirs(&c);
        assert!(!c.probe_dir().exists() && !c.trial_dir().exists());
    }

    /// 每 100ms 发一个关键帧、不主动断开的 FLV 服务；返回地址与接受过的连接数。
    async fn serve_live() -> (String, Arc<std::sync::atomic::AtomicUsize>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/live.flv", listener.local_addr().unwrap());
        let connections = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let accepted = connections.clone();
        tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                accepted.fetch_add(1, Ordering::Relaxed);
                tokio::spawn(async move {
                    let _ = socket.read(&mut [0; 4_096]).await;
                    let head = "HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n";
                    let flv = [b'F', b'L', b'V', 1, 1, 0, 0, 0, 9, 0, 0, 0, 0];
                    if socket.write_all(head.as_bytes()).await.is_err()
                        || socket.write_all(&flv).await.is_err()
                    {
                        return;
                    }
                    for t in (0u32..).map(|i| i * 100) {
                        let mut tag = vec![9, 0, 0, 6, (t >> 16) as u8, (t >> 8) as u8, t as u8];
                        tag.extend_from_slice(&[0, 0, 0, 0, 0x17, 1, 0, 0, 0, 0x65]);
                        tag.extend_from_slice(&17u32.to_be_bytes());
                        if socket.write_all(&tag).await.is_err() {
                            return;
                        }
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                });
            }
        });
        (url, connections)
    }

    /// 测试用平台：有地址时一直在播（同一场），没有时一直不在播。
    struct Live(Option<String>);

    #[async_trait]
    impl LivePlugin for Live {
        fn name(&self) -> &'static str {
            "test"
        }
        fn matches(&self, _: &str) -> bool {
            true
        }
        async fn check_stream(&self, _: LiveRequest) -> LiveResult<LiveStatus> {
            Ok(match &self.0 {
                Some(url) => LiveStatus::Live {
                    stream: Box::new(LiveStream {
                        name: "r".into(),
                        url: String::new(),
                        title: String::new(),
                        raw_stream_url: url.clone(),
                        platform: "test".into(),
                        stream_headers: Default::default(),
                        suffix: "flv".into(),
                        recording_quality: None,
                        recording_quality_code: None,
                        live_session_key: Some("s".into()),
                    }),
                },
                None => LiveStatus::Offline,
            })
        }
    }

    async fn wait_for(what: &str, mut done: impl FnMut() -> bool) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while !done() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{what}"));
    }

    fn files_under(dir: &Path) -> Vec<PathBuf> {
        let mut out = Vec::new();
        for entry in std::fs::read_dir(dir).into_iter().flatten().flatten() {
            match entry.path() {
                p if p.is_dir() => out.extend(files_under(&p)),
                p => out.push(p),
            }
        }
        out
    }

    /// 故障试拉写进暂存目录：出结论前不发事件（不进上传队列、边录边传的房间也不起跟随上传），
    /// 不报开录、不算产出；取消就停掉并删光暂存内容，同样不发事件。
    #[tokio::test]
    async fn trial_stages_without_events_and_cancel_clears_it() {
        let (url, connections) = serve_live().await;
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(tmp.path().join("rec")).unwrap();
        let mut c = config();
        c.output = tmp.path().join("rec/{name}").display().to_string();
        let (tx, mut rx) = unbounded_channel();
        let monitor = Monitor::default();
        let rooms = Rooms {
            config: &c,
            events: &tx,
            monitor: &monitor,
            plugins: vec![Arc::new(Live(Some(url)))],
            tz_offset: None,
        };
        let mut t = task("r", "u");
        t.tail_upload = true;
        t.split_size_mb = Some(64);
        let mut slot = Trials::default();
        slot.reconcile(None, Some(&t), false, &rooms).await;
        assert_eq!(slot.trial_room(), Some("r"));
        let pace = slot.trialing.as_ref().unwrap().recorder.pace.clone();
        wait_for("staged media", || {
            pace.read().media >= Duration::from_secs(1)
        })
        .await;
        let staged = files_under(&c.trial_dir().join("r"));
        assert_eq!(staged.len(), 1, "{staged:?}");
        assert!(files_under(&tmp.path().join("rec/r")).is_empty());
        assert!(rx.try_recv().is_err(), "暂存期间不发事件");
        let later = std::time::Instant::now() + Duration::from_secs(30);
        assert!(monitor.check_started(later).is_empty(), "不报开录");
        monitor.sample(later, &Pace::default());
        let mut status = Status::default();
        monitor.fill(&mut status);
        assert_eq!(status.produce_bps, 0.0, "试拉的字节不算产出");
        assert!(status.record_lags.iter().any(|(k, _)| k == "trial:r"));

        slot.reconcile(None, None, false, &rooms).await;
        assert_eq!(slot.trial_room(), None);
        assert!(!c.trial_dir().exists());
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(rx.try_recv().is_err(), "取消后也不发");
        assert!(files_under(&tmp.path().join("rec/r")).is_empty());
        assert_eq!(connections.load(Ordering::Relaxed), 1);
    }

    /// 转正：同一房间出现在正式清单里，同一条连接继续录、不重连；已收尾的暂存分段移进正式目录并补发
    /// `PartClosed`，下一个关键帧切段后写正式目录，补报开录，之后按正常录制对账、计产出、报监测键。
    #[tokio::test]
    async fn promotion_keeps_the_connection_and_releases_files_and_events() {
        let (url, connections) = serve_live().await;
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(tmp.path().join("rec")).unwrap();
        let mut c = config();
        c.output = tmp.path().join("rec/{name}").display().to_string();
        let (tx, mut rx) = unbounded_channel();
        let monitor = Monitor::default();
        let rooms = Rooms {
            config: &c,
            events: &tx,
            monitor: &monitor,
            plugins: vec![Arc::new(Live(Some(url)))],
            tz_offset: None,
        };
        let t = task("r", "u");
        let mut slot = Trials::default();
        let mut running = HashMap::new();
        slot.reconcile(None, Some(&t), false, &rooms).await;
        let pace = slot.trialing.as_ref().unwrap().recorder.pace.clone();
        // 暂存分段写出内容；转正时它在下一个关键帧收尾、移进正式目录（转正前已收尾的见 `trial` 的测试）。
        wait_for("staged media", || {
            pace.read().media >= Duration::from_secs(1)
        })
        .await;
        assert!(rx.try_recv().is_err());

        // 停录中不转正。总控没让丢弃（旧总控也不发）：保留全部暂存内容。
        slot.promote(std::slice::from_ref(&t), &[], &mut running, true, &rooms);
        assert_eq!(slot.trial_room(), Some("r"));
        slot.promote(std::slice::from_ref(&t), &[], &mut running, false, &rooms);
        assert_eq!(slot.trial_room(), None);
        assert!(running.contains_key("r"));
        let mut events = Vec::new();
        wait_for("formal segment opened", || {
            events.extend(std::iter::from_fn(|| rx.try_recv().ok()));
            events.iter().any(|e| matches!(e, Event::PartOpened(_)))
        })
        .await;
        let formal = tmp.path().join("rec/r");
        let [Event::PartClosed(staged), Event::PartOpened(opened)] = &events[..] else {
            panic!("{events:?}");
        };
        assert_eq!(staged.path.parent(), Some(formal.as_path()));
        assert!(staged.path.exists() && staged.size > 13);
        assert_eq!(staged.owner.room, "r");
        assert_eq!(opened.path.parent(), Some(formal.as_path()));
        assert!(files_under(&c.trial_dir()).is_empty(), "暂存目录里不留文件");
        let later = std::time::Instant::now() + Duration::from_secs(10);
        assert!(
            monitor.check_started(later).is_empty(),
            "没有 SEI：等 3 秒再报"
        );
        let started = monitor.check_started(later + Duration::from_secs(5));
        assert_eq!(started.len(), 1, "补报开录");
        assert_eq!(started[0].room, "r");

        // 之后就是正常录制：对账不重启，停录照常收尾发事件。
        reconcile(&mut running, std::slice::from_ref(&t), &rooms).await;
        monitor.sample(std::time::Instant::now(), &Pace::default());
        // 正式分段录够 1 秒，停录时才会保留（`MIN_MEDIA_MS`）。
        tokio::time::sleep(Duration::from_millis(1_200)).await;
        assert_eq!(connections.load(Ordering::Relaxed), 1, "没有重连");
        reconcile(&mut running, &[], &rooms).await;
        let rest: Vec<_> = std::iter::from_fn(|| rx.try_recv().ok()).collect();
        assert!(
            matches!(&rest[..], [Event::PartClosed(p)] if p.part_id == opened.part_id && p.path.exists()),
            "{rest:?}"
        );
        monitor.sample(std::time::Instant::now(), &Pace::default());
        let mut status = Status::default();
        monitor.fill(&mut status);
        assert!(status.produce_bps > 0.0, "转正后算产出");
        assert!(status.trial_support);
    }

    /// 转正时总控让丢弃（原机在试拉期间写出过视频，§6.2）：同一条连接在下一个关键帧切到正式分段，
    /// 切点前的暂存分段删掉、一个事件都不发；之后的正式分段照常。别的房间的丢弃名单不影响它。
    #[tokio::test]
    async fn promotion_with_drop_discards_the_staged_part_silently() {
        let (url, connections) = serve_live().await;
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(tmp.path().join("rec")).unwrap();
        let mut c = config();
        c.output = tmp.path().join("rec/{name}").display().to_string();
        let (tx, mut rx) = unbounded_channel();
        let monitor = Monitor::default();
        let rooms = Rooms {
            config: &c,
            events: &tx,
            monitor: &monitor,
            plugins: vec![Arc::new(Live(Some(url)))],
            tz_offset: None,
        };
        let t = task("r", "u");
        let mut slot = Trials::default();
        let mut running = HashMap::new();
        slot.reconcile(None, Some(&t), false, &rooms).await;
        let pace = slot.trialing.as_ref().unwrap().recorder.pace.clone();
        wait_for("staged media", || {
            pace.read().media >= Duration::from_secs(1)
        })
        .await;
        let drop = ["other".to_string(), "r".to_string()];
        slot.promote(std::slice::from_ref(&t), &drop, &mut running, false, &rooms);
        assert!(running.contains_key("r"));
        let mut events = Vec::new();
        wait_for("formal segment opened", || {
            events.extend(std::iter::from_fn(|| rx.try_recv().ok()));
            !events.is_empty()
        })
        .await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        events.extend(std::iter::from_fn(|| rx.try_recv().ok()));
        let formal = tmp.path().join("rec/r");
        let [Event::PartOpened(opened)] = &events[..] else {
            panic!("暂存分段不补发：{events:?}");
        };
        assert_eq!(opened.path.parent(), Some(formal.as_path()));
        assert!(files_under(&c.trial_dir()).is_empty(), "暂存分段删掉");
        assert_eq!(files_under(&formal), std::slice::from_ref(&opened.path));
        // 正式分段录够 1 秒，停录时才会保留（`MIN_MEDIA_MS`）。
        tokio::time::sleep(Duration::from_millis(1_200)).await;
        reconcile(&mut running, &[], &rooms).await;
        let rest: Vec<_> = std::iter::from_fn(|| rx.try_recv().ok()).collect();
        assert!(
            matches!(&rest[..], [Event::PartClosed(p)] if p.part_id == opened.part_id),
            "{rest:?}"
        );
        assert_eq!(connections.load(Ordering::Relaxed), 1, "没有重连");
    }

    /// 试拉名额只有一个：故障试拉优先于到期前试拉，挤掉的到期前试拉在故障试拉结束后恢复；停录时两种都不跑。
    #[tokio::test]
    async fn one_slot_where_trial_beats_probe_and_nothing_runs_while_paused() {
        let tmp = tempfile::tempdir().unwrap();
        let mut c = config();
        c.output = tmp.path().join("{name}").display().to_string();
        let (tx, _rx) = unbounded_channel();
        let monitor = Monitor::default();
        let rooms = Rooms {
            config: &c,
            events: &tx,
            monitor: &monitor,
            plugins: vec![Arc::new(Live(None))],
            tz_offset: None,
        };
        let (p, t) = (task("p", "u"), task("t", "u"));
        let mut slot = Trials::default();
        let state = |s: &Trials| {
            (
                s.probe_room().map(String::from),
                s.trial_room().map(String::from),
            )
        };
        slot.reconcile(Some(&p), Some(&t), true, &rooms).await;
        assert_eq!(state(&slot), (None, None), "停录时不试拉");
        slot.reconcile(Some(&p), None, false, &rooms).await;
        assert_eq!(state(&slot), (Some("p".into()), None));
        slot.reconcile(Some(&p), Some(&t), false, &rooms).await;
        assert_eq!(
            state(&slot),
            (None, Some("t".into())),
            "故障试拉挤掉到期前试拉"
        );
        slot.reconcile(Some(&p), None, false, &rooms).await;
        assert_eq!(state(&slot), (Some("p".into()), None));
        slot.reconcile(Some(&p), Some(&t), true, &rooms).await;
        assert_eq!(state(&slot), (None, None));
        slot.stop(&rooms).await;
    }

    #[test]
    fn record_dir_and_disk_usage() {
        assert_eq!(record_dir("{name}/x"), PathBuf::from("."));
        assert_eq!(record_dir("/tmp/rec/{name}/x"), PathBuf::from("/tmp"));
        let (u, free) = disk_usage(Path::new(".")).unwrap();
        assert!((0.0..=1.0).contains(&u), "{u}");
        assert!(free > 0);
    }

    #[test]
    fn stale_rooms_are_the_removed_and_the_changed() {
        let a = task("a", "u");
        let b = task("b", "u");
        let mut a2 = a.clone();
        a2.split_time = Some(Duration::from_secs(60));
        let running = [a.clone(), b.clone()];
        assert!(stale(running.iter(), &[b.clone(), a.clone()]).is_empty());
        assert_eq!(stale(running.iter(), &[a2, b]), ["a"]);
        assert_eq!(stale(running.iter(), &[a]), ["b"]);
    }
}
