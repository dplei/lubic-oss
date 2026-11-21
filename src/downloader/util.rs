use chrono::Local;
use lubic_events::{PartClosed, PartOpened};
use rand::Rng;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use std::time::{Duration, Instant};
use tracing::{error, info, warn};

/// 分段收尾成功后调用，拿到的就是对外发出的事件。
pub type CallbackFn<'a> = Box<dyn FnMut(&PartClosed) + Send + Sync + 'a>;
/// 新分段建好文件后调用。
pub type OpenFn<'a> = Box<dyn FnMut(&PartOpened) + Send + Sync + 'a>;

/// 结构化事件统一走这个 target，方便单独过滤或采集。
pub(crate) const EVENT_TARGET: &str = "lubic::event";

/// 录制归属：哪个房间、哪一场。由调用方提供，文件层只负责把它带进事件，绝不从文件名反推。
pub type RecordingOwner = lubic_events::Owner;
/// 关闭原因属于事件契约，录制侧沿用旧名。
pub type SegmentCloseReason = lubic_events::CloseReason;

/// 分段身份在建文件时分配（而非关闭时），段内事件因此也能指明所属分段。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentIdentity {
    pub segment_id: String,
    pub original_file: String,
}

static SEGMENT_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// 无需协调者即唯一：进程内序号 + 墙钟 + 随机数，多进程/多机同录一个房间也不会撞。
pub fn allocate_id(prefix: &str) -> String {
    let sequence = SEGMENT_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let now = chrono::Utc::now().timestamp_millis().max(0) as u64;
    let salt: u32 = rand::thread_rng().r#gen();
    format!("{prefix}-{now:x}-{sequence:x}-{salt:08x}")
}

/// 关闭原因的稳定代码，供日志检索；未映射的一律 `unknown`，不发明自由文本。
pub fn close_reason_code(reason: SegmentCloseReason) -> &'static str {
    match reason {
        SegmentCloseReason::TimedSplit | SegmentCloseReason::SizeSplit => "split_limit",
        SegmentCloseReason::StreamEnded => "stream_end",
        SegmentCloseReason::TransportError => "transport_error",
        SegmentCloseReason::Cancelled => "user_cancel",
        SegmentCloseReason::Unknown => "unknown",
    }
}

/// 分段生命周期事件：创建、关闭、收尾失败三种，字段集一致。
pub fn segment_created(event: &PartOpened, identity: &SegmentIdentity) {
    tracing::info!(
        target: EVENT_TARGET,
        event_name = "recording.segment_created",
        outcome = "executed",
        segment_id = event.part_id,
        original_file = identity.original_file,
        room = event.owner.room.as_str(),
        session = event.owner.session.as_str(),
        "开始写入新的录制分段"
    );
}

pub fn segment_closed(event: &PartClosed) {
    tracing::info!(
        target: EVENT_TARGET,
        event_name = "recording.segment_closed",
        outcome = "executed",
        reason_code = close_reason_code(event.reason),
        segment_id = event.part_id,
        original_file = %event.path.display(),
        size_bytes = event.size,
        duration_ms = event.duration.as_millis() as u64,
        room = event.owner.room.as_str(),
        session = event.owner.session.as_str(),
        "录制分段已关闭"
    );
}

/// `.part` 改名失败：仍报出身份以便找到临时文件，outcome 记 failed 而不是假装已关闭。
pub fn segment_close_failed(
    owner: &RecordingOwner,
    segment_id: &str,
    original_file: &str,
    error: &str,
) {
    warn!(
        target: EVENT_TARGET,
        event_name = "recording.segment_closed",
        outcome = "failed",
        reason_code = "unknown",
        segment_id,
        original_file,
        error,
        room = owner.room.as_str(),
        session = owner.session.as_str(),
        "录制分段收尾失败，临时文件保留"
    );
}

#[derive(Debug, Clone)]
pub struct SegmentCloseHandle(Arc<Mutex<SegmentCloseReason>>);

impl Default for SegmentCloseHandle {
    fn default() -> Self {
        Self(Arc::new(Mutex::new(SegmentCloseReason::Unknown)))
    }
}

impl SegmentCloseHandle {
    pub fn set(&self, reason: SegmentCloseReason) {
        if let Ok(mut current) = self.0.lock() {
            *current = reason;
        }
    }

    pub fn get(&self) -> SegmentCloseReason {
        self.0
            .lock()
            .map(|reason| *reason)
            .unwrap_or(SegmentCloseReason::Unknown)
    }
}

#[derive(Debug)]
pub enum Segment {
    Time(Duration, Duration),
    Size(u64, u64),
    Never,
}

#[derive(Debug, Clone)]
pub struct Segmentable {
    time: Time,
    size: Size,
    pace: Option<Arc<Pace>>,
}

/// 录制实时倍率与产出速度的原始计数：几个只增不减的累计值，调用方定时采样相减。
/// 上传侧也复用它（`wall` 记忙碌时长，`bytes` 记已传字节，`media`、`active` 不用）。
#[derive(Debug, Default)]
pub struct Pace(Mutex<PaceInner>);

#[derive(Debug, Default)]
struct PaceInner {
    total: PaceCount,
    /// 正在计时的那一段从何时开始、开始时已收到多少媒体。
    since: Option<Instant>,
    media_at_since: Duration,
    active_at_since: Duration,
    /// 最近一次收到字节的时刻。
    last_data: Option<Instant>,
    /// 有几处在计时：上传侧整段上传与边录边传可能同时在传，全停了才停。
    running: u32,
    /// 本条连接最近一次写盘时，写到的内容落后直播多少毫秒（按推流时刻算），以及那一刻。
    live_lag: Option<(i64, Instant)>,
    /// 平台实际选中的画质的显示名（同 `LiveStream::recording_quality`）。
    quality: Option<String>,
    /// 换机交接的切点（推流时刻 unix 毫秒）：旧机在推流时刻 ≥ 它的第一个关键帧前收尾。
    cut_live_ms: Option<i64>,
    /// 已按切点收尾：录制循环不再复查或重连，直接退出。
    cut_done: bool,
    /// 连续异常拉流次数，以及尚未恢复的视频停滞；上传侧不用。
    record_failures: (u32, bool),
    recording_ready: bool,
    /// 本次拉流尝试建立连接的时刻（FLV 读到文件头、HLS 取到播放列表），录制循环在连接结束时取走。
    connected_at: Option<Instant>,
    /// 计入健康检测的拉流连接结束：(结束时刻 unix 毫秒, 连接时长毫秒, 是否写出过有效视频)，
    /// 见 `handoff-health.md` §3。有没有视频沿用录制循环的 productive 口径。
    connections: Vec<(i64, u64, bool)>,
    /// 最近一次写出有效视频的时刻（unix 毫秒）：FLV 本连接写满两帧视频后每写一帧、HLS 每写一个非空分片。
    last_video_ms: Option<i64>,
    /// 录制器这次确认平台在播的起点（unix 毫秒）：开录、或下播宽限后复查又在播。平台不确认在播、录制停了为 `None`。
    live_since_ms: Option<i64>,
    /// 可靠推流时刻测得的重连接缝：(测得时刻 unix 毫秒, 缺口毫秒)，重叠记 0；无法测量的不记。
    reconnect_gaps: Vec<(i64, u64)>,
    /// 暂存录制已转正（见 `RecordConfig::staging`）。
    promoted: bool,
    /// 转正后已切到正式分段（见 [`Pace::formal_started`]）。
    formal: bool,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PaceCount {
    /// 计时期间（录制：拉流连接存活；上传：正在传）经过的墙钟时长。
    pub wall: Duration,
    /// 收到的媒体时长（连续前向增量之和，见 [`Segmentable`]）。
    pub media: Duration,
    pub bytes: u64,
    /// 在持续收数据的时长：相邻两次收到字节的间隔不超过 [`DATA_GAP`] 才累加。录制倍率用它而不是 `wall`：
    /// 主播断流后 CDN 常让连接空挂到停顿看门狗超时，那段墙钟照走、媒体为 0，会把倍率拉低（2026-10-23 线上误判）。
    /// 连着但收得慢的连接，TCP 数据一直在流，照常累加。
    pub active: Duration,
    /// 数据一直在流的时长：相邻两次收到字节的间隔不超过 [`FLOW_GAP`] 才累加。和 `active` 不同，主播推流卡几秒、
    /// CDN 停发的那几秒不算；CDN 限速时 TCP 数据照样连续，照常累加。快速判慢只认它（见 agent 的 `monitor`）。
    pub flowing: Duration,
    /// 超过 [`DATA_GAP`] 没收到数据（断流、空挂、断开重连）的次数。监测据此清空窗口、从头积累。
    pub gaps: u32,
    /// 本条连接落后直播多少毫秒：连接后收数据的时长减去这期间收到的媒体时长（连上时的 GOP 缓存会让它略为负）。
    /// 换机交接靠它判断旧机录到了直播的哪一刻：它只是一段时长，不依赖 CDN 的时间戳基准
    /// （实测同一场直播，有的连接是绝对时间轴、有的从 0 起），也不受机器间时钟偏差影响。没连着时为 `None`。
    pub lag_ms: Option<i64>,
    /// 本次录制写出的第一帧是直播的哪一刻（unix 毫秒，推流端经 NTP 校准的时钟，取自抖音码流 SEI 里的 `ts`）。
    /// 只认第一条写出内容的连接；它没有 SEI 就一直是 `None`。
    pub first_live_ms: Option<i64>,
    /// 本条连接写到的内容落后直播多少毫秒：本机此刻减去最后写出那帧的推流时刻。各机都对 NTP，可以跨机器比较，
    /// 换机交接靠它和 [`Self::first_live_ms`] 判断旧机录到了新机的起点没有。没连着、或这条连接还没见到 SEI 时为 `None`。
    pub live_lag_ms: Option<i64>,
    /// 时间戳前跳跳过的媒体时长累计：确认换到新时间轴时，新轴第一个样本减旧轴最后一个源时间戳。
    /// CDN 一阵一阵给数据时，没收到的那段直播就是这么跳过去的，不进 `media`，也不受 [`DATA_GAP`] 影响。
    pub skipped: Duration,
    /// 这种前跳的次数。与 `skipped` 一样只增不减，不受 `gaps` 影响。
    pub jumps: u32,
}

/// 两次收到字节之间超过这么久，就算断了一次：这段不计入 [`PaceCount::active`]。
/// 比 HLS 两次拉 ts 的间隔（目标时长，通常几秒）长，比停顿看门狗（30 秒）短。
pub const DATA_GAP: Duration = Duration::from_secs(15);
/// 两次收到字节之间不超过这么久，才算数据一直在流（[`PaceCount::flowing`]）。
pub const FLOW_GAP: Duration = Duration::from_secs(2);

pub fn unix_now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64)
}

impl Pace {
    pub fn record_ready(&self) {
        let mut inner = self.0.lock().unwrap();
        inner.recording_ready = true;
        inner.record_failures = (0, false);
    }

    pub fn recording_ready(&self) -> bool {
        self.0.lock().unwrap().recording_ready
    }

    pub fn record_failure(&self, video_stalled: bool) {
        let mut inner = self.0.lock().unwrap();
        inner.record_failures.0 = inner.record_failures.0.saturating_add(1);
        inner.record_failures.1 |= video_stalled;
    }

    pub fn record_recovered(&self) {
        self.0.lock().unwrap().record_failures = (0, false);
    }

    pub fn record_failures(&self) -> (u32, bool) {
        self.0.lock().unwrap().record_failures
    }

    pub fn mark_connected(&self) {
        self.0.lock().unwrap().connected_at = Some(Instant::now());
    }

    pub fn take_connected(&self) -> Option<Instant> {
        self.0.lock().unwrap().connected_at.take()
    }

    pub fn connection_ended(&self, end_unix_ms: i64, duration_ms: u64, video: bool) {
        let mut inner = self.0.lock().unwrap();
        inner.connections.push((end_unix_ms, duration_ms, video));
        // ponytail: 只靠 agent 每次心跳读时裁掉旧的；没人读（不在 agent 里跑）也只是每条连接 16 字节。
    }

    pub fn video_written(&self) {
        self.0.lock().unwrap().last_video_ms = Some(unix_now_ms());
    }

    /// 平台确认在播：已在确认中就保留原起点。
    pub fn confirm_live(&self) {
        self.0
            .lock()
            .unwrap()
            .live_since_ms
            .get_or_insert_with(unix_now_ms);
    }

    pub fn unconfirm_live(&self) {
        self.0.lock().unwrap().live_since_ms = None;
    }

    /// 平台确认在播时：(最近一次写出有效视频的时刻, 确认在播的起点)，都是 unix 毫秒；不确认在播为 `None`。
    pub fn video_status(&self) -> Option<(Option<i64>, i64)> {
        let inner = self.0.lock().unwrap();
        Some((inner.last_video_ms, inner.live_since_ms?))
    }

    pub fn reconnect_gap(&self, gap_ms: u64) {
        let mut inner = self.0.lock().unwrap();
        inner.reconnect_gaps.push((unix_now_ms(), gap_ms));
    }

    /// 测得时刻不早于 `since_unix_ms` 的重连接缝；更早的丢掉。
    pub fn recent_gaps(&self, since_unix_ms: i64) -> Vec<(i64, u64)> {
        let mut inner = self.0.lock().unwrap();
        inner.reconnect_gaps.retain(|(at, _)| *at >= since_unix_ms);
        inner.reconnect_gaps.clone()
    }

    /// 结束时刻不早于 `since_unix_ms` 的连接；更早的丢掉。
    pub fn recent_connections(&self, since_unix_ms: i64) -> Vec<(i64, u64, bool)> {
        let mut inner = self.0.lock().unwrap();
        inner.connections.retain(|(end, ..)| *end >= since_unix_ms);
        inner.connections.clone()
    }

    /// 开始计 `wall`，返回的守卫 drop 时停。
    pub fn running(&self) -> PaceRunning<'_> {
        self.start();
        PaceRunning(self)
    }

    /// 开始计 `wall`。可重入：每个 `start` 配一个 [`Pace::stop`]，全部停了才停止计时。
    pub fn start(&self) {
        let mut inner = self.0.lock().unwrap();
        inner.running += 1;
        if inner.since.is_none() {
            inner.since = Some(Instant::now());
            inner.media_at_since = inner.total.media;
            inner.active_at_since = inner.total.active;
            inner.live_lag = None;
        }
    }

    pub fn stop(&self) {
        let mut inner = self.0.lock().unwrap();
        inner.running = inner.running.saturating_sub(1);
        if inner.running == 0
            && let Some(since) = inner.since.take()
        {
            inner.total.wall += since.elapsed();
        }
    }

    pub fn add(&self, media: Duration, bytes: u64) {
        let mut inner = self.0.lock().unwrap();
        inner.total.media += media;
        inner.total.bytes += bytes;
    }

    /// 时间戳向前换基准，跳过了 `d` 的源时长（见 [`PaceCount::skipped`]）。
    pub fn skip(&self, d: Duration) {
        let mut inner = self.0.lock().unwrap();
        inner.total.skipped += d;
        inner.total.jumps += 1;
    }

    /// 刚写出一批帧：本条连接写出的第一帧与最后一帧各是直播的哪一刻（unix 毫秒，推流端时钟）。
    pub fn written_live(&self, first_ms: i64, last_ms: i64) {
        let unix_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_millis() as i64);
        self.written_live_at(first_ms, last_ms, unix_ms, Instant::now());
    }

    pub fn written_live_at(&self, first_ms: i64, last_ms: i64, unix_ms: i64, now: Instant) {
        let mut inner = self.0.lock().unwrap();
        // 之前的连接已写过内容却没给出时刻：本条连接的起点不是这次录制的起点，不填。
        // 转正切段时没给出时刻也不填：本条连接的起点是暂存内容的起点，不是切点。
        if inner.total.first_live_ms.is_none() && inner.media_at_since.is_zero() && !inner.formal {
            inner.total.first_live_ms = Some(first_ms);
        }
        if inner.since.is_some() {
            inner.live_lag = Some((unix_ms - last_ms, now));
        }
    }

    pub fn set_quality(&self, quality: Option<String>) {
        self.0.lock().unwrap().quality = quality;
    }

    pub fn quality(&self) -> Option<String> {
        self.0.lock().unwrap().quality.clone()
    }

    pub fn set_cut_live(&self, ms: i64) {
        self.0.lock().unwrap().cut_live_ms = Some(ms);
    }

    pub fn cut_live(&self) -> Option<i64> {
        self.0.lock().unwrap().cut_live_ms
    }

    /// 暂存录制转正：录制器在下一个关键帧切段，之后写正式目录。
    pub fn promote(&self) {
        self.0.lock().unwrap().promoted = true;
    }

    pub fn promoted(&self) -> bool {
        self.0.lock().unwrap().promoted
    }

    /// 转正后第一个正式分段从推流时刻 `live_ms` 的那一帧开始（不知道时为 `None`）：它就是交接的切点，
    /// 之后 [`PaceCount::first_live_ms`] 报它，不再报暂存录制的第一帧（`handoff-health.md` §6.2）。
    pub fn formal_started(&self, live_ms: Option<i64>) {
        let mut inner = self.0.lock().unwrap();
        inner.formal = true;
        inner.total.first_live_ms = live_ms;
    }

    pub fn formal(&self) -> bool {
        self.0.lock().unwrap().formal
    }

    pub fn mark_cut_done(&self) {
        self.0.lock().unwrap().cut_done = true;
    }

    pub fn cut_done(&self) -> bool {
        self.0.lock().unwrap().cut_done
    }

    /// 收到了一批字节（拉流连接每读到一块就调一次）。
    pub fn data(&self) {
        self.data_at(Instant::now());
    }

    pub fn data_at(&self, now: Instant) {
        let mut inner = self.0.lock().unwrap();
        match inner.last_data.map(|t| now.saturating_duration_since(t)) {
            Some(gap) if gap <= DATA_GAP => {
                inner.total.active += gap;
                if gap <= FLOW_GAP {
                    inner.total.flowing += gap;
                }
            }
            Some(_) => inner.total.gaps += 1,
            None => {}
        }
        inner.last_data = Some(now);
    }

    /// 当前累计值，正在计时的那一段算到此刻。
    pub fn read(&self) -> PaceCount {
        self.read_at(Instant::now())
    }

    pub fn read_at(&self, now: Instant) -> PaceCount {
        let inner = self.0.lock().unwrap();
        let mut total = inner.total;
        // 正在断着（已超过 DATA_GAP 没收到数据）也先算一次：监测不必等数据回来才清窗口。
        // 数据回来时 `data_at` 记的正是这一次，读数前后一致。
        if inner
            .last_data
            .is_some_and(|t| now.saturating_duration_since(t) > DATA_GAP)
        {
            total.gaps += 1;
        }
        if let Some(since) = inner.since {
            total.wall += now.saturating_duration_since(since);
            // 最近一次收数据到现在还没超过 DATA_GAP 的这一截也算在收。
            if let Some(gap) = inner
                .last_data
                .map(|t| now.saturating_duration_since(t))
                .filter(|g| *g <= DATA_GAP)
            {
                total.active += gap;
                if gap <= FLOW_GAP {
                    total.flowing += gap;
                }
            }
            let receiving = total.active.saturating_sub(inner.active_at_since);
            let received = total.media - inner.media_at_since;
            total.lag_ms = Some(receiving.as_millis() as i64 - received.as_millis() as i64);
            total.live_lag_ms = inner
                .live_lag
                .map(|(lag, at)| lag + now.saturating_duration_since(at).as_millis() as i64);
        }
        total
    }
}

pub struct PaceRunning<'a>(&'a Pace);

impl Drop for PaceRunning<'_> {
    fn drop(&mut self) {
        self.0.stop();
    }
}

/// 关键帧间隔正常在 10s 内，而段内合法空档的上限由停顿看门狗兜住（`httpflv` 的
/// `DEFAULT_STALL_TIMEOUT`，默认 30s——再长就断连另起一段了）。超过这个步长的跳变是
/// 上游换了时间基准，不是真的过了这么久。两个常量没有编译期耦合，改一个记得看另一个。
const MAX_STEP: Duration = Duration::from_secs(30);

/// 本段已录到的媒体时长。它是**连续前向增量的累加**，不是 `current - start` 的减法：
/// 媒体时间轴不保证单调，CDN 重发的 timestamp=0 tag 会让减法瞬间得出小时量级（dplei/biliup#35）。
#[derive(Debug, Clone)]
struct Time {
    expected: Option<Duration>,
    elapsed: Duration,
    /// 当前基准上最后一个被采纳的时间戳。
    last: Option<Duration>,
    /// 一个与当前基准不连续的时间戳。只有当它上面再出现一个连续的时间戳时才落实为新基准，
    /// 单个离群 tag 因此拉不动本段计时。
    pending_base: Option<Duration>,
}

/// 两个时间戳是否构成一次可信的推进。相等不算：重发的 tag 既不带来时长，
/// 也不构成「这个基准仍然有效」的证据。
fn continuous_step(from: Duration, to: Duration) -> Option<Duration> {
    let delta = to.saturating_sub(from);
    (to > from && delta <= MAX_STEP).then_some(delta)
}

#[derive(Debug, Clone)]
struct Size {
    expected: Option<u64>,
    current: u64,
}

impl Segmentable {
    pub fn new(expected_time: Option<Duration>, expected_size: Option<u64>) -> Self {
        Self {
            time: Time {
                expected: expected_time,
                elapsed: Duration::ZERO,
                last: None,
                pending_base: None,
            },
            size: Size {
                expected: expected_size,
                current: 0,
            },
            pace: None,
        }
    }

    /// 媒体时长与写盘字节同时累加进 `pace`。
    pub fn with_pace(mut self, pace: Option<Arc<Pace>>) -> Self {
        self.pace = pace;
        self
    }

    pub fn pace(&self) -> Option<&Pace> {
        self.pace.as_deref()
    }

    fn count(&self, media: Duration, bytes: u64) {
        if let Some(pace) = &self.pace {
            pace.add(media, bytes);
        }
    }

    /// 时间或大小任一超限即需切段。
    pub fn needed(&self) -> bool {
        let needed = self.time_needed() || self.size_needed();
        if needed {
            info!(
                elapsed = ?self.time.elapsed,
                size = self.size.current,
                "segment limit reached"
            );
        }
        needed
    }

    #[cfg(test)]
    fn elapsed_time(&self) -> Duration {
        self.time.elapsed
    }

    /// 检查单独的时间条件
    pub fn time_needed(&self) -> bool {
        self.time
            .expected
            .is_some_and(|max| self.time.elapsed >= max)
    }

    /// 检查单独的大小条件
    pub fn size_needed(&self) -> bool {
        self.size
            .expected
            .is_some_and(|max| self.size.current >= max)
    }

    pub fn increase_time(&mut self, number: Duration) {
        self.time.elapsed += number;
        self.count(number, 0);
    }

    pub fn set_time_position(&mut self, number: Duration) {
        let Some(last) = self.time.last else {
            self.time.last = Some(number);
            return;
        };
        if number == last {
            return;
        }
        if let Some(delta) = continuous_step(last, number) {
            self.time.elapsed += delta;
            self.count(delta, 0);
            self.time.last = Some(number);
            self.time.pending_base = None;
        } else if let Some(delta) = self
            .time
            .pending_base
            .and_then(|base| continuous_step(base, number))
        {
            // 候选基准上出现了第二个连续的时间戳：上游确实换了基准，而不是单个 tag 抖动。
            // 落实时只计入基准内部的那一步，跨基准的差值永远不进 elapsed。
            self.time.elapsed += delta;
            self.count(delta, 0);
            self.time.last = Some(number);
            self.time.pending_base = None;
        } else {
            self.time.pending_base = Some(number);
        }
    }

    pub fn set_start_time(&mut self, number: Duration) {
        self.time.elapsed = Duration::ZERO;
        self.time.last = Some(number);
        self.time.pending_base = None;
    }

    pub fn increase_size(&mut self, number: u64) {
        self.size.current += number;
        self.count(Duration::ZERO, number);
    }

    pub fn set_size_position(&mut self, number: u64) {
        self.size.current = number
    }

    /// 重置计数器，通常在创建新分割后调用
    pub fn reset(&mut self) {
        self.size.current = 0;
        // 只清本段时长；`last` 保留，下一段接着当前基准算增量。
        self.time.elapsed = Duration::ZERO;
    }

    /// 完全重置所有状态
    pub fn full_reset(&mut self) {
        self.size.current = 0;
        self.time.elapsed = Duration::ZERO;
        self.time.last = None;
        self.time.pending_base = None;
    }
}

impl Default for Segmentable {
    fn default() -> Self {
        Self::new(None, None)
    }
}

pub struct LifecycleFile<'a> {
    pub fmt_file_name: String,
    pub file_name: String,
    pub path: PathBuf,
    pub hook: CallbackFn<'a>,
    on_open: Option<OpenFn<'a>>,
    pub extension: &'static str,
    active: bool,
    close_handle: SegmentCloseHandle,
    owner: RecordingOwner,
    title: String,
    identity: Option<SegmentIdentity>,
    opened_at: Option<Instant>,
    last_closed: Option<PartClosed>,
    relocation: Option<Relocation>,
}

/// 返回 `Some(新模板)` 时，当前文件该收尾、之后的文件改用新模板（暂存录制转正）。
pub type Relocation = Box<dyn Fn() -> Option<String> + Send + Sync>;

impl<'a> LifecycleFile<'a> {
    pub fn new(fmt_file_name: &str, extension: &'static str) -> Self {
        Self::with_hook(fmt_file_name, extension, |_| {})
    }

    pub fn with_hook<F>(fmt_file_name: &str, extension: &'static str, hook: F) -> Self
    where
        F: FnMut(&PartClosed) + Send + Sync + 'a,
    {
        Self::with_hook_and_close_handle(
            fmt_file_name,
            extension,
            SegmentCloseHandle::default(),
            hook,
        )
    }

    pub fn with_hook_and_close_handle<F>(
        fmt_file_name: &str,
        extension: &'static str,
        close_handle: SegmentCloseHandle,
        hook: F,
    ) -> Self
    where
        F: FnMut(&PartClosed) + Send + Sync + 'a,
    {
        Self {
            fmt_file_name: fmt_file_name.to_string(),
            file_name: "".to_string(),
            path: Default::default(),
            hook: Box::new(hook),
            on_open: None,
            extension,
            active: false,
            close_handle,
            owner: RecordingOwner::default(),
            title: String::new(),
            identity: None,
            opened_at: None,
            last_closed: None,
            relocation: None,
        }
    }

    /// 见 [`Relocation`]。
    pub fn with_relocation(mut self, relocation: Option<Relocation>) -> Self {
        self.relocation = relocation;
        self
    }

    /// 当前文件写的是旧模板、该在下一个切段点换到新模板。
    pub fn relocating(&self) -> bool {
        self.relocation.as_ref().is_some_and(|r| r().is_some())
    }

    /// The caller owns the business identity; the file layer only carries it onto its own events.
    pub fn with_owner(mut self, owner: RecordingOwner) -> Self {
        self.owner = owner;
        self
    }

    /// 每建一个新文件就回调一次。
    pub fn with_open_hook<F>(mut self, hook: F) -> Self
    where
        F: FnMut(&PartOpened) + Send + Sync + 'a,
    {
        self.on_open = Some(Box::new(hook));
        self
    }

    /// 直播标题，原样带进 [`PartOpened`] / [`PartClosed`]。
    pub fn with_title(mut self, title: String) -> Self {
        self.title = title;
        self
    }

    /// Identity of the file currently open, available from the moment it is created.
    pub fn identity(&self) -> Option<&SegmentIdentity> {
        self.identity.as_ref()
    }

    pub fn owner(&self) -> &RecordingOwner {
        &self.owner
    }

    /// 最近一次成功收尾产出的事件。
    pub fn last_closed(&self) -> Option<&PartClosed> {
        self.last_closed.as_ref()
    }

    pub fn create(&mut self) -> Result<&Path, std::io::Error> {
        if let Some(template) = self.relocation.as_ref().and_then(|r| r()) {
            self.fmt_file_name = template;
            self.relocation = None;
        }
        // 构建最终文件名。模板通常只精确到秒：同一秒内再建文件会与上一段同名，
        // finalize 的 rename 会静默覆盖上一段（dplei/biliup#88），所以撞名时追加序号。
        let base = format_filename(&self.fmt_file_name);
        self.file_name = format!("{base}.{}", self.extension);
        let mut n = 0;
        while Path::new(&self.file_name).exists()
            || Path::new(&format!("{}.part", self.file_name)).exists()
        {
            n += 1;
            self.file_name = format!("{base}-{n}.{}", self.extension);
        }

        // 构建临时文件路径（带 .part 后缀）
        self.path = PathBuf::from(&self.file_name);
        self.path.set_extension(format!("{}.part", self.extension));

        // 确保父目录存在
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }

        info!("Save to {}", self.path.display());
        self.active = true;
        let identity = SegmentIdentity {
            segment_id: allocate_id("seg"),
            original_file: self.file_name.clone(),
        };
        let opened = PartOpened {
            owner: self.owner.clone(),
            part_id: identity.segment_id.clone(),
            path: self.path.clone(),
            title: self.title.clone(),
        };
        segment_created(&opened, &identity);
        if let Some(hook) = &mut self.on_open {
            hook(&opened);
        }
        self.identity = Some(identity);
        self.opened_at = Some(Instant::now());
        Ok(self.path.as_path())
    }

    pub fn finalize(&mut self, reason: SegmentCloseReason) -> Result<(), std::io::Error> {
        if !self.active {
            return Ok(());
        }
        // 去掉 .part 后缀
        match fs::rename(&self.path, &self.file_name) {
            Ok(_) => {
                self.active = false;
                // Identity is allocated by create(); a missing one means the file was never
                // opened through this type, so the event says unknown instead of guessing.
                let identity = self.identity.take().unwrap_or_else(|| SegmentIdentity {
                    segment_id: String::new(),
                    original_file: self.file_name.clone(),
                });
                let event = PartClosed {
                    owner: self.owner.clone(),
                    part_id: identity.segment_id.clone(),
                    path: PathBuf::from(&self.file_name),
                    duration: self
                        .opened_at
                        .take()
                        .map(|t| t.elapsed())
                        .unwrap_or_default(),
                    size: fs::metadata(&self.file_name).map(|m| m.len()).unwrap_or(0),
                    reason,
                    title: self.title.clone(),
                    closed_at_ms: None,
                };
                segment_closed(&event);
                (self.hook)(&event);
                self.last_closed = Some(event);
                Ok(())
            }
            Err(e) => {
                // The file stays active: identity is kept so a later retry still names it.
                let identity = self.identity.as_ref();
                segment_close_failed(
                    &self.owner,
                    identity.map(|i| i.segment_id.as_str()).unwrap_or(""),
                    &self.file_name,
                    &format!("{e}"),
                );
                error!("finalize {} {e}", self.path.display());
                Err(e)
            }
        }
    }

    pub fn is_active(&self) -> bool {
        self.active
    }

    /// 录制器要求停止：写盘循环应在下一个关键帧（HLS：当前 ts 段之后）收尾。
    pub fn stopping(&self) -> bool {
        self.close_handle.get() == SegmentCloseReason::Cancelled
    }

    pub fn set_close_reason(&self, reason: SegmentCloseReason) {
        self.close_handle.set(reason);
    }

    pub fn fallback_reason(&self, default: SegmentCloseReason) -> SegmentCloseReason {
        match self.close_handle.get() {
            SegmentCloseReason::Unknown => default,
            reason => reason,
        }
    }
}

/// 带缓冲的分段写入器：每个新文件先写 `prelude`（FLV 文件头；TS 为空），
/// 切段时刷盘、收尾旧文件、再开新文件。drop 时兜底收尾，不留 `.part`。
pub struct SegmentWriter<'a> {
    pub buf_writer: BufWriter<File>,
    pub file: LifecycleFile<'a>,
    prelude: &'static [u8],
    drop_reason: SegmentCloseReason,
}

impl<'a> SegmentWriter<'a> {
    pub fn new(
        mut file: LifecycleFile<'a>,
        prelude: &'static [u8],
        drop_reason: SegmentCloseReason,
    ) -> std::io::Result<Self> {
        let buf_writer = Self::open(file.create()?, prelude)?;
        Ok(Self {
            buf_writer,
            file,
            prelude,
            drop_reason,
        })
    }

    pub fn create_new(&mut self, reason: SegmentCloseReason) -> std::io::Result<()> {
        self.finish(reason)?;
        self.buf_writer = Self::open(self.file.create()?, self.prelude)?;
        Ok(())
    }

    pub fn finish(&mut self, reason: SegmentCloseReason) -> std::io::Result<()> {
        self.buf_writer.flush()?;
        self.file.finalize(reason)
    }

    fn open(path: &Path, prelude: &[u8]) -> std::io::Result<BufWriter<File>> {
        let out = File::create(path).map_err(|e| {
            std::io::Error::new(
                e.kind(),
                format!("Unable to create file {}", path.display()),
            )
        })?;
        let mut buf_writer = BufWriter::new(out);
        buf_writer.write_all(prelude)?;
        Ok(buf_writer)
    }
}

impl Drop for SegmentWriter<'_> {
    fn drop(&mut self) {
        if self.file.is_active() {
            let _ = self.buf_writer.flush();
            let reason = self.file.fallback_reason(self.drop_reason);
            let _ = self.file.finalize(reason);
        }
    }
}

/// 用本地时间展开 strftime 模板（如 `%Y-%m-%d_%H-%M-%S`）。
pub fn format_filename(file_name: &str) -> String {
    Local::now().format(file_name).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::{Path, PathBuf};

    #[test]
    fn it_works() -> Result<(), Box<dyn std::error::Error>> {
        let mut p = PathBuf::from("/feel/the");

        p.set_extension("force");
        assert_eq!(Path::new("/feel/the.force"), p.as_path());

        p.set_extension("");
        assert_eq!(Path::new("/feel/the"), p.as_path());

        Ok(())
    }

    #[test]
    fn finalize_produces_part_closed() {
        let dir = tempfile::tempdir().unwrap();
        let owner = RecordingOwner {
            room: "r".into(),
            session: "s".into(),
        };
        let mut file = LifecycleFile::new(dir.path().join("a").to_str().unwrap(), "flv")
            .with_owner(owner.clone());
        fs::write(file.create().unwrap(), b"0123").unwrap();
        let id = file.identity().unwrap().segment_id.clone();
        file.finalize(SegmentCloseReason::TimedSplit).unwrap();

        let event = file.last_closed().unwrap();
        assert_eq!(event.owner, owner);
        assert_eq!(event.part_id, id);
        assert_eq!(event.path, dir.path().join("a.flv"));
        assert_eq!(event.size, 4);
        assert_eq!(event.reason, SegmentCloseReason::TimedSplit);
    }

    #[test]
    fn test_segmentation_logic() -> Result<(), Box<dyn std::error::Error>> {
        // 测试时间分割
        let mut seg = Segmentable::new(Some(Duration::from_secs(10)), None);
        assert!(!seg.needed());

        seg.increase_time(Duration::from_secs(15));
        assert!(seg.needed());
        assert!(seg.time_needed());
        assert!(!seg.size_needed());

        // 测试大小分割
        let mut seg = Segmentable::new(None, Some(1024));
        assert!(!seg.needed());

        seg.increase_size(2048);
        assert!(seg.needed());
        assert!(!seg.time_needed());
        assert!(seg.size_needed());

        // 测试双重条件
        let mut seg = Segmentable::new(Some(Duration::from_secs(10)), Some(1024));
        assert!(!seg.needed());

        // 只满足时间条件
        seg.increase_time(Duration::from_secs(15));
        assert!(seg.needed());

        // 重置并只满足大小条件
        seg.full_reset();
        seg.increase_size(2048);
        assert!(seg.needed());

        // 同时满足两个条件
        seg.increase_time(Duration::from_secs(15));
        assert!(seg.needed());
        assert!(seg.time_needed());
        assert!(seg.size_needed());

        Ok(())
    }

    /// 倍率计数只收连续前向增量：换基准的跳变不算媒体时长，否则一次 CDN 重发就把倍率撑到几百。
    #[test]
    fn pace_counts_continuous_media_bytes_and_running_wall() {
        let pace = Arc::new(Pace::default());
        let mut seg = Segmentable::new(None, None).with_pace(Some(pace.clone()));
        seg.set_time_position(Duration::from_secs(100));
        seg.set_time_position(Duration::from_secs(102));
        seg.set_time_position(Duration::from_secs(5_000)); // 离群，不算
        seg.set_time_position(Duration::from_secs(103));
        seg.increase_time(Duration::from_secs(4)); // HLS 口径
        seg.increase_size(500);
        let count = pace.read();
        assert_eq!(count.media, Duration::from_secs(7));
        assert_eq!((count.bytes, count.wall), (500, Duration::ZERO));

        {
            let _running = pace.running();
            std::thread::sleep(Duration::from_millis(30));
            assert!(
                pace.read().wall >= Duration::from_millis(30),
                "计时中途也能读到"
            );
            // lag 只算这条连接：连上之前收到的 7 秒媒体不抵扣。连上后约 30ms 里收到 1 秒：领先约 970ms。
            pace.add(Duration::from_secs(1), 0);
            let lag = pace.read().lag_ms.unwrap();
            assert!((-1000..=-900).contains(&lag), "{lag}");
        }
        assert_eq!(pace.read().lag_ms, None, "没连着就没有 lag");
        let stopped = pace.read().wall;
        std::thread::sleep(Duration::from_millis(20));
        assert_eq!(pace.read().wall, stopped, "守卫 drop 后不再计时");

        // 两处同时在计（整段上传与边录边传）：先停的那个不掐断另一个。
        pace.start();
        {
            let _other = pace.running();
        }
        std::thread::sleep(Duration::from_millis(20));
        assert!(pace.read().lag_ms.is_some(), "还有一处在计时");
        pace.stop();
        let stopped = pace.read().wall;
        assert!(stopped >= Duration::from_millis(50));
        std::thread::sleep(Duration::from_millis(10));
        assert_eq!(pace.read().wall, stopped, "全停了才停");
    }

    /// 只在持续收数据时计 `active`：断流空挂、断开重连超过 DATA_GAP 的那段不算，并记一次 gap；
    /// 短的停顿（限速连接被 CDN 断开、几秒内重连）照常算。
    #[test]
    fn active_counts_only_while_data_keeps_arriving() {
        let pace = Pace::default();
        let _connected = pace.running();
        let t0 = Instant::now();
        let at = |s: u64| t0 + Duration::from_secs(s);
        for s in 0..=60 {
            pace.data_at(at(s));
        }
        assert_eq!(pace.read_at(at(60)).active, Duration::from_secs(60));
        // 空挂中：DATA_GAP 以内的尾巴先算着，超过就不再涨。
        assert_eq!(pace.read_at(at(70)).active, Duration::from_secs(70));
        let silent = pace.read_at(at(200));
        assert_eq!(silent.active, Duration::from_secs(60));
        assert_eq!(silent.gaps, 1, "断着的这一次读数时就算上");
        // 3 分钟后数据回来：中间那段不算，记一次 gap。
        pace.data_at(at(240));
        let c = pace.read_at(at(240));
        assert_eq!((c.active, c.gaps), (Duration::from_secs(60), 1));
        // 3 秒的短停顿照常算，不记 gap。
        pace.data_at(at(243));
        let c = pace.read_at(at(243));
        assert_eq!((c.active, c.gaps), (Duration::from_secs(63), 1));
    }

    /// 跳过量与跳变次数只增不减，断流（gaps 变化）、`active` 停涨都不影响它们。
    #[test]
    fn skip_accumulates_regardless_of_gaps() {
        let pace = Pace::default();
        let _connected = pace.running();
        let t0 = Instant::now();
        let at = |s: u64| t0 + Duration::from_secs(s);
        pace.data_at(at(0));
        pace.skip(Duration::from_millis(20_040));
        pace.skip(Duration::from_millis(19_960));
        let c = pace.read_at(at(1));
        assert_eq!((c.skipped, c.jumps), (Duration::from_secs(40), 2));
        assert_eq!(c.gaps, 0);
        // 一次超过 DATA_GAP 的断流：gaps 变了，跳过量与次数不变。
        pace.data_at(at(60));
        let c = pace.read_at(at(60));
        assert_eq!(c.gaps, 1);
        assert_eq!((c.skipped, c.jumps), (Duration::from_secs(40), 2));
    }

    /// 时间戳回绕后 current 会永远小于 start，`elapsed_time` 恒为 0，本段将再也切不了片。
    #[test]
    fn a_backward_timeline_rearms_the_segment_start() {
        let mut seg = Segmentable::new(Some(Duration::from_secs(10)), None);
        seg.set_start_time(Duration::from_secs(1_000));
        seg.set_time_position(Duration::from_secs(1_005));
        assert!(!seg.time_needed());

        // 上游时间轴回到 0，起点必须跟着回落
        seg.set_time_position(Duration::ZERO);
        assert!(!seg.time_needed());
        seg.set_time_position(Duration::from_secs(10));
        assert!(seg.time_needed(), "回绕后仍应按新基准正常切片");
    }

    /// dplei/biliup#35:重连后 CDN 重发 timestamp=0 的 tag,而正常帧带着绝对媒体时钟。
    /// 单个离群时间戳不能把本段时长撑到小时量级。
    #[test]
    fn a_single_out_of_base_tag_does_not_inflate_elapsed() {
        let base = Duration::from_millis(32_891_256);
        let mut seg = Segmentable::new(Some(Duration::from_secs(3)), None);
        seg.set_start_time(base);
        seg.set_time_position(base + Duration::from_secs(1));
        seg.set_time_position(Duration::ZERO);
        seg.set_time_position(base + Duration::from_secs(2));

        assert_eq!(seg.elapsed_time(), Duration::from_secs(2));
        assert!(
            !seg.time_needed(),
            "单个 timestamp=0 不该让本段立刻满足时间条件"
        );
    }

    /// CDN 会成批重发,0 与正常帧可能逐帧交替。此时本段计时仍必须沿主基准推进,
    /// 否则就从「秒级乱切」翻回 dplei/biliup#32 的「永远不切」。
    #[test]
    fn alternating_zero_keyframes_still_advance_the_clock() {
        let base = Duration::from_millis(32_891_256);
        let mut seg = Segmentable::new(Some(Duration::from_secs(3)), None);
        seg.set_start_time(Duration::ZERO);
        for step in 1..=10u64 {
            seg.set_time_position(base + Duration::from_secs(step));
            seg.set_time_position(Duration::ZERO);
        }

        assert!(
            seg.elapsed_time() >= Duration::from_secs(8),
            "主基准上的增量必须照常累加,实际 {:?}",
            seg.elapsed_time()
        );
        assert!(seg.time_needed());
    }

    /// 超过一个关键帧间隔量级的前向跳变是换基准,不是真的过了这么久。
    #[test]
    fn a_gap_longer_than_max_step_is_not_counted() {
        let mut seg = Segmentable::new(Some(Duration::from_secs(30)), None);
        seg.set_start_time(Duration::from_secs(100));
        seg.set_time_position(Duration::from_secs(400));
        assert_eq!(seg.elapsed_time(), Duration::ZERO);
        assert!(!seg.time_needed());
    }

    #[test]
    fn cancellation_close_reason_is_shared_with_active_file() {
        let handle = SegmentCloseHandle::default();
        let file =
            LifecycleFile::with_hook_and_close_handle("unused", "flv", handle.clone(), |_| {});
        handle.set(SegmentCloseReason::Cancelled);
        assert_eq!(
            file.fallback_reason(SegmentCloseReason::TransportError),
            SegmentCloseReason::Cancelled
        );
    }
}
