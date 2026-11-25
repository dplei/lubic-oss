use crate::downloader::flv_parser::{
    AACPacketType, AVCPacketType, CodecId, FrameType, SoundFormat, TagData, TagHeader, TagType,
    aac_audio_packet_header, avc_video_packet_header, script_data, tag_data, tag_header,
};
use crate::downloader::flv_writer::FlvFile;
use crate::downloader::util::{EVENT_TARGET, LifecycleFile, Pace, SegmentCloseReason, Segmentable};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use nom::{Err, IResult};
use reqwest::Response;

use std::time::{Duration, Instant};
use tokio::time::timeout;
use tracing::{info, warn};

/// 录制一条 FLV 连接直到流结束或出错。调用方保留 `Connection`，返回后可读 `diagnostics()`。
/// 调用前需已读掉 9 字节 FLV 文件头。
pub async fn download(
    connection: &mut Connection,
    file: LifecycleFile<'_>,
    segment: Segmentable,
) -> crate::downloader::error::Result<()> {
    download_with_progress(connection, file, segment, &mut FlvProgress::default()).await
}

pub(crate) async fn download_with_progress(
    connection: &mut Connection,
    file: LifecycleFile<'_>,
    segment: Segmentable,
    progress: &mut FlvProgress,
) -> crate::downloader::error::Result<()> {
    download_with_progress_and_upgrade(connection, file, segment, progress, None).await
}

pub(crate) async fn download_with_progress_and_upgrade(
    connection: &mut Connection,
    file: LifecycleFile<'_>,
    segment: Segmentable,
    progress: &mut FlvProgress,
    upgrade_signal: Option<&QualityUpgradeSignal>,
) -> crate::downloader::error::Result<()> {
    let owner = file.owner().clone();
    let result = parse_flv_with_upgrade(connection, file, segment, progress, upgrade_signal).await;
    emit_disconnected(
        &owner,
        &connection.diagnostics(),
        progress,
        result.as_ref().err(),
    );
    result
}

/// 计划升档时的待上报信息：新流写入可用视频后才发布画质并记日志。
#[derive(Debug, Clone)]
pub(crate) struct PendingQualityUpgrade {
    pub from_quality: String,
    pub to_quality: String,
    pub pace_quality: Option<String>,
}

/// 可撤销的计划升档收尾信号：由候选探测就绪后发出，旧 FLV 在下一个关键帧边界确认收尾。
#[derive(Debug, Default)]
pub(crate) struct QualityUpgradeSignal {
    state: std::sync::atomic::AtomicU8,
}

impl QualityUpgradeSignal {
    const IDLE: u8 = 0;
    const REQUESTED: u8 = 1;
    const ACKNOWLEDGED: u8 = 2;

    pub fn request(&self) {
        self.state
            .store(Self::REQUESTED, std::sync::atomic::Ordering::Release);
    }

    pub fn cancel(&self) -> bool {
        self.state
            .compare_exchange(
                Self::REQUESTED,
                Self::IDLE,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            )
            .is_ok()
    }

    pub fn try_acknowledge(&self) -> bool {
        self.state
            .compare_exchange(
                Self::REQUESTED,
                Self::ACKNOWLEDGED,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            )
            .is_ok()
    }

    pub fn acknowledged(&self) -> bool {
        self.state.load(std::sync::atomic::Ordering::Acquire) == Self::ACKNOWLEDGED
    }
}

/// 一条连接内的分段与媒体时间戳进度。
///
/// 断连时写进日志：「距上次切段多久」用于判断断连是否由切段触发；时间戳是 CDN 原始值，
/// 同一源时间基准下可相减；跨 CDN 或重新推流时，还需用 SEI 推流时刻校验。
#[derive(Debug, Clone, Default)]
pub struct FlvProgress {
    /// 本连接内发生的分段次数
    pub splits: u32,
    /// 最后一次分段的本地时刻
    pub last_split_at: Option<Instant>,
    /// 本连接写入文件的第一帧音视频的源时间戳（不含 Script 与序列头）
    pub first_timestamp_ms: Option<u64>,
    /// 本连接写入文件的最后一帧音视频的源时间戳
    pub last_timestamp_ms: Option<u64>,
    /// 连接收尾时补写的完整 tag 数量与字节数（不含读到一半的 tag）。
    pub tail_tags: u64,
    pub tail_bytes: u64,
    /// 本次是否是同一场内的重连；上次保留媒体的推流时刻，缺 SEI 时为 None。
    pub reconnecting: bool,
    pub previous_live_ms: Option<i64>,
    /// 上一条有效连接之后经历过下播宽限（平台不确认在播）：这个接缝照常打日志，但不计入健康检测。
    pub crossed_grace: bool,
    pub first_live_ms: Option<i64>,
    pub last_live_ms: Option<i64>,
    pub video_stalled: bool,
    /// 解析到过音视频帧（不含 Script 与序列头）。没收到帧的空录多是下播后平台状态滞后，不算本机异常。
    pub received_media: bool,
    pub video_frames: u64,
    pub valid_video: bool,
    pub gap_reported: bool,
    pub(crate) edge_switched: bool,
    pub(crate) edge_attempt: Option<EdgeAttempt>,
    pub(crate) connection_diagnostics: Option<ConnectionDiagnostics>,
    /// 无法精确计量（例如磁盘写失败）时为 None。
    pub discarded_tail_tags: Option<u64>,
    pub discarded_tail_bytes: Option<u64>,
    /// 计划升档主动切换：照常写接缝日志，但不计入接缝健康检测。
    pub(crate) skip_gap_health: bool,
    /// 计划升档新连接待发布的画质变更。
    pub(crate) pending_quality_upgrade: Option<PendingQualityUpgrade>,
}

#[derive(Debug, Clone)]
pub(crate) struct EdgeAttempt {
    pub id: String,
    pub previous_edge: Option<std::net::IpAddr>,
    pub previous_ended: Instant,
    pub first_video_wall_ms: Option<u64>,
}

impl FlvProgress {
    fn note_reconnect_video(&mut self) {
        if let Some(attempt) = &mut self.edge_attempt {
            attempt
                .first_video_wall_ms
                .get_or_insert_with(|| attempt.previous_ended.elapsed().as_millis() as u64);
        }
    }

    pub(crate) fn report_reconnect(
        &mut self,
        owner: &crate::downloader::util::RecordingOwner,
        pace: Option<&Pace>,
    ) {
        if !self.reconnecting || self.gap_reported {
            return;
        }
        let bounds = self
            .valid_video
            .then(|| self.previous_live_ms.zip(self.first_live_ms))
            .flatten();
        let (gap_ms, overlap_ms) = bounds.map_or((-1, -1), |(previous, first)| {
            ((first - previous).max(0), (previous - first).max(0))
        });
        // 只有可靠推流时刻测得的接缝计入健康检测；跨过下播宽限的是源站本来就没内容，不算；
        // 计划升档主动切换本身也不计入接缝健康检测。
        if let Some(pace) =
            pace.filter(|_| bounds.is_some() && !self.crossed_grace && !self.skip_gap_health)
        {
            pace.reconnect_gap(gap_ms as u64);
        }
        let reason_code = if bounds.is_some() {
            "sei"
        } else if !self.valid_video {
            "no_retained_video"
        } else if self.previous_live_ms.is_none() {
            "previous_sei_missing"
        } else {
            "current_clock_unknown"
        };
        info!(
            target: EVENT_TARGET,
            event_name = "recording.reconnect_gap",
            outcome = if bounds.is_some() { "measured" } else { "unknown" },
            reason_code,
            gap_ms, overlap_ms,
            edge_switched = self.edge_switched,
            edge_attempt_id = self.edge_attempt.as_ref().map(|attempt| attempt.id.as_str()),
            first_video_wall_ms = self.edge_attempt.as_ref().and_then(|attempt| attempt.first_video_wall_ms),
            previous_live_ms = self.previous_live_ms.unwrap_or(-1),
            first_live_ms = self.first_live_ms.unwrap_or(-1),
            room = owner.room.as_str(), session = owner.session.as_str(),
            "重连画面衔接"
        );
        self.gap_reported = true;
    }

    fn since_last_split_ms(&self) -> i64 {
        self.last_split_at
            .map(|at| at.elapsed().as_millis() as i64)
            .unwrap_or(-1)
    }
}

fn optional_ms(value: Option<u64>) -> i64 {
    value.map(|v| v as i64).unwrap_or(-1)
}

/// 时间戳跳变的源端汇总：每段第一次跳变单独报，其余计数后合成一条；换段重新计。
#[derive(Default)]
struct DtsBackwardRollup {
    segment_id: String,
    original_file: String,
    reason_code: &'static str,
    count: u64,
    first_ms: u64,
    last_ms: u64,
    max_backward_ms: u64,
}

impl DtsBackwardRollup {
    fn record(
        &mut self,
        file: &LifecycleFile<'_>,
        previous_ms: u64,
        current_ms: u64,
        emitted_ms: u64,
        reason_code: &'static str,
    ) {
        let segment_id = file
            .identity()
            .map(|identity| identity.segment_id.clone())
            .unwrap_or_default();
        if segment_id != self.segment_id {
            self.flush(file);
            self.segment_id = segment_id;
            self.original_file = original_file(file).to_owned();
        }
        let backward = previous_ms.saturating_sub(current_ms);
        self.count += 1;
        if self.count == 1 {
            self.first_ms = current_ms;
            self.max_backward_ms = backward;
            self.reason_code = reason_code;
            emit_dts_first(file, previous_ms, current_ms, emitted_ms, reason_code);
        } else {
            self.max_backward_ms = self.max_backward_ms.max(backward);
        }
        self.last_ms = current_ms;
    }

    fn flush(&mut self, file: &LifecycleFile<'_>) {
        if self.count > 1 {
            emit_dts_summary(file, self);
        }
        self.count = 0;
        self.reason_code = "";
        self.first_ms = 0;
        self.last_ms = 0;
        self.max_backward_ms = 0;
    }
}

/// 同一条流内允许的最大前跳。真实帧间隔远小于它；超过它的不论是换基准还是源流真断了一截，
/// 都按换基准接到已写出的最大值之后——B 站转码拒收段内的时间戳空档（实测 19s 被拒，
/// 稿件 6 P7）。曾经与 `util.rs` 的 `MAX_STEP` 一样取看门狗的 30s，那边只管倍率统计，不跟着改。
/// 只有一条流断档时（另一条照常推进），新 offset 由 high_water 算出，约等于原 offset，音画不错位。
const REBASE_MAX_STEP_MS: i64 = 1_000;
/// 换基准处给新基准留的名义间隔。取 10ms 是因为它小于任何真实帧间隔（60fps 约 16ms，
/// 一帧 AAC 约 23ms），既保证同流严格递增，又不会在 CDN 逐帧交替重发时把时长撑长。
const REBASE_NOMINAL_GAP_MS: i64 = 10;

#[derive(Default, Clone, Copy)]
struct StreamBase {
    /// 本流已确认基准上的最后一个源时间戳
    last_src: Option<i64>,
    /// 本流最后写出的时间戳
    last_emit: Option<i64>,
    /// 已见过一次、还等第二个样本确认的候选新基准
    pending: Option<i64>,
    /// 本流当前的修正量。同一基准内各流相同；某流确认换基准后，其它流通过 `adopt` 跟上。
    offset: i64,
    /// 兄弟流刚确认的新 offset，等本流自己也偏离旧基准时采纳；本流仍在旧基准上则作废。
    /// 曾经是「确认时直接清空兄弟的判据，下一个 tag 无条件套新 offset」——CDN 成对重发
    /// timestamp=0 的垃圾 tag 就能凑满两个样本，兄弟流带着旧基准的大时间戳套上按 0 算出
    /// 的 offset，落盘前跳几千秒（dplei/biliup#62）。
    adopt: Option<i64>,
}

/// 写盘侧的时间戳重基。
///
/// CDN 在分段中途重发 script tag 并换时间基准（通常归零，偶尔大幅前跳）时，原样落盘的
/// 时间戳会让下游解复用器按 32 位 wrap 展开成天文数字，转码据此拒稿（dplei/biliup#13）。
/// 这里在写出前统一套一层 `emit = src + offset`，`offset` 只在基准确认改变时更新。
///
/// 连续性判据按 **tag 类型分别** 维护：audio 与 video 交错送达，跨流比较会把每个合法交错的
/// audio tag 误判成换基准，把音画同步打散；各自流内则本来就是单调的。修正量按流持有，但
/// 同一基准内必须是同一个值，同基准内的相对关系（音画同步）才原样保留——所以某流确认换
/// 基准后，其它流在自己也偏离旧基准时采纳同一个 offset，而不是各自再算一个。
#[derive(Default)]
struct TimestampRebase {
    /// 最近一次确认的 offset，只给某流的第一个 tag（还没有判据）用。
    offset: i64,
    /// 已写出的最大时间戳。换基准时新基准接在它之后，不能用「上一个写出值」——
    /// 合法交错本来就允许小于它。
    high_water: i64,
    streams: [StreamBase; 3],
}

struct Mapped {
    emit: u32,
    /// 本 tag 偏离了所在流的基准：`(该流上一个源时间戳, reason_code)`。
    deviation: Option<(u64, &'static str)>,
    /// 本 tag 确认了一次向前换基准时跳过的源时长（毫秒）。
    skipped_ms: Option<u64>,
}

/// `to` 是否仍落在以 `from` 为基准的同一条时间轴上。允许停在原地（CDN 重发同一时间戳），
/// 不允许倒退，也不允许超过看门狗阈值的前跳。
fn follows(from: i64, to: i64) -> bool {
    to >= from && to - from <= REBASE_MAX_STEP_MS
}

impl TimestampRebase {
    fn map(&mut self, tag_type: TagType, src: u32) -> Mapped {
        // script tag 不走连续性判据：切段 prelude 的 onMetaData 一段只有一个，相邻两个隔着整段
        // 时长，必然超过 `REBASE_MAX_STEP_MS`，于是每次都判成偏离、写成「上一个 script +1ms」，
        // 文件起点被这一个包拉回整整一个分段（dplei/biliup#75，转码据此拒稿）。它不承载时间轴，
        // 贴在已写出的最大值上即可；也就不能再凭它确认换基准。还没写出过任何 tag 时照旧映射。
        if tag_type == TagType::Script && self.streams.iter().any(|s| s.last_emit.is_some()) {
            let script = &mut self.streams[2];
            let emit = script
                .last_emit
                .map_or(self.high_water, |last| self.high_water.max(last + 1))
                .clamp(0, u32::MAX as i64);
            script.last_src = Some(src as i64);
            script.last_emit = Some(emit);
            self.high_water = self.high_water.max(emit);
            return Mapped {
                emit: emit as u32,
                deviation: None,
                skipped_ms: None,
            };
        }
        let slot = slot(tag_type);
        let src = src as i64;
        let stream = self.streams[slot];
        let mut deviation = None;
        let mut skipped_ms = None;
        let emit = match stream.last_src {
            // 本流的第一个 tag：没有判据可用，沿用最近确认的基准。
            None => {
                self.streams[slot].last_src = Some(src);
                self.streams[slot].offset = self.offset;
                self.streams[slot].adopt = None;
                src + self.offset
            }
            Some(last) if follows(last, src) => {
                self.streams[slot].last_src = Some(src);
                // 时钟停在原地不算推进，不能清掉待确认的新基准，否则 CDN 逐帧交替重发
                // （`[0, B+1000, 0, B+2000, …]`）时新基准永远等不到第二个样本，整条时间轴
                // 会被压成每帧 `REBASE_NOMINAL_GAP_MS`。`util.rs` 的 `set_time_position`
                // 靠 `number == last` 提前返回拿到同一效果。
                if src > last {
                    self.streams[slot].pending = None;
                }
                // 拿到一个旧基准的时间戳就足以说明本流没跟着换：兄弟那次确认对本流不成立。
                self.streams[slot].adopt = None;
                src + stream.offset
            }
            // 兄弟流已经确认了新基准，本流现在也偏离了旧基准：直接采纳同一个 offset。
            // 不算本流自己的发现，不报 deviation，事件语义与原先的「清空后沿用」一致。
            Some(_) if stream.adopt.is_some() => {
                let offset = stream.adopt.unwrap_or(self.offset);
                self.streams[slot] = StreamBase {
                    last_src: Some(src),
                    pending: None,
                    offset,
                    adopt: None,
                    ..stream
                };
                src + offset
            }
            Some(last) => {
                deviation = Some((
                    last as u64,
                    if src < last {
                        "timestamp_backward"
                    } else {
                        "timestamp_jump_forward"
                    },
                ));
                match stream.pending {
                    // 连续两个样本落在同一条新时间轴上：确认换基准，把它接到已写出的最大值之后。
                    // 第二个样本必须真的推进：CDN 成对重发的 timestamp=0 初始化 tag（aac/avc
                    // sequence header）两个样本相等，不是新时间轴，不能凭它换基准——落后的流
                    // 会被拉到兄弟流的 high_water 上，之后整段音画错位 1s 以上（实测 1.1s）。
                    Some(pending) if src > pending && follows(pending, src) => {
                        // 向前换基准：旧轴最后一个到新轴第一个之间的直播没收到。只在确认处报，
                        // 孤立噪声走不到这里；兄弟流经 `adopt` 跟上，不重复报，一次跳变只算一次。
                        if pending > last {
                            skipped_ms = Some((pending - last) as u64);
                        }
                        self.offset = self.high_water + REBASE_NOMINAL_GAP_MS - src;
                        self.streams[slot].last_src = Some(src);
                        self.streams[slot].pending = None;
                        self.streams[slot].offset = self.offset;
                        // 其它流大概率也换了基准，但只有它们自己的下一个 tag 能证明：挂上
                        // 新 offset 等它们偏离时采纳，判据不动。
                        for other in 0..self.streams.len() {
                            if other != slot {
                                self.streams[other].adopt = Some(self.offset);
                            }
                        }
                        src + self.offset
                    }
                    // 首次偏离：还分不清是孤立噪声（重发的初始化帧）还是新基准，先紧跟在
                    // **本流**已写出的最后一个之后，`last_src` 不动，等下一个样本表态。
                    // 不能接到全局 high_water 之后：B 帧编码器的 video 比 audio 晚到 1s 以上，
                    // 一个重发的 sequence header 会把 video 推到 audio 的位置，之后一秒的真实
                    // 帧全被 `last_emit + 1` 压成 1ms 间隔，上传侧判为时间戳异常且修不好。
                    _ => {
                        self.streams[slot].pending = Some(src);
                        stream
                            .last_emit
                            .map_or(self.high_water + REBASE_NOMINAL_GAP_MS, |last| last + 1)
                    }
                }
            }
        };
        // 同一个 tag 类型的输出必须严格递增；跨类型的小幅倒退是合法交错，不碰。
        let emit = match self.streams[slot].last_emit {
            Some(last_emit) => emit.max(last_emit + 1),
            None => emit,
        }
        .clamp(0, u32::MAX as i64);
        self.streams[slot].last_emit = Some(emit);
        self.high_water = self.high_water.max(emit);
        Mapped {
            emit: emit as u32,
            deviation,
            skipped_ms,
        }
    }
}

impl TimestampRebase {
    /// 序列头（AAC/AVC 配置）不承载时间，不参与连续性判据、也不建立基准。CDN 常给它们填 0
    /// 或陈旧值（实测 B 站 AAC 头带着 2147178，真实媒体从 2963844 起）；当普通 tag 处理时，
    /// 开录首段的第一个关键帧被压到 ~0ms，其余帧接到陈旧基准之后。这里只把它贴在本流已写出
    /// 的最后一个之后，还没写出过就贴在全局最大值上。
    fn map_config(&mut self, tag_type: TagType) -> u32 {
        let stream = &mut self.streams[slot(tag_type)];
        let emit = stream
            .last_emit
            .map_or(self.high_water, |last| last + 1)
            .clamp(0, u32::MAX as i64);
        stream.last_emit = Some(emit);
        self.high_water = self.high_water.max(emit);
        emit as u32
    }
}

/// 每种 tag 在 `TimestampRebase::streams` 里的槽位。
fn slot(tag_type: TagType) -> usize {
    match tag_type {
        TagType::Audio => 0,
        TagType::Video => 1,
        TagType::Script => 2,
    }
}

/// AAC 或 AVC/HEVC 的序列头（解码器配置），按 FLV 包头字节判断。
fn is_sequence_header(tag_type: TagType, body: &[u8]) -> bool {
    match (tag_type, body) {
        (TagType::Audio, [format, 0, ..]) => format >> 4 == 10,
        (TagType::Video, [codec, 0, ..]) => matches!(codec & 0x0f, 7 | 12),
        _ => false,
    }
}

/// 切段时重发的段首 prelude（onMetaData / sequence header）要带当前时间戳。沿用建档时的
/// 旧值（常是 0）会在每次切段制造一次假的基准跳变。
fn restamp(tag: &(TagHeader, Bytes, Bytes), timestamp: u32) -> (TagHeader, Bytes, Bytes) {
    let mut header = tag.0;
    header.timestamp = timestamp;
    (header, tag.1.clone(), tag.2.clone())
}

fn emit_dts_first(
    file: &LifecycleFile<'_>,
    previous_ms: u64,
    current_ms: u64,
    emitted_ms: u64,
    reason_code: &'static str,
) {
    let owner = file.owner();
    warn!(
        target: EVENT_TARGET,
        event_name = "recording.dts_backward",
        outcome = "executed",
        reason_code,
        segment_id = segment_id(file),
        original_file = original_file(file),
        previous_ms,
        current_ms,
        emitted_ms,
        room = owner.room.as_str(),
        session = owner.session.as_str(),
        "检测到时间戳基准跳变，已重基后继续录制"
    );
}

fn emit_dts_summary(file: &LifecycleFile<'_>, rollup: &DtsBackwardRollup) {
    let owner = file.owner();
    warn!(
        target: EVENT_TARGET,
        event_name = "recording.dts_backward",
        outcome = "executed",
        reason_code = rollup.reason_code,
        segment_id = rollup.segment_id,
        original_file = rollup.original_file,
        count = rollup.count,
        first_ms = rollup.first_ms,
        last_ms = rollup.last_ms,
        max_backward_ms = rollup.max_backward_ms,
        room = owner.room.as_str(),
        session = owner.session.as_str(),
        "本分段时间戳基准跳变汇总"
    );
}

/// 一条连接结束时报一次：正常结束 info，异常 warn；只报本连接自己观测到的东西，
/// 之后的重连缺口由录制循环计量。
fn emit_disconnected(
    owner: &crate::downloader::util::RecordingOwner,
    diagnostics: &ConnectionDiagnostics,
    progress: &FlvProgress,
    error: Option<&crate::downloader::error::Error>,
) {
    let silent_ms = diagnostics.silent_for.as_millis() as u64;
    let duration_ms = diagnostics.connected_for.as_millis() as u64;
    let edge = diagnostics
        .edge
        .map_or_else(|| "-".to_string(), |a| a.ip().to_string());
    let Some(error) = error else {
        info!(
            target: EVENT_TARGET,
            event_name = "recording.disconnected",
            outcome = "succeeded",
            reason_code = "stream_end",
            silent_ms,
            duration_ms,
            received_bytes = diagnostics.received_bytes,
            edge,
            edge_switched = progress.edge_switched,
            edge_attempt_id = progress.edge_attempt.as_ref().map(|attempt| attempt.id.as_str()),
            splits = progress.splits,
            first_timestamp_ms = optional_ms(progress.first_timestamp_ms),
            last_timestamp_ms = optional_ms(progress.last_timestamp_ms),
            tail_tags = progress.tail_tags,
            tail_bytes = progress.tail_bytes,
            video_frames = progress.video_frames,
            first_live_ms = progress.first_live_ms.unwrap_or(-1),
            last_live_ms = progress.last_live_ms.unwrap_or(-1),
            discarded_tail_tags = optional_ms(progress.discarded_tail_tags),
            discarded_tail_bytes = optional_ms(progress.discarded_tail_bytes),
            buffered = diagnostics.buffered,
            room = owner.room.as_str(),
            session = owner.session.as_str(),
            "拉流连接正常结束"
        );
        return;
    };
    let reason_code = match error {
        crate::downloader::error::Error::HttpFlvReadTimeout { .. } => "read_timeout",
        crate::downloader::error::Error::HttpFlvVideoStalled { .. } => "video_stalled",
        _ => "transport_error",
    };
    warn!(
        target: EVENT_TARGET,
        event_name = "recording.disconnected",
        outcome = "failed",
        reason_code,
        silent_ms,
        duration_ms,
        error = %error,
        http_status = diagnostics.http_status,
        received_bytes = diagnostics.received_bytes,
        edge,
        edge_switched = progress.edge_switched,
        edge_attempt_id = progress.edge_attempt.as_ref().map(|attempt| attempt.id.as_str()),
        buffered = diagnostics.buffered,
        stall_timeout_secs = diagnostics.stall_timeout.as_secs(),
        splits = progress.splits,
        since_last_split_ms = progress.since_last_split_ms(),
        first_timestamp_ms = optional_ms(progress.first_timestamp_ms),
        last_timestamp_ms = optional_ms(progress.last_timestamp_ms),
        tail_tags = progress.tail_tags,
        tail_bytes = progress.tail_bytes,
        video_frames = progress.video_frames,
        first_live_ms = progress.first_live_ms.unwrap_or(-1),
        last_live_ms = progress.last_live_ms.unwrap_or(-1),
        discarded_tail_tags = optional_ms(progress.discarded_tail_tags),
        discarded_tail_bytes = optional_ms(progress.discarded_tail_bytes),
        room = owner.room.as_str(),
        session = owner.session.as_str(),
        "拉流连接异常结束"
    );
}

fn segment_id<'b>(file: &'b LifecycleFile<'_>) -> &'b str {
    file.identity()
        .map(|identity| identity.segment_id.as_str())
        .unwrap_or("")
}

fn original_file<'b>(file: &'b LifecycleFile<'_>) -> &'b str {
    file.identity()
        .map(|identity| identity.original_file.as_str())
        .unwrap_or("")
}

#[derive(Default)]
struct FlvWriteState {
    // 当前文件还没写出任何 tag：段首的 onMetaData 要写成 0（见下方 flush）。
    file_is_empty: bool,
    // 当前文件已写出音视频帧（不算 onMetaData 与序列头）。没有帧的文件不能切出去：
    // 2026-10-27 稿件 13 每次连上都先来两个不同的 H264 序列头，`create_new` 在第一个关键帧处切段，
    // 切出只有 prelude 的 1.6KB 文件当分P投了，B 站转码报「缺少视频轨」。
    file_has_media: bool,
    // 写出前的时间戳重基。跨段保持，不在切段处复位：各段仍是「CDN 绝对时间轴 + 累计修正」。
    rebase: TimestampRebase,
    // 本连接写出的第一帧与最后一帧音视频（不含 onMetaData 与序列头）的 CDN 时间戳。
    written: Option<(u32, u32)>,
    video_frames: u32,
    first_video_ms: Option<u32>,
    last_video_ms: Option<u32>,
    live_offset: Option<i64>,
    first_basis_changed: bool,
}

impl FlvWriteState {
    fn video_written(
        &mut self,
        timestamp: u32,
        body: &[u8],
        progress: &mut FlvProgress,
        owner: &crate::downloader::util::RecordingOwner,
        pace: Option<&Pace>,
    ) {
        progress.note_reconnect_video();
        // 无 SEI 的换基准不能沿用旧 offset，宁可报告未知，也不捏造跨连接缺口。
        if self
            .last_video_ms
            .is_some_and(|last| !follows(last as i64, timestamp as i64))
        {
            self.live_offset = None;
            self.first_basis_changed |= progress.first_live_ms.is_none();
        }
        self.first_video_ms.get_or_insert(timestamp);
        self.last_video_ms = Some(timestamp);
        if let Some(live) = sei_live_ms(body) {
            self.live_offset = Some(live - timestamp as i64);
        }
        if !self.first_basis_changed
            && let Some(offset) = self.live_offset
        {
            progress
                .first_live_ms
                .get_or_insert(self.first_video_ms.unwrap() as i64 + offset);
        }
        progress.last_live_ms = self.live_offset.map(|offset| timestamp as i64 + offset);
        self.video_frames = self.video_frames.saturating_add(1);
        progress.video_frames += 1;
        if self.video_frames >= 2 {
            progress.valid_video = true;
            if progress.first_live_ms.is_some() {
                progress.report_reconnect(owner, pace);
            }
        }
    }

    fn flush(
        &mut self,
        flv_tags_cache: &mut Vec<(TagHeader, Bytes, Bytes)>,
        out: &mut FlvFile<'_>,
        segment: &mut Segmentable,
        progress: &mut FlvProgress,
        dts_rollup: &mut DtsBackwardRollup,
    ) -> crate::downloader::error::Result<()> {
        for (tag_header, flv_tag_data, previous_tag_size_bytes) in flv_tags_cache.iter() {
            let source_ms = tag_header.timestamp;
            // 段首 onMetaData 写 0：FFmpeg 只在 dts==0 时把它当元数据跳过，否则会多出一路
            // subtitle 流并报 non monotonically increasing dts；0 也不会把文件起点拉回去。
            let mapped = if self.file_is_empty && tag_header.tag_type == TagType::Script {
                Mapped {
                    emit: 0,
                    deviation: None,
                    skipped_ms: None,
                }
            } else if is_sequence_header(tag_header.tag_type, flv_tag_data) {
                Mapped {
                    emit: self.rebase.map_config(tag_header.tag_type),
                    deviation: None,
                    skipped_ms: None,
                }
            } else {
                if tag_header.tag_type != TagType::Script {
                    self.written = Some((self.written.map_or(source_ms, |w| w.0), source_ms));
                    // 走到关键帧说明流里有视频，只有音频帧的文件切出去还是没视频轨
                    // （2026-11-08 稿件 106：空 H264 头 + 一帧音频就切出 755 字节）。
                    self.file_has_media |= tag_header.tag_type == TagType::Video;
                }
                self.rebase.map(tag_header.tag_type, source_ms)
            };
            if let Some((previous_ms, reason_code)) = mapped.deviation {
                warn!(
                    "Non-monotonous DTS in output stream; previous: {previous_ms}, current: {source_ms}, written as {};",
                    mapped.emit
                );
                dts_rollup.record(
                    &out.file,
                    previous_ms,
                    source_ms as u64,
                    mapped.emit as u64,
                    reason_code,
                );
            }
            if let (Some(ms), Some(pace)) = (mapped.skipped_ms, segment.pace()) {
                pace.skip(Duration::from_millis(ms));
            }
            let mut tag_header = *tag_header;
            tag_header.timestamp = mapped.emit;
            out.write_tag(&tag_header, flv_tag_data, previous_tag_size_bytes)?;
            if tag_header.tag_type == TagType::Video
                && !is_sequence_header(tag_header.tag_type, flv_tag_data)
            {
                self.video_written(
                    source_ms,
                    flv_tag_data,
                    progress,
                    out.file.owner(),
                    segment.pace(),
                );
                if self.video_frames >= 2 {
                    if let Some(pace) = segment.pace() {
                        pace.record_ready();
                        pace.video_written();
                    }
                    if let Some(upgrade) = progress.pending_quality_upgrade.take() {
                        if let Some(pace) = segment.pace() {
                            pace.set_quality(upgrade.pace_quality);
                        }
                        let owner = out.file.owner();
                        info!(
                            target: EVENT_TARGET,
                            event_name = "recording.quality_upgraded",
                            room = owner.room.as_str(),
                            session = owner.session.as_str(),
                            from_quality = upgrade.from_quality.as_str(),
                            to_quality = upgrade.to_quality.as_str(),
                            "直播画质已升档"
                        );
                    }
                }
            }
            self.file_is_empty = false;
            segment.increase_size((11 + tag_header.data_size + 4) as u64);
            // 进度记的是 CDN 的原始时间戳：它的用途是跨连接相减算缺口，和文件里写的
            // 重基后时间轴不是一回事（见 `FlvProgress` 的字段说明）。
            if tag_header.tag_type != TagType::Script
                && !is_sequence_header(tag_header.tag_type, flv_tag_data)
            {
                progress.first_timestamp_ms.get_or_insert(source_ms as u64);
                progress.last_timestamp_ms = Some(source_ms as u64);
            }
        }
        flv_tags_cache.clear();
        Ok(())
    }
}

pub(crate) const CANDIDATE_PROBE_TIMEOUT: Duration = Duration::from_secs(8);
pub(crate) const CANDIDATE_MAX_PREFIX_BYTES: usize = 8 * 1024 * 1024;

struct ParsedFlvTag {
    tag_header_bytes: Bytes,
    tag_header: TagHeader,
    bytes: Bytes,
    previous_tag_size: Bytes,
    is_video_keyframe: bool,
    is_decodable_video: bool,
    aac_sequence_header: bool,
    h264_sequence_header: bool,
    script_tag: bool,
}

async fn read_parsed_tag(
    connection: &mut Connection,
    remaining_prefix: Option<usize>,
) -> crate::downloader::error::Result<Option<ParsedFlvTag>> {
    let tag_header_bytes = connection.read_frame(11).await?;
    if tag_header_bytes.is_empty() {
        return Ok(None);
    }

    let (_, tag_header) = map_parse_err(tag_header(&tag_header_bytes), "tag header")?;

    if remaining_prefix.is_some_and(|remaining| remaining < 11 + tag_header.data_size as usize + 4)
    {
        return Err(crate::downloader::error::Error::Custom(
            "candidate FLV prefix exceeded 8 MiB before keyframe".to_string(),
        ));
    }
    let bytes = connection.read_frame(tag_header.data_size as usize).await?;
    let previous_tag_size = connection.read_frame(4).await?;
    if bytes.len() != tag_header.data_size as usize || previous_tag_size.len() != 4 {
        return Err(crate::downloader::error::Error::HttpFlvIncompleteFrame {
            buffered: bytes.len() + previous_tag_size.len(),
        });
    }
    let (i, flv_tag_data) = map_parse_err(
        tag_data(tag_header.tag_type, tag_header.data_size as usize)(&bytes),
        "tag data",
    )?;
    let mut aac_sequence_header = false;
    let mut h264_sequence_header = false;
    let mut script_tag = false;
    let mut is_video_keyframe = false;
    let mut is_decodable_video = false;
    match flv_tag_data {
        TagData::Audio(audio_data) => {
            if audio_data.sound_format == SoundFormat::AAC {
                let (_, packet_header) = map_parse_err(
                    aac_audio_packet_header(audio_data.sound_data),
                    "aac packet header",
                )?;
                aac_sequence_header = packet_header.packet_type == AACPacketType::SequenceHeader;
            }
        }
        TagData::Video(video_data) => {
            let packet_type = if CodecId::H264 == video_data.codec_id {
                let (_, avc_video_header) = map_parse_err(
                    avc_video_packet_header(video_data.video_data),
                    "avc packet header",
                )?;
                h264_sequence_header =
                    avc_video_header.packet_type == AVCPacketType::SequenceHeader;
                Some(avc_video_header.packet_type)
            } else {
                None
            };
            is_video_keyframe = video_data.frame_type == FrameType::Key;
            is_decodable_video = matches!(packet_type, Some(AVCPacketType::NALU) | None);
        }
        TagData::Script => {
            let _ = map_parse_err(script_data(i), "script tag")?;
            script_tag = true;
        }
    }
    Ok(Some(ParsedFlvTag {
        tag_header_bytes,
        tag_header,
        bytes,
        previous_tag_size,
        is_video_keyframe,
        is_decodable_video,
        aac_sequence_header,
        h264_sequence_header,
        script_tag,
    }))
}

fn is_decodable_video_keyframe(tag: &ParsedFlvTag) -> bool {
    !tag.bytes.is_empty()
        && !is_sequence_header(tag.tag_header.tag_type, &tag.bytes)
        && tag.is_video_keyframe
        && tag.is_decodable_video
}

/// 候选流预探测：开单条 HTTP 连接、校验 FLV 头、预读至首个可解码视频关键帧，
/// 不写盘也不触发 Pace 或事件，交付原 `Connection`（9 字节 FLV 头之后可重放前缀 + 未读缓冲）。
pub(crate) async fn probe_candidate(
    client: &crate::client::StatelessClient,
    url: &str,
    stall_timeout: Duration,
) -> crate::downloader::error::Result<Connection> {
    probe_candidate_with_diagnostics(client, url, stall_timeout, &mut CandidateProbe::default())
        .await
}

#[derive(Default)]
pub(crate) struct CandidateProbe {
    pub stage: &'static str,
    pub edge: Option<std::net::IpAddr>,
    pub http_status: Option<u16>,
    pub budget_exhausted: bool,
}

pub(crate) async fn probe_candidate_with_diagnostics(
    client: &crate::client::StatelessClient,
    url: &str,
    stall_timeout: Duration,
    probe: &mut CandidateProbe,
) -> crate::downloader::error::Result<Connection> {
    probe.stage = "http";
    let result = timeout(CANDIDATE_PROBE_TIMEOUT, async {
        let response = client
            .client
            .get(url)
            .headers(client.headers.clone())
            .send()
            .await
            .map_err(|e| crate::downloader::error::Error::Custom(e.without_url().to_string()))?;
        probe.edge = response.remote_addr().map(|addr| addr.ip());
        probe.http_status = Some(response.status().as_u16());
        let response = response
            .error_for_status()
            .map_err(|e| crate::downloader::error::Error::Custom(e.without_url().to_string()))?;
        probe.stage = "flv_header";
        let mut connection = Connection::with_stall_timeout(response, stall_timeout);
        let head = connection.read_frame(9).await?;
        if crate::downloader::flv_parser::header(&head).is_err() {
            return Err(crate::downloader::error::Error::Custom(
                "candidate response is not FLV".to_string(),
            ));
        }
        let previous_tag_size_0 = connection.read_frame(4).await?;
        if previous_tag_size_0.len() != 4 {
            return Err(crate::downloader::error::Error::HttpFlvIncompleteFrame {
                buffered: previous_tag_size_0.len(),
            });
        }
        probe.stage = "keyframe";
        let mut prefix = BytesMut::with_capacity(4096);
        prefix.extend_from_slice(&previous_tag_size_0);
        let mut has_avc_config = false;
        loop {
            let remaining = CANDIDATE_MAX_PREFIX_BYTES - prefix.len();
            let Some(parsed) = read_parsed_tag(&mut connection, Some(remaining)).await? else {
                return Err(crate::downloader::error::Error::Custom(
                    "candidate FLV ended before decodable video keyframe".to_string(),
                ));
            };
            prefix.extend_from_slice(&parsed.tag_header_bytes);
            prefix.extend_from_slice(&parsed.bytes);
            prefix.extend_from_slice(&parsed.previous_tag_size);
            if parsed.h264_sequence_header {
                has_avc_config = parsed.bytes.len() > 5;
            }
            let has_video_payload = match parsed.bytes.first().map(|header| header & 0x0f) {
                Some(7) => has_avc_config && parsed.bytes.len() > 5,
                _ => parsed.bytes.len() > 1,
            };
            if is_decodable_video_keyframe(&parsed) && has_video_payload {
                connection.prepend_replay(&prefix);
                return Ok(connection);
            }
        }
    })
    .await;
    result.map_err(|_| {
        probe.budget_exhausted = true;
        crate::downloader::error::Error::Custom("candidate FLV probe timed out".to_string())
    })?
}

#[cfg(test)]
pub(crate) async fn parse_flv(
    connection: &mut Connection,
    file: LifecycleFile<'_>,
    segment: Segmentable,
    progress: &mut FlvProgress,
) -> crate::downloader::error::Result<()> {
    parse_flv_with_upgrade(connection, file, segment, progress, None).await
}

pub(crate) async fn parse_flv_with_upgrade(
    connection: &mut Connection,
    file: LifecycleFile<'_>,
    mut segment: Segmentable,
    progress: &mut FlvProgress,
    upgrade_signal: Option<&QualityUpgradeSignal>,
) -> crate::downloader::error::Result<()> {
    let mut flv_tags_cache: Vec<(TagHeader, Bytes, Bytes)> = Vec::new();
    let mut state = FlvWriteState {
        file_is_empty: true,
        ..Default::default()
    };
    let _previous_tag_size = connection.read_frame(4).await?;

    let mut out = FlvFile::flv(file)?;
    let mut dts_rollup = DtsBackwardRollup::default();
    // 首个可解码关键帧到来前，不把孤立的非关键帧当作可保留尾部。
    let mut has_keyframe = false;
    connection.video_watch = Some(VideoWatch::new());
    // 推流时刻减 CDN 时间戳：由最近一个带时刻的 SEI 得出，用来把写出的帧换算成直播的哪一刻。
    let mut live_offset: Option<i64> = None;
    let result: crate::downloader::error::Result<()> = async {
    segment.set_size_position(9 + 4);
    let mut on_meta_data = None;
    let mut aac_sequence_header = None;
    let mut h264_sequence_header: Option<(TagHeader, Bytes, Bytes)> = None;
    // 本段起点是否已锚定。曾经用「上一批写出 tag 的最后一个时间戳是否为 0」代替这个状态，
    // 但抖音重发的 timestamp=0 Script tag 会让下一个关键帧误判成「流刚初始化」，把 start
    // 推到当前，定时分段从此失效（dplei/biliup#32）。
    let mut start_anchored = false;
    let mut create_new = false;
    loop {
        let Some(parsed) = read_parsed_tag(connection, None).await? else {
            break;
        };
        let decodable_video_keyframe = is_decodable_video_keyframe(&parsed);
        let is_video_keyframe = parsed.is_video_keyframe;
        let is_decodable_video = parsed.is_decodable_video;
        let tag_header = parsed.tag_header;
        let bytes = parsed.bytes;
        let previous_tag_size = parsed.previous_tag_size;
        if parsed.aac_sequence_header {
            if aac_sequence_header.is_some() {
                warn!("Unexpected aac sequence header tag. {tag_header:?}");
            }
            aac_sequence_header = Some((tag_header, bytes.clone(), previous_tag_size.clone()));
        }
        if parsed.h264_sequence_header {
            if let Some((_, binary_data, _)) = &h264_sequence_header {
                warn!("Unexpected h264 sequence header tag. {tag_header:?}");
                if bytes != binary_data {
                    create_new = true;
                    warn!("Different h264 sequence header tag. {tag_header:?}");
                }
            }
            h264_sequence_header = Some((tag_header, bytes.clone(), previous_tag_size.clone()));
        }
        if parsed.script_tag {
            if on_meta_data.is_some() {
                warn!("Unexpected script tag. {tag_header:?}");
            }
            on_meta_data = Some((tag_header, bytes.clone(), previous_tag_size.clone()));
        }
        if tag_header.tag_type == TagType::Video
            && is_decodable_video
            && !is_sequence_header(tag_header.tag_type, &bytes)
            && (has_keyframe || is_video_keyframe)
        {
            connection.video_watch.as_mut().unwrap().progress(tag_header.timestamp);
        }
        progress.received_media |= tag_header.tag_type != TagType::Script
            && !is_sequence_header(tag_header.tag_type, &bytes);
        if tag_header.tag_type == TagType::Video
            && let Some(ts) = sei_live_ms(&bytes)
        {
            live_offset = Some(ts - tag_header.timestamp as i64);
        }
        if is_video_keyframe {
                let timestamp = tag_header.timestamp as u64;
                if !start_anchored {
                    start_anchored = true;
                    // 重连后 CDN 可能给延续的非零时间基准，首个关键帧必须成为起点，
                    // 否则它会立刻满足时间条件。
                    segment.set_start_time(Duration::from_millis(timestamp));
                }
                segment.set_time_position(Duration::from_millis(timestamp));
                state.flush(
                    &mut flv_tags_cache, &mut out, &mut segment, progress, &mut dts_rollup,
                )?;
                if let (Some(offset), Some((first, last))) =
                    (live_offset, state.written)
                    && let Some(pace) = segment.pace()
                {
                    pace.written_live(first as i64 + offset, last as i64 + offset);
                }
                // 要求停止：上一个 GOP 已写完，在这个关键帧之前收尾。
                if out.file.stopping() {
                    break;
                }
                // 换机交接切点：上一个 GOP 已写完，这个关键帧已到切点（留 100ms 容差吸收毫秒取整），
                // 不写这个关键帧，按 user_cancel 收尾。
                if let (Some(pace), Some(offset)) = (segment.pace(), live_offset)
                    && let Some(cut_live_ms) = pace.cut_live()
                    && timestamp as i64 + offset >= cut_live_ms - 100
                {
                    let end_live_ms = timestamp as i64 + offset;
                    let last_written_live_ms = progress.last_live_ms;
                    info!(
                        target: EVENT_TARGET,
                        event_name = "recording.handoff_gap",
                        outcome = if last_written_live_ms.is_some() { "measured" } else { "unknown" },
                        room = out.file.owner().room.as_str(),
                        cut_live_ms,
                        end_live_ms,
                        overlap_ms = end_live_ms - cut_live_ms,
                        last_written_live_ms = last_written_live_ms.unwrap_or(-1),
                        gap_ms = last_written_live_ms.map_or(-1, |last| (cut_live_ms - last).max(0)),
                        content_overlap_ms = last_written_live_ms.map_or(-1, |last| (last - cut_live_ms).max(0)),
                        "cut reached, closing segment"
                    );
                    out.file.set_close_reason(SegmentCloseReason::Cancelled);
                    pace.mark_cut_done();
                    break;
                }
                // 计划升档收尾：用户停录与换机切点优先，之后才在真正视频关键帧边界响应升档信号。
                if decodable_video_keyframe
                    && segment.pace().and_then(Pace::cut_live).is_none()
                    && upgrade_signal.is_some_and(QualityUpgradeSignal::try_acknowledge)
                {
                    break;
                }

                if !state.file_has_media {
                    // 新序列头已随缓存写进本文件，接着写就是，不必另开文件。
                    create_new = false;
                } else if segment.needed() || create_new || out.file.relocating() {
                    // The reason must be read before the counters are reset: reading it after
                    // `set_size_position`/`set_start_time` always saw a fresh segment and
                    // reported every configured split as `Unknown`.
                    let reason = if segment.size_needed() {
                        SegmentCloseReason::SizeSplit
                    } else if segment.time_needed() {
                        SegmentCloseReason::TimedSplit
                    } else {
                        // 转正切段也走这里：冻结的 v1 词表没有贴切的值（不是切段上限、流也没断），记 `Unknown`。
                        SegmentCloseReason::Unknown
                    };
                    segment.set_start_time(Duration::from_millis(timestamp));
                    segment.set_size_position(9 + 4);

                    // 新文件的段首 prelude：onMetaData + AAC/H264 序列头。缺哪个（纯音频/纯视频流）
                    // 就跳过哪个，不能 panic。H264 序列头变了时，新头随当前 tag 一起写，不重发旧的。
                    let h264 = h264_sequence_header.as_ref().filter(|_| !create_new);
                    for prelude in [&on_meta_data, &aac_sequence_header]
                        .into_iter()
                        .flatten()
                        .chain(h264)
                    {
                        flv_tags_cache.push(restamp(prelude, tag_header.timestamp));
                    }
                    info!("{} splitting.{segment:?}", out.file.file_name);
                    // 转正切段：这个关键帧是正式分段的第一帧，它的推流时刻就是交接切点（§6.2）。
                    // 在建新文件前记下，抢在建文件时的兜底（只知道切了、不知道时刻）之前。
                    if out.file.relocating()
                        && let Some(pace) = segment.pace()
                    {
                        pace.formal_started(live_offset.map(|o| timestamp as i64 + o));
                    }
                    // Flush before the split so the summary still names the segment it counted.
                    dts_rollup.flush(&out.file);
                    out.create_new(reason)?;
                    state.file_is_empty = true;
                    state.file_has_media = false;
                    state.video_frames = 0;
                    if !progress.valid_video {
                        state.first_video_ms = None;
                        state.first_basis_changed = false;
                        progress.first_live_ms = None;
                    }
                    progress.splits = progress.splits.saturating_add(1);
                    progress.last_split_at = Some(Instant::now());
                    create_new = false;
                }
                has_keyframe |= decodable_video_keyframe;
                flv_tags_cache.push((tag_header, bytes.clone(), previous_tag_size.clone()));
        } else {
            flv_tags_cache.push((tag_header, bytes.clone(), previous_tag_size.clone()));
        }
    }
    Ok(())
    }
    .await;
    progress.video_stalled = matches!(
        &result,
        Err(crate::downloader::error::Error::HttpFlvVideoStalled { .. })
    );
    // 网络断开只丢未读完整的 tag；磁盘写失败时不能重试同一批，避免重复写出。
    let tail_result =
        if has_keyframe && !matches!(&result, Err(crate::downloader::error::Error::IOError(_))) {
            let tags = flv_tags_cache.len() as u64;
            let bytes = flv_tags_cache
                .iter()
                .map(|(h, _, _)| 11 + h.data_size as u64 + 4)
                .sum();
            state
                .flush(
                    &mut flv_tags_cache,
                    &mut out,
                    &mut segment,
                    progress,
                    &mut dts_rollup,
                )
                .map(|()| {
                    progress.tail_tags = tags;
                    progress.tail_bytes = bytes;
                    if let (Some(offset), Some((first, last))) = (live_offset, state.written)
                        && let Some(pace) = segment.pace()
                    {
                        pace.written_live(first as i64 + offset, last as i64 + offset);
                    }
                })
        } else {
            Ok(())
        };
    if tail_result.is_ok() && !matches!(&result, Err(crate::downloader::error::Error::IOError(_))) {
        progress.discarded_tail_tags = Some(flv_tags_cache.len() as u64);
        progress.discarded_tail_bytes = Some(
            flv_tags_cache
                .iter()
                .map(|(h, _, _)| 11 + h.data_size as u64 + 4)
                .sum(),
        );
    }
    if state.video_frames < 2 {
        progress.last_live_ms = None;
    }
    progress.report_reconnect(out.file.owner(), segment.pace());
    let result = match (result, tail_result) {
        (result, Ok(())) => result,
        (Ok(()), Err(error)) => Err(error),
        (Err(error), Err(tail_error)) => Err(crate::downloader::error::Error::Custom(format!(
            "{error}; additionally failed to write cached tail: {tail_error}"
        ))),
    };
    dts_rollup.flush(&out.file);
    let upgrade_acked = upgrade_signal.is_some_and(QualityUpgradeSignal::acknowledged);
    let close_reason = if result.is_ok() {
        if upgrade_acked {
            SegmentCloseReason::Unknown
        } else {
            out.file.fallback_reason(SegmentCloseReason::StreamEnded)
        }
    } else {
        SegmentCloseReason::TransportError
    };
    let finalize_result = out.finish(close_reason);
    match (result, finalize_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Ok(()), Err(finalize_error)) => Err(finalize_error.into()),
        (Err(download_error), Ok(())) => Err(download_error),
        (Err(download_error), Err(finalize_error)) => Err(crate::downloader::error::Error::Custom(
            format!("{download_error}; additionally failed to finalize segment: {finalize_error}"),
        )),
    }
}

/// 视频 tag 里 SEI 带的推流时刻（unix 毫秒）。抖音码流约每 2 秒有一个 SEI，JSON 里的 `"ts"` 是推流端
/// 经 NTP 校准的时钟（`"tt_ntp":1`）：同一帧在哪台机器、哪个 CDN 节点收到都是同一个值，
/// 所以换机交接能据此跨机器比较录到了直播的哪一刻（2026-10-25 在云服务器 A、云服务器 B 同时拉流实测一致）。
/// 只认 AVC（7）与 HEVC（12）的 NALU 包，NALU 长度按 4 字节。
fn sei_live_ms(body: &[u8]) -> Option<i64> {
    let codec = body.first()? & 0x0f;
    if body.get(1) != Some(&1) {
        return None;
    }
    let mut rest = body.get(5..)?;
    while rest.len() > 4 {
        let len = u32::from_be_bytes(rest[..4].try_into().ok()?) as usize;
        let nal = rest.get(4..4 + len)?;
        rest = &rest[4 + len..];
        let sei = match (codec, nal.first()) {
            (7, Some(h)) => h & 0x1f == 6,
            (12, Some(h)) => matches!((h >> 1) & 0x3f, 39 | 40),
            _ => false,
        };
        if sei && let Some(ts) = sei_ts(nal) {
            return Some(ts);
        }
    }
    None
}

fn sei_ts(nal: &[u8]) -> Option<i64> {
    const KEY: &[u8] = b"\"ts\":";
    let at = nal.windows(KEY.len()).position(|w| w == KEY)? + KEY.len();
    let digits = nal[at..].iter().take_while(|b| b.is_ascii_digit()).count();
    // unix 毫秒是 13 位；别的长度不是我们认识的时刻。
    if digits != 13 {
        return None;
    }
    std::str::from_utf8(&nal[at..at + digits])
        .ok()?
        .parse()
        .ok()
}

pub fn map_parse_err<'a, T>(
    i_result: IResult<&'a [u8], T>,
    msg: &str,
) -> core::result::Result<(&'a [u8], T), crate::downloader::error::Error> {
    match i_result {
        Ok((i, res)) => Ok((i, res)),
        Err(nom::Err::Incomplete(needed)) => Err(crate::downloader::error::Error::NomIncomplete(
            msg.to_string(),
            needed,
        )),
        Err(Err::Error(e)) => Err(crate::downloader::error::Error::Custom(format!(
            "parse {msg} err: {e:?}"
        ))),
        Err(Err::Failure(f)) => Err(crate::downloader::error::Error::Custom(format!(
            "{msg} Failure: {f:?}"
        ))),
    }
}

/// 码流停顿看门狗：连续这么久一个字节都没收到就判断连。每个 chunk 重置，不是连接总时长。
pub const DEFAULT_STALL_TIMEOUT: Duration = Duration::from_secs(30);

struct VideoWatch {
    data_at: tokio::time::Instant,
    active: Duration,
    active_at_progress: Duration,
    timestamp: Option<u32>,
    pending: Option<u32>,
}

impl VideoWatch {
    fn new() -> Self {
        Self {
            data_at: tokio::time::Instant::now(),
            active: Duration::ZERO,
            active_at_progress: Duration::ZERO,
            timestamp: None,
            pending: None,
        }
    }

    fn data(&mut self) {
        let gap = self.data_at.elapsed();
        // 长空档封顶，既不把整段静默算成收包，也不让稀疏音频永远绕过检测。
        self.active += gap.min(crate::downloader::util::DATA_GAP);
        self.data_at = tokio::time::Instant::now();
    }

    fn progress(&mut self, timestamp: u32) {
        let advancing = match self.timestamp {
            None => true,
            Some(last) if timestamp > last => true,
            // 回退后的第二个前进样本才确认新基准；反复重发相同时间戳不能续命。
            Some(last) => self
                .pending
                .is_some_and(|pending| timestamp > pending && timestamp < last),
        };
        if advancing {
            self.timestamp = Some(timestamp);
            self.pending = None;
            self.active_at_progress = self.active;
        } else {
            self.pending = Some(timestamp);
        }
    }

    fn stalled_for(&self, timeout: Duration) -> Option<Duration> {
        let elapsed = self.active.saturating_sub(self.active_at_progress);
        (elapsed >= timeout && self.data_at.elapsed() <= crate::downloader::util::DATA_GAP)
            .then_some(elapsed)
    }
}

pub struct Connection {
    resp: Response,
    buffer: BytesMut,
    http_status: u16,
    received_bytes: u64,
    started_at: Instant,
    /// 最后一次成功收到 chunk 的时刻；构造时等于 `started_at`
    last_chunk_at: Instant,
    stall_timeout: Duration,
    /// 每收到一块就打点：录制倍率只算持续收数据的时段（见 `PaceCount::active`）。
    pace: Option<std::sync::Arc<crate::downloader::util::Pace>>,
    /// 实际连上的节点（跟完 302 之后），见 [`ConnectionDiagnostics::edge`]。
    edge: Option<std::net::SocketAddr>,
    video_watch: Option<VideoWatch>,
}

#[derive(Debug, Clone)]
pub struct ConnectionDiagnostics {
    pub http_status: u16,
    pub received_bytes: u64,
    pub connected_for: Duration,
    /// 上游最后一个字节到现在的静默时长。缺口的大头在这一段里，
    /// 而不是在报错之后的重连里。
    pub silent_for: Duration,
    pub stall_timeout: Duration,
    pub buffered: usize,
    /// 实际连上的 CDN 边缘节点。抖音火山 CDN 每次连接都由 302 重新分派节点，记下它才分得清
    /// 一阵变慢是同一个坏节点还是本机到这批节点的线路（2026-11-03 某房间 云服务器 A）。
    pub edge: Option<std::net::SocketAddr>,
}

impl Connection {
    pub fn new(resp: Response) -> Connection {
        Connection::with_stall_timeout(resp, DEFAULT_STALL_TIMEOUT)
    }

    pub fn with_stall_timeout(resp: Response, stall_timeout: Duration) -> Connection {
        let http_status = resp.status().as_u16();
        let edge = resp.remote_addr();
        let started_at = Instant::now();
        Connection {
            resp,
            buffer: BytesMut::with_capacity(8 * 1024),
            http_status,
            received_bytes: 0,
            started_at,
            last_chunk_at: started_at,
            stall_timeout,
            pace: None,
            edge,
            video_watch: None,
        }
    }

    pub fn with_pace(
        mut self,
        pace: Option<std::sync::Arc<crate::downloader::util::Pace>>,
    ) -> Self {
        self.pace = pace;
        self
    }

    pub(crate) fn set_pace(&mut self, pace: Option<std::sync::Arc<crate::downloader::util::Pace>>) {
        self.pace = pace;
    }

    pub(crate) fn started_at(&self) -> Instant {
        self.started_at
    }

    pub(crate) fn prepend_replay(&mut self, prefix: &[u8]) {
        if prefix.is_empty() {
            return;
        }
        let mut combined = BytesMut::with_capacity(prefix.len() + self.buffer.len());
        combined.extend_from_slice(prefix);
        combined.extend_from_slice(&self.buffer);
        self.buffer = combined;
    }

    pub fn diagnostics(&self) -> ConnectionDiagnostics {
        // 两个时长取同一个「现在」，否则数据刚连上就到时，静默时长可能比连接时长还长。
        let now = Instant::now();
        ConnectionDiagnostics {
            http_status: self.http_status,
            received_bytes: self.received_bytes,
            connected_for: now - self.started_at,
            silent_for: now - self.last_chunk_at,
            stall_timeout: self.stall_timeout,
            buffered: self.buffer.len(),
            edge: self.edge,
        }
    }

    pub async fn read_frame(
        &mut self,
        chunk_size: usize,
    ) -> crate::downloader::error::Result<Bytes> {
        loop {
            if chunk_size <= self.buffer.len() {
                let bytes = Bytes::copy_from_slice(&self.buffer[..chunk_size]);
                self.buffer.advance(chunk_size);
                return Ok(bytes);
            }
            if let Some(stalled) = self
                .video_watch
                .as_ref()
                .and_then(|watch| watch.stalled_for(self.stall_timeout))
            {
                return Err(crate::downloader::error::Error::HttpFlvVideoStalled {
                    stalled_ms: stalled.as_millis() as u64,
                });
            }
            match timeout(self.stall_timeout, self.resp.chunk()).await {
                Ok(Ok(Some(chunk))) => {
                    self.received_bytes = self.received_bytes.saturating_add(chunk.len() as u64);
                    self.last_chunk_at = Instant::now();
                    if !chunk.is_empty()
                        && let Some(watch) = &mut self.video_watch
                    {
                        watch.data();
                    }
                    if let Some(pace) = &self.pace {
                        pace.data();
                    }
                    self.buffer.put(chunk);
                }
                Ok(Ok(None)) => {
                    let buffered = self.buffer.len();
                    if buffered == 0 {
                        return Ok(self.buffer.split().freeze());
                    }
                    warn!(
                        buffered,
                        "httpflv chunk stream ended before requested frame was complete"
                    );
                    return Err(crate::downloader::error::Error::HttpFlvIncompleteFrame {
                        buffered,
                    });
                }
                Ok(Err(err)) => {
                    let err = err.without_url();
                    warn!(error = ?err, buffered = self.buffer.len(), "httpflv chunk read failed");
                    return Err(err.into());
                }
                Err(err) => {
                    let buffered = self.buffer.len();
                    warn!(
                        error = %err,
                        buffered,
                        stall_timeout_secs = self.stall_timeout.as_secs(),
                        connected_ms = self.started_at.elapsed().as_millis() as u64,
                        "httpflv chunk read timed out"
                    );
                    return Err(crate::downloader::error::Error::HttpFlvReadTimeout { buffered });
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Connection, DEFAULT_STALL_TIMEOUT, DtsBackwardRollup};
    use crate::downloader::util::LifecycleFile;
    use bytes::{Buf, BufMut, Bytes, BytesMut};
    use futures::StreamExt;
    use std::cell::RefCell;
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex, Once};
    use std::time::Duration;
    use tracing::field::{Field, Visit};
    use tracing_subscriber::Layer;
    use tracing_subscriber::layer::SubscriberExt;

    type EventFields = BTreeMap<String, String>;

    thread_local! {
        static CAPTURE: RefCell<Option<Vec<EventFields>>> = const { RefCell::new(None) };
    }

    struct Captured;

    impl Captured {
        fn start() -> Self {
            // 全局订阅者避免别的线程首次打日志时把该日志点缓存为禁用。
            static INSTALL: Once = Once::new();
            INSTALL.call_once(|| {
                tracing::subscriber::set_global_default(tracing_subscriber::registry().with(Self))
                    .unwrap();
            });
            CAPTURE.set(Some(Vec::new()));
            Self
        }

        fn finish(self) -> Vec<EventFields> {
            CAPTURE.take().unwrap()
        }
    }

    struct Collector<'a>(&'a mut BTreeMap<String, String>);
    impl Visit for Collector<'_> {
        fn record_str(&mut self, field: &Field, value: &str) {
            self.0.insert(field.name().to_string(), value.to_string());
        }
        fn record_u64(&mut self, field: &Field, value: u64) {
            self.0.insert(field.name().to_string(), value.to_string());
        }
        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            self.0
                .insert(field.name().to_string(), format!("{value:?}"));
        }
    }

    impl<S: tracing::Subscriber> Layer<S> for Captured {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _: tracing_subscriber::layer::Context<'_, S>,
        ) {
            if event.metadata().target() != super::EVENT_TARGET {
                return;
            }
            CAPTURE.with_borrow_mut(|capture| {
                if let Some(events) = capture {
                    let mut fields = BTreeMap::new();
                    event.record(&mut Collector(&mut fields));
                    events.push(fields);
                }
            });
        }
    }

    #[test]
    fn captures_event_when_another_thread_hits_the_callsite_first() {
        fn emit() {
            tracing::warn!(target: super::EVENT_TARGET, event_name = "capture.probe");
        }

        let captured = Captured::start();
        std::thread::spawn(emit).join().unwrap();
        emit();

        let events = captured.finish();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["event_name"], "capture.probe");
    }

    fn append_tag(bytes: &mut Vec<u8>, tag_type: u8, body: &[u8], timestamp: u32) {
        bytes.push(tag_type);
        bytes.extend_from_slice(&[
            ((body.len() >> 16) & 0xff) as u8,
            ((body.len() >> 8) & 0xff) as u8,
            (body.len() & 0xff) as u8,
            ((timestamp >> 16) & 0xff) as u8,
            ((timestamp >> 8) & 0xff) as u8,
            (timestamp & 0xff) as u8,
            ((timestamp >> 24) & 0xff) as u8,
            0,
            0,
            0,
        ]);
        bytes.extend_from_slice(body);
        bytes.extend_from_slice(&((11 + body.len()) as u32).to_be_bytes());
    }

    /// A complete stream: metadata, both sequence headers, then keyframes carrying real payload.
    fn splittable_flv(keyframes: usize, payload: usize) -> Vec<u8> {
        let mut bytes = vec![b'F', b'L', b'V', 1, 5, 0, 0, 0, 9, 0, 0, 0, 0];
        let mut metadata = vec![0x02, 0x00, 0x0a];
        metadata.extend_from_slice(b"onMetaData");
        metadata.push(0x05); // AMF null: enough to be a valid onMetaData tag
        append_tag(&mut bytes, 18, &metadata, 0);
        append_tag(&mut bytes, 8, &[0xaf, 0x00, 0x12, 0x10], 0);
        append_tag(&mut bytes, 9, &[0x17, 0x00, 0, 0, 0, 0x01, 0x64, 0x00], 0);
        for index in 0..keyframes {
            let mut frame = vec![0x17, 0x01, 0, 0, 0];
            frame.resize(5 + payload, 0x41);
            append_tag(&mut bytes, 9, &frame, (index as u32 + 1) * 1_000);
        }
        bytes
    }

    /// 同一条流里反复重发 timestamp=0 的 onMetaData——抖音 CDN 的实际行为。
    /// 每个关键帧前插一个，保证下一个关键帧看到的 `prev_timestamp` 是 0。
    fn flv_with_repeated_metadata(keyframes: usize, payload: usize) -> Vec<u8> {
        let mut bytes = vec![b'F', b'L', b'V', 1, 5, 0, 0, 0, 9, 0, 0, 0, 0];
        let mut metadata = vec![0x02, 0x00, 0x0a];
        metadata.extend_from_slice(b"onMetaData");
        metadata.push(0x05);
        append_tag(&mut bytes, 18, &metadata, 0);
        append_tag(&mut bytes, 8, &[0xaf, 0x00, 0x12, 0x10], 0);
        append_tag(&mut bytes, 9, &[0x17, 0x00, 0, 0, 0, 0x01, 0x64, 0x00], 0);
        for index in 0..keyframes {
            append_tag(&mut bytes, 18, &metadata, 0);
            let mut frame = vec![0x17, 0x01, 0, 0, 0];
            frame.resize(5 + payload, 0x41);
            append_tag(&mut bytes, 9, &frame, (index as u32 + 1) * 1_000);
        }
        bytes
    }

    /// 重连之后的抖音 FLV：正常帧带着绝对媒体时钟（约 9 小时），CDN 在其间反复重发
    /// timestamp=0 的**关键帧**（#32 那条覆盖的是重发 Script tag，两者路径不同）。
    fn flv_with_absolute_base_and_zero_keyframes(keyframes: usize, payload: usize) -> Vec<u8> {
        const BASE: u32 = 32_891_256;
        let mut bytes = vec![b'F', b'L', b'V', 1, 5, 0, 0, 0, 9, 0, 0, 0, 0];
        let mut metadata = vec![0x02, 0x00, 0x0a];
        metadata.extend_from_slice(b"onMetaData");
        metadata.push(0x05);
        append_tag(&mut bytes, 18, &metadata, 0);
        append_tag(&mut bytes, 8, &[0xaf, 0x00, 0x12, 0x10], 0);
        append_tag(&mut bytes, 9, &[0x17, 0x00, 0, 0, 0, 0x01, 0x64, 0x00], 0);
        for index in 0..keyframes {
            // CDN 重发的初始化关键帧：时间戳 0，负载只有几个字节
            append_tag(&mut bytes, 9, &[0x17, 0x01, 0, 0, 0, 0x41], 0);
            let mut frame = vec![0x17, 0x01, 0, 0, 0];
            frame.resize(5 + payload, 0x41);
            append_tag(&mut bytes, 9, &frame, BASE + (index as u32 + 1) * 1_000);
        }
        bytes
    }

    fn complete_response(body: Vec<u8>) -> reqwest::Response {
        reqwest::Response::from(http::Response::new(reqwest::Body::from(body)))
    }

    #[tokio::test(start_paused = true)]
    async fn receiving_audio_or_frozen_video_does_not_hide_a_video_stall() {
        for mode in ["audio", "frozen", "no_key", "long_gop", "rewind"] {
            let directory = tempfile::tempdir().unwrap();
            let template = directory.path().join("watchdog").display().to_string();
            let mut chunks = std::collections::VecDeque::from([Bytes::from(splittable_flv(
                usize::from(mode != "no_key"),
                32,
            ))]);
            for second in 1..=12 {
                let mut bytes = Vec::new();
                append_tag(&mut bytes, 8, &[0xaf, 1, 0x41], second * 1_000);
                if !matches!(mode, "audio" | "no_key") {
                    let timestamp = match mode {
                        "frozen" => 1_000,
                        "rewind" if second >= 6 => (second - 6) * 1_000,
                        _ => (second + 1) * 1_000,
                    };
                    append_tag(&mut bytes, 9, &[0x27, 1, 0, 0, 0, 0x41], timestamp);
                }
                chunks.push_back(Bytes::from(bytes));
            }
            let stream = futures::stream::unfold(chunks, |mut chunks| async move {
                let chunk = chunks.pop_front()?;
                tokio::time::sleep(Duration::from_secs(1)).await;
                Some((Ok::<_, std::io::Error>(chunk), chunks))
            });
            let response =
                reqwest::Response::from(http::Response::new(reqwest::Body::wrap_stream(stream)));
            let mut connection = Connection::with_stall_timeout(response, Duration::from_secs(5));
            connection.read_frame(9).await.unwrap();
            let file = LifecycleFile::new(&template, "flv");
            let result = tokio::time::timeout(
                Duration::from_secs(20),
                super::download(&mut connection, file, Default::default()),
            )
            .await
            .unwrap();
            assert_eq!(
                result.is_err(),
                matches!(mode, "audio" | "frozen" | "no_key"),
                "{mode}: {result:?}"
            );
            if let Err(error) = result {
                assert!(error.to_string().contains("video"), "{error}");
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_source_pause_does_not_count_as_receiving_without_video() {
        let mut watch = super::VideoWatch::new();
        watch.progress(1_000);
        for _ in 0..10 {
            tokio::time::advance(Duration::from_secs(1)).await;
            watch.data();
        }
        tokio::time::advance(Duration::from_secs(20)).await;
        watch.data();
        assert!(watch.stalled_for(Duration::from_secs(30)).is_none());
        watch.progress(2_000);
        assert!(watch.stalled_for(Duration::from_secs(30)).is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn sparse_audio_chunks_cannot_keep_a_video_connection_alive_forever() {
        let mut watch = super::VideoWatch::new();
        watch.progress(1_000);
        for _ in 0..2 {
            tokio::time::advance(Duration::from_secs(16)).await;
            watch.data();
        }
        assert!(watch.stalled_for(Duration::from_secs(30)).is_some());
    }

    #[tokio::test]
    async fn a_disconnected_stream_keeps_complete_tags_in_the_last_gop() {
        for ending in [
            "eof",
            "reset",
            "torn_body",
            "missing_body",
            "torn_trailer",
            "missing_trailer",
        ] {
            let directory = tempfile::tempdir().unwrap();
            let template = directory.path().join("tail").display().to_string();
            let mut bytes = splittable_flv(1, 32);
            for timestamp in [1_040, 1_080] {
                append_tag(&mut bytes, 9, &[0x27, 1, 0, 0, 0, 0x41], timestamp);
            }
            if ending != "eof" && ending != "reset" {
                append_tag(&mut bytes, 9, &[0x27, 1, 0, 0, 0, 0x41], 1_120);
                let remove = match ending {
                    "torn_body" => 5,
                    "missing_body" => 10,
                    "torn_trailer" => 2,
                    "missing_trailer" => 4,
                    _ => unreachable!(),
                };
                bytes.truncate(bytes.len() - remove);
            }
            let response = if ending == "reset" {
                let chunks = futures::stream::iter([
                    Ok(Bytes::from(bytes)),
                    Err(std::io::Error::from(std::io::ErrorKind::ConnectionReset)),
                ]);
                reqwest::Response::from(http::Response::new(reqwest::Body::wrap_stream(chunks)))
            } else {
                complete_response(bytes)
            };
            let mut connection = Connection::new(response);
            connection.read_frame(9).await.unwrap();
            let file = crate::downloader::util::LifecycleFile::new(&template, "flv");
            let mut progress = super::FlvProgress::default();
            let pace = Arc::new(crate::downloader::util::Pace::default());
            pace.record_failure(false);
            pace.record_failure(false);
            let segment =
                crate::downloader::util::Segmentable::default().with_pace(Some(pace.clone()));
            let result = super::parse_flv(&mut connection, file, segment, &mut progress).await;
            assert_eq!(result.is_ok(), ending == "eof", "{ending}: {result:?}");
            let timestamps: Vec<_> = written_tags(&directory.path().join("tail.flv"))
                .into_iter()
                .filter(|(kind, _, config)| *kind == 9 && !*config)
                .map(|(_, timestamp, _)| timestamp)
                .collect();
            assert_eq!(timestamps, [1_000, 1_040, 1_080], "{ending}");
            assert_eq!(progress.first_timestamp_ms, Some(1_000), "{ending}");
            assert_eq!(progress.last_timestamp_ms, Some(1_080), "{ending}");
            assert!(pace.recording_ready());
            assert_eq!(pace.record_failures(), (0, false));
            assert_eq!(progress.tail_tags, 3, "{ending}");
            assert_eq!(
                progress.tail_bytes,
                2 * (11 + 6 + 4) + 11 + 37 + 4,
                "{ending}"
            );
        }
    }

    #[tokio::test]
    async fn a_tail_without_a_video_keyframe_is_not_written() {
        let directory = tempfile::tempdir().unwrap();
        let template = directory.path().join("no-key").display().to_string();
        let mut bytes = splittable_flv(0, 0);
        for timestamp in [1_000, 1_040] {
            append_tag(&mut bytes, 9, &[0x27, 1, 0, 0, 0, 0x41], timestamp);
        }
        let mut connection = Connection::new(complete_response(bytes));
        connection.read_frame(9).await.unwrap();
        let file = crate::downloader::util::LifecycleFile::new(&template, "flv");
        let mut progress = super::FlvProgress::default();
        super::parse_flv(&mut connection, file, Default::default(), &mut progress)
            .await
            .unwrap();
        assert!(
            written_tags(&directory.path().join("no-key.flv"))
                .iter()
                .all(|(kind, _, config)| *kind != 9 || *config)
        );
        assert_eq!(progress.tail_tags, 0);
        assert_eq!(progress.discarded_tail_tags, Some(3));
        assert_eq!(progress.discarded_tail_bytes, Some(65));
        assert_eq!(progress.first_timestamp_ms, None);
        assert_eq!(progress.last_timestamp_ms, None);
    }

    #[tokio::test]
    async fn disconnect_logs_report_media_progress_and_saved_tail() {
        for reset in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let template = directory.path().join("logged").display().to_string();
            let captured = Captured::start();
            let bytes = splittable_flv(2, 32);
            let response = if reset {
                let chunks = futures::stream::iter([
                    Ok(Bytes::from(bytes)),
                    Err(std::io::Error::from(std::io::ErrorKind::ConnectionReset)),
                ]);
                reqwest::Response::from(http::Response::new(reqwest::Body::wrap_stream(chunks)))
            } else {
                complete_response(bytes)
            };
            let mut connection = Connection::new(response);
            connection.read_frame(9).await.unwrap();
            let file = crate::downloader::util::LifecycleFile::new(&template, "flv");
            let result = super::download(&mut connection, file, Default::default()).await;
            assert_eq!(result.is_err(), reset);
            let events = captured.finish();
            let event = events
                .iter()
                .find(|e| e.get("event_name").map(String::as_str) == Some("recording.disconnected"))
                .unwrap();
            assert_eq!(event["first_timestamp_ms"], "1000");
            assert_eq!(event["last_timestamp_ms"], "2000");
            assert_eq!(event["tail_tags"], "1");
            assert_eq!(event["tail_bytes"], "52");
            assert_eq!(
                event["reason_code"],
                if reset {
                    "transport_error"
                } else {
                    "stream_end"
                }
            );
        }
    }

    #[tokio::test]
    async fn reconnect_gaps_use_sei_and_missing_sei_is_unknown() {
        const LIVE: i64 = 1_792_908_000_000;
        for (previous, sei_from, source_jump, expected_gap, expected_overlap) in [
            (Some(LIVE - 31_000), Some(0), false, "31000", "0"),
            (Some(LIVE + 2_000), Some(0), false, "0", "2000"),
            (Some(LIVE - 31_000), None, false, "-1", "-1"),
            (None, Some(0), false, "-1", "-1"),
            (Some(LIVE - 31_000), Some(1), false, "31000", "0"),
            (Some(LIVE - 31_000), Some(1), true, "-1", "-1"),
        ] {
            let directory = tempfile::tempdir().unwrap();
            let template = directory.path().join("gap").display().to_string();
            let captured = Captured::start();
            let mut bytes = splittable_flv(0, 0);
            for index in 0..3u32 {
                let mut frame = vec![0x17, 1, 0, 0, 0];
                if sei_from.is_some_and(|first| index >= first) {
                    let nal = format!("\x06\x05{{\"ts\":{}}}", LIVE + index as i64 * 1_000);
                    frame.extend_from_slice(&(nal.len() as u32).to_be_bytes());
                    frame.extend_from_slice(nal.as_bytes());
                }
                frame.extend_from_slice(&[0, 0, 0, 2, 0x65, 0x88]);
                let source = index * 1_000
                    + if source_jump && index > 0 {
                        1_000_000
                    } else {
                        0
                    };
                append_tag(&mut bytes, 9, &frame, source);
            }
            let mut connection = Connection::new(complete_response(bytes));
            connection.read_frame(9).await.unwrap();
            let mut progress = super::FlvProgress {
                reconnecting: true,
                previous_live_ms: previous,
                edge_attempt: Some(super::EdgeAttempt {
                    id: "test-attempt".to_string(),
                    previous_edge: None,
                    previous_ended: std::time::Instant::now(),
                    first_video_wall_ms: None,
                }),
                ..Default::default()
            };
            let pace = std::sync::Arc::new(crate::downloader::util::Pace::default());
            super::download_with_progress(
                &mut connection,
                LifecycleFile::new(&template, "flv"),
                crate::downloader::util::Segmentable::default().with_pace(Some(pace.clone())),
                &mut progress,
            )
            .await
            .unwrap();
            // 测得的接缝原样计入健康检测（重叠记 0），无法测量的不计。
            let counted: Vec<_> = pace.recent_gaps(0).into_iter().map(|(_, ms)| ms).collect();
            let expected: Vec<u64> = expected_gap.parse().into_iter().collect();
            assert_eq!(counted, expected, "gap={expected_gap}");
            let events = captured.finish();
            let gaps: Vec<_> = events
                .iter()
                .filter(|e| {
                    e.get("event_name").map(String::as_str) == Some("recording.reconnect_gap")
                })
                .collect();
            assert_eq!(gaps.len(), 1);
            assert_eq!(gaps[0]["gap_ms"], expected_gap);
            assert_eq!(gaps[0]["overlap_ms"], expected_overlap);
            let disconnected = events
                .iter()
                .find(|event| event["event_name"] == "recording.disconnected")
                .unwrap();
            assert_eq!(gaps[0]["edge_attempt_id"], "test-attempt");
            assert_eq!(disconnected["edge_attempt_id"], gaps[0]["edge_attempt_id"]);
            assert!(gaps[0]["first_video_wall_ms"].parse::<u64>().is_ok());
        }
    }

    /// The close reason used to be read after the counters had already been reset, so every
    /// configured split reported `Unknown`. The recorded reason must name the limit that fired.
    #[tokio::test]
    async fn a_size_split_closes_the_segment_with_split_limit() {
        let directory = tempfile::tempdir().unwrap();
        let template = directory.path().join("split").display().to_string();
        let captured = Captured::start();

        let mut connection = Connection::new(complete_response(splittable_flv(6, 2_000)));
        // The caller consumes the 9 byte FLV header before handing the stream to the parser.
        connection.read_frame(9).await.unwrap();
        let file = crate::downloader::util::LifecycleFile::new(&template, "flv");
        let segment = crate::downloader::util::Segmentable::new(None, Some(4_000));
        let mut progress = super::FlvProgress::default();
        super::parse_flv(&mut connection, file, segment, &mut progress)
            .await
            .unwrap();

        let events = captured.finish();
        let closes: Vec<_> = events
            .iter()
            .filter(|fields| {
                fields.get("event_name").map(String::as_str) == Some("recording.segment_closed")
            })
            .collect();
        assert!(
            progress.splits >= 2,
            "the fixture must split more than once"
        );
        assert_eq!(
            closes.len() as u32,
            progress.splits + 1,
            "one close per file"
        );
        let reasons: Vec<_> = closes
            .iter()
            .map(|fields| fields["reason_code"].as_str())
            .collect();
        assert!(
            reasons
                .iter()
                .filter(|reason| **reason == "split_limit")
                .count()
                >= 2,
            "configured splits must be reported as split_limit: {reasons:?}"
        );
        assert_eq!(reasons.last(), Some(&"stream_end"));
    }

    /// 抖音 SEI 里的推流时刻换算成「写到了直播的哪一刻」：起点取第一帧，落后量按最后写出的那帧算；
    /// SEI 每隔几帧才有一个，中间的帧按 CDN 时间戳推算。
    #[tokio::test]
    async fn sei_timestamps_tell_which_live_moment_was_written() {
        const LIVE0: i64 = 1_792_908_000_000;
        let mut bytes = vec![b'F', b'L', b'V', 1, 5, 0, 0, 0, 9, 0, 0, 0, 0];
        append_tag(&mut bytes, 9, &[0x17, 0x00, 0, 0, 0, 0x01, 0x64, 0x00], 0);
        for index in 0..6u32 {
            let ts = 50_000 + index * 1_000;
            let mut frame = vec![0x17, 0x01, 0, 0, 0];
            if index % 2 == 1 {
                let sei = format!(
                    "\x06\x05{{\"heart_beat\":{{\"ts\":{}}},\"tt_ntp\":1}}",
                    LIVE0 + ts as i64
                );
                frame.extend_from_slice(&(sei.len() as u32).to_be_bytes());
                frame.extend_from_slice(sei.as_bytes());
            }
            frame.extend_from_slice(&[0, 0, 0, 2, 0x65, 0x88]);
            append_tag(&mut bytes, 9, &frame, ts);
        }
        let directory = tempfile::tempdir().unwrap();
        let template = directory.path().join("sei").display().to_string();
        let mut connection = Connection::new(complete_response(bytes));
        connection.read_frame(9).await.unwrap();
        let pace = std::sync::Arc::new(crate::downloader::util::Pace::default());
        let _running = pace.running();
        let file = crate::downloader::util::LifecycleFile::new(&template, "flv");
        let segment =
            crate::downloader::util::Segmentable::new(None, None).with_pace(Some(pace.clone()));
        let mut progress = super::FlvProgress::default();
        super::parse_flv(&mut connection, file, segment, &mut progress)
            .await
            .unwrap();

        let count = pace.read();
        assert_eq!(count.first_live_ms, Some(LIVE0 + 50_000));
        // 收尾补写最后一个 GOP 后，进度必须推进到最后一帧（55 秒处）。
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let lag = count.live_lag_ms.unwrap();
        assert!((lag - (now - LIVE0 - 55_000)).abs() < 1_000, "{lag}");

        assert_eq!(
            super::sei_live_ms(&[0x17, 0x00, 0, 0, 0, 0x01]),
            None,
            "序列头不看"
        );
        assert_eq!(
            super::sei_live_ms(&[0x17, 0x01, 0, 0, 0, 0, 0, 0, 2, 0x65, 0x88]),
            None
        );
    }

    /// 录制器要求停止（换机交接）时，写盘循环在下一个关键帧收尾、不再切段，分段标为 user_cancel。
    #[tokio::test]
    async fn a_stop_request_closes_at_the_next_keyframe_as_cancelled() {
        let directory = tempfile::tempdir().unwrap();
        let template = directory.path().join("stop").display().to_string();
        let mut connection = Connection::new(complete_response(splittable_flv(6, 2_000)));
        connection.read_frame(9).await.unwrap();
        let handle = crate::downloader::util::SegmentCloseHandle::default();
        let closed = Arc::new(Mutex::new(Vec::new()));
        let sink = closed.clone();
        let file = crate::downloader::util::LifecycleFile::with_hook_and_close_handle(
            &template,
            "flv",
            handle.clone(),
            move |event| sink.lock().unwrap().push(event.reason),
        );
        handle.set(super::SegmentCloseReason::Cancelled);
        let segment = crate::downloader::util::Segmentable::new(None, Some(4_000));
        let mut progress = super::FlvProgress::default();
        super::parse_flv(&mut connection, file, segment, &mut progress)
            .await
            .unwrap();
        assert_eq!(progress.splits, 0, "停止后不该再切段");
        assert_eq!(
            *closed.lock().unwrap(),
            [super::SegmentCloseReason::Cancelled]
        );
    }

    /// 换机交接下发切点后，在推流时刻 ≥ 切点（含 100ms 容差）的第一个关键帧前收尾：上一个 GOP 写完、
    /// 该关键帧不写、分段标为 user_cancel、Pace 置已切标记；码流没有 SEI 时不切。
    #[tokio::test]
    async fn cut_live_closes_before_the_matching_keyframe_and_ignores_streams_without_sei() {
        const LIVE0: i64 = 1_792_908_000_000;
        let captured = Captured::start();
        let mut bytes = vec![b'F', b'L', b'V', 1, 5, 0, 0, 0, 9, 0, 0, 0, 0];
        append_tag(&mut bytes, 9, &[0x17, 0x00, 0, 0, 0, 0x01, 0x64, 0x00], 0);
        for index in 0..5u32 {
            let ts = 50_000 + index * 1_000;
            let mut key = vec![0x17, 0x01, 0, 0, 0];
            let sei = format!(
                "\x06\x05{{\"heart_beat\":{{\"ts\":{}}},\"tt_ntp\":1}}",
                LIVE0 + ts as i64
            );
            key.extend_from_slice(&(sei.len() as u32).to_be_bytes());
            key.extend_from_slice(sei.as_bytes());
            key.extend_from_slice(&[0, 0, 0, 2, 0x65, 0x88]);
            append_tag(&mut bytes, 9, &key, ts);
            // 每个 GOP 里再带一帧非关键帧，确认切点之前的整段 GOP 都写进了文件。
            append_tag(
                &mut bytes,
                9,
                &[0x27, 0x01, 0, 0, 0, 0, 0, 0, 2, 0x41, 0x9a],
                ts + 500,
            );
        }

        let directory = tempfile::tempdir().unwrap();
        let template = directory.path().join("cut").display().to_string();
        let mut connection = Connection::new(complete_response(bytes));
        connection.read_frame(9).await.unwrap();
        let pace = Arc::new(crate::downloader::util::Pace::default());
        // 切点设在第三个关键帧（52_000）：前两个 GOP（50_000..=51_500）写盘，52_000 不写。
        pace.set_cut_live(LIVE0 + 52_000);
        let closed = Arc::new(Mutex::new(Vec::new()));
        let sink = closed.clone();
        let file =
            crate::downloader::util::LifecycleFile::with_hook(&template, "flv", move |event| {
                sink.lock().unwrap().push(event.reason)
            });
        let segment =
            crate::downloader::util::Segmentable::new(None, None).with_pace(Some(pace.clone()));
        let mut progress = super::FlvProgress::default();
        super::parse_flv(&mut connection, file, segment, &mut progress)
            .await
            .unwrap();

        assert!(pace.cut_done());
        assert_eq!(
            *closed.lock().unwrap(),
            [super::SegmentCloseReason::Cancelled]
        );
        let media_ts: Vec<u32> = written_tags(&directory.path().join("cut.flv"))
            .into_iter()
            .filter(|(t, _, config)| *t == 9 && !*config)
            .map(|(_, ts, _)| ts)
            .collect();
        assert_eq!(media_ts, [50_000, 50_500, 51_000, 51_500]);
        assert!(!media_ts.contains(&52_000));
        {
            let events = captured.finish();
            let gap = events
                .iter()
                .find(|e| e.get("event_name").map(String::as_str) == Some("recording.handoff_gap"))
                .unwrap();
            assert_eq!(gap["last_written_live_ms"], (LIVE0 + 51_500).to_string());
            assert_eq!(gap["gap_ms"], "500");
            assert_eq!(gap["content_overlap_ms"], "0");
        }

        // 没有 SEI 的流：即使设了切点也不切，照常录到流尾。
        let no_sei_tpl = directory.path().join("no-sei").display().to_string();
        let mut conn2 = Connection::new(complete_response(splittable_flv(4, 200)));
        conn2.read_frame(9).await.unwrap();
        let pace2 = Arc::new(crate::downloader::util::Pace::default());
        pace2.set_cut_live(1);
        let closed2 = Arc::new(Mutex::new(Vec::new()));
        let sink2 = closed2.clone();
        let file2 =
            crate::downloader::util::LifecycleFile::with_hook(&no_sei_tpl, "flv", move |event| {
                sink2.lock().unwrap().push(event.reason)
            });
        let seg2 =
            crate::downloader::util::Segmentable::new(None, None).with_pace(Some(pace2.clone()));
        super::parse_flv(&mut conn2, file2, seg2, &mut super::FlvProgress::default())
            .await
            .unwrap();
        assert!(!pace2.cut_done());
        assert_eq!(
            *closed2.lock().unwrap(),
            [super::SegmentCloseReason::StreamEnded]
        );
        let media_ts2: Vec<u32> = written_tags(&directory.path().join("no-sei.flv"))
            .into_iter()
            .filter(|(t, _, config)| *t == 9 && !*config)
            .map(|(_, ts, _)| ts)
            .collect();
        assert_eq!(media_ts2, [1_000, 2_000, 3_000, 4_000]);
    }

    /// B 站 CDN 开头的真实时间戳（实测）：script=0、AAC 头带着陈旧的 2147178、AVC 头=0，
    /// 真实媒体从 2963844 开始且音视频同步。序列头曾被当成普通 tag 建立基准，首个关键帧被压到
    /// 1ms、其余帧接到 2147178 之后，开录第一段因此有一次 ~2147s 的跳变。
    #[tokio::test]
    async fn leading_sequence_headers_do_not_set_the_time_base() {
        let directory = tempfile::tempdir().unwrap();
        let template = directory.path().join("first").display().to_string();
        let mut bytes = vec![b'F', b'L', b'V', 1, 5, 0, 0, 0, 9, 0, 0, 0, 0];
        let mut metadata = vec![0x02, 0x00, 0x0a];
        metadata.extend_from_slice(b"onMetaData");
        metadata.push(0x05);
        append_tag(&mut bytes, 18, &metadata, 0);
        append_tag(&mut bytes, 8, &[0xaf, 0x00, 0x12, 0x10], 2_147_178);
        append_tag(&mut bytes, 9, &[0x17, 0x00, 0, 0, 0, 0x01, 0x64, 0x00], 0);
        const BASE: u32 = 2_963_844;
        for frame in 0..90u32 {
            let ts = BASE + frame * 33;
            let key = frame % 30 == 0;
            let mut body = vec![if key { 0x17 } else { 0x27 }, 0x01, 0, 0, 0];
            body.resize(5 + 200, 0x41);
            append_tag(&mut bytes, 9, &body, ts);
            append_tag(&mut bytes, 8, &[0xaf, 0x01, 0x21], ts + 17);
        }

        let mut connection = Connection::new(complete_response(bytes));
        connection.read_frame(9).await.unwrap();
        let file = crate::downloader::util::LifecycleFile::new(&template, "flv");
        let mut progress = super::FlvProgress::default();
        super::parse_flv(&mut connection, file, Default::default(), &mut progress)
            .await
            .unwrap();

        let tags = written_tags(&directory.path().join("first.flv"));
        let media: Vec<_> = tags
            .iter()
            .filter(|(t, _, config)| *t != 18 && !config)
            .collect();
        let first_video = media.iter().find(|(t, ..)| *t == 9).unwrap().1;
        let first_audio = media.iter().find(|(t, ..)| *t == 8).unwrap().1;
        assert!(
            first_video.abs_diff(first_audio) < 100,
            "音视频首帧应相邻：video {first_video}，audio {first_audio}"
        );
        for tag_type in [8u8, 9] {
            let series: Vec<u32> = media
                .iter()
                .filter(|(t, ..)| *t == tag_type)
                .map(|(_, ts, _)| *ts)
                .collect();
            for pair in series.windows(2) {
                assert!(
                    pair[1] > pair[0] && pair[1] - pair[0] < 1_000,
                    "type {tag_type} 落盘时间戳出现跳变：{} → {}",
                    pair[0],
                    pair[1]
                );
            }
        }
    }

    /// 生产实测（2026-10-27，稿件 13）：连上后先来两个不同的 H264 序列头，曾在第一个关键帧处
    /// 切出只有 prelude 的文件，当分P投上去被 B 站以「缺少视频轨」拒稿。没有帧的文件不切。
    #[tokio::test]
    async fn differing_leading_sequence_headers_do_not_split_off_an_empty_file() {
        let directory = tempfile::tempdir().unwrap();
        let template = directory.path().join("two-headers").display().to_string();
        let mut bytes = vec![b'F', b'L', b'V', 1, 5, 0, 0, 0, 9, 0, 0, 0, 0];
        append_tag(&mut bytes, 8, &[0xaf, 0x00, 0x12, 0x10], 0);
        append_tag(&mut bytes, 9, &[0x17, 0x00, 0, 0, 0, 0x01, 0x64, 0x00], 0);
        append_tag(&mut bytes, 9, &[0x17, 0x00, 0, 0, 0, 0x01, 0x4d, 0x00], 0);
        for index in 0..3u32 {
            let mut frame = vec![0x17, 0x01, 0, 0, 0];
            frame.resize(5 + 100, 0x41);
            append_tag(&mut bytes, 9, &frame, (index + 1) * 1_000);
        }

        let mut connection = Connection::new(complete_response(bytes));
        connection.read_frame(9).await.unwrap();
        let file = crate::downloader::util::LifecycleFile::new(&template, "flv");
        let segment = crate::downloader::util::Segmentable::new(None, None);
        let mut progress = super::FlvProgress::default();
        super::parse_flv(&mut connection, file, segment, &mut progress)
            .await
            .unwrap();

        assert_eq!(progress.splits, 0, "没有帧的文件不能被切出去");
    }

    /// 生产实测（2026-11-08，稿件 106）：连上后「AAC 头 → 空 H264 头 → 一帧音频 → 真正的 H264 头」，
    /// 曾因那一帧音频算作有帧，在新序列头处切出 755 字节没有视频的文件。只有视频帧才算。
    #[tokio::test]
    async fn an_audio_frame_before_a_new_sequence_header_does_not_split_off_a_file() {
        let directory = tempfile::tempdir().unwrap();
        let template = directory.path().join("audio-first").display().to_string();
        let mut bytes = vec![b'F', b'L', b'V', 1, 5, 0, 0, 0, 9, 0, 0, 0, 0];
        append_tag(&mut bytes, 8, &[0xaf, 0x00, 0x12, 0x10], 0);
        append_tag(&mut bytes, 9, &[0x17, 0x00, 0, 0, 0], 0);
        append_tag(&mut bytes, 8, &[0xaf, 0x01, 7, 7], 26_997);
        append_tag(
            &mut bytes,
            9,
            &[0x17, 0x00, 0, 0, 0, 0x01, 0x4d, 0x00],
            27_011,
        );
        for index in 0..3u32 {
            let mut frame = vec![0x17, 0x01, 0, 0, 0];
            frame.resize(5 + 100, 0x41);
            append_tag(&mut bytes, 9, &frame, 27_011 + index * 1_000);
        }

        let mut connection = Connection::new(complete_response(bytes));
        connection.read_frame(9).await.unwrap();
        let file = crate::downloader::util::LifecycleFile::new(&template, "flv");
        let segment = crate::downloader::util::Segmentable::new(None, None);
        let mut progress = super::FlvProgress::default();
        super::parse_flv(&mut connection, file, segment, &mut progress)
            .await
            .unwrap();

        assert_eq!(progress.splits, 0, "只有音频帧的文件不能被切出去");
    }

    /// 纯视频流没有 AAC 序列头。切段时重发 prelude 曾对缺失的头 `expect`，整场录制直接 panic；
    /// 现在缺哪个跳过哪个，照常切段。
    #[tokio::test]
    async fn a_video_only_stream_still_splits() {
        let directory = tempfile::tempdir().unwrap();
        let template = directory.path().join("video-only").display().to_string();
        let mut bytes = vec![b'F', b'L', b'V', 1, 1, 0, 0, 0, 9, 0, 0, 0, 0];
        let mut metadata = vec![0x02, 0x00, 0x0a];
        metadata.extend_from_slice(b"onMetaData");
        metadata.push(0x05);
        append_tag(&mut bytes, 18, &metadata, 0);
        append_tag(&mut bytes, 9, &[0x17, 0x00, 0, 0, 0, 0x01, 0x64, 0x00], 0);
        for index in 0..6u32 {
            let mut frame = vec![0x17, 0x01, 0, 0, 0];
            frame.resize(5 + 2_000, 0x41);
            append_tag(&mut bytes, 9, &frame, (index + 1) * 1_000);
        }

        let mut connection = Connection::new(complete_response(bytes));
        connection.read_frame(9).await.unwrap();
        let file = crate::downloader::util::LifecycleFile::new(&template, "flv");
        let segment = crate::downloader::util::Segmentable::new(None, Some(4_000));
        let mut progress = super::FlvProgress::default();
        super::parse_flv(&mut connection, file, segment, &mut progress)
            .await
            .unwrap();
        assert!(progress.splits >= 2, "纯视频流也必须按大小切段");
    }

    /// 起点曾经用 `prev_timestamp == 0` 判断是否已初始化，而 `prev_timestamp` 是上一批写出
    /// tag 的最后一个时间戳。重发的 timestamp=0 Script tag 会让每个关键帧都把 start 推到当前，
    /// 定时分段永远不满足（dplei/biliup#32：配置 30 分钟，实测单段录了 3 小时 16 分）。
    #[tokio::test]
    async fn repeated_zero_timestamp_metadata_does_not_postpone_the_timed_split() {
        let directory = tempfile::tempdir().unwrap();
        let template = directory
            .path()
            .join("metadata-flood")
            .display()
            .to_string();

        let mut connection = Connection::new(complete_response(flv_with_repeated_metadata(10, 16)));
        connection.read_frame(9).await.unwrap();
        let file = crate::downloader::util::LifecycleFile::new(&template, "flv");
        // 关键帧步进 1s，10 个关键帧覆盖 10s；3s 一刀应该切出 3 刀。
        let segment = crate::downloader::util::Segmentable::new(Some(Duration::from_secs(3)), None);
        let mut progress = super::FlvProgress::default();
        super::parse_flv(&mut connection, file, segment, &mut progress)
            .await
            .unwrap();

        assert_eq!(
            progress.splits, 3,
            "timestamp=0 的元数据重发不该重置本段计时起点"
        );
    }

    /// dplei/biliup#35：`elapsed` 曾是 `current - start`，一个 timestamp=0 的关键帧把 start 拉到 0，
    /// 下一个带绝对时钟的关键帧立刻满足时间条件，切出十几秒、几百字节的碎片（随后被判无效删除）。
    #[tokio::test]
    async fn a_zero_timestamp_keyframe_does_not_shatter_the_segment() {
        let directory = tempfile::tempdir().unwrap();
        let template = directory.path().join("rebase").display().to_string();
        let captured = Captured::start();

        let mut connection = Connection::new(complete_response(
            flv_with_absolute_base_and_zero_keyframes(10, 4_000),
        ));
        connection.read_frame(9).await.unwrap();
        let file = crate::downloader::util::LifecycleFile::new(&template, "flv");
        // 正常帧步进 1s，10 帧覆盖 10s；3s 一刀。
        let segment = crate::downloader::util::Segmentable::new(Some(Duration::from_secs(3)), None);
        let mut progress = super::FlvProgress::default();
        super::parse_flv(&mut connection, file, segment, &mut progress)
            .await
            .unwrap();

        assert!(
            progress.splits <= 3,
            "重发的 timestamp=0 关键帧不该触发额外切片，实测 {} 刀",
            progress.splits
        );
        assert!(progress.splits >= 2, "配置的 3s 定时分段仍必须生效");

        let events = captured.finish();
        let shards: Vec<_> = events
            .iter()
            .filter(|fields| {
                fields.get("event_name").map(String::as_str) == Some("recording.segment_closed")
                    && fields.get("reason_code").map(String::as_str) == Some("split_limit")
                    && fields
                        .get("size_bytes")
                        .and_then(|size| size.parse::<u64>().ok())
                        .is_some_and(|size| size < 4_000)
            })
            .collect();
        assert!(
            shards.is_empty(),
            "定时分段不该产出装不下一个关键帧的碎片：{shards:?}"
        );
    }

    /// The old per-tag DTS warning stays one to one; the native stream reports the first jump of
    /// a segment and then one rollup, and a new segment always starts its own record.
    #[test]
    fn dts_rollup_reports_the_first_jump_then_one_summary_per_segment() {
        let directory = tempfile::tempdir().unwrap();
        let template = directory.path().join("dts-%Y%m%d").display().to_string();
        let captured = Captured::start();
        let (first_id, second_id) = {
            let mut file = LifecycleFile::new(&template, "flv");
            let mut rollup = DtsBackwardRollup::default();

            file.create().unwrap();
            let first_id = file.identity().unwrap().segment_id.clone();
            rollup.record(&file, 1_000, 400, 1_010, "timestamp_backward");
            rollup.record(&file, 1_200, 900, 1_020, "timestamp_backward");
            rollup.record(&file, 1_500, 100, 1_030, "timestamp_backward");

            // A split allocates a new identity; the counts must not leak across it.
            file.create().unwrap();
            let second_id = file.identity().unwrap().segment_id.clone();
            rollup.record(&file, 2_000, 1_900, 2_010, "timestamp_backward");
            rollup.flush(&file);
            (first_id, second_id)
        };

        let events = captured.finish();
        let names: Vec<_> = events
            .iter()
            .filter(|fields| {
                fields.get("event_name").map(String::as_str) == Some("recording.dts_backward")
            })
            .cloned()
            .collect();
        assert_eq!(names.len(), 3, "first jump, first summary, second jump");

        assert_eq!(names[0]["segment_id"], first_id);
        assert_eq!(names[0]["previous_ms"], "1000");
        assert_eq!(names[0]["current_ms"], "400");
        assert_eq!(names[0]["emitted_ms"], "1010", "事件要说清实际写出的值");
        assert!(!names[0].contains_key("count"));

        assert_eq!(names[1]["segment_id"], first_id);
        assert_eq!(names[1]["count"], "3", "count includes the first jump");
        assert_eq!(names[1]["first_ms"], "400");
        assert_eq!(names[1]["last_ms"], "100");
        assert_eq!(names[1]["max_backward_ms"], "1400");

        assert_eq!(names[2]["segment_id"], second_id);
        assert_eq!(names[2]["previous_ms"], "2000");
        // A single jump in the new segment produces no summary of its own.
        assert!(!names[2].contains_key("count"));
    }

    /// 重基是 `parse_flv` 唯一改写落盘时间戳的地方，判据全在 `TimestampRebase` 里，
    /// 所以这一组直接喂时间戳序列，不跑网络也不落文件。
    mod rebase {
        use super::super::{Mapped, TimestampRebase};
        use crate::downloader::flv_parser::TagType;

        fn video(rebase: &mut TimestampRebase, src: u32) -> u32 {
            rebase.map(TagType::Video, src).emit
        }

        fn video_mapped(rebase: &mut TimestampRebase, src: u32) -> Mapped {
            rebase.map(TagType::Video, src)
        }

        /// 回归底线：没有跳变时本改动必须是恒等映射。
        #[test]
        fn a_clean_stream_is_mapped_one_to_one() {
            let mut rebase = TimestampRebase::default();
            for src in [0, 40, 80, 120, 1_000, 1_040] {
                assert_eq!(video(&mut rebase, src), src, "src {src}");
            }
        }

        /// audio/video 交错送达时流内各自单调，判据按 tag 类型分开维护，
        /// 因此两条流都保持恒等，相对关系原样保留。
        #[test]
        fn interleaved_audio_and_video_keep_their_own_timeline() {
            let mut rebase = TimestampRebase::default();
            let feed = [
                (TagType::Video, 1_000u32),
                (TagType::Audio, 980),
                (TagType::Video, 1_040),
                (TagType::Audio, 1_020),
                (TagType::Video, 1_080),
                (TagType::Audio, 1_060),
            ];
            for (tag_type, src) in feed {
                let mapped = rebase.map(tag_type, src);
                assert_eq!(mapped.emit, src, "{tag_type:?} {src}");
                assert!(mapped.deviation.is_none(), "合法交错不该被判成基准跳变");
            }
        }

        /// dplei/biliup#13 的真实形态：段中途换基准后一直用新基准。
        /// 跳变处只推进一个名义间隔，之后按真实增量累加。
        #[test]
        fn a_single_rebase_keeps_the_real_frame_intervals() {
            let mut rebase = TimestampRebase::default();
            for src in [98_000, 99_000, 100_000] {
                video(&mut rebase, src);
            }
            let jump = video_mapped(&mut rebase, 0);
            assert_eq!(jump.emit, 100_001, "首个样本紧跟本流最后一个");
            assert_eq!(jump.deviation, Some((100_000, "timestamp_backward")));
            assert_eq!(video(&mut rebase, 1_000), 100_011, "第二个样本确认新基准");
            assert_eq!(video(&mut rebase, 2_000), 101_011);
            assert_eq!(video(&mut rebase, 3_000), 102_011, "此后按真实增量累加");
        }

        /// 换基准不一定归零，也可能跳到更大的值——不能当成真的过了这么久。
        #[test]
        fn a_forward_jump_beyond_the_watchdog_is_a_rebase_too() {
            let mut rebase = TimestampRebase::default();
            video(&mut rebase, 1_000);
            video(&mut rebase, 2_000);
            let jump = video_mapped(&mut rebase, 3_600_000);
            assert_eq!(jump.emit, 2_001);
            assert_eq!(
                jump.deviation.map(|(_, reason)| reason),
                Some("timestamp_jump_forward")
            );
            assert_eq!(video(&mut rebase, 3_601_000), 2_011);
            assert_eq!(video(&mut rebase, 3_602_000), 3_011);
        }

        /// 生产实测（2026-10-25，稿件 6 P7）：源流断了 19s，音画在同一处从 5575s 跳到 5594s。
        /// 原样落盘时 B 站转码拒稿（「内部存在时间戳跳变」）。空档要压掉，音画相对关系不变。
        #[test]
        fn a_source_gap_is_collapsed_and_keeps_av_skew() {
            const GAP: u32 = 19_000;
            let mut rebase = TimestampRebase::default();
            let av = |rebase: &mut TimestampRebase, ts: u32| {
                let a = rebase.map(TagType::Audio, ts - 20).emit as i64;
                let v = rebase.map(TagType::Video, ts).emit as i64;
                (a, v)
            };
            let mut before = (0, 0);
            for i in 1..=50u32 {
                before = av(&mut rebase, 5_575_000 + i * 40 - 2_000);
            }
            let mut after = before;
            for i in 0..50u32 {
                after = av(&mut rebase, 5_575_000 + GAP + i * 40);
            }
            assert!(
                after.1 - before.1 < 3_000,
                "19s 空档必须压掉：{before:?} -> {after:?}"
            );
            assert_eq!(after.1 - after.0, 20, "音画相对关系不变");
        }

        /// 只有 video 断了 2s、audio 照常推进（编码器卡顿）：阈值调小后这也算换基准，
        /// 但新 offset 接在 audio 推进到的 high_water 上，音画仍在同一时间轴。
        #[test]
        fn a_video_only_stall_keeps_av_in_sync() {
            let mut rebase = TimestampRebase::default();
            for ms in (40..4_000u32).step_by(40) {
                rebase.map(TagType::Audio, ms);
                if !(2_000..4_000).contains(&ms) {
                    rebase.map(TagType::Video, ms);
                }
            }
            // 卡顿后第一帧还在等第二个样本确认，紧跟 video 最后一个之后（画面停在那一帧，与旧行为相同）
            for ms in (4_000..5_000u32).step_by(40) {
                let a = rebase.map(TagType::Audio, ms).emit as i64;
                let v = rebase.map(TagType::Video, ms).emit as i64;
                if ms > 4_000 {
                    assert!((v - a).abs() <= 50, "{ms}: audio {a}, video {v}");
                }
            }
        }

        /// spec 断言「交替重发下不需要二次确认」——推演结果是**反的**：没有 `pending`
        /// 时每一帧都走不连续分支，10 秒内容会被压成 100 毫秒。这条测试锁住真实增量必须留下。
        #[test]
        fn alternating_resends_do_not_collapse_the_timeline() {
            const BASE: u32 = 32_891_256;
            let mut rebase = TimestampRebase::default();
            let mut emits = Vec::new();
            for index in 0..10u32 {
                // CDN 重发的初始化关键帧：时间戳恒为 0
                emits.push(video(&mut rebase, 0));
                emits.push(video(&mut rebase, BASE + (index + 1) * 1_000));
            }
            assert!(
                emits.windows(2).all(|pair| pair[0] < pair[1]),
                "同一条流写出的时间戳必须严格递增：{emits:?}"
            );
            let span = emits.last().unwrap() - emits.first().unwrap();
            assert!(
                (8_000..12_000).contains(&span),
                "10 帧 × 1s 的真实时长必须保住，实测 {span}ms：{emits:?}"
            );
        }

        /// dplei/biliup#62：Script 槽的 `last_src` 停在切段 restamp 的 K，424s 后 CDN 连发两个
        /// timestamp=0 的 script tag。第二个被当成「新基准确认」，曾经会清空 A/V 槽的判据，
        /// A/V 下一个 tag（仍在旧基准）无条件套上新 offset，落盘前跳 ≈ K（实测 3794020ms）。
        #[test]
        fn a_phantom_confirmation_does_not_drag_the_other_streams() {
            const K: u32 = 3_790_667;
            let mut rebase = TimestampRebase::default();
            let mut emit = |t: TagType, src: u32| rebase.map(t, src).emit;
            emit(TagType::Script, K);
            emit(TagType::Audio, K);
            emit(TagType::Video, K);
            let mut last_v = 0;
            let mut last_a = 0;
            for i in 1..=100u32 {
                last_a = emit(TagType::Audio, K + i * 33 - 10);
                last_v = emit(TagType::Video, K + i * 33);
            }
            emit(TagType::Script, 0);
            emit(TagType::Script, 0);
            let next_a = emit(TagType::Audio, K + 101 * 33 - 10);
            let next_v = emit(TagType::Video, K + 101 * 33);
            assert_eq!(next_a - last_a, 33, "audio 必须按真实增量继续");
            assert_eq!(next_v - last_v, 33, "video 必须按真实增量继续");
        }

        /// 同一条路的 A/V 形态：CDN 成对重发 timestamp=0 的初始化 tag（sequence header +
        /// 关键帧），码流仍留在旧基准。#35 只推演过逐帧交替 `[0, B, 0, B]`，成对 `[0, 0, B]`
        /// 刚好凑满二次确认的两个样本。
        #[test]
        fn a_paired_resend_in_one_stream_leaves_the_other_untouched() {
            const B: u32 = 4_000_000;
            let mut rebase = TimestampRebase::default();
            let mut audio = Vec::new();
            let mut video = Vec::new();
            let mut feed = |t: TagType, src: u32| {
                let emit = rebase.map(t, src).emit;
                match t {
                    TagType::Audio => audio.push((src, emit)),
                    _ => video.push((src, emit)),
                }
            };
            for i in 1..100u32 {
                feed(TagType::Audio, B + i * 33 - 10);
                feed(TagType::Video, B + i * 33);
            }
            feed(TagType::Video, 0);
            feed(TagType::Video, 0);
            for i in 100..110u32 {
                feed(TagType::Audio, B + i * 33 - 10);
                feed(TagType::Video, B + i * 33);
            }
            assert!(
                audio.iter().all(|(src, emit)| src == emit),
                "audio 不在场的确认不该动 audio：{audio:?}"
            );
            let emits: Vec<u32> = video.iter().map(|(_, emit)| *emit).collect();
            assert!(
                emits
                    .windows(2)
                    .all(|pair| pair[0] < pair[1] && pair[1] - pair[0] <= 33),
                "video 单调且每步不超过一帧：{emits:?}"
            );
            let (src, emit) = *video.last().unwrap();
            let (asrc, aemit) = *audio.last().unwrap();
            assert!(
                (emit as i64 - src as i64 - (aemit as i64 - asrc as i64)).abs() <= 33,
                "占位 tag 之后 video 与 audio 回到同一时间轴：video {src}->{emit}, audio {asrc}->{aemit}"
            );
        }

        /// 真换基准时先确认的流把 offset 挂给兄弟，兄弟偏离时采纳同一个值：
        /// 换基准前后 audio 领先 video 的 20ms 逐 ms 相同，不是「大致」。
        #[test]
        fn a_real_rebase_keeps_audio_video_skew_exactly() {
            const B: u32 = 100_000;
            let mut rebase = TimestampRebase::default();
            let skew = |rebase: &mut TimestampRebase, ts: u32| {
                let a = rebase.map(TagType::Audio, ts - 20).emit as i64;
                let v = rebase.map(TagType::Video, ts).emit as i64;
                v - a
            };
            for i in 1..10u32 {
                assert_eq!(skew(&mut rebase, B + i * 1_000), 20);
            }
            rebase.map(TagType::Script, 0);
            skew(&mut rebase, 1_000);
            skew(&mut rebase, 2_000);
            for i in 3..10u32 {
                assert_eq!(skew(&mut rebase, i * 1_000), 20, "第 {i} 帧");
            }
        }

        /// dplei/biliup#75：切段 prelude 的 onMetaData 按关键帧的源时间戳重发，和上一个 script tag
        /// 隔着整段时长（40 分钟）。曾经它被判成偏离、写成「上一个 script +1ms」，比同段的
        /// video 早 2400s，文件起点被拉回一整段。它必须落在本段第一个关键帧附近。
        #[test]
        fn split_prelude_script_tag_stays_with_the_segment() {
            const SEGMENT: u32 = 2_400_000;
            let mut rebase = TimestampRebase::default();
            assert_eq!(rebase.map(TagType::Script, 0).emit, 0);
            for split in 0..3u32 {
                let start = split * SEGMENT;
                if split > 0 {
                    let script = rebase.map(TagType::Script, start).emit as i64;
                    let video = rebase.map(TagType::Video, start).emit as i64;
                    assert!(
                        (0..=100).contains(&(video - script)),
                        "第 {split} 段：script {script} 必须紧贴 video {video}"
                    );
                }
                for ms in (start + 40..start + SEGMENT).step_by(40) {
                    rebase.map(TagType::Audio, ms);
                    rebase.map(TagType::Video, ms);
                }
            }
        }

        /// 孤立的一帧噪声不该改基准：下一帧回到原基准时，原基准继续。
        #[test]
        fn one_off_noise_does_not_move_the_base() {
            let mut rebase = TimestampRebase::default();
            video(&mut rebase, 1_000);
            video(&mut rebase, 2_000);
            assert_eq!(video(&mut rebase, 0), 2_001, "噪声紧跟本流最后一个之后");
            assert_eq!(video(&mut rebase, 3_000), 3_000, "基准没变，恒等映射继续");
        }

        /// 生产实测（2026-09-20，iPhone 硬编 B 帧推流）：video 比 audio 晚到 1.1s，CDN 中途
        /// 重发 timestamp=0 的初始化 tag——avc sequence header 一个、aac sequence header 一对。
        /// 曾经：avc header 被接到 audio 的 high_water 之后，随后 1.1s 的真实 video 帧全被压成
        /// 1ms 间隔（上传侧判 Unfixable）；一对 aac header 又凑成「新基准确认」，audio 被拉到
        /// video 的位置上，整段音画错位。两条流都必须按各自真实增量继续，且相对关系不变。
        #[test]
        fn resent_headers_do_not_disturb_a_lagging_stream() {
            const LAG: u32 = 1_100;
            const B: u32 = 20_000_000;
            let mut rebase = TimestampRebase::default();
            let mut feed = |t: TagType, src: u32| rebase.map(t, src).emit;
            let mut last_a = 0;
            let mut last_v = 0;
            for i in 1..=100u32 {
                last_a = feed(TagType::Audio, B + i * 50);
                last_v = feed(TagType::Video, B + i * 50 - LAG);
            }
            feed(TagType::Script, 0);
            feed(TagType::Audio, 0);
            feed(TagType::Video, 0);
            feed(TagType::Audio, 0);
            for i in 101..=130u32 {
                let a = feed(TagType::Audio, B + i * 50);
                let v = feed(TagType::Video, B + i * 50 - LAG);
                assert_eq!(a - last_a, 50, "audio 第 {i} 帧必须按真实增量继续");
                assert_eq!(v - last_v, 50, "video 第 {i} 帧必须按真实增量继续");
                assert_eq!(a - v, LAG, "第 {i} 帧音画相对关系必须原样保留");
                last_a = a;
                last_v = v;
            }
        }
    }

    /// 段中途换基准的完整码流：audio/video 交错、中途时间戳归零，并且跨越一次切段。
    /// 落盘文件里每条流的时间戳都必须严格递增——这是 dplei/biliup#13 里被 B 站转码拒稿的那个性质。
    fn flv_with_a_mid_stream_base_change(frames: usize, payload: usize) -> Vec<u8> {
        const BASE: u32 = 100_000;
        let mut bytes = vec![b'F', b'L', b'V', 1, 5, 0, 0, 0, 9, 0, 0, 0, 0];
        let mut metadata = vec![0x02, 0x00, 0x0a];
        metadata.extend_from_slice(b"onMetaData");
        metadata.push(0x05);
        append_tag(&mut bytes, 18, &metadata, 0);
        append_tag(&mut bytes, 8, &[0xaf, 0x00, 0x12, 0x10], 0);
        append_tag(&mut bytes, 9, &[0x17, 0x00, 0, 0, 0, 0x01, 0x64, 0x00], 0);
        for index in 0..frames {
            let index = index as u32;
            // 后半段 CDN 重发 script tag 并把基准换成从 0 起算
            let timestamp = if (index as usize) < frames / 2 {
                BASE + (index + 1) * 1_000
            } else {
                if index as usize == frames / 2 {
                    append_tag(&mut bytes, 18, &metadata, 0);
                }
                (index + 1 - frames as u32 / 2) * 1_000
            };
            // audio 比 video 早 20ms 到：合法交错，不该被判成换基准
            let mut audio = vec![0xaf, 0x01];
            audio.resize(2 + payload / 4, 0x41);
            append_tag(&mut bytes, 8, &audio, timestamp.saturating_sub(20));
            let mut frame = vec![0x17, 0x01, 0, 0, 0];
            frame.resize(5 + payload, 0x41);
            append_tag(&mut bytes, 9, &frame, timestamp);
        }
        bytes
    }

    /// 按 FLV 结构走一遍落盘文件，返回 `(tag_type, timestamp, 是否 AAC/AVC 序列头)`。
    fn written_tags(path: &std::path::Path) -> Vec<(u8, u32, bool)> {
        let data = std::fs::read(path).unwrap();
        let mut offset = 13;
        let mut tags = Vec::new();
        while offset + 11 <= data.len() {
            let size = u32::from_be_bytes([0, data[offset + 1], data[offset + 2], data[offset + 3]])
                as usize;
            let timestamp = u32::from_be_bytes([
                data[offset + 7],
                data[offset + 4],
                data[offset + 5],
                data[offset + 6],
            ]);
            let config = matches!(data[offset], 8 | 9) && data.get(offset + 12) == Some(&0);
            tags.push((data[offset], timestamp, config));
            offset += 11 + size + 4;
        }
        tags
    }

    #[tokio::test]
    async fn a_mid_stream_base_change_is_written_as_a_monotonous_timeline() {
        let directory = tempfile::tempdir().unwrap();
        // 同一秒内切段会撞同名文件，用纳秒占位符把两段分开
        let template = directory.path().join("rebased-%f").display().to_string();
        let captured = Captured::start();

        let mut connection = Connection::new(complete_response(flv_with_a_mid_stream_base_change(
            10, 4_000,
        )));
        connection.read_frame(9).await.unwrap();
        let file = crate::downloader::util::LifecycleFile::new(&template, "flv");
        // 切一刀，顺带覆盖段首 prelude 重发
        let segment = crate::downloader::util::Segmentable::new(None, Some(20_000));
        let mut progress = super::FlvProgress::default();
        super::parse_flv(&mut connection, file, segment, &mut progress)
            .await
            .unwrap();

        let mut files: Vec<_> = std::fs::read_dir(directory.path())
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.extension().is_some_and(|ext| ext == "flv"))
            .collect();
        files.sort();
        assert!(
            files.len() >= 2,
            "本用例要覆盖切段，实测 {} 个文件",
            files.len()
        );

        for path in &files {
            let tags = written_tags(path);
            assert!(tags.len() > 3, "{path:?} 没写出内容");
            // 段首 onMetaData 必须是 0：FFmpeg 只在 dts==0 时把它当元数据跳过，否则当成一路
            // subtitle 流，ffprobe 多出一路流、解码报 non monotonically increasing dts。
            assert_eq!((tags[0].0, tags[0].1), (18, 0), "{path:?} 段首 script tag");
            for tag_type in [8u8, 9, 18] {
                let stamps: Vec<_> = tags
                    .iter()
                    .filter(|(kind, ..)| *kind == tag_type)
                    .map(|(_, timestamp, _)| *timestamp)
                    .collect();
                assert!(
                    stamps.windows(2).all(|pair| pair[0] < pair[1]),
                    "{path:?} 的 tag_type={tag_type} 时间戳不单调：{stamps:?}"
                );
            }
        }

        let events = captured.finish();
        assert!(
            events.iter().any(|fields| {
                fields.get("event_name").map(String::as_str) == Some("recording.dts_backward")
            }),
            "换基准仍然要留下事件，运维才知道文件被重基过"
        );
    }

    /// 按 `(tag_type, 时间戳)` 序列造一条码流（视频帧全是关键帧，每帧都会把缓存写出），
    /// 带 `Pace` 跑一遍 `parse_flv`，返回最后的计数。
    async fn pace_after(
        feed: impl IntoIterator<Item = (u8, u32)>,
    ) -> crate::downloader::util::PaceCount {
        let mut bytes = vec![b'F', b'L', b'V', 1, 5, 0, 0, 0, 9, 0, 0, 0, 0];
        append_tag(&mut bytes, 8, &[0xaf, 0x00, 0x12, 0x10], 0);
        append_tag(&mut bytes, 9, &[0x17, 0x00, 0, 0, 0, 0x01, 0x64, 0x00], 0);
        for (tag_type, ts) in feed {
            match tag_type {
                8 => append_tag(&mut bytes, 8, &[0xaf, 0x01, 0x21], ts),
                _ => append_tag(&mut bytes, 9, &[0x17, 0x01, 0, 0, 0, 0x41], ts),
            }
        }
        let directory = tempfile::tempdir().unwrap();
        let template = directory.path().join("skip").display().to_string();
        let mut connection = Connection::new(complete_response(bytes));
        connection.read_frame(9).await.unwrap();
        let pace = Arc::new(crate::downloader::util::Pace::default());
        let file = crate::downloader::util::LifecycleFile::new(&template, "flv");
        let segment =
            crate::downloader::util::Segmentable::new(None, None).with_pace(Some(pace.clone()));
        super::parse_flv(
            &mut connection,
            file,
            segment,
            &mut super::FlvProgress::default(),
        )
        .await
        .unwrap();
        pace.read()
    }

    /// 2026-10-30 云服务器 B 的形态：CDN 每约 20 秒只给约 1 秒画面，音视频在同一处前跳约 20 秒、在新轴上继续。
    /// 跳过量记进 `Pace`，音视频两条流只算一次。
    #[tokio::test]
    async fn a_forward_jump_on_both_streams_is_counted_once_as_skipped() {
        let frame = |i: u32| {
            if i < 100 {
                1_000 + i * 40
            } else {
                21_000 + i * 40
            }
        };
        let c = pace_after((0..200u32).flat_map(|i| [(8, frame(i) - 20), (9, frame(i))])).await;
        assert_eq!(c.jumps, 1, "音视频两条流只算一次");
        let skipped = c.skipped.as_millis() as i64;
        assert!(
            (skipped - 20_000).abs() <= 40,
            "跳过量误差在一帧内：{skipped}"
        );
    }

    /// 单个 tag 前跳后下一个又回到旧轴：孤立噪声，不算跳过。
    #[tokio::test]
    async fn one_off_forward_noise_is_not_counted_as_skipped() {
        let feed = (0..100u32)
            .map(|i| (9, 1_000 + i * 40))
            .chain([(9, 25_000)])
            .chain((100..150u32).map(|i| (9, 1_000 + i * 40)));
        let c = pace_after(feed).await;
        assert_eq!((c.jumps, c.skipped), (0, Duration::ZERO));
    }

    /// 向后换基准（时间戳归零）不是丢内容，不算跳过。
    #[tokio::test]
    async fn a_backward_rebase_is_not_counted_as_skipped() {
        let feed = (0..100u32)
            .map(|i| (9, 100_000 + i * 40))
            .chain((1..50u32).map(|i| (9, i * 40)));
        let c = pace_after(feed).await;
        assert_eq!((c.jumps, c.skipped), (0, Duration::ZERO));
    }

    /// 构造一个「先吐若干 chunk、之后永远不再产出」的响应。
    /// 用来模拟上游停发但连接未关闭——本 effort 里真实发生的正是这种静默。
    fn stalling_response(chunks: Vec<&'static [u8]>) -> reqwest::Response {
        let stream = futures::stream::iter(
            chunks
                .into_iter()
                .map(|chunk| Ok::<_, std::io::Error>(Bytes::from_static(chunk))),
        )
        .chain(futures::stream::pending());
        reqwest::Response::from(http::Response::new(reqwest::Body::wrap_stream(stream)))
    }

    /// 构造一个持续按 `interval` 产出 chunk 的响应。
    fn dripping_response(
        chunk: &'static [u8],
        count: usize,
        interval: Duration,
    ) -> reqwest::Response {
        let stream = futures::stream::unfold(0usize, move |sent| async move {
            if sent >= count {
                return None;
            }
            tokio::time::sleep(interval).await;
            Some((Ok::<_, std::io::Error>(Bytes::from_static(chunk)), sent + 1))
        });
        reqwest::Response::from(http::Response::new(reqwest::Body::wrap_stream(stream)))
    }

    #[tokio::test]
    async fn connection_new_keeps_the_thirty_second_default() {
        let connection = Connection::new(stalling_response(vec![]));
        let diagnostics = connection.diagnostics();
        assert_eq!(diagnostics.stall_timeout, DEFAULT_STALL_TIMEOUT);
        assert_eq!(diagnostics.stall_timeout, Duration::from_secs(30));
        // 刚建连时静默时长应接近 0，而不是未初始化的大数
        assert!(diagnostics.silent_for < Duration::from_millis(100));
    }

    #[tokio::test]
    async fn read_frame_gives_up_after_the_configured_stall_timeout() {
        let mut connection = Connection::with_stall_timeout(
            stalling_response(vec![b"abcd"]),
            Duration::from_millis(300),
        );
        let started = std::time::Instant::now();
        let error = connection
            .read_frame(8)
            .await
            .expect_err("stalled upstream must be judged dead");
        let elapsed = started.elapsed();

        assert!(
            matches!(
                error,
                crate::downloader::error::Error::HttpFlvReadTimeout { buffered: 4 }
            ),
            "unexpected error: {error:?}"
        );
        // 按配置的阈值判死，而不是等满 30 秒
        assert!(elapsed >= Duration::from_millis(300), "elapsed {elapsed:?}");
        assert!(elapsed < Duration::from_secs(5), "elapsed {elapsed:?}");
        // 静默口径覆盖「上游最后一个字节 → 判死」这一段
        let silent = connection.diagnostics().silent_for;
        assert!(silent >= Duration::from_millis(300), "silent {silent:?}");
        assert!(silent < Duration::from_millis(1000), "silent {silent:?}");
    }

    #[tokio::test]
    async fn stall_timeout_is_reset_by_every_chunk() {
        // 8 × 100ms = 800ms 总时长，远超 300ms 阈值；只要计时按 chunk 重置就不该超时。
        let mut connection = Connection::with_stall_timeout(
            dripping_response(b"ab", 8, Duration::from_millis(100)),
            Duration::from_millis(300),
        );
        let frame = connection
            .read_frame(16)
            .await
            .expect("steady stream must not trip the stall watchdog");
        assert_eq!(frame.len(), 16);
        assert_eq!(connection.diagnostics().received_bytes, 16);
    }

    #[tokio::test]
    async fn silent_for_tracks_the_time_since_the_last_byte() {
        let mut connection = Connection::with_stall_timeout(
            stalling_response(vec![b"abcd"]),
            Duration::from_secs(30),
        );
        connection.read_frame(4).await.expect("first frame");
        tokio::time::sleep(Duration::from_millis(250)).await;

        let diagnostics = connection.diagnostics();
        let silent = diagnostics.silent_for;
        assert!(
            silent >= Duration::from_millis(250) && silent < Duration::from_millis(600),
            "silent {silent:?}"
        );
        // 连接总时长与静默时长是两个口径，不能混用
        assert!(diagnostics.connected_for >= silent);
    }

    #[tokio::test]
    async fn probe_candidate_replays_prefix_without_double_counting_bytes() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let body = splittable_flv(2, 32);
        let expected_len = body.len() as u64;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let body_srv = body.clone();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut req = [0u8; 1024];
            let _ = socket.read(&mut req).await.unwrap();
            socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body_srv.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            socket.write_all(&body_srv).await.unwrap();
        });

        let client = crate::client::StatelessClient::new(reqwest::header::HeaderMap::new());
        let mut conn = super::probe_candidate(
            &client,
            &format!("http://{addr}/live.flv"),
            Duration::from_secs(5),
        )
        .await
        .expect("probe should succeed on valid FLV");

        let dir = tempfile::tempdir().unwrap();
        let file = LifecycleFile::new(dir.path().join("out").to_str().unwrap(), "flv");
        let mut progress = super::FlvProgress::default();
        super::parse_flv(&mut conn, file, Default::default(), &mut progress)
            .await
            .expect("replayed parse_flv should succeed");
        server.await.unwrap();

        assert_eq!(conn.diagnostics().received_bytes, expected_len);
        assert_eq!(progress.video_frames, 2);
        assert!(dir.path().join("out.flv").exists());
    }

    #[tokio::test]
    async fn probe_candidate_rejects_bad_flv_audio_only_oversized_and_timeout() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        async fn serve_once(body: Vec<u8>, stall: bool) -> String {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut req = [0u8; 1024];
                let _ = socket.read(&mut req).await.unwrap();
                let headers = if stall {
                    "HTTP/1.1 200 OK\r\nContent-Type: video/x-flv\r\n\r\n".to_string()
                } else {
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    )
                };
                socket.write_all(headers.as_bytes()).await.unwrap();
                let _ = socket.write_all(&body).await;
                if stall {
                    tokio::time::sleep(Duration::from_secs(20)).await;
                }
            });
            format!("http://{addr}/live.flv")
        }

        let client = crate::client::StatelessClient::new(reqwest::header::HeaderMap::new());

        // 1. 坏 FLV 头
        let bad_url = serve_once(b"NOT_FLV_DATA".to_vec(), false).await;
        let mut probe = super::CandidateProbe::default();
        assert!(
            super::probe_candidate_with_diagnostics(
                &client,
                &bad_url,
                Duration::from_secs(5),
                &mut probe
            )
            .await
            .is_err()
        );
        assert_eq!(probe.stage, "flv_header");
        assert_eq!(probe.edge, Some(std::net::Ipv4Addr::LOCALHOST.into()));
        assert_eq!(probe.http_status, Some(200));
        assert!(!probe.budget_exhausted);

        // 2. 只有序列头和音频，无关键帧就 EOF
        let mut audio_only = vec![b'F', b'L', b'V', 1, 5, 0, 0, 0, 9, 0, 0, 0, 0];
        append_tag(&mut audio_only, 8, &[0xaf, 0x00, 0x12, 0x10], 0);
        append_tag(
            &mut audio_only,
            9,
            &[0x17, 0x00, 0, 0, 0, 0x01, 0x64, 0x00],
            0,
        );
        append_tag(&mut audio_only, 8, &[0xaf, 0x01, 0x22, 0x33], 1000);
        let audio_url = serve_once(audio_only, false).await;
        assert!(
            super::probe_candidate(&client, &audio_url, Duration::from_secs(5))
                .await
                .is_err()
        );

        let valid_url = serve_once(splittable_flv(2, 32), false).await;
        assert!(
            super::probe_candidate(&client, &valid_url, Duration::from_secs(5))
                .await
                .is_ok(),
            "模拟 HTTP 服务须能提供可接受的 FLV"
        );

        for (sequence_header, keyframe) in [
            (None, &[0x17, 1, 0, 0, 0, 0x65][..]),
            (Some(&[0x17, 0, 0, 0, 0][..]), &[0x17, 1, 0, 0, 0, 0x65][..]),
            (
                Some(&[0x17, 0, 0, 0, 0, 1, 0x64, 0][..]),
                &[0x17, 1, 0, 0, 0][..],
            ),
        ] {
            let mut body = b"FLV\x01\x05\x00\x00\x00\x09\x00\x00\x00\x00".to_vec();
            if let Some(sequence_header) = sequence_header {
                append_tag(&mut body, 9, sequence_header, 0);
            }
            append_tag(&mut body, 9, keyframe, 1_000);
            let url = serve_once(body, false).await;
            assert!(
                super::probe_candidate(&client, &url, Duration::from_secs(5))
                    .await
                    .is_err(),
                "缺少有效 AVC 序列头或视频负载时不能切换"
            );
        }

        // 3. 前缀超过 8 MiB 仍无关键帧
        let mut oversized = vec![b'F', b'L', b'V', 1, 5, 0, 0, 0, 9, 0, 0, 0, 0];
        let big_audio = vec![0xaf, 0x01, 0x11, 0x22];
        let mut big_frame = vec![0xaf, 0x01];
        big_frame.resize(4 * 1024 * 1024 + 100, 0x55);
        append_tag(&mut oversized, 8, &big_audio, 0);
        append_tag(&mut oversized, 8, &big_frame, 1000);
        append_tag(&mut oversized, 8, &big_frame, 2000);
        let oversized_url = serve_once(oversized, false).await;
        assert!(
            super::probe_candidate(&client, &oversized_url, Duration::from_secs(5))
                .await
                .is_err()
        );

        // 4. 超过 8 秒超时
        let stall_url =
            serve_once(vec![b'F', b'L', b'V', 1, 5, 0, 0, 0, 9, 0, 0, 0, 0], true).await;
        let mut probe = super::CandidateProbe::default();
        assert!(
            super::probe_candidate_with_diagnostics(
                &client,
                &stall_url,
                Duration::from_secs(30),
                &mut probe
            )
            .await
            .is_err()
        );
        assert_eq!(probe.stage, "keyframe");
        assert_eq!(probe.edge, Some(std::net::Ipv4Addr::LOCALHOST.into()));
        assert!(probe.budget_exhausted);
    }

    #[test]
    fn byte_it_works() -> Result<(), Box<dyn std::error::Error>> {
        let mut bb = bytes::BytesMut::with_capacity(10);
        println!("chunk {:?}", bb.chunk());
        println!("capacity {}", bb.capacity());
        bb.put(&b"hello"[..]);
        println!("chunk {:?}", bb.chunk());
        println!("remaining {}", bb.remaining());
        bb.advance(5);
        println!("capacity {}", bb.capacity());
        println!("chunk {:?}", bb.chunk());
        println!("remaining {}", bb.remaining());
        bb.put(&b"hello"[..]);
        bb.put(&b"hello"[..]);
        println!("chunk {:?}", bb.chunk());
        println!("capacity {}", bb.capacity());
        println!("remaining {}", bb.remaining());

        let mut buf = BytesMut::with_capacity(11);
        buf.put(&b"hello world"[..]);

        let other = buf.split();
        // buf.advance_mut()

        assert!(buf.is_empty());
        assert_eq!(0, buf.capacity());
        assert_eq!(11, other.capacity());
        assert_eq!(other, b"hello world"[..]);

        Ok(())
    }

    #[test]
    fn it_works() -> Result<(), Box<dyn std::error::Error>> {
        // download(
        //     "test.flv")?;
        Ok(())
    }
}
