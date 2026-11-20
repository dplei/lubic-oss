//! 录制循环：轮询开播 → 录一场 → 断流重连 → 下播宽限期满回到轮询。
//!
//! 稳定性逻辑提炼自 biliup 服务端的 `DownloadTask::execute`，去掉了数据库、会话、上传通道。

use crate::client::StatelessClient;
use crate::downloader::flv_parser::header;
use crate::downloader::hls;
use crate::downloader::httpflv::{self, Connection};
use crate::downloader::live::{
    LiveCredentials, LiveOptions, LivePlugin, LiveRequest, LiveStatus, LiveStream,
    media_ext_from_url,
};
use crate::downloader::util::{
    LifecycleFile, RecordingOwner, Relocation, SegmentCloseHandle, SegmentCloseReason, Segmentable,
};
pub use crate::downloader::util::{Pace, PaceCount, unix_now_ms};
use lubic_events::Event;
use reqwest::header::{ACCEPT_ENCODING, HeaderMap, HeaderName, HeaderValue};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::mpsc::UnboundedSender;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

const RETRY_BASE_DELAY: Duration = Duration::from_secs(2);
const RETRY_MAX_DELAY: Duration = Duration::from_secs(30);
/// 停止时等录制在下一个关键帧收尾的上限，超时直接丢弃（drop 兜底收尾，丢最后一个 GOP）。
const STOP_GRACE: Duration = Duration::from_secs(10);

#[derive(Debug, Clone)]
pub struct RecordConfig {
    /// 输出模板，支持 `{name}`、`{title}` 与 strftime 占位符。`filename` 为空时它就是完整的
    /// 文件模板（不含扩展名），否则只是录制目录。
    pub output: String,
    /// 文件名模板（不含扩展名，同 biliup 的 `filename_prefix`），见 [`render_filename`]。
    pub filename: Option<String>,
    /// `filename` 里 `{streamer}` 的值（房间显示名）。
    pub streamer: String,
    pub split_time: Option<Duration>,
    pub split_size: Option<u64>,
    /// 未开播时的检测间隔。
    pub poll_interval: Duration,
    /// 断流后在这段时间内持续复查，连续离线超过它才算下播；避免短暂断流把一场切成多场。
    pub offline_grace: Duration,
    /// 码流停顿看门狗阈值。
    pub stall_timeout: Duration,
    pub options: LiveOptions,
    pub credentials: LiveCredentials,
    /// 分段收尾、下播时往这里发事件（agent 用）；`None` 只写日志。
    pub events: Option<UnboundedSender<Event>>,
    /// 录制实时倍率与产出字节的计数（agent 用），拉流连接存活期间计墙钟。
    pub pace: Option<Arc<Pace>>,
    /// 暂存录制（故障试拉，见 agent）：`pace` 转正（[`Pace::promote`]）之前的分段写这个目录模板，代替 `output`；
    /// 转正后同一条连接在下一个关键帧切段，之后的分段写 `output`。事件照常发，由接收方决定要不要放行。
    pub staging: Option<String>,
}

impl Default for RecordConfig {
    fn default() -> Self {
        Self {
            output: "{name}_%Y-%m-%dT%H_%M_%S".to_string(),
            filename: None,
            streamer: String::new(),
            split_time: None,
            split_size: None,
            poll_interval: Duration::from_secs(30),
            offline_grace: Duration::from_secs(300),
            stall_timeout: httpflv::DEFAULT_STALL_TIMEOUT,
            options: LiveOptions::default(),
            credentials: LiveCredentials::default(),
            events: None,
            pace: None,
            staging: None,
        }
    }
}

/// 监视一个房间直到 `cancel` 触发。`name` 用于文件名与日志。
pub async fn watch_room(
    plugin: Arc<dyn LivePlugin>,
    name: String,
    url: String,
    config: Arc<RecordConfig>,
    cancel: CancellationToken,
) {
    let client = reqwest::Client::new();
    let request = || LiveRequest {
        client: client.clone(),
        url: url.clone(),
        name: name.clone(),
        options: config.options.clone(),
        credentials: config.credentials.clone(),
    };
    info!(room = name, platform = plugin.name(), "watching");
    // 上一场录到一半平台换了场次：直接接着录新的这场，不再查一次。
    let mut next: Option<LiveStream> = None;
    while !cancel.is_cancelled() && !cut_done(&config) {
        let status = match next.take() {
            Some(stream) => Ok(LiveStatus::Live {
                stream: Box::new(stream),
            }),
            None => plugin.check_stream(request()).await,
        };
        match status {
            Ok(LiveStatus::Live { stream }) => {
                next = record_session(plugin.as_ref(), *stream, &request, &config, &cancel).await;
                // 停录、交接收尾、下播、换场：不再报平台在播（持续无有效视频只在确认在播时算）。
                if let Some(pace) = &config.pace {
                    pace.unconfirm_live();
                }
                // 一场结束后立刻再查一次，不等轮询间隔。
                continue;
            }
            Ok(LiveStatus::Offline) => {}
            Err(e) => warn!(room = name, error = %e, "check stream failed"),
        }
        sleep_or_cancel(config.poll_interval, &cancel).await;
    }
    info!(room = name, "stopped watching");
}

fn cut_done(config: &RecordConfig) -> bool {
    config.pace.as_ref().is_some_and(|p| p.cut_done())
}

/// 录完一整场：断流就复查，仍在播则换新地址重连，连续离线超过宽限期才返回。
/// 复查发现平台已换了一场（场次键变了，如主播断开重开）：报这场结束，返回新的那场接着录。
/// 各机都按平台的场次键归档，换机交接时两边才对得上同一份稿件。
async fn record_session(
    plugin: &dyn LivePlugin,
    mut stream: LiveStream,
    request: &impl Fn() -> LiveRequest,
    config: &RecordConfig,
    cancel: &CancellationToken,
) -> Option<LiveStream> {
    let session = stream.live_session_key.clone().unwrap_or_default();
    info!(
        room = stream.name,
        session,
        title = stream.title,
        quality = stream.recording_quality.as_deref().unwrap_or("unknown"),
        "live, recording"
    );
    let mut offline = OfflineRetryState::default();
    let mut failures = 0u32;
    let mut reconnecting = false;
    let mut previous_live_ms = None;
    // 上一条有效连接之后复查过不在播：下一个接缝跨过了下播宽限，不计入健康检测。
    let mut crossed_grace = false;
    if let Some(pace) = &config.pace {
        pace.record_recovered();
        pace.confirm_live();
    }
    loop {
        if let Some(pace) = &config.pace {
            pace.set_quality(stream.recording_quality.clone());
        }
        let mut progress = httpflv::FlvProgress {
            reconnecting,
            previous_live_ms,
            crossed_grace,
            ..Default::default()
        };
        // 下播宽限中发起的重连不算频繁短连接（handoff-health.md §3）。
        let in_grace = offline.offline_since.is_some();
        let productive = record_once(&stream, &session, config, cancel, &mut progress).await;
        let connected = config.pace.as_ref().and_then(|p| p.take_connected());
        let ended = (Instant::now(), unix_now_ms());
        reconnecting = true;
        if productive {
            previous_live_ms = progress.last_live_ms;
            crossed_grace = false;
        }
        if cancel.is_cancelled() || cut_done(config) {
            return None;
        }
        // 拿到过真实媒体就清零失败计数和下播宽限（断流后可能直接重试就恢复了，
        // 不经过返回 Live 的复查）；否则递增，退避逐次加长。
        if productive {
            offline.record_live();
        }
        failures = if productive && !progress.video_stalled {
            0
        } else {
            failures.saturating_add(1)
        };

        match plugin.check_stream(request()).await {
            Ok(LiveStatus::Live { stream: next }) => {
                offline.record_live();
                if new_session(&session, next.live_session_key.as_deref()) {
                    info!(
                        room = stream.name,
                        session,
                        next = next.live_session_key.as_deref().unwrap_or_default(),
                        "platform started a new live session, ending this one"
                    );
                    emit(
                        config,
                        Event::SessionEnded(RecordingOwner {
                            room: stream.name.clone(),
                            session,
                        }),
                    );
                    return Some(*next);
                }
                stream = *next;
                if let Some(pace) = &config.pace {
                    // 平台确认仍是同一场在播时结束的连接，才计入频繁短连接；切段不结束连接，
                    // 主动停止、交接收尾、换场、下播宽限都在这之前返回或另走分支。
                    pace.confirm_live();
                    if let Some(at) = connected.filter(|_| !in_grace) {
                        let duration = ended.0.saturating_duration_since(at).as_millis() as u64;
                        pace.connection_ended(ended.1, duration, productive);
                    }
                    // 一帧都没收到的空录（连不上、404、只有文件头）不计数也不清零：下播后平台常滞后
                    // 仍报在播，计数会让每次下播都触发交接并把本机标为受限。
                    if progress.video_stalled || (!productive && progress.received_media) {
                        pace.record_failure(progress.video_stalled);
                    } else if productive {
                        pace.record_recovered();
                    }
                }
                if failures > 0 {
                    let delay = exponential_backoff(failures);
                    warn!(
                        room = stream.name,
                        failures,
                        ?delay,
                        "live but download failed, backing off"
                    );
                    sleep_or_cancel(delay, cancel).await;
                }
            }
            status => {
                // 平台不确认在播时按下播宽限处理，不把源站离线当成本机受限。
                if let Some(pace) = &config.pace {
                    pace.record_recovered();
                    pace.unconfirm_live();
                }
                crossed_grace = true;
                if let Err(e) = &status {
                    warn!(room = stream.name, error = %e, "recheck failed, treating as unavailable");
                }
                if offline.record_unavailable(Instant::now(), config.offline_grace) {
                    info!(room = stream.name, session, "offline, session ended");
                    emit(
                        config,
                        Event::SessionEnded(RecordingOwner {
                            room: stream.name.clone(),
                            session,
                        }),
                    );
                    return None;
                }
                let delay = offline.retry_delay();
                info!(
                    room = stream.name,
                    ?delay,
                    "stream unavailable, rechecking within grace"
                );
                sleep_or_cancel(delay, cancel).await;
            }
        }
        if cancel.is_cancelled() {
            return None;
        }
    }
}

/// 复查拿到的场次键与在录的不同：平台已是另一场。任一边没有键（平台不给）时不算。
fn new_session(current: &str, next: Option<&str>) -> bool {
    !current.is_empty() && next.is_some_and(|k| !k.is_empty() && k != current)
}

/// 一次拉流尝试，返回是否拿到了真实媒体。已知 HLS 直接走 HLS；其余先试 FLV，
/// 响应不是 FLV 头时回落 HLS。
async fn record_once(
    stream: &LiveStream,
    session: &str,
    config: &RecordConfig,
    cancel: &CancellationToken,
    progress: &mut httpflv::FlvProgress,
) -> bool {
    let headers = match header_map(&stream.stream_headers) {
        Ok(headers) => headers,
        Err(e) => {
            error!(room = stream.name, error = e, "invalid stream headers");
            return false;
        }
    };
    let mut client = StatelessClient::new(headers);
    let owner = RecordingOwner {
        room: stream.name.clone(),
        session: session.to_string(),
    };
    let template = file_template(config, &stream.name, &stream.title);
    // 暂存中：分段先写暂存目录，转正后在下一个关键帧切到正式目录。
    let formal = RecordConfig {
        staging: None,
        ..config.clone()
    };
    let formal = file_template(&formal, &stream.name, &stream.title);
    let relocate = (formal != template)
        .then_some(formal)
        .zip(config.pace.clone());
    let segment =
        Segmentable::new(config.split_time, config.split_size).with_pace(config.pace.clone());
    let pace = config.pace.as_deref();
    let close_handle = SegmentCloseHandle::default();
    let productive = AtomicBool::new(false);
    let started = AtomicBool::new(false);
    let file = |ext: &'static str| {
        let productive = &productive;
        LifecycleFile::with_hook_and_close_handle(
            &template,
            ext,
            close_handle.clone(),
            move |event| {
                let existed = event.path.exists();
                if !discard_if_header_only(&event.path, ext, event.reason) {
                    productive.store(true, Ordering::Relaxed);
                    if let Some(pace) = pace {
                        pace.record_ready();
                    }
                    emit(config, Event::PartClosed(event.clone()));
                } else if existed {
                    emit(config, Event::PartDiscarded(event.clone()));
                }
            },
        )
        .with_owner(owner.clone())
        .with_title(stream.title.clone())
        .with_open_hook(move |event| {
            // 转正后第一个正式分段：FLV 切段时已带推流时刻记过；HLS、转正后重连的新连接在这里记，不知道时刻。
            if let Some(pace) =
                pace.filter(|p| config.staging.is_some() && p.promoted() && !p.formal())
            {
                pace.formal_started(None);
            }
            emit(config, Event::PartOpened(event.clone()))
        })
        .with_relocation(relocate.clone().map(|(formal, pace)| {
            Box::new(move || pace.promoted().then(|| formal.clone())) as Relocation
        }))
    };
    let url = stream.raw_stream_url.as_str();

    let download = async {
        if matches!(media_ext_from_url(url).as_deref(), Some("m3u8" | "ts")) {
            return hls_download(url, &client, file("ts"), segment, &productive, pace, progress).await;
        }
        client
            .headers
            .append(ACCEPT_ENCODING, HeaderValue::from_static("gzip, deflate"));
        let response = client
            .retryable(url)
            .await
            .map_err(|e| e.without_url().to_string())?;
        let mut connection = Connection::with_stall_timeout(response, config.stall_timeout)
            .with_pace(config.pace.clone());
        let head = connection.read_frame(9).await.map_err(|e| e.to_string())?;
        if header(&head).is_err() {
            warn!(
                room = stream.name,
                "response is not FLV, falling back to HLS"
            );
            return hls_download(url, &client, file("ts"), segment, &productive, pace, progress).await;
        }
        started.store(true, Ordering::Relaxed);
        if let Some(pace) = pace {
            pace.mark_connected();
        }
        let _wall = pace.map(Pace::running);
        httpflv::download_with_progress(&mut connection, file("flv"), segment, progress)
            .await
            .map_err(|e| e.to_string())
    };
    tokio::pin!(download);
    // 取消时先记下原因：写盘循环看到它就在下一个关键帧收尾（换机交接靠这个不丢尾巴）。
    // FLV 已启动或 HLS 已拿到媒体时给 STOP_GRACE 收尾；超时直接丢弃 future，drop 同样标为 user_cancel。
    let result = tokio::select! {
        result = &mut download => result,
        _ = cancel.cancelled() => {
            close_handle.set(SegmentCloseReason::Cancelled);
            if started.load(Ordering::Relaxed) || productive.load(Ordering::Relaxed) {
                tokio::time::timeout(STOP_GRACE, &mut download).await.unwrap_or(Ok(()))
            } else {
                Ok(())
            }
        }
    };
    if let Err(e) = result {
        warn!(
            room = stream.name,
            error = e,
            "download attempt ended with error"
        );
    }
    productive.load(Ordering::Relaxed)
}

async fn hls_download(
    url: &str,
    client: &StatelessClient,
    file: LifecycleFile<'_>,
    segment: Segmentable,
    productive: &AtomicBool,
    pace: Option<&Pace>,
    progress: &mut httpflv::FlvProgress,
) -> Result<(), String> {
    let _wall = pace.map(Pace::running);
    let owner = file.owner().clone();
    hls::download_with_ready(url, client, file, segment, || {
        productive.store(true, Ordering::Relaxed);
        if let Some(pace) = pace {
            pace.record_ready();
        }
        progress.valid_video = true;
        progress.report_reconnect(&owner, pace);
    })
    .await
    .map_err(|e| e.to_string())
}

fn emit(config: &RecordConfig, event: Event) {
    if let Some(tx) = &config.events {
        let _ = tx.send(event);
    }
}

/// 收回崩溃遗留的 `x.flv.part` / `x.ts.part`，规则与正常收尾相同：只有文件头的删掉返回 `None`，
/// 否则改名为最终文件并返回它。
pub fn salvage_part(part: &Path) -> Option<PathBuf> {
    let final_path = PathBuf::from(part.to_str()?.strip_suffix(".part")?);
    let ext = final_path.extension()?.to_str()?;
    if discard_if_header_only(part, ext, SegmentCloseReason::Unknown) {
        return None;
    }
    match std::fs::rename(part, &final_path) {
        Ok(()) => Some(final_path),
        Err(e) => {
            warn!(path = %part.display(), error = %e, "failed to finalize leftover segment");
            None
        }
    }
}

/// 没有任何音视频帧的分段直接删掉（断连瞬间常见）：TS 是空文件，FLV 是只有文件头、
/// 或只有 onMetaData 与序列头（刚连上就被停掉、限速的连接还没等到第一个 GOP）。
/// 音视频不满 [`MIN_MEDIA_MS`] 的 FLV 同样丢掉（见 [`flv_has_media`]）。
/// 返回是否已丢弃（或文件已不在）。
fn discard_if_header_only(path: &Path, ext: &str, reason: SegmentCloseReason) -> bool {
    let shown = path.display();
    match std::fs::metadata(path) {
        Ok(meta) if meta.len() == 0 || (ext == "flv" && !flv_has_media(path)) => {
            match std::fs::remove_file(path) {
                Ok(()) => info!(path = %shown, ?reason, "discarded header-only segment"),
                Err(e) => warn!(path = %shown, error = %e, "failed to discard header-only segment"),
            }
            true
        }
        Ok(meta) => {
            info!(path = %shown, size = meta.len(), ?reason, "segment saved");
            false
        }
        Err(e) => {
            warn!(path = %shown, error = %e, "closed segment is missing");
            true
        }
    }
}

/// 能投稿的分P至少要有这么长的音视频：B 站对更短的分P整份稿件报 21588「视频时长不足 1 秒」。
/// 2026-11-20 稿件 182：交接时新连接 10 秒后断开，只写下 46 帧、跨 764 ms 的分段。
const MIN_MEDIA_MS: u32 = 1_000;

/// FLV 里有没有够投稿的音视频（序列头不算）：音视频帧的时间戳要跨满 [`MIN_MEDIA_MS`]；
/// 有视频帧时还要至少两帧，只有一个关键帧的段（2026-10-28 稿件 21，重连时两个不同序列头之间夹了一帧就切段）
/// B 站报「缺少视频轨」；纯音频流（连视频序列头都没有）只看音频帧；有视频序列头却只有音频帧的不算
/// （2026-11-08 稿件 106：空序列头 + 一帧音频的 755 字节段，同样被拒）。
/// 从头逐个跳过 tag，正常分段读到开头一秒多就够，代价可以忽略。读不了就当有，宁可多传也不误删。
fn flv_has_media(path: &Path) -> bool {
    use std::io::{Read, Seek, SeekFrom};
    let Ok(mut f) = std::fs::File::open(path).map(std::io::BufReader::new) else {
        return true;
    };
    if f.seek(SeekFrom::Start(13)).is_err() {
        return true;
    }
    let mut head = [0u8; 13]; // 11 字节 tag 头 + 正文前 2 字节
    let (mut video, mut audio, mut has_video_tag) = (0u32, false, false);
    let (mut first, mut last) = (u32::MAX, 0u32);
    let long_enough = |first: u32, last: u32| first <= last && last - first >= MIN_MEDIA_MS;
    loop {
        if f.read_exact(&mut head).is_err() {
            // 读到末尾（或半个 tag）
            return !has_video_tag && audio && long_enough(first, last);
        }
        let size = u32::from_be_bytes([0, head[1], head[2], head[3]]) as i64;
        let (kind, b0, b1) = (head[0] & 0x1f, head[11], head[12]);
        let ts = u32::from_be_bytes([head[7], head[4], head[5], head[6]]);
        let config = match kind {
            // 视频：增强格式看 packet type（0 序列头、4 元数据），传统 AVC/HEVC 看 AVCPacketType。
            9 if b0 & 0x80 != 0 => matches!(b0 & 0x0f, 0 | 4),
            9 => matches!(b0 & 0x0f, 7 | 12) && b1 == 0,
            // 音频：AAC 的 AACPacketType 0 是序列头。
            8 => b0 >> 4 == 10 && b1 == 0,
            _ => true,
        };
        has_video_tag |= kind == 9;
        if !config && size > 0 {
            match kind {
                9 => video += 1,
                8 => audio = true,
                _ => {}
            }
            (first, last) = (first.min(ts), last.max(ts));
            if video >= 2 && long_enough(first, last) {
                return true;
            }
        }
        // 正文剩下的部分 + 4 字节 PreviousTagSize。
        if f.seek_relative(size - 2 + 4).is_err() {
            return !has_video_tag && audio && long_enough(first, last);
        }
    }
}

/// 一场录制的文件模板（交给 strftime，不含扩展名）：目录 `output` 加上文件名模板 `filename`；
/// 没有 `filename` 时 `output` 本身就是完整模板。
/// 暂存录制还没转正时目录换成 `staging`。
pub fn file_template(config: &RecordConfig, name: &str, title: &str) -> String {
    let dir = match &config.staging {
        Some(staging) if !config.pace.as_ref().is_some_and(|p| p.promoted()) => staging,
        _ => &config.output,
    };
    let output = output_template(dir, name, title);
    match &config.filename {
        Some(f) => format!(
            "{}/{}",
            output.trim_end_matches('/'),
            render_filename(f, &config.streamer, title)
        ),
        None => output,
    }
}

/// 展开 `{name}`、`{title}`；替换路径非法字符，避免标题里的 `/` 把文件写进别的目录。
fn output_template(template: &str, name: &str, title: &str) -> String {
    // strftime 会解释 `%`，用户内容里的 `%` 必须转义。
    let safe = |s: &str| sanitize(s).replace('%', "%%");
    template
        .replace("{name}", &safe(name))
        .replace("{title}", &safe(title))
}

/// 把用户内容里的路径非法字符与控制字符换成 `_`（同 biliup `sanitize_filename_chars`）。
fn sanitize(s: &str) -> String {
    s.chars()
        .map(|c| {
            if r#"\/:*?"<>|"#.contains(c) || c.is_control() {
                '_'
            } else {
                c
            }
        })
        .collect()
}

/// 展开文件名模板（同 biliup `render_filename_template`），结果再交给 strftime：
/// - `{streamer}`、`{title}` 换成清洗过的值，值里的 `%` 转义、`{colon}` 之类的文字不再展开；
/// - `{colon}` 是得到冒号的唯一写法，模板里其余的 `\ / : * ? " < > |` 与控制字符都换成 `_`；
/// - 去掉末尾的空格和点，空了就用 `_`。
pub fn render_filename(template: &str, streamer: &str, title: &str) -> String {
    let value = |s: &str| sanitize(s).replace('%', "%%");
    let mut out = String::new();
    let mut rest = template;
    while let Some(start) = rest.find('{') {
        out.push_str(&sanitize(&rest[..start]));
        rest = &rest[start..];
        let (text, len) = if rest.starts_with("{streamer}") {
            (value(streamer), "{streamer}".len())
        } else if rest.starts_with("{title}") {
            (value(title), "{title}".len())
        } else if rest.starts_with("{colon}") {
            (":".into(), "{colon}".len())
        } else {
            ("{".into(), 1)
        };
        out.push_str(&text);
        rest = &rest[len..];
    }
    out.push_str(&sanitize(rest));
    let out = out.trim_end_matches([' ', '.']);
    if out.is_empty() {
        "_".into()
    } else {
        out.into()
    }
}

fn header_map(headers: &std::collections::HashMap<String, String>) -> Result<HeaderMap, String> {
    headers
        .iter()
        .map(|(k, v)| {
            let name = HeaderName::try_from(k.as_str()).map_err(|e| format!("{k}: {e}"))?;
            let value = HeaderValue::try_from(v.as_str()).map_err(|e| format!("{k}: {e}"))?;
            Ok((name, value))
        })
        .collect()
}

async fn sleep_or_cancel(duration: Duration, cancel: &CancellationToken) {
    tokio::select! {
        _ = tokio::time::sleep(duration) => {}
        _ = cancel.cancelled() => {}
    }
}

/// 2s 起步、每次翻倍、30s 封顶。
fn exponential_backoff(failures: u32) -> Duration {
    RETRY_BASE_DELAY
        .saturating_mul(2u32.saturating_pow(failures.saturating_sub(1).min(5)))
        .min(RETRY_MAX_DELAY)
}

/// 下播宽限：记下第一次不可用的时刻，连续不可用超过宽限期才判下播。
#[derive(Debug, Default)]
struct OfflineRetryState {
    offline_since: Option<Instant>,
    retries: u32,
}

impl OfflineRetryState {
    fn record_live(&mut self) {
        *self = Self::default();
    }

    /// 记一次不可用，返回是否已超出宽限期。
    fn record_unavailable(&mut self, now: Instant, grace: Duration) -> bool {
        let since = *self.offline_since.get_or_insert(now);
        self.retries = self.retries.saturating_add(1);
        now.saturating_duration_since(since) >= grace
    }

    /// 复查间隔：首次 4s，之后翻倍，30s 封顶。
    fn retry_delay(&self) -> Duration {
        RETRY_BASE_DELAY
            .saturating_mul(2u32.saturating_pow(self.retries.min(5)))
            .min(RETRY_MAX_DELAY)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[tokio::test]
    async fn a_flv_header_without_media_is_not_a_productive_download() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        for (frames, sequence_header, reset) in [
            (0, false, false),
            (0, true, false),
            (1, true, false),
            (2, true, false),
            (2, true, true),
        ] {
            let directory = tempfile::tempdir().unwrap();
            let mut body = vec![b'F', b'L', b'V', 1, 5, 0, 0, 0, 9, 0, 0, 0, 0];
            if sequence_header {
                body.extend_from_slice(&[9, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0]);
                body.extend_from_slice(&[0x17, 0, 0, 0, 0, 1, 0x64, 0]);
                body.extend_from_slice(&19u32.to_be_bytes());
            }
            for index in 0..frames {
                // 两帧相隔 1 秒，够 `MIN_MEDIA_MS` 才保留；不从 0 起，写文件时 0 会调成 1。
                let ts = ((index as u32 + 1) * 1_000).to_be_bytes();
                body.extend_from_slice(&[9, 0, 0, 6, ts[1], ts[2], ts[3], 0, 0, 0, 0]);
                body.extend_from_slice(&[0x17, 1, 0, 0, 0, 0x65]);
                body.extend_from_slice(&17u32.to_be_bytes());
            }
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = [0; 4_096];
                socket.read(&mut request).await.unwrap();
                let length = body.len() + usize::from(reset);
                socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Length: {length}\r\nConnection: close\r\n\r\n").as_bytes()).await.unwrap();
                socket.write_all(&body).await.unwrap();
            });
            let stream = LiveStream {
                name: "r".into(),
                url: String::new(),
                title: String::new(),
                raw_stream_url: format!("http://{addr}/live.flv"),
                platform: "script".into(),
                stream_headers: Default::default(),
                suffix: "flv".into(),
                recording_quality: None,
                live_session_key: None,
            };
            let config = RecordConfig {
                output: directory.path().join("part").display().to_string(),
                ..Default::default()
            };
            let mut progress = httpflv::FlvProgress::default();
            let productive = tokio::time::timeout(
                Duration::from_secs(5),
                record_once(&stream, "s", &config, &CancellationToken::new(), &mut progress),
            )
            .await
            .unwrap();
            server.await.unwrap();
            // 收到帧却没留下有效分段才计为本机异常；只有文件头或序列头的不算。
            assert_eq!(progress.received_media, frames > 0, "frames={frames}");
            assert_eq!(
                productive,
                frames >= 2,
                "frames={frames}, sequence_header={sequence_header}, reset={reset}"
            );
            assert_eq!(directory.path().join("part.flv").exists(), frames >= 2);
        }
    }

    /// 按脚本依次回答检测：`Some(键)` 在播，`None` 不在播；脚本放完一直不在播。
    struct Script(std::sync::Mutex<std::collections::VecDeque<Option<&'static str>>>);

    #[tokio::test(start_paused = true)]
    async fn consecutive_unproductive_attempts_are_reported_only_while_platform_is_live() {
        for live in [false, true] {
            let pace = Arc::new(Pace::default());
            let plugin = Arc::new(Script(std::sync::Mutex::new(
                if live {
                    vec![Some("a"); 10]
                } else {
                    vec![Some("a"), None]
                }
                .into(),
            )));
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
            let config = Arc::new(RecordConfig {
                pace: Some(pace.clone()),
                events: Some(tx),
                offline_grace: Duration::ZERO,
                ..Default::default()
            });
            let cancel = CancellationToken::new();
            let task = tokio::spawn(watch_room(
                plugin.clone(),
                "r".into(),
                String::new(),
                config,
                cancel.clone(),
            ));
            tokio::time::timeout(Duration::from_secs(60), async {
                if live {
                    // 下播后平台常滞后几十秒仍报在播，这时拉流连不上或 404，一个 tag 都收不到。
                    // 这种空录不算本机异常，否则每次下播都会触发交接并把本机标为受限。
                    while plugin.0.lock().unwrap().len() > 6 {
                        assert_eq!(pace.record_failures(), (0, false));
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                    assert_eq!(pace.record_failures(), (0, false));
                    // 连都没连上的尝试不是「连接结束」，不计入频繁短连接（见 handoff-health.md §3）。
                    assert!(pace.recent_connections(0).is_empty());
                    // 连不上也在报「平台在播、没写出过视频」，持续无有效视频据此覆盖它。
                    assert!(matches!(pace.video_status(), Some((None, _))));
                } else {
                    while !matches!(
                        rx.recv().await.expect("session end event"),
                        Event::SessionEnded(_)
                    ) {}
                    assert_eq!(pace.record_failures(), (0, false));
                }
            })
            .await
            .unwrap();
            cancel.cancel();
            task.await.unwrap();
        }
    }

    /// 每 1 秒一个关键帧、带抖音 SEI 推流时刻的 FLV：每条连接都从同一刻起，接缝测得为重叠。
    fn sei_keyframes(frames: u32) -> Vec<u8> {
        const LIVE: i64 = 1_792_908_000_000;
        let mut body = vec![b'F', b'L', b'V', 1, 1, 0, 0, 0, 9, 0, 0, 0, 0];
        for index in 0..frames {
            let mut frame = vec![0x17, 1, 0, 0, 0];
            let nal = format!("\x06\x05{{\"ts\":{}}}", LIVE + index as i64 * 1_000);
            frame.extend_from_slice(&(nal.len() as u32).to_be_bytes());
            frame.extend_from_slice(nal.as_bytes());
            frame.extend_from_slice(&[0, 0, 0, 2, 0x65, 0x88]);
            let (n, t) = (frame.len() as u32, index * 1000);
            body.extend_from_slice(&[9, (n >> 16) as u8, (n >> 8) as u8, n as u8]);
            body.extend_from_slice(&[(t >> 16) as u8, (t >> 8) as u8, t as u8, 0, 0, 0, 0]);
            body.extend_from_slice(&frame);
            body.extend_from_slice(&(11 + n).to_be_bytes());
        }
        body
    }

    /// 可靠推流时刻测得的重连接缝才计入健康检测：同一场在播时的重连算，
    /// 跨过下播宽限（中间复查过不在播）的不算，换场后的第一条连接没有接缝。
    #[tokio::test]
    async fn reconnect_gaps_skip_offline_grace_and_session_changes() {
        let url = serve(sei_keyframes(3), None).await;
        let pace = Arc::new(Pace::default());
        // a 在播：连接 1 → 复查在播 → 连接 2（接缝 1→2 算）→ 复查不在播（宽限）→ 连接 3（接缝 2→3 跨宽限，不算）
        // → 复查在播 → 连接 4（接缝 3→4 算）→ 复查换成 b → b 的连接 5（新场，没有接缝）→ 之后一直不在播，
        // 连接 6 的接缝 5→6 跨宽限，不算。
        let plugin = Arc::new(Served(
            Script(std::sync::Mutex::new(
                [Some("a"), Some("a"), None, Some("a"), Some("b")].into(),
            )),
            url,
        ));
        let directory = tempfile::tempdir().unwrap();
        let config = Arc::new(RecordConfig {
            output: directory
                .path()
                .join("{name}_%H%M%S%f")
                .display()
                .to_string(),
            pace: Some(pace.clone()),
            offline_grace: Duration::from_secs(300),
            ..Default::default()
        });
        let cancel = CancellationToken::new();
        let task = tokio::spawn(watch_room(
            plugin.clone(),
            "r".into(),
            String::new(),
            config,
            cancel.clone(),
        ));
        // 连接 6 在第二次宽限复查（4 秒后）之后才开始，等它写完。
        tokio::time::timeout(Duration::from_secs(30), async {
            while !plugin.0.0.lock().unwrap().is_empty() {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_secs(6)).await;
        cancel.cancel();
        task.await.unwrap();
        let gaps = pace.recent_gaps(0);
        assert_eq!(
            gaps.iter().map(|g| g.1).collect::<Vec<_>>(),
            [0, 0],
            "{gaps:?}"
        );
    }

    /// 每 1 秒一个关键帧的 FLV，`frames` 帧。
    fn keyframes(frames: u32) -> Vec<u8> {
        let mut body = vec![b'F', b'L', b'V', 1, 1, 0, 0, 0, 9, 0, 0, 0, 0];
        for index in 0..frames {
            let t = index * 1000;
            body.extend_from_slice(&[
                9,
                0,
                0,
                6,
                (t >> 16) as u8,
                (t >> 8) as u8,
                t as u8,
                0,
                0,
                0,
                0,
            ]);
            body.extend_from_slice(&[0x17, 1, 0, 0, 0, 0x65]);
            body.extend_from_slice(&17u32.to_be_bytes());
        }
        body
    }

    /// 每个连接隔约 200ms 回同一段 FLV 后关闭（`stream_end`）；`hold` 为真时发完不关，等测试放行。
    async fn serve(body: Vec<u8>, hold: Option<tokio::sync::oneshot::Receiver<()>>) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let mut hold = hold;
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = [0; 4_096];
                let _ = socket.read(&mut request).await;
                let length = body.len() + usize::from(hold.is_some());
                let head = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {length}\r\nConnection: close\r\n\r\n"
                );
                let _ = socket.write_all(head.as_bytes()).await;
                // 每条连接撑一会儿，测试才看得到连接期间的状态。
                tokio::time::sleep(Duration::from_millis(200)).await;
                let _ = socket.write_all(&body).await;
                if let Some(release) = hold.take() {
                    let _ = release.await;
                }
            }
        });
        format!("http://{addr}/live.flv")
    }

    /// 同 [`Script`]，拉流地址指向本地服务器。
    struct Served(Script, String);

    #[async_trait::async_trait]
    impl LivePlugin for Served {
        fn name(&self) -> &'static str {
            "script"
        }
        fn matches(&self, _: &str) -> bool {
            true
        }
        async fn check_stream(
            &self,
            request: LiveRequest,
        ) -> crate::downloader::live::LiveResult<LiveStatus> {
            let mut status = self.0.check_stream(request).await?;
            if let LiveStatus::Live { stream } = &mut status {
                stream.raw_stream_url = self.1.clone();
            }
            Ok(status)
        }
    }

    /// 频繁短连接只统计平台确认仍是同一场在播时结束的拉流连接：切段不是连接结束，
    /// 复查不在播（下播宽限）、宽限期间发起的重连、换场都不算。
    #[tokio::test]
    async fn connection_ends_skip_splits_offline_grace_and_session_changes() {
        let url = serve(keyframes(6), None).await;
        let pace = Arc::new(Pace::default());
        // 1 a 在播 → 连接 1 结束、复查同场：算；连接 2 结束、复查不在播：不算；
        // 连接 3 在宽限中发起、复查在播：不算；连接 4 结束、复查换成 b：不算；
        // b 的连接 5 结束、复查同场：算；之后一直不在播。
        let plugin = Arc::new(Served(
            Script(std::sync::Mutex::new(
                [Some("a"), Some("a"), None, Some("a"), Some("b"), Some("b")].into(),
            )),
            url,
        ));
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let directory = tempfile::tempdir().unwrap();
        let config = Arc::new(RecordConfig {
            output: directory
                .path()
                .join("{name}_%H%M%S%f")
                .display()
                .to_string(),
            split_time: Some(Duration::from_secs(2)),
            pace: Some(pace.clone()),
            events: Some(tx),
            offline_grace: Duration::from_secs(300),
            ..Default::default()
        });
        let cancel = CancellationToken::new();
        let task = tokio::spawn(watch_room(
            plugin.clone(),
            "r".into(),
            String::new(),
            config,
            cancel.clone(),
        ));
        tokio::time::timeout(Duration::from_secs(30), async {
            while !plugin.0.0.lock().unwrap().is_empty() || pace.recent_connections(0).len() < 2 {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("two counted connection ends");
        tokio::time::sleep(Duration::from_millis(500)).await;
        cancel.cancel();
        task.await.unwrap();
        let connections = pace.recent_connections(0);
        assert_eq!(connections.len(), 2, "{connections:?}");
        assert!(
            connections
                .iter()
                .all(|(end, ms, video)| *end > 0 && *ms < 30_000 && *video)
        );
        let mut parts = 0;
        while let Ok(event) = rx.try_recv() {
            parts += usize::from(matches!(event, Event::PartClosed(_)));
        }
        assert!(parts > 6, "每条连接都切过段：{parts} 个分段");
    }

    /// 只有文件头就断开的连接（下播后 CDN 常这样）照样算连接结束，但标成没写出有效视频，
    /// 总控据此不计入频繁短连接。
    #[tokio::test]
    async fn header_only_connections_are_reported_without_video() {
        let url = serve(keyframes(0), None).await;
        let pace = Arc::new(Pace::default());
        let plugin = Arc::new(Served(
            Script(std::sync::Mutex::new([Some("a"); 3].into())),
            url,
        ));
        let config = Arc::new(RecordConfig {
            pace: Some(pace.clone()),
            offline_grace: Duration::from_secs(300),
            ..Default::default()
        });
        let cancel = CancellationToken::new();
        let task = tokio::spawn(watch_room(
            plugin.clone(),
            "r".into(),
            String::new(),
            config,
            cancel.clone(),
        ));
        tokio::time::timeout(Duration::from_secs(30), async {
            while pace.recent_connections(0).len() < 2 {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("two connection ends");
        cancel.cancel();
        task.await.unwrap();
        let connections = pace.recent_connections(0);
        assert!(
            connections.iter().all(|(_, _, video)| !video),
            "{connections:?}"
        );
    }

    /// 主动停止与交接切点收尾结束的连接不复查，也不算连接结束；录制停了就不再报平台在播。
    #[tokio::test]
    async fn stopped_and_cut_recordings_report_no_connection_end_or_live_state() {
        for cut in [false, true] {
            let (release, hold) = tokio::sync::oneshot::channel();
            let url = serve(keyframes(4), Some(hold)).await;
            let pace = Arc::new(Pace::default());
            let plugin = Arc::new(Served(
                Script(std::sync::Mutex::new(vec![Some("a"); 10].into())),
                url,
            ));
            let directory = tempfile::tempdir().unwrap();
            let config = Arc::new(RecordConfig {
                output: directory
                    .path()
                    .join("{name}_%H%M%S%f")
                    .display()
                    .to_string(),
                pace: Some(pace.clone()),
                ..Default::default()
            });
            let cancel = CancellationToken::new();
            let task = tokio::spawn(watch_room(
                plugin,
                "r".into(),
                String::new(),
                config,
                cancel.clone(),
            ));
            tokio::time::timeout(Duration::from_secs(10), async {
                while !pace.recording_ready() {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .unwrap();
            assert!(matches!(pace.video_status(), Some((Some(_), _))));
            if cut {
                pace.mark_cut_done();
            } else {
                cancel.cancel();
            }
            let _ = release.send(());
            tokio::time::timeout(Duration::from_secs(15), task)
                .await
                .unwrap()
                .unwrap();
            assert!(pace.recent_connections(0).is_empty(), "cut={cut}");
            assert_eq!(pace.video_status(), None, "cut={cut}");
        }
    }

    /// 暂存录制（故障试拉）：转正前写暂存目录；转正后同一条连接在下一个关键帧切段，之后写正式目录，不重连。
    #[tokio::test]
    async fn staged_recording_splits_into_the_output_on_promotion_without_reconnecting() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let body = keyframes(8);
        let half = 13 + 4 * 21;
        let (more, wait_more) = tokio::sync::oneshot::channel::<()>();
        let (release, hold) = tokio::sync::oneshot::channel::<()>();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/live.flv", listener.local_addr().unwrap());
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let _ = socket.read(&mut [0; 4_096]).await;
            let head = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", body.len() + 1);
            socket.write_all(head.as_bytes()).await.unwrap();
            socket.write_all(&body[..half]).await.unwrap();
            let _ = wait_more.await;
            socket.write_all(&body[half..]).await.unwrap();
            let _ = hold.await;
        });
        let plugin = Arc::new(Served(
            Script(std::sync::Mutex::new(vec![Some("a"); 10].into())),
            url,
        ));
        let directory = tempfile::tempdir().unwrap();
        let formal = directory.path().join("rec");
        let staging = directory.path().join(".trial");
        let pace = Arc::new(Pace::default());
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let config = Arc::new(RecordConfig {
            output: formal.join("{name}").display().to_string(),
            staging: Some(staging.join("{name}").display().to_string()),
            filename: Some("{streamer}_%H%M%S%f".into()),
            streamer: "s".into(),
            pace: Some(pace.clone()),
            events: Some(tx),
            ..Default::default()
        });
        let cancel = CancellationToken::new();
        let task = tokio::spawn(watch_room(
            plugin.clone(),
            "r".into(),
            String::new(),
            config,
            cancel.clone(),
        ));
        tokio::time::timeout(Duration::from_secs(10), async {
            while pace.read().media < Duration::from_secs(2) {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        assert!(!pace.promoted());
        pace.promote();
        more.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            while pace.read().media < Duration::from_secs(6) {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        cancel.cancel();
        let _ = release.send(());
        task.await.unwrap();
        let mut events = Vec::new();
        while let Ok(e) = rx.try_recv() {
            events.push(match e {
                Event::PartOpened(p) => ("opened", p.path),
                Event::PartClosed(p) => ("closed", p.path),
                e => panic!("{e:?}"),
            });
        }
        let dirs: Vec<_> = events
            .iter()
            .map(|(kind, path)| (*kind, path.parent().unwrap().to_path_buf()))
            .collect();
        let (s, f) = (staging.join("r"), formal.join("r"));
        assert_eq!(
            dirs,
            [
                ("opened", s.clone()),
                ("closed", s),
                ("opened", f.clone()),
                ("closed", f)
            ]
        );
        let mut gone = events.iter().filter(|(_, p)| !p.exists());
        assert!(gone.all(|(kind, _)| *kind == "opened"), "{events:?}");
        let left = plugin.0.0.lock().unwrap().len();
        assert_eq!(left, 9, "只查过一次开播，没有重连");
        // 没有 SEI：切到了正式分段，但不知道切点的推流时刻，交接退回按 lag（§6.2 第 4 条）。
        assert!(pace.formal());
        assert_eq!(pace.read().first_live_ms, None);
    }

    /// FLV 各视频 tag 的时间戳（毫秒）。
    fn video_timestamps(path: &Path) -> Vec<u32> {
        let bytes = std::fs::read(path).unwrap();
        let (mut at, mut out) = (13, Vec::new());
        while at + 11 <= bytes.len() {
            let size =
                u32::from_be_bytes([0, bytes[at + 1], bytes[at + 2], bytes[at + 3]]) as usize;
            let ts =
                u32::from_be_bytes([bytes[at + 7], bytes[at + 4], bytes[at + 5], bytes[at + 6]]);
            if bytes[at] == 9 {
                out.push(ts);
            }
            at += 11 + size + 4;
        }
        out
    }

    /// 决定时刻切段（§6.2）：转正后候选机在下一个关键帧切到正式分段，这一帧的推流时刻作为切点报出；
    /// 旧机按这个切点收尾。两边在切点处对接：旧机最后一帧在切点前一个 GOP，新机正式分段从切点那一帧开始，
    /// 中间不重复也不缺。
    #[tokio::test]
    async fn promotion_cut_is_the_live_moment_where_the_old_recording_ends() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        const LIVE: i64 = 1_792_908_000_000; // 同 `sei_keyframes`
        let body = sei_keyframes(8);
        let half = sei_keyframes(4).len();
        let (more, wait_more) = tokio::sync::oneshot::channel::<()>();
        let (release, hold) = tokio::sync::oneshot::channel::<()>();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/live.flv", listener.local_addr().unwrap());
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let _ = socket.read(&mut [0; 4_096]).await;
            let head = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n",
                body.len() + 1
            );
            socket.write_all(head.as_bytes()).await.unwrap();
            socket.write_all(&body[..half]).await.unwrap();
            let _ = wait_more.await;
            socket.write_all(&body[half..]).await.unwrap();
            let _ = hold.await;
        });
        let directory = tempfile::tempdir().unwrap();
        let (formal, staging) = (
            directory.path().join("rec"),
            directory.path().join(".trial"),
        );
        let pace = Arc::new(Pace::default());
        let config = Arc::new(RecordConfig {
            output: formal.join("{name}").display().to_string(),
            staging: Some(staging.join("{name}").display().to_string()),
            filename: Some("{streamer}_%H%M%S%f".into()),
            pace: Some(pace.clone()),
            ..Default::default()
        });
        let plugin = Arc::new(Served(
            Script(std::sync::Mutex::new(vec![Some("a"); 10].into())),
            url,
        ));
        let cancel = CancellationToken::new();
        let task = tokio::spawn(watch_room(
            plugin,
            "r".into(),
            String::new(),
            config,
            cancel.clone(),
        ));
        let media = |d| {
            let pace = pace.clone();
            async move {
                tokio::time::timeout(Duration::from_secs(10), async {
                    while pace.read().media < Duration::from_secs(d) {
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                })
                .await
                .unwrap()
            }
        };
        media(2).await;
        assert_eq!(
            pace.read().first_live_ms,
            Some(LIVE),
            "暂存期间报暂存录制的第一帧"
        );
        pace.promote();
        assert!(!pace.formal(), "要等下一个关键帧才切");
        more.send(()).unwrap();
        media(6).await;
        cancel.cancel();
        let _ = release.send(());
        task.await.unwrap();
        // 转正前收到的是第 0～3 帧，第 3 帧还在缓存里；第 4 帧到达时切段，它是正式分段的第一帧。
        let cut = LIVE + 4_000;
        assert!(pace.formal());
        assert_eq!(pace.read().first_live_ms, Some(cut));
        let [staged] = &files_under(&staging.join("r"))[..] else {
            panic!()
        };
        let [promoted] = &files_under(&formal.join("r"))[..] else {
            panic!()
        };
        assert_eq!(video_timestamps(staged), [0, 1_000, 2_000, 3_000]);
        assert_eq!(video_timestamps(promoted)[0], 4_000);

        // 旧机拉同一路流，按这个切点收尾。
        let (release, hold) = tokio::sync::oneshot::channel();
        let url = serve(sei_keyframes(8), Some(hold)).await;
        let old_dir = tempfile::tempdir().unwrap();
        let old_pace = Arc::new(Pace::default());
        old_pace.set_cut_live(cut);
        let config = Arc::new(RecordConfig {
            output: old_dir.path().join("{name}_%H%M%S%f").display().to_string(),
            pace: Some(old_pace.clone()),
            ..Default::default()
        });
        let plugin = Arc::new(Served(
            Script(std::sync::Mutex::new(vec![Some("a"); 10].into())),
            url,
        ));
        let task = tokio::spawn(watch_room(
            plugin,
            "r".into(),
            String::new(),
            config,
            CancellationToken::new(),
        ));
        tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .unwrap()
            .unwrap();
        let _ = release.send(());
        assert!(old_pace.cut_done());
        let [old] = &files_under(old_dir.path())[..] else {
            panic!()
        };
        let old = video_timestamps(old);
        assert_eq!(
            old.last(),
            Some(&3_000),
            "旧机最后一帧在切点前一个 GOP：{old:?}"
        );
    }

    fn files_under(dir: &Path) -> Vec<PathBuf> {
        let mut out: Vec<_> = std::fs::read_dir(dir)
            .into_iter()
            .flatten()
            .flatten()
            .map(|e| e.path())
            .collect();
        out.sort();
        out
    }

    /// HLS 没有推流时刻：转正后在分片边界切到正式分段，切点时刻未知，交接退回按 lag（§6.2 第 4 条）。
    #[tokio::test]
    async fn hls_promotion_starts_the_formal_part_without_a_live_moment() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/live.m3u8", listener.local_addr().unwrap());
        let (more, wait_more) = tokio::sync::oneshot::channel::<()>();
        let wait_more = Arc::new(tokio::sync::Mutex::new(Some(wait_more)));
        tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let wait_more = wait_more.clone();
                tokio::spawn(async move {
                    let mut request = [0; 4_096];
                    let n = socket.read(&mut request).await.unwrap_or(0);
                    let line = String::from_utf8_lossy(&request[..n]).to_string();
                    let body: &[u8] = if line.starts_with("GET /live.m3u8") {
                        b"#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:0\n#EXTINF:1,\na.ts\n#EXTINF:1,\nb.ts\n#EXTINF:1,\nc.ts\n#EXT-X-ENDLIST\n"
                    } else if line.starts_with("GET /a.ts") {
                        b"A"
                    } else if line.starts_with("GET /b.ts") {
                        if let Some(wait) = wait_more.lock().await.take() {
                            let _ = wait.await;
                        }
                        b"B"
                    } else {
                        b"C"
                    };
                    let head = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    let _ = socket.write_all(head.as_bytes()).await;
                    let _ = socket.write_all(body).await;
                });
            }
        });
        let directory = tempfile::tempdir().unwrap();
        let (formal, staging) = (
            directory.path().join("rec"),
            directory.path().join(".trial"),
        );
        let pace = Arc::new(Pace::default());
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let config = Arc::new(RecordConfig {
            output: formal.join("{name}").display().to_string(),
            staging: Some(staging.join("{name}").display().to_string()),
            filename: Some("{streamer}_%H%M%S%f".into()),
            pace: Some(pace.clone()),
            events: Some(tx),
            offline_grace: Duration::ZERO,
            ..Default::default()
        });
        let plugin = Arc::new(Served(
            Script(std::sync::Mutex::new([Some("a")].into())),
            url,
        ));
        let cancel = CancellationToken::new();
        let task = tokio::spawn(watch_room(
            plugin,
            "r".into(),
            String::new(),
            config,
            cancel.clone(),
        ));
        let until = async |done: &dyn Fn() -> bool| {
            tokio::time::timeout(Duration::from_secs(10), async {
                while !done() {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .unwrap()
        };
        until(&|| pace.recording_ready()).await;
        pace.promote();
        assert!(!pace.formal());
        more.send(()).unwrap();
        until(&|| pace.formal()).await;
        // 等这一场录完（ENDLIST 后下播）再停。
        tokio::time::sleep(Duration::from_millis(500)).await;
        cancel.cancel();
        task.await.unwrap();
        assert_eq!(pace.read().first_live_ms, None);
        let opened: Vec<_> = std::iter::from_fn(|| rx.try_recv().ok())
            .filter_map(|e| match e {
                Event::PartOpened(p) => Some(p.path.parent().unwrap().to_path_buf()),
                _ => None,
            })
            .collect();
        assert_eq!(opened, [staging.join("r"), formal.join("r")]);
    }

    /// 持续无有效视频的原始数只在平台确认在播时报：下播宽限中不报，宽限后复查又在播时，
    /// 确认在播的起点换成那一刻，主播断推的那段不算成没写出视频。
    #[tokio::test]
    async fn video_status_is_reported_only_while_the_platform_confirms_live() {
        let url = serve(keyframes(3), None).await;
        let pace = Arc::new(Pace::default());
        // 开录在播 → 连接 1 后复查不在播（宽限）→ 连接 2 后复查在播 → 之后一直不在播。
        let plugin = Arc::new(Served(
            Script(std::sync::Mutex::new([Some("a"), None, Some("a")].into())),
            url,
        ));
        let directory = tempfile::tempdir().unwrap();
        let config = Arc::new(RecordConfig {
            output: directory
                .path()
                .join("{name}_%H%M%S%f")
                .display()
                .to_string(),
            pace: Some(pace.clone()),
            offline_grace: Duration::from_secs(300),
            ..Default::default()
        });
        let cancel = CancellationToken::new();
        let task = tokio::spawn(watch_room(
            plugin.clone(),
            "r".into(),
            String::new(),
            config,
            cancel.clone(),
        ));
        // 记下状态的每次变化。
        let mut seen: Vec<Option<i64>> = Vec::new();
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let since = pace.video_status().map(|(_, since)| since);
                if seen.last() != Some(&since) {
                    seen.push(since);
                }
                if seen.iter().flatten().count() >= 2 && seen.last() == Some(&None) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{seen:?}"));
        cancel.cancel();
        task.await.unwrap();
        let first = seen.iter().position(Option::is_some).expect("在播时报");
        let [Some(a), None, Some(b), None] = seen[first..first + 4] else {
            panic!("{seen:?}");
        };
        assert!(b >= a + 3_000, "宽限后重新起算：{seen:?}");
        assert_eq!(pace.video_status(), None);
    }

    #[async_trait::async_trait]
    impl LivePlugin for Script {
        fn name(&self) -> &'static str {
            "script"
        }
        fn matches(&self, _: &str) -> bool {
            true
        }
        async fn check_stream(
            &self,
            _: LiveRequest,
        ) -> crate::downloader::live::LiveResult<LiveStatus> {
            Ok(match self.0.lock().unwrap().pop_front().flatten() {
                Some(key) => LiveStatus::Live {
                    stream: Box::new(LiveStream {
                        name: "r".into(),
                        url: String::new(),
                        title: String::new(),
                        // 无效 URL 立即失败，避免虚拟时钟推进时等待真实网络 I/O。
                        raw_stream_url: "http://[".into(),
                        platform: "script".into(),
                        stream_headers: Default::default(),
                        suffix: "flv".into(),
                        recording_quality: None,
                        live_session_key: Some(key.into()),
                    }),
                },
                None => LiveStatus::Offline,
            })
        }
    }

    /// 2026-10-25 线上：主播断开重开，重连时平台已是新的一场。旧场要报结束，新场按新键录，
    /// 否则交接时接手的机器按新键另开稿件，旧稿件永远停在录制中。
    #[tokio::test(start_paused = true)]
    async fn reconnect_into_a_new_live_session_ends_the_old_one() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let config = Arc::new(RecordConfig {
            offline_grace: Duration::ZERO,
            events: Some(tx),
            ..Default::default()
        });
        let plugin = Arc::new(Script(std::sync::Mutex::new(
            [Some("a"), Some("a"), Some("b"), None].into(),
        )));
        let cancel = CancellationToken::new();
        let task = tokio::spawn(watch_room(
            plugin,
            "r".into(),
            String::new(),
            config,
            cancel.clone(),
        ));
        let mut ended = Vec::new();
        let collect = async {
            while ended.len() < 2 {
                if let Event::SessionEnded(o) = rx.recv().await.unwrap() {
                    ended.push(o.session);
                }
            }
        };
        // 虚拟时钟上的一小时：没报出两场就失败，不挂住。
        let _ = tokio::time::timeout(Duration::from_secs(3600), collect).await;
        assert_eq!(ended, ["a", "b"]);
        cancel.cancel();
        task.await.unwrap();
    }

    #[test]
    fn only_a_different_non_empty_key_is_a_new_session() {
        assert!(new_session("a", Some("b")));
        assert!(!new_session("a", Some("a")));
        assert!(!new_session("a", None));
        assert!(!new_session("a", Some("")));
        assert!(!new_session("", Some("b")));
    }

    /// 移植自 biliup `util.rs` 的文件名用例，外加冒号转义与 `%` 转义。
    #[test]
    fn filename_template_like_biliup() {
        let at = chrono::Local
            .with_ymd_and_hms(2026, 6, 17, 14, 57, 18)
            .unwrap();
        let name = |tpl: &str, streamer: &str| {
            at.format(&render_filename(tpl, streamer, "标题"))
                .to_string()
        };
        let colon = "{streamer} %Y-%m-%d %H{colon}%M{colon}%S";
        assert_eq!(
            name(colon, "小桐人"),
            "小桐人 2026-06-17 14:57:18",
            "{{colon}} 变冒号"
        );
        assert_eq!(
            name("{streamer} %Y-%m-%d %H:%M:%S", "小桐人"),
            "小桐人 2026-06-17 14_57_18",
            "普通冒号变 _"
        );
        assert_eq!(name("{streamer}: %H{colon}%M", "小桐人"), "小桐人_ 14:57");
        assert_eq!(
            name("{streamer} %H{colon}%M{colon}%S", "小桐人{colon}:备注"),
            "小桐人{colon}_备注 14:57:18",
            "值里的 {{colon}} 不展开"
        );
        assert_eq!(
            name(colon, "Secret（王者组排）"),
            "Secret（王者组排） 2026-06-17 14:57:18"
        );
        assert_eq!(
            name("{streamer}/{title} %%", "a/b 100%"),
            "a_b 100%_标题 %",
            "值里的 % 与 / 不生效"
        );
        assert_eq!(
            name("{streamer}{unknown} . ", "x"),
            "x{unknown}",
            "末尾空格和点去掉、未知占位符原样"
        );
        assert_eq!(render_filename("..", "", ""), "_");
        // 目录跟房间名，文件名跟显示名。
        let config = RecordConfig {
            output: "/rec/{name}/".into(),
            filename: Some(colon.into()),
            streamer: "Secret（王者组排）".into(),
            ..Default::default()
        };
        assert_eq!(
            file_template(&config, "Secret", "t"),
            "/rec/Secret/Secret（王者组排） %Y-%m-%d %H:%M:%S"
        );
    }

    #[test]
    fn backoff_doubles_and_caps() {
        let delays: Vec<u64> = (1..=7).map(|n| exponential_backoff(n).as_secs()).collect();
        assert_eq!(delays, [2, 4, 8, 16, 30, 30, 30]);
    }

    #[test]
    fn offline_grace_counts_from_first_unavailable_and_resets_on_live() {
        let grace = Duration::from_secs(60);
        let t0 = Instant::now();
        let mut state = OfflineRetryState::default();
        assert!(!state.record_unavailable(t0, grace));
        assert_eq!(state.retry_delay(), Duration::from_secs(4));
        assert!(!state.record_unavailable(t0 + Duration::from_secs(59), grace));
        assert!(state.record_unavailable(t0 + Duration::from_secs(60), grace));

        state.record_live();
        assert!(!state.record_unavailable(t0 + Duration::from_secs(120), grace));
    }

    #[test]
    fn productive_download_resets_offline_grace() {
        // 断流复查不可用 → 直接重试拉流恢复（没经过 Live 复查）→ 几小时后再断一次。
        let grace = Duration::from_secs(300);
        let t0 = Instant::now();
        let mut state = OfflineRetryState::default();
        assert!(!state.record_unavailable(t0, grace));
        state.record_live();
        assert!(!state.record_unavailable(t0 + Duration::from_secs(4 * 3600), grace));
    }

    #[test]
    fn zero_grace_ends_on_first_unavailable() {
        let mut state = OfflineRetryState::default();
        assert!(state.record_unavailable(Instant::now(), Duration::ZERO));
    }

    /// 每个 tag 是（类型，时间戳毫秒，正文）。
    fn flv(tags: &[(u8, u32, &[u8])]) -> Vec<u8> {
        let mut out = vec![b'F', b'L', b'V', 1, 5, 0, 0, 0, 9, 0, 0, 0, 0];
        for (kind, ts, body) in tags {
            let (n, t) = (body.len() as u32, ts.to_be_bytes());
            out.extend([
                *kind,
                (n >> 16) as u8,
                (n >> 8) as u8,
                n as u8,
                t[1],
                t[2],
                t[3],
                t[0],
                0,
                0,
                0,
            ]);
            out.extend(*body);
            out.extend((11 + n).to_be_bytes());
        }
        out
    }

    /// 交接收尾时实测留下过 511 字节、只有 onMetaData 与序列头的分段：传上去是个空分P，要丢。
    #[test]
    fn segments_without_audio_or_video_frames_are_discarded() {
        let dir = tempfile::tempdir().unwrap();
        let save = |name: &str, bytes: Vec<u8>| {
            let p = dir.path().join(name);
            std::fs::write(&p, bytes).unwrap();
            p
        };
        let script: &[u8] = &[
            2, 0, 10, b'o', b'n', b'M', b'e', b't', b'a', b'D', b'a', b't', b'a',
        ];
        let avc_header: &[u8] = &[0x17, 0, 0, 0, 0, 1, 2, 3];
        let aac_header: &[u8] = &[0xaf, 0, 0x12, 0x10];
        let keyframe: &[u8] = &[0x17, 1, 0, 0, 0, 9, 9, 9];
        let inter_frame: &[u8] = &[0x27, 1, 0, 0, 0, 9, 9, 9];
        let aac_frame: &[u8] = &[0xaf, 1, 7, 7];
        let empty_avc_header: &[u8] = &[0x17, 0, 0, 0, 0];

        let header_only = save("a.flv", flv(&[]));
        let config_only = save(
            "b.flv",
            flv(&[(18, 0, script), (9, 0, avc_header), (8, 0, aac_header)]),
        );
        let with_video = save(
            "c.flv",
            flv(&[
                (18, 0, script),
                (9, 0, avc_header),
                (9, 0, keyframe),
                (9, 1_000, inter_frame),
            ]),
        );
        // 2026-10-28 稿件 21：重连后「序列头 A → 一个关键帧 → 序列头 B」在 B 处切段，
        // 切出 prelude 加一个关键帧的 1.6KB 文件，B 站报「缺少视频轨」。
        let one_video_frame = save(
            "h.flv",
            flv(&[
                (18, 0, script),
                (8, 0, aac_header),
                (9, 0, avc_header),
                (9, 0, keyframe),
                (8, 2_000, aac_frame),
            ]),
        );
        let audio_only = save(
            "d.flv",
            flv(&[(8, 0, aac_header), (8, 0, aac_frame), (8, 1_000, aac_frame)]),
        );
        // 2026-11-08 稿件 106：重连后「AAC 头 → 空 H264 头 → 一帧音频」在下一个序列头处切段，
        // 755 字节的段被当纯音频保留，B 站报「缺少视频轨」。
        let audio_frame_after_avc_header = save(
            "i.flv",
            flv(&[
                (18, 0, script),
                (8, 0, aac_header),
                (9, 0, empty_avc_header),
                (8, 0, aac_frame),
            ]),
        );
        // 2026-11-20 稿件 182：46 帧只跨 764 ms，B 站对整份稿件报 21588「视频时长不足 1 秒」。
        // 时间戳不从 0 开始，比的是跨度。
        let short_video = save(
            "j.flv",
            flv(&[
                (9, 0, avc_header),
                (9, 18_150_249, keyframe),
                (9, 18_150_700, inter_frame),
                (9, 18_151_013, inter_frame),
            ]),
        );
        let short_audio = save(
            "k.flv",
            flv(&[(8, 5_000, aac_frame), (8, 5_999, aac_frame)]),
        );
        let truncated = save("e.flv", {
            let mut bytes = flv(&[(9, 0, keyframe), (9, 1_000, inter_frame)]);
            bytes.truncate(bytes.len() - 6);
            bytes
        });
        let torn_header = save("g.flv", flv(&[(9, 0, keyframe)])[..20].to_vec());
        let reason = SegmentCloseReason::Cancelled;
        assert!(discard_if_header_only(&header_only, "flv", reason));
        assert!(discard_if_header_only(&config_only, "flv", reason));
        assert!(!config_only.exists(), "丢弃就是删掉");
        assert!(!discard_if_header_only(&with_video, "flv", reason));
        assert!(discard_if_header_only(&one_video_frame, "flv", reason));
        assert!(!discard_if_header_only(&audio_only, "flv", reason));
        assert!(discard_if_header_only(&audio_frame_after_avc_header, "flv", reason));
        assert!(discard_if_header_only(&short_video, "flv", reason));
        assert!(discard_if_header_only(&short_audio, "flv", reason));
        // 半个媒体 tag：tag 头与正文前两字节在，算有媒体。
        assert!(!discard_if_header_only(&truncated, "flv", reason));
        // 连第一个 tag 头都没写完：没有媒体。
        assert!(discard_if_header_only(&torn_header, "flv", reason));
        let empty_ts = save("f.ts", Vec::new());
        assert!(discard_if_header_only(&empty_ts, "ts", reason));
    }

    #[test]
    fn template_escapes_path_separators_and_percent() {
        assert_eq!(
            output_template("rec/{name}_{title}_%H", "a/b", "100% 好"),
            "rec/a_b_100%% 好_%H"
        );
    }
}
