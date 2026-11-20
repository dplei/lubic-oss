use crate::downloader::error::{Error, Result};
use crate::downloader::util::{
    LifecycleFile, Pace, SegmentCloseReason, SegmentWriter, Segmentable,
};
use m3u8_rs::Playlist;

use std::io::Write;
use std::time::Duration;
use tracing::{debug, info, warn};
use url::Url;

use crate::client::StatelessClient;

pub async fn download(
    url: &str,
    client: &StatelessClient,
    file: LifecycleFile<'_>,
    splitting: Segmentable,
) -> Result<()> {
    download_with_ready(url, client, file, splitting, || {}).await
}

/// 收到第一个非空媒体分片后才回调 `on_ready`：拿到播放列表（或一个 HTML 错误页）不算连上。
pub async fn download_with_ready(
    url: &str,
    client: &StatelessClient,
    file: LifecycleFile<'_>,
    splitting: Segmentable,
    on_ready: impl FnOnce() + Send,
) -> Result<()> {
    let owner = file.owner().clone();
    let result = download_inner(url, client, file, splitting, on_ready).await;
    if let Err(error) = &result {
        let reason = match error {
            Error::HlsInvalidPlaylist => "invalid_playlist",
            Error::IOError(_) => "source_io",
            Error::ReqwestError(e) if e.status().is_some() => "http_error",
            Error::ReqwestError(e) if e.is_timeout() => "read_timeout",
            Error::UrlParseError(_) => "invalid_playlist",
            _ => "transport_error",
        };
        warn!(target: super::util::EVENT_TARGET,
            event_name = "recording.disconnected", outcome = "failed", reason_code = reason,
            stage = "hls", room = owner.room.as_str(),
        session = owner.session.as_str(), "HLS 下载未完成");
    }
    result
}

async fn download_inner(
    url: &str,
    client: &StatelessClient,
    file: LifecycleFile<'_>,
    mut splitting: Segmentable,
    on_ready: impl FnOnce() + Send,
) -> Result<()> {
    let mut on_ready = Some(on_ready);
    let stream_host = Url::parse(url)
        .ok()
        .and_then(|parsed| parsed.host_str().map(ToString::to_string))
        .unwrap_or_else(|| "unknown".to_string());
    info!(stream_host, protocol = "hls", "starting hls download");
    let resp = client
        .retryable(url)
        .await
        .map_err(reqwest::Error::without_url)?;
    let bytes = resp.bytes().await.map_err(reqwest::Error::without_url)?;

    let mut media_url = Url::parse(url)?;
    let mut pl = match m3u8_rs::parse_playlist(&bytes) {
        Ok((_i, Playlist::MasterPlaylist(pl))) => {
            info!(
                variant_count = pl.variants.len(),
                "parsed hls master playlist"
            );
            // 选码率最高、带分辨率的可播放变体（第一个不一定最好）；跳过 I 帧预览流。
            let best = pl
                .variants
                .iter()
                .filter(|v| !v.is_i_frame && v.resolution.is_some())
                .max_by_key(|v| v.bandwidth)
                .or_else(|| {
                    pl.variants
                        .iter()
                        .filter(|v| !v.is_i_frame)
                        .max_by_key(|v| v.bandwidth)
                })
                .ok_or(Error::HlsInvalidPlaylist)?;
            info!(
                "Selected variant: bandwidth={}, resolution={:?}, video={:?}",
                best.bandwidth, best.resolution, best.video
            );
            media_url = media_url.join(&best.uri)?;
            info!(
                media_host = media_url.host_str().unwrap_or("unknown"),
                "selected hls media playlist"
            );
            let resp = client
                .retryable(media_url.as_str())
                .await
                .map_err(reqwest::Error::without_url)?;
            let bs = resp.bytes().await.map_err(reqwest::Error::without_url)?;
            match m3u8_rs::parse_media_playlist(&bs) {
                Ok((_, pl)) => pl,
                Err(_) => return Err(Error::HlsInvalidPlaylist),
            }
        }
        Ok((_i, Playlist::MediaPlaylist(pl))) => {
            info!(
                media_sequence = pl.media_sequence,
                segment_count = pl.segments.len(),
                "parsed hls media playlist"
            );
            pl
        }
        Err(_) => return Err(Error::HlsInvalidPlaylist),
    };
    // 取到播放列表才算建立了一次连接（健康检测里 HLS 以一次播放列表会话为一次连接）。
    if let Some(pace) = splitting.pace() {
        pace.mark_connected();
    }
    let mut ts_file = SegmentWriter::new(file, &[], SegmentCloseReason::TransportError)?;
    let mut previous_last_segment: Option<u64> = None;
    // 当前文件已写入的分片字节数。
    let mut file_bytes: u64 = 0;
    loop {
        if pl.segments.is_empty() && pl.end_list {
            info!("Segments array is empty - stream finished");
            break;
        }
        for (index, segment) in pl.segments.iter().enumerate() {
            let seq = pl
                .media_sequence
                .checked_add(index as u64)
                .ok_or(Error::HlsInvalidPlaylist)?;
            if previous_last_segment.is_none_or(|previous| seq > previous) {
                if let Some(previous) = previous_last_segment.filter(|previous| seq - previous > 1)
                {
                    warn!("SEGMENT INFO SKIPPED");
                    boundary(
                        &ts_file.file,
                        "recording.hls_gap",
                        "media_sequence_gap",
                        seq,
                        Some(previous),
                        Some(seq - previous - 1),
                    );
                }
                debug!("Yield segment");
                if segment.discontinuity {
                    warn!("#EXT-X-DISCONTINUITY");
                    // 当前文件还没写进分片（重连后首片就带标记）时不切：切出的是空文件，
                    // 同一秒新建的下一段还会撞名多出 -1 后缀。
                    if file_bytes > 0 {
                        ts_file.create_new(SegmentCloseReason::Unknown)?;
                        file_bytes = 0;
                    }
                    boundary(
                        &ts_file.file,
                        "recording.hls_discontinuity",
                        "hls_discontinuity",
                        seq,
                        None,
                        None,
                    );
                    splitting.reset();
                }
                let length = download_to_file(
                    media_url.join(&segment.uri)?,
                    client,
                    &mut ts_file.buf_writer,
                    splitting.pace(),
                )
                .await?;
                if length > 0 {
                    if let Some(on_ready) = on_ready.take() {
                        on_ready();
                    }
                    if let Some(pace) = splitting.pace() {
                        pace.video_written();
                    }
                }
                file_bytes += length;
                splitting.increase_size(length);
                let duration = Duration::try_from_secs_f64(segment.duration as f64)
                    .map_err(|_| Error::HlsInvalidPlaylist)?;
                splitting.increase_time(duration);
                if splitting.needed() || ts_file.file.relocating() {
                    let reason = if splitting.size_needed() {
                        SegmentCloseReason::SizeSplit
                    } else if splitting.time_needed() {
                        SegmentCloseReason::TimedSplit
                    } else {
                        // 转正切段也走这里：冻结的 v1 词表没有贴切的值（不是切段上限、流也没断），记 `Unknown`。
                        SegmentCloseReason::Unknown
                    };
                    ts_file.create_new(reason)?;
                    file_bytes = 0;
                    splitting.reset();
                }
                previous_last_segment = Some(seq);
                if ts_file.file.stopping() {
                    break;
                }
            }
        }
        if pl.end_list || ts_file.file.stopping() {
            break;
        }
        // Do not spin on an unchanged live playlist; leave cancellation responsive.
        tokio::time::sleep(Duration::from_secs(pl.target_duration.max(1))).await;
        let resp = client
            .retryable(media_url.as_str())
            .await
            .map_err(reqwest::Error::without_url)?;
        let bs = resp.bytes().await.map_err(reqwest::Error::without_url)?;
        pl = m3u8_rs::parse_media_playlist(&bs)
            .map_err(|_| Error::HlsInvalidPlaylist)?
            .1;
    }
    info!("Done...");
    let reason = ts_file
        .file
        .fallback_reason(SegmentCloseReason::StreamEnded);
    ts_file.finish(reason)?;
    Ok(())
}

fn boundary(
    file: &LifecycleFile<'_>,
    event_name: &str,
    reason: &str,
    sequence: u64,
    previous: Option<u64>,
    missing: Option<u64>,
) {
    let owner = file.owner();
    let identity = file.identity();
    warn!(target: super::util::EVENT_TARGET, event_name, outcome = "executed", reason_code = reason,
        media_sequence = sequence, previous_media_sequence = previous, missing_segments = missing,
        segment_id = identity.map(|i| i.segment_id.as_str()).unwrap_or(""),
        original_file = identity.map(|i| i.original_file.as_str()).unwrap_or(""),
        room = owner.room.as_str(),
        session = owner.session.as_str(),
        "HLS 媒体序列出现缺口或不连续，继续录制");
}

async fn download_to_file(
    url: Url,
    client: &StatelessClient,
    out: &mut impl Write,
    pace: Option<&Pace>,
) -> Result<u64> {
    debug!(
        segment_host = url.host_str().unwrap_or("unknown"),
        "downloading hls segment"
    );
    let mut response = client
        .retryable(url.as_str())
        .await
        .map_err(reqwest::Error::without_url)?;
    let mut length: u64 = 0;
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(reqwest::Error::without_url)?
    {
        length += chunk.len() as u64;
        if let Some(pace) = pace {
            pace.data();
        }
        out.write_all(&chunk)?;
    }
    Ok(length)
}
