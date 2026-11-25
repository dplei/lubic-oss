//! 平台插件：只回答「开播了吗、流地址是什么」，不碰下载与落盘。
//! 新增平台 = 新增一个实现 [`LivePlugin`] 的文件 + 在 [`builtin_plugins`] 注册。

use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

pub(crate) mod bilibili;
pub(crate) mod douyin;
pub(crate) mod wbi;

pub use bilibili::Bilibili;
pub use douyin::Douyin;

pub type LiveResult<T> = Result<T, LiveError>;

#[derive(Debug, thiserror::Error)]
pub enum LiveError {
    #[error("{0}")]
    Custom(String),
    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Header(#[from] reqwest::header::InvalidHeaderValue),
}

impl LiveError {
    pub fn custom(message: impl Into<String>) -> Self {
        Self::Custom(message.into())
    }
}

#[async_trait]
pub trait LivePlugin: Send + Sync {
    fn name(&self) -> &'static str;
    fn matches(&self, url: &str) -> bool;
    async fn check_stream(&self, request: LiveRequest) -> LiveResult<LiveStatus>;
}

#[derive(Debug, Clone)]
pub struct LiveRequest {
    pub client: Client,
    pub url: String,
    pub name: String,
    pub options: LiveOptions,
    pub credentials: LiveCredentials,
}

#[derive(Debug, Clone, Default)]
pub struct LiveOptions {
    pub bilibili: BilibiliOptions,
    pub douyin: DouyinOptions,
}

#[derive(Debug, Clone)]
pub struct BilibiliOptions {
    /// 画质：10000 原画、25000 原画真彩等，取不到时平台自动降档。
    pub qn: u32,
    /// `stream`（FLV）或 `hls_fmp4`。
    pub protocol: String,
    /// 优先使用的 CDN 名，按顺序匹配。
    pub cdn: Vec<String>,
    /// 选中的 CDN 探测失败时，依次试其余候选。
    pub cdn_fallback: bool,
    /// 开播后多久内等 HLS 转码就绪，超时退回 FLV。
    pub hls_transcode_timeout: u64,
    /// 用 master m3u8 取匿名原画。
    pub anonymous_origin: bool,
    pub live_api: Option<String>,
    pub fallback_api: Option<String>,
}

impl Default for BilibiliOptions {
    fn default() -> Self {
        Self {
            qn: 25000,
            protocol: "stream".to_string(),
            cdn: Vec::new(),
            cdn_fallback: false,
            hls_transcode_timeout: 60,
            anonymous_origin: false,
            live_api: None,
            fallback_api: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct DouyinOptions {
    /// origin/uhd/hd/sd/ld/md，缺档时就近选。
    pub quality: String,
    /// `flv` 或 `hls`。
    pub protocol: String,
    pub double_screen: bool,
    pub true_origin: bool,
}

impl Default for DouyinOptions {
    fn default() -> Self {
        Self {
            quality: "origin".to_string(),
            protocol: "flv".to_string(),
            double_screen: false,
            true_origin: false,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct LiveCredentials {
    pub bilibili_cookie: Option<String>,
    pub bilibili_cookie_file: Option<PathBuf>,
    pub douyin_cookie: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LiveStatus {
    Live { stream: Box<LiveStream> },
    Offline,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveStream {
    pub name: String,
    pub url: String,
    pub title: String,
    /// 带签名的拉流地址，不要写进日志。
    pub raw_stream_url: String,
    pub platform: String,
    pub stream_headers: HashMap<String, String>,
    pub suffix: String,
    /// 平台实际选中的画质的显示名（如「超清」），由各平台插件按平台叫法生成。
    pub recording_quality: Option<String>,
    /// 平台实际选中的画质代码（如 `"origin"`），未知或平台不提供时为 `None`。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recording_quality_code: Option<String>,
    /// 标识「这一场直播」：同一场多次检测、跨进程重启都不变。多机接续录制时用它归并分段。
    pub live_session_key: Option<String>,
}

pub fn builtin_plugins() -> Vec<Arc<dyn LivePlugin>> {
    vec![Arc::new(Bilibili::new()), Arc::new(Douyin::new())]
}

/// 从 URL 的路径后缀或常见 query 参数推断媒体扩展名。
pub fn media_ext_from_url(input: &str) -> Option<String> {
    fn clean_ext(val: &str) -> Option<String> {
        let ext = val
            .trim()
            .trim_matches(|c: char| !c.is_ascii_alphanumeric())
            .to_ascii_lowercase();
        matches!(ext.as_str(), "flv" | "ts" | "m3u8" | "mp4" | "m4s").then_some(ext)
    }

    let Ok(url) = url::Url::parse(input) else {
        let before_q = input.split('?').next().unwrap_or(input);
        return before_q
            .rsplit_once('.')
            .and_then(|(_, ext)| clean_ext(ext));
    };
    if let Some(ext) = url
        .path_segments()
        .and_then(|mut s| s.next_back())
        .and_then(|seg| seg.rsplit_once('.'))
        .and_then(|(_, ext)| clean_ext(ext))
    {
        return Some(ext);
    }
    const KEYS: [&str; 5] = ["format", "type", "ext", "filetype", "fmt"];
    url.query_pairs().find_map(|(k, v)| {
        KEYS.iter()
            .any(|key| k.eq_ignore_ascii_case(key))
            .then(|| clean_ext(&v))
            .flatten()
    })
}

#[cfg(test)]
mod tests {
    use super::{LiveStream, media_ext_from_url};

    #[test]
    fn media_ext_from_path_query_and_bare_string() {
        assert_eq!(
            media_ext_from_url("https://a.b/live/x.flv?s=1").as_deref(),
            Some("flv")
        );
        assert_eq!(
            media_ext_from_url("https://a.b/live/x?format=m3u8").as_deref(),
            Some("m3u8")
        );
        assert_eq!(media_ext_from_url("https://a.b/live/x"), None);
        assert_eq!(media_ext_from_url("x.ts?y").as_deref(), Some("ts"));
    }

    #[test]
    fn live_stream_serde_handles_missing_and_none_quality_code() {
        let legacy_json = r#"{
            "name": "room",
            "url": "https://example.com/1",
            "title": "t",
            "raw_stream_url": "https://example.com/live.flv",
            "platform": "douyin",
            "stream_headers": {},
            "suffix": "flv",
            "recording_quality": "高清",
            "live_session_key": "sess-1"
        }"#;
        let stream: LiveStream = serde_json::from_str(legacy_json).unwrap();
        assert_eq!(stream.recording_quality_code, None);
        let serialized = serde_json::to_string(&stream).unwrap();
        assert!(!serialized.contains("recording_quality_code"));

        let mut with_code = stream;
        with_code.recording_quality_code = Some("sd".to_string());
        let serialized_with_code = serde_json::to_string(&with_code).unwrap();
        assert!(serialized_with_code.contains("\"recording_quality_code\":\"sd\""));
        let roundtrip: LiveStream = serde_json::from_str(&serialized_with_code).unwrap();
        assert_eq!(roundtrip.recording_quality_code.as_deref(), Some("sd"));
    }
}
