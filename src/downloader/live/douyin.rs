use super::{
    LiveError, LivePlugin, LiveRequest, LiveResult, LiveStatus, LiveStream, media_ext_from_url,
};
use async_trait::async_trait;
use libsm::sm3::hash::Sm3Hash;
use rand::Rng;
use rand::seq::SliceRandom;
use regex::Regex;
use reqwest::Client;
use reqwest::header::{COOKIE, HeaderMap, HeaderValue, REFERER, USER_AGENT};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

const DOUYIN_LIVE_URL: &str = "https://live.douyin.com/";
const DOUYIN_WEBCAST_ENTER_URL: &str = "https://live.douyin.com/webcast/room/web/enter/";
const DOUYIN_APP_REFLOW_URL: &str = "https://webcast.amemv.com/webcast/room/reflow/info/";
const DOUYIN_UNION_REGISTER_URL: &str = "https://ttwid.bytedance.com/ttwid/union/register/";
const DOUYIN_BASE_URL: &str = "https://www.douyin.com";
/// 抖音网页端请求共用的 User-Agent。
pub(crate) const DOUYIN_USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36";
const DEFAULT_TTWID: &str = "1%7Cu7ogdHsSmHtxbt4hjDCNvcLfVJz78CTM0TTWU8Hio8w%7C1751545220%7C18aac967e501e9d6c13384335ced3523c46a0b1cc4535c7213bc2506a7f462c8";

pub struct Douyin {
    re: Regex,
}

impl Default for Douyin {
    fn default() -> Self {
        Self::new()
    }
}

impl Douyin {
    pub fn new() -> Self {
        Self {
            re: Regex::new(r"https?://(?:(?:www|m|live|v)\.)?douyin\.com").unwrap(),
        }
    }
}

#[async_trait]
impl LivePlugin for Douyin {
    fn name(&self) -> &'static str {
        "Douyin"
    }

    fn matches(&self, url: &str) -> bool {
        self.re.is_match(url)
    }

    async fn check_stream(&self, request: LiveRequest) -> LiveResult<LiveStatus> {
        DouyinLive::new(request).check_stream().await
    }
}

struct DouyinLive {
    client: Client,
    url: String,
    name: String,
    quality: String,
    protocol: String,
    double_screen: bool,
    true_origin: bool,
    cookie: String,
    web_rid: Option<String>,
    room_id: Option<String>,
    sec_uid: Option<String>,
}

/// Web 接口的查询结果。
enum WebRoom {
    /// 平台明确告知直播已结束。
    Ended,
    /// 当前房间数据；接口没给房间时为 None。
    Room(Option<Value>),
}

impl DouyinLive {
    fn new(request: LiveRequest) -> Self {
        let options = request.options.douyin;
        Self {
            client: request.client,
            url: request.url,
            name: request.name,
            quality: options.quality,
            protocol: options.protocol,
            double_screen: options.double_screen,
            true_origin: options.true_origin,
            cookie: request.credentials.douyin_cookie.unwrap_or_default(),
            web_rid: None,
            room_id: None,
            sec_uid: None,
        }
    }

    async fn check_stream(&mut self) -> LiveResult<LiveStatus> {
        self.ensure_cookie().await;
        self.resolve_room_keys().await?;
        let Some(room_info) = self.get_room_info().await? else {
            return Ok(LiveStatus::Offline);
        };
        let (raw_stream_url, recording_quality) = self.select_stream_url(&room_info)?;
        let title = room_info
            .get("title")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();

        Ok(LiveStatus::Live {
            stream: Box::new(LiveStream {
                name: self.name.clone(),
                url: self.url.clone(),
                title,
                suffix: media_ext_from_url(&raw_stream_url).unwrap_or_else(|| "flv".to_string()),
                raw_stream_url,
                platform: "douyin".to_string(),
                stream_headers: self.stream_headers(),
                recording_quality: Some(recording_quality),
                // `room.id_str` 标识这一场直播，跨进程重启不变，可用作场次键。
                live_session_key: self.room_id.clone(),
            }),
        })
    }

    async fn ensure_cookie(&mut self) {
        if !self.cookie.is_empty() && !self.cookie.ends_with(';') {
            self.cookie.push(';');
        }
        if !self.cookie.contains("ttwid") {
            let ttwid = fetch_ttwid(&self.client).await;
            self.cookie.push_str(&format!("ttwid={ttwid};"));
        }
        if !self.cookie.contains("odin_ttid=") {
            self.cookie
                .push_str(&format!("odin_ttid={};", generate_odin_ttid()));
        }
        if !self.cookie.contains("__ac_nonce=") {
            self.cookie
                .push_str(&format!("__ac_nonce={};", generate_nonce()));
        }
    }

    fn headers(&self) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(USER_AGENT, HeaderValue::from_static(DOUYIN_USER_AGENT));
        headers.insert(REFERER, HeaderValue::from_static(DOUYIN_LIVE_URL));
        if !self.cookie.is_empty()
            && let Ok(cookie) = HeaderValue::from_str(&self.cookie)
        {
            headers.insert(COOKIE, cookie);
        }
        headers
    }

    async fn resolve_room_keys(&mut self) -> LiveResult<()> {
        if self.url.contains("v.douyin") {
            let resp = self
                .client
                .get(&self.url)
                .headers(self.headers())
                .send()
                .await
                .map_err(|err| {
                    LiveError::custom(format!("解析抖音短链接失败: {}", redact_reqwest_error(err)))
                })?;
            let url = resp.url().to_string();
            if url.contains("webcast.amemv") {
                self.sec_uid = capture(&url, r"[?&]sec_user_id=([^&]+)");
                self.room_id =
                    capture(&url, r"/reflow/(\d+)").or_else(|| capture(&url, r"room_id=(\d+)"));
                return Ok(());
            }
            if url.contains("isedouyin.com/share/user") {
                self.sec_uid = capture(&url, r"[?&]sec_uid=([^&]+)");
                return Ok(());
            }
            self.url = url;
        }

        if self.url.contains("/user/") {
            let sec_uid = self
                .url
                .split("/user/")
                .nth(1)
                .and_then(|part| part.split('?').next())
                .unwrap_or_default();
            if matches!(sec_uid.len(), 55 | 76) {
                self.sec_uid = Some(sec_uid.to_string());
                return Ok(());
            }

            let text = self
                .client
                .get(&self.url)
                .headers(self.headers())
                .send()
                .await
                .map_err(|err| {
                    LiveError::custom(format!("获取抖音用户页失败: {}", redact_reqwest_error(err)))
                })?
                .text()
                .await
                .map_err(|err| {
                    LiveError::custom(format!("读取抖音用户页失败: {}", redact_reqwest_error(err)))
                })?;
            let render_data = text
                .split(r#"<script id="RENDER_DATA" type="application/json">"#)
                .nth(1)
                .and_then(|part| part.split("</script>").next())
                .and_then(|part| urlencoding::decode(part).ok())
                .map(|cow| cow.to_string())
                .ok_or_else(|| LiveError::custom("抖音房间号获取失败"))?;
            self.web_rid = capture(&render_data, r#""web_rid":"([^"]+)""#);
            return Ok(());
        }

        let web_rid = self
            .url
            .split("douyin.com/")
            .nth(1)
            .and_then(|part| part.split('/').next())
            .and_then(|part| part.split('?').next())
            .map(|part| part.trim_start_matches('+').to_string())
            .filter(|part| !part.is_empty())
            .ok_or_else(|| LiveError::custom("抖音直播间地址错误"))?;
        self.web_rid = Some(web_rid);
        Ok(())
    }

    async fn get_web_room_info(&self, web_rid: &str) -> LiveResult<Value> {
        let mut params = common_params();
        params.push(("web_rid".to_string(), web_rid.to_string()));
        params.push(("is_need_double_stream".to_string(), "false".to_string()));
        params.push(("msToken".to_string(), generate_ms_token()));
        let query = sign_query(&params);
        self.client
            .get(format!("{DOUYIN_WEBCAST_ENTER_URL}?{query}"))
            .headers(self.headers())
            .send()
            .await
            .map_err(|err| {
                LiveError::custom(format!(
                    "获取抖音直播间信息失败: {}",
                    redact_reqwest_error(err)
                ))
            })?
            .json()
            .await
            .map_err(|err| {
                LiveError::custom(format!(
                    "解析抖音直播间信息失败: {}",
                    redact_reqwest_error(err)
                ))
            })
    }

    async fn get_h5_room_info(&self) -> LiveResult<Value> {
        let sec_uid = self
            .sec_uid
            .as_deref()
            .ok_or_else(|| LiveError::custom("抖音 sec_user_id 为空"))?;
        let verify_fp = gen_verify_fp();
        let params = vec![
            (
                "room_id".to_string(),
                self.room_id.clone().unwrap_or_else(|| "2".to_string()),
            ),
            ("sec_user_id".to_string(), sec_uid.to_string()),
            ("type_id".to_string(), "0".to_string()),
            ("live_id".to_string(), "1".to_string()),
            ("version_code".to_string(), "99.99.99".to_string()),
            ("app_id".to_string(), "1128".to_string()),
            ("aid".to_string(), "6383".to_string()),
            ("verifyFp".to_string(), verify_fp),
            ("msToken".to_string(), generate_ms_token()),
        ];
        let query = sign_query(&params);
        self.client
            .get(format!("{DOUYIN_APP_REFLOW_URL}?{query}"))
            .headers(self.headers())
            .send()
            .await
            .map_err(|err| {
                LiveError::custom(format!(
                    "获取抖音 H5 直播间信息失败: {}",
                    redact_reqwest_error(err)
                ))
            })?
            .json()
            .await
            .map_err(|err| {
                LiveError::custom(format!(
                    "解析抖音 H5 直播间信息失败: {}",
                    redact_reqwest_error(err)
                ))
            })
    }

    /// 查 Web 接口并校验主播身份；主播与订阅的主页不一致时报错。
    async fn web_room_info(&mut self, web_rid: &str) -> LiveResult<WebRoom> {
        let response = self.get_web_room_info(web_rid).await?;
        if let Some(sec_uid) = web_user_sec_uid(self.sec_uid.as_deref(), &response)? {
            self.sec_uid = Some(sec_uid);
        }
        if web_says_ended(&response) {
            return Ok(WebRoom::Ended);
        }
        Ok(WebRoom::Room(web_room(&response)))
    }

    async fn get_room_info(&mut self) -> LiveResult<Option<Value>> {
        let mut queried_web_rid: Option<String> = None;
        let mut room_info = None;
        if let Some(web_rid) = self.web_rid.clone() {
            match self.web_room_info(&web_rid).await? {
                WebRoom::Ended => return Ok(None),
                WebRoom::Room(room) => room_info = room,
            }
            queried_web_rid = Some(web_rid);
        }

        if room_info.is_none() {
            let response = self.get_h5_room_info().await?;
            let owner_web_rid = response
                .pointer("/data/room/owner/web_rid")
                .and_then(Value::as_str)
                .filter(|web_rid| !web_rid.is_empty())
                .map(ToString::to_string);
            if let Some(web_rid) = &owner_web_rid {
                self.web_rid = Some(web_rid.clone());
            }
            room_info = response.pointer("/data/room").cloned();

            // 下播后的回放房间不代表主播此刻没在播：用 H5 给的主播 web_rid 回到 Web 查当前这场。
            if let Some(web_rid) = h5_recheck_web_rid(
                room_info.as_ref(),
                owner_web_rid.as_deref(),
                queried_web_rid.as_deref(),
            ) {
                match self.web_room_info(&web_rid).await? {
                    WebRoom::Ended => return Ok(None),
                    WebRoom::Room(room) => room_info = room,
                }
            }
        }

        let Some(room_info) = room_info else {
            return Ok(None);
        };
        if !is_live_room(&room_info) {
            return Ok(None);
        }
        self.room_id = live_session_key_from_room(&room_info);
        Ok(Some(room_info))
    }

    /// 返回（流地址，实际画质在本房间的显示名）。
    fn select_stream_url(&self, room_info: &Value) -> LiveResult<(String, String)> {
        let mut pull_data = room_info.pointer("/stream_url/live_core_sdk_data/pull_data");
        if self.double_screen
            && let Some(double_screen) = room_info
                .pointer("/stream_url/pull_datas")
                .and_then(Value::as_object)
                .and_then(|pull_datas| pull_datas.values().next())
        {
            pull_data = Some(double_screen);
        }
        let stream_data_text = pull_data
            .and_then(|pull_data| pull_data.get("stream_data"))
            .and_then(Value::as_str)
            .ok_or_else(|| LiveError::custom("抖音直播流数据为空"))?;
        let stream_data: Value = serde_json::from_str(stream_data_text)
            .map_err(|err| LiveError::custom(format!("解析抖音直播流数据失败: {err}")))?;
        let stream_data = stream_data
            .get("data")
            .and_then(Value::as_object)
            .ok_or_else(|| LiveError::custom("抖音直播流清晰度为空"))?;

        // 「真原画」藏在纯音频档 ao 里，去掉 only_audio 参数即为带画面的原画流。
        if self.true_origin
            && self.quality == "origin"
            && self.protocol != "hls"
            && let Some(url) = stream_data
                .get("ao")
                .and_then(|quality| quality.pointer("/main/flv"))
                .and_then(|value| response_urls(Some(value)).into_iter().next())
                .filter(|url| !is_replay_url(url))
        {
            let url = normalize_stream_url(&url.replace("&only_audio=1", ""));
            return Ok((url, quality_name(pull_data, "origin")));
        }

        let available: Vec<&str> = stream_data.keys().map(String::as_str).collect();
        let selected_quality = select_quality_code(&available, &self.quality)
            .ok_or_else(|| LiveError::custom("抖音没有可用清晰度"))?;
        let protocol = if self.protocol == "hls" { "hls" } else { "flv" };
        let url = stream_data
            .get(selected_quality)
            .and_then(|quality| quality.pointer(&format!("/main/{protocol}")))
            .and_then(|value| response_urls(Some(value)).into_iter().next())
            .filter(|url| !is_replay_url(url))
            .map(normalize_stream_url)
            .ok_or_else(|| LiveError::custom("抖音可用直播流为空"))?;
        Ok((url, quality_name(pull_data, selected_quality)))
    }

    fn stream_headers(&self) -> HashMap<String, String> {
        HashMap::from([
            ("referer".to_string(), DOUYIN_LIVE_URL.to_string()),
            ("user-agent".to_string(), DOUYIN_USER_AGENT.to_string()),
            ("cookie".to_string(), self.cookie.clone()),
        ])
    }
}

const QUALITY_CODES: [&str; 6] = ["origin", "uhd", "hd", "sd", "ld", "md"];

/// 房间在直播：status=2，且没有结束时间。下播后跳转的回放房间仍回 status=2，
/// 但已写入 finish_time（biliup #1663）。
fn is_live_room(room_info: &Value) -> bool {
    room_info.get("status").and_then(Value::as_i64) == Some(2)
        && room_info
            .get("finish_time")
            .and_then(Value::as_i64)
            .unwrap_or(0)
            <= 0
}

/// Web 接口响应里的当前房间（`data.data[0]`）。
fn web_room(response: &Value) -> Option<Value> {
    response
        .pointer("/data/data")
        .and_then(Value::as_array)
        .and_then(|data| data.first())
        .cloned()
}

/// Web 接口明确告知直播已结束：没有主播信息且提示「直播已结束」。
fn web_says_ended(response: &Value) -> bool {
    response.pointer("/data/user").is_none()
        && response.pointer("/data/prompts").and_then(Value::as_str) == Some("直播已结束")
}

/// 取 Web 响应里的主播 sec_uid；与订阅的主播不是同一人时报错，免得录到别人。
fn web_user_sec_uid(expected: Option<&str>, response: &Value) -> LiveResult<Option<String>> {
    let actual = response
        .pointer("/data/user/sec_uid")
        .and_then(Value::as_str)
        .filter(|sec_uid| !sec_uid.is_empty());
    if let (Some(expected), Some(actual)) = (expected, actual)
        && expected != actual
    {
        return Err(LiveError::custom("抖音直播间主播与订阅的主页不一致"));
    }
    Ok(actual.map(ToString::to_string))
}

/// H5 回的房间不在播（多为下播后的回放房间）时，返回应回到 Web 接口复核的主播 web_rid。
/// 回放只说明那一场已结束，不代表主播此刻没在播。本轮已查过同一个 web_rid 时不重复查。
fn h5_recheck_web_rid(
    h5_room: Option<&Value>,
    owner_web_rid: Option<&str>,
    queried_web_rid: Option<&str>,
) -> Option<String> {
    if h5_room.is_some_and(is_live_room) {
        return None;
    }
    let owner_web_rid = owner_web_rid.filter(|web_rid| !web_rid.is_empty())?;
    if Some(owner_web_rid) == queried_web_rid {
        return None;
    }
    Some(owner_web_rid.to_string())
}

/// 回放流的地址带 reflow 标记，不能当直播流录（biliup #1663）。
fn is_replay_url(url: &str) -> bool {
    url.contains("rtm_expr_tag=reflow_room_info")
}

fn normalize_stream_url(url: &str) -> String {
    url.strip_prefix("http://")
        .map(|rest| format!("https://{rest}"))
        .unwrap_or_else(|| url.to_string())
}

fn redact_reqwest_error(mut error: reqwest::Error) -> String {
    if let Some(url) = error.url_mut() {
        url.set_query(None);
        url.set_fragment(None);
    }
    error.to_string()
}

fn response_urls(value: Option<&Value>) -> Vec<&str> {
    match value {
        Some(Value::String(url)) => (!url.trim().is_empty())
            .then_some(url.trim())
            .into_iter()
            .collect(),
        Some(Value::Array(urls)) => urls
            .iter()
            .filter_map(Value::as_str)
            .map(str::trim)
            .filter(|url| !url.is_empty())
            .collect(),
        _ => Vec::new(),
    }
}

/// The platform's id for *this* broadcast, taken from the room payload.
///
/// Stable across process restarts within one live stream, which is what session continuation
/// needs; `date` cannot serve that role because every platform fills it with `Utc::now()` at
/// detection time, not with the broadcast's start.
fn live_session_key_from_room(room_info: &Value) -> Option<String> {
    room_info
        .get("id_str")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
        .map(ToString::to_string)
}

/// 画质代码在本房间的显示名。各房间档位叫法不同（同是 origin，有的房间叫「超清」，有的叫「原画」），
/// 先取 `pull_data.options.qualities` 里同 `sdk_key` 的 `name`；没有（如 origin 不在列表里、
/// Web 接口不给 options）再按常见叫法兜底。
fn quality_name(pull_data: Option<&Value>, code: &str) -> String {
    let named = pull_data
        .and_then(|pull_data| pull_data.pointer("/options/qualities"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .find(|quality| quality.get("sdk_key").and_then(Value::as_str) == Some(code))
        .and_then(|quality| quality.get("name").and_then(Value::as_str))
        .filter(|name| !name.is_empty());
    let name = named.unwrap_or(match code {
        "origin" => "原画",
        "uhd" => "蓝光",
        "hd" => "超清",
        "sd" => "高清",
        "ld" => "标清",
        "md" => "流畅",
        other => other,
    });
    name.to_string()
}

/// 在 available 列表中，按 origin>uhd>hd>sd>ld>md 的优先级为 requested 选一档可用画质：
/// 命中则用之；否则先往更低档找，再往更高档找；都没有返回 None。
fn select_quality_code(available: &[&str], requested: &str) -> Option<&'static str> {
    let requested = if QUALITY_CODES.contains(&requested) {
        requested
    } else {
        "origin"
    };
    let idx = QUALITY_CODES
        .iter()
        .position(|i| *i == requested)
        .unwrap_or(0);
    let has = |q: &str| available.contains(&q);
    if has(requested) {
        return QUALITY_CODES.iter().copied().find(|i| *i == requested);
    }
    QUALITY_CODES[idx + 1..]
        .iter()
        .copied()
        .find(|i| has(i))
        .or_else(|| QUALITY_CODES[..idx].iter().rev().copied().find(|i| has(i)))
}

/// 抖音网页端接口的公共 query 参数。
pub(crate) fn common_params() -> Vec<(String, String)> {
    vec![
        ("app_name".to_string(), "douyin_web".to_string()),
        ("enter_from".to_string(), "web_live".to_string()),
        ("live_id".to_string(), "1".to_string()),
        ("aid".to_string(), "6383".to_string()),
        ("compress".to_string(), "gzip".to_string()),
        ("device_platform".to_string(), "web".to_string()),
        ("browser_language".to_string(), "zh-CN".to_string()),
        ("browser_platform".to_string(), "Win32".to_string()),
        ("browser_name".to_string(), "Mozilla".to_string()),
        (
            "browser_version".to_string(),
            DOUYIN_USER_AGENT
                .split("Chrome/")
                .nth(1)
                .and_then(|part| part.split(' ').next())
                .unwrap_or("0.0.0.0")
                .to_string(),
        ),
    ]
}

/// 编码 query 并追加 a_bogus 签名。
fn sign_query(params: &[(String, String)]) -> String {
    let query = url::form_urlencoded::Serializer::new(String::new())
        .extend_pairs(params)
        .finish();
    ABogus::new(Some(DOUYIN_USER_AGENT)).generate_abogus(&query, "")
}

fn capture(input: &str, pattern: &str) -> Option<String> {
    Regex::new(pattern)
        .ok()?
        .captures(input)
        .map(|captures| captures[1].to_string())
}

/// 向字节跳动注册接口申请匿名 `ttwid` cookie 值，失败时退回内置默认值。
pub(crate) async fn fetch_ttwid(client: &Client) -> String {
    let resp = client
        .post(DOUYIN_UNION_REGISTER_URL)
        .header(USER_AGENT, DOUYIN_USER_AGENT)
        .json(&json!({
            "region": "cn",
            "aid": 6383,
            "needFid": false,
            "service": DOUYIN_BASE_URL,
            "union": true,
            "fid": ""
        }))
        .send()
        .await;
    let Ok(resp) = resp else {
        return DEFAULT_TTWID.to_string();
    };
    resp.headers()
        .get_all("set-cookie")
        .iter()
        .find_map(|value| {
            let cookie = value.to_str().ok()?;
            cookie
                .split(';')
                .next()?
                .strip_prefix("ttwid=")
                .map(ToString::to_string)
        })
        .unwrap_or_else(|| DEFAULT_TTWID.to_string())
}

/// 生成 184 位随机 `msToken`。
pub(crate) fn generate_ms_token() -> String {
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-";
    random_from_charset(CHARSET, 184)
}

fn generate_nonce() -> String {
    random_from_charset(b"abcdef0123456789", 21)
}

fn generate_odin_ttid() -> String {
    random_from_charset(b"abcdef0123456789", 160)
}

fn random_from_charset(charset: &[u8], len: usize) -> String {
    let mut rng = rand::thread_rng();
    (0..len)
        .map(|_| charset[rng.gen_range(0..charset.len())] as char)
        .collect()
}

const BASE_CHARS: &[u8; 62] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
const BASE36_CHARS: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";
const FIXED_UNDERSCORE_POSITIONS: [usize; 4] = [8, 13, 18, 23];
const FIXED_4_POSITION: usize = 14;
const VARIANT_POSITION: usize = 19;
const UUID_PART_LEN: usize = 36;
const MAX_BASE36_LEN: usize = 13;

fn gen_verify_fp() -> String {
    let mut rng = rand::thread_rng();
    let milliseconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    let mut base36_buf = [0u8; MAX_BASE36_LEN];
    let base36_len = write_base36(milliseconds, &mut base36_buf);
    let mut uuid_buf = [0u8; UUID_PART_LEN];
    let base_len = BASE_CHARS.len();

    for (i, byte) in uuid_buf.iter_mut().enumerate() {
        *byte = if FIXED_UNDERSCORE_POSITIONS.contains(&i) {
            b'_'
        } else if i == FIXED_4_POSITION {
            b'4'
        } else {
            let n = rng.gen_range(0..base_len);
            let char_idx = if i == VARIANT_POSITION {
                (3 & n) | 8
            } else {
                n
            };
            BASE_CHARS[char_idx]
        };
    }

    let mut result = String::with_capacity(7 + base36_len + 1 + UUID_PART_LEN);
    result.push_str("verify_");
    result.push_str(std::str::from_utf8(&base36_buf[MAX_BASE36_LEN - base36_len..]).unwrap_or(""));
    result.push('_');
    result.push_str(std::str::from_utf8(&uuid_buf).unwrap_or(""));
    result
}

fn write_base36(mut num: u64, buf: &mut [u8; MAX_BASE36_LEN]) -> usize {
    if num == 0 {
        buf[MAX_BASE36_LEN - 1] = b'0';
        return 1;
    }
    let mut pos = MAX_BASE36_LEN;
    while num > 0 {
        pos -= 1;
        buf[pos] = BASE36_CHARS[(num % 36) as usize];
        num /= 36;
    }
    MAX_BASE36_LEN - pos
}

struct StringProcessor;

impl StringProcessor {
    fn to_char_str(bytes: &[u8]) -> String {
        bytes.iter().map(|&byte| byte as char).collect()
    }

    fn to_char_array(input: &str) -> Vec<u8> {
        input.bytes().collect()
    }

    fn generate_random_bytes(length: usize) -> String {
        let mut rng = rand::thread_rng();
        let mut result = Vec::new();
        for _ in 0..length {
            let rd = rng.gen_range(0..10000) as u64;
            result.extend([
                (((rd & 255) & 170) | 1) as u8,
                (((rd & 255) & 85) | 2) as u8,
                (((rd >> 8) & 170) | 5) as u8,
                (((rd >> 8) & 85) | 40) as u8,
            ]);
        }
        Self::to_char_str(&result)
    }
}

struct CryptoUtility {
    salt: String,
    base64_alphabet: Vec<Vec<char>>,
    big_array: Vec<u8>,
}

impl CryptoUtility {
    fn new(salt: &str, custom_base64_alphabet: Vec<&str>) -> Self {
        let base64_alphabet = custom_base64_alphabet
            .into_iter()
            .map(|value| value.chars().collect())
            .collect();
        let big_array = vec![
            121, 243, 55, 234, 103, 36, 47, 228, 30, 231, 106, 6, 115, 95, 78, 101, 250, 207, 198,
            50, 139, 227, 220, 105, 97, 143, 34, 28, 194, 215, 18, 100, 159, 160, 43, 8, 169, 217,
            180, 120, 247, 45, 90, 11, 27, 197, 46, 3, 84, 72, 5, 68, 62, 56, 221, 75, 144, 79, 73,
            161, 178, 81, 64, 187, 134, 117, 186, 118, 16, 241, 130, 71, 89, 147, 122, 129, 65, 40,
            88, 150, 110, 219, 199, 255, 181, 254, 48, 4, 195, 248, 208, 32, 116, 167, 69, 201, 17,
            124, 125, 104, 96, 83, 80, 127, 236, 108, 154, 126, 204, 15, 20, 135, 112, 158, 13, 1,
            188, 164, 210, 237, 222, 98, 212, 77, 253, 42, 170, 202, 26, 22, 29, 182, 251, 10, 173,
            152, 58, 138, 54, 141, 185, 33, 157, 31, 252, 132, 233, 235, 102, 196, 191, 223, 240,
            148, 39, 123, 92, 82, 128, 109, 57, 24, 38, 113, 209, 245, 2, 119, 153, 229, 189, 214,
            230, 174, 232, 63, 52, 205, 86, 140, 66, 175, 111, 171, 246, 133, 238, 193, 99, 60, 74,
            91, 225, 51, 76, 37, 145, 211, 166, 151, 213, 206, 0, 200, 244, 176, 218, 44, 184, 172,
            49, 216, 93, 168, 53, 21, 183, 41, 67, 85, 224, 155, 226, 242, 87, 177, 146, 70, 190,
            12, 162, 19, 137, 114, 25, 165, 163, 192, 23, 59, 9, 94, 179, 107, 35, 7, 142, 131,
            239, 203, 149, 136, 61, 249, 14, 156,
        ];
        Self {
            salt: salt.to_string(),
            base64_alphabet,
            big_array,
        }
    }

    fn sm3_to_array(input_data: &[u8]) -> Vec<u8> {
        Sm3Hash::new(input_data).get_hash().to_vec()
    }

    fn params_to_array(&self, param: &str, add_salt: bool) -> Vec<u8> {
        let processed_param = if add_salt {
            format!("{param}{}", self.salt)
        } else {
            param.to_string()
        };
        Self::sm3_to_array(processed_param.as_bytes())
    }

    fn transform_bytes(&mut self, values_list: &[u32]) -> Vec<u32> {
        let mut result_vec = Vec::with_capacity(values_list.len());
        let mut index_b = self.big_array[1] as usize;
        let mut initial_value: u8 = 0;
        let mut value_e: u8 = 0;
        let array_len = self.big_array.len();

        for (index, &char_code) in values_list.iter().enumerate() {
            let sum_initial = if index == 0 {
                initial_value = self.big_array[index_b];
                let sum_val = (index_b as u8).wrapping_add(initial_value);
                self.big_array[1] = initial_value;
                self.big_array[index_b] = index_b as u8;
                sum_val
            } else {
                initial_value.wrapping_add(value_e)
            };

            let sum_initial_idx = (sum_initial as usize) % array_len;
            let value_f = self.big_array[sum_initial_idx];
            result_vec.push(char_code ^ (value_f as u32));

            let next_idx = (index + 2) % array_len;
            value_e = self.big_array[next_idx];
            let new_sum_initial_idx = ((index_b as u8).wrapping_add(value_e) as usize) % array_len;
            initial_value = self.big_array[new_sum_initial_idx];
            self.big_array.swap(new_sum_initial_idx, next_idx);
            index_b = new_sum_initial_idx;
        }
        result_vec
    }

    fn base64_encode(&self, bytes: &[u8], selected_alphabet: usize) -> String {
        let alphabet = &self.base64_alphabet[selected_alphabet];
        let mut output_string = String::with_capacity((bytes.len() * 4).div_ceil(3));
        for chunk in bytes.chunks(3) {
            let b1 = chunk[0];
            let b2 = chunk.get(1).copied().unwrap_or(0);
            let b3 = chunk.get(2).copied().unwrap_or(0);
            let combined = ((b1 as u32) << 16) | ((b2 as u32) << 8) | (b3 as u32);
            output_string.push(alphabet[((combined >> 18) & 63) as usize]);
            output_string.push(alphabet[((combined >> 12) & 63) as usize]);
            if chunk.len() > 1 {
                output_string.push(alphabet[((combined >> 6) & 63) as usize]);
            }
            if chunk.len() > 2 {
                output_string.push(alphabet[(combined & 63) as usize]);
            }
        }
        let padding_needed = (4 - output_string.len() % 4) % 4;
        if padding_needed > 0 {
            output_string.push_str(&"=".repeat(padding_needed));
        }
        output_string
    }

    fn abogus_encode(&self, values: &[u32], selected_alphabet: usize) -> String {
        let alphabet = &self.base64_alphabet[selected_alphabet];
        let mut abogus = String::with_capacity((values.len() * 4).div_ceil(3));
        for chunk in values.chunks(3) {
            let v1 = chunk[0];
            let v2 = chunk.get(1).copied().unwrap_or(0);
            let v3 = chunk.get(2).copied().unwrap_or(0);
            let n = (v1 << 16) | (v2 << 8) | v3;
            abogus.push(alphabet[((n & 0xFC0000) >> 18) as usize]);
            abogus.push(alphabet[((n & 0x03F000) >> 12) as usize]);
            if chunk.len() > 1 {
                abogus.push(alphabet[((n & 0x0FC0) >> 6) as usize]);
            }
            if chunk.len() > 2 {
                abogus.push(alphabet[(n & 0x3F) as usize]);
            }
        }
        let padding = (4 - abogus.len() % 4) % 4;
        if padding > 0 {
            abogus.push_str(&"=".repeat(padding));
        }
        abogus
    }

    fn rc4_encrypt(key: &[u8], plaintext: &str) -> Vec<u8> {
        let mut state: [u8; 256] = [0; 256];
        for (i, elem) in state.iter_mut().enumerate() {
            *elem = i as u8;
        }
        let mut j: u8 = 0;
        for i in 0..256 {
            j = j.wrapping_add(state[i]).wrapping_add(key[i % key.len()]);
            state.swap(i, j as usize);
        }
        let mut i: u8 = 0;
        let mut j: u8 = 0;
        let mut ciphertext = Vec::with_capacity(plaintext.len());
        for &char_val in plaintext.as_bytes() {
            i = i.wrapping_add(1);
            j = j.wrapping_add(state[i as usize]);
            state.swap(i as usize, j as usize);
            let k = state[state[i as usize].wrapping_add(state[j as usize]) as usize];
            ciphertext.push(char_val ^ k);
        }
        ciphertext
    }
}

struct BrowserFingerprintGenerator;

impl BrowserFingerprintGenerator {
    fn generate_fingerprint() -> String {
        let mut rng = rand::thread_rng();
        let inner_width = rng.gen_range(1024..=1920);
        let inner_height = rng.gen_range(768..=1080);
        let outer_width = inner_width + rng.gen_range(24..=32);
        let outer_height = inner_height + rng.gen_range(75..=90);
        let screen_y = [0, 30].choose(&mut rng).copied().unwrap_or(0);
        let size_width = rng.gen_range(1024..=1920);
        let size_height = rng.gen_range(768..=1080);
        let avail_width = rng.gen_range(1280..=1920);
        let avail_height = rng.gen_range(800..=1080);
        format!(
            "{inner_width}|{inner_height}|{outer_width}|{outer_height}|0|{screen_y}|0|0|{size_width}|{size_height}|{avail_width}|{avail_height}|{inner_width}|{inner_height}|24|24|Win32",
        )
    }
}

#[cfg(test)]
mod quality_tests {
    use super::{quality_name, select_quality_code};
    use serde_json::json;

    #[test]
    fn quality_name_comes_from_the_room_options() {
        // 房间甲：origin 在 qualities 里叫「超清」。
        let room_a = json!({"options": {"qualities": [
            {"name": "标清", "sdk_key": "ld", "level": 1, "resolution": "960x540"},
            {"name": "高清", "sdk_key": "sd", "level": 2, "resolution": "1280x720"},
            {"name": "超清", "sdk_key": "origin", "level": 3, "resolution": ""}
        ]}});
        assert_eq!(quality_name(Some(&room_a), "origin"), "超清");
        assert_eq!(quality_name(Some(&room_a), "sd"), "高清");
        // 房间乙：qualities 里没有 origin，按兜底叫「原画」。
        let room_b = json!({"options": {"qualities": [
            {"name": "标清", "sdk_key": "ld"},
            {"name": "高清", "sdk_key": "sd"},
            {"name": "超清", "sdk_key": "hd"},
            {"name": "蓝光", "sdk_key": "uhd"}
        ]}});
        assert_eq!(quality_name(Some(&room_b), "origin"), "原画");
        assert_eq!(quality_name(Some(&room_b), "uhd"), "蓝光");
    }

    #[test]
    fn quality_name_falls_back_without_options() {
        let no_options = json!({"stream_data": "{}"});
        assert_eq!(quality_name(Some(&no_options), "origin"), "原画");
        assert_eq!(quality_name(Some(&no_options), "hd"), "超清");
        assert_eq!(quality_name(None, "md"), "流畅");
    }

    #[test]
    fn requested_available_returns_itself() {
        assert_eq!(
            select_quality_code(&["origin", "hd"], "origin"),
            Some("origin")
        );
    }

    #[test]
    fn missing_requested_falls_back_to_next_lower() {
        // 请求 origin 不在，向低优先：uhd
        assert_eq!(select_quality_code(&["uhd", "hd"], "origin"), Some("uhd"));
    }

    #[test]
    fn falls_back_upward_when_only_higher_exists() {
        // 请求 sd 不在、低档也没有，回退到更高档 hd
        assert_eq!(select_quality_code(&["origin", "hd"], "sd"), Some("hd"));
    }

    #[test]
    fn none_when_empty() {
        assert_eq!(select_quality_code(&[], "origin"), None);
    }
}

struct ABogus {
    crypto_utility: CryptoUtility,
    user_agent: String,
    browser_fp: String,
    options: Vec<u64>,
    page_id: u64,
    aid: u64,
    ua_key: Vec<u8>,
    sort_index: Vec<u8>,
    sort_index_2: Vec<u8>,
}

impl ABogus {
    fn new(user_agent: Option<&str>) -> Self {
        Self {
            crypto_utility: CryptoUtility::new(
                "cus",
                vec![
                    "Dkdpgh2ZmsQB80/MfvV36XI1R45-WUAlEixNLwoqYTOPuzKFjJnry79HbGcaStCe",
                    "ckdp1h4ZKsUB80/Mfvw36XIgR25+WQAlEi7NLboqYTOPuzmFjJnryx9HVGDaStCe",
                ],
            ),
            user_agent: user_agent.unwrap_or(DOUYIN_USER_AGENT).to_string(),
            browser_fp: BrowserFingerprintGenerator::generate_fingerprint(),
            options: vec![0, 1, 14],
            page_id: 0,
            aid: 6383,
            ua_key: vec![0x00, 0x01, 0x0E],
            sort_index: vec![
                18, 20, 52, 26, 30, 34, 58, 38, 40, 53, 42, 21, 27, 54, 55, 31, 35, 57, 39, 41, 43,
                22, 28, 32, 60, 36, 23, 29, 33, 37, 44, 45, 59, 46, 47, 48, 49, 50, 24, 25, 65, 66,
                70, 71,
            ],
            sort_index_2: vec![
                18, 20, 26, 30, 34, 38, 40, 42, 21, 27, 31, 35, 39, 41, 43, 22, 28, 32, 36, 23, 29,
                33, 37, 44, 45, 46, 47, 48, 49, 50, 24, 25, 52, 53, 54, 55, 57, 58, 59, 60, 65, 66,
                70, 71,
            ],
        }
    }

    fn generate_abogus(&mut self, params: &str, body: &str) -> String {
        let mut ab_dir: HashMap<u8, u64> = HashMap::new();
        ab_dir.insert(8, 3);
        ab_dir.insert(18, 44);
        ab_dir.insert(66, 0);
        ab_dir.insert(69, 0);
        ab_dir.insert(70, 0);
        ab_dir.insert(71, 0);

        let start_encryption = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let params_hash_1 = self.crypto_utility.params_to_array(params, true);
        let array1 = CryptoUtility::sm3_to_array(&params_hash_1);
        let body_hash_1 = self.crypto_utility.params_to_array(body, true);
        let array2 = CryptoUtility::sm3_to_array(&body_hash_1);
        let rc4_ua = CryptoUtility::rc4_encrypt(&self.ua_key, &self.user_agent);
        let ua_b64 = self.crypto_utility.base64_encode(&rc4_ua, 1);
        let array3 = self.crypto_utility.params_to_array(&ua_b64, false);
        let end_encryption = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(start_encryption);

        ab_dir.insert(20, (start_encryption >> 24) & 255);
        ab_dir.insert(21, (start_encryption >> 16) & 255);
        ab_dir.insert(22, (start_encryption >> 8) & 255);
        ab_dir.insert(23, start_encryption & 255);
        ab_dir.insert(24, start_encryption / 0x100000000);
        ab_dir.insert(25, start_encryption / 0x10000000000);
        ab_dir.insert(26, (self.options[0] >> 24) & 255);
        ab_dir.insert(27, (self.options[0] >> 16) & 255);
        ab_dir.insert(28, (self.options[0] >> 8) & 255);
        ab_dir.insert(29, self.options[0] & 255);
        ab_dir.insert(30, (self.options[1] / 256) & 255);
        ab_dir.insert(31, self.options[1] % 256);
        ab_dir.insert(32, (self.options[1] >> 24) & 255);
        ab_dir.insert(33, (self.options[1] >> 16) & 255);
        ab_dir.insert(34, (self.options[2] >> 24) & 255);
        ab_dir.insert(35, (self.options[2] >> 16) & 255);
        ab_dir.insert(36, (self.options[2] >> 8) & 255);
        ab_dir.insert(37, self.options[2] & 255);
        ab_dir.insert(38, array1[21] as u64);
        ab_dir.insert(39, array1[22] as u64);
        ab_dir.insert(40, array2[21] as u64);
        ab_dir.insert(41, array2[22] as u64);
        ab_dir.insert(42, array3[23] as u64);
        ab_dir.insert(43, array3[24] as u64);
        ab_dir.insert(44, (end_encryption >> 24) & 255);
        ab_dir.insert(45, (end_encryption >> 16) & 255);
        ab_dir.insert(46, (end_encryption >> 8) & 255);
        ab_dir.insert(47, end_encryption & 255);
        ab_dir.insert(48, *ab_dir.get(&8).unwrap_or(&0));
        ab_dir.insert(49, end_encryption / 0x100000000);
        ab_dir.insert(50, end_encryption / 0x10000000000);
        ab_dir.insert(51, (self.page_id >> 24) & 255);
        ab_dir.insert(52, (self.page_id >> 16) & 255);
        ab_dir.insert(53, (self.page_id >> 8) & 255);
        ab_dir.insert(54, self.page_id & 255);
        ab_dir.insert(55, self.page_id);
        ab_dir.insert(56, self.aid);
        ab_dir.insert(57, self.aid & 255);
        ab_dir.insert(58, (self.aid >> 8) & 255);
        ab_dir.insert(59, (self.aid >> 16) & 255);
        ab_dir.insert(60, (self.aid >> 24) & 255);
        ab_dir.insert(64, self.browser_fp.len() as u64);
        ab_dir.insert(65, self.browser_fp.len() as u64);

        let mut sorted_values: Vec<u32> = self
            .sort_index
            .iter()
            .map(|&i| *ab_dir.get(&i).unwrap_or(&0) as u32)
            .collect();
        let fp_array = StringProcessor::to_char_array(&self.browser_fp);
        let mut ab_xor: u32 = 0;
        for (index, &key) in self.sort_index_2.iter().enumerate() {
            let val = *ab_dir.get(&key).unwrap_or(&0) as u32;
            if index == 0 {
                ab_xor = val;
            } else {
                ab_xor ^= val;
            }
        }
        sorted_values.extend(fp_array.iter().map(|&b| b as u32));
        sorted_values.push(ab_xor);

        let transformed_values = self.crypto_utility.transform_bytes(&sorted_values);
        let random_prefix: Vec<u32> = StringProcessor::generate_random_bytes(3)
            .chars()
            .map(|c| c as u32)
            .collect();
        let final_values: Vec<u32> = random_prefix
            .into_iter()
            .chain(transformed_values)
            .collect();
        let abogus = self.crypto_utility.abogus_encode(&final_values, 0);
        format!("{params}&a_bogus={abogus}")
    }
}

#[cfg(test)]
mod replay_tests {
    use super::{is_live_room, is_replay_url};
    use serde_json::json;

    #[test]
    fn a_replay_room_with_finish_time_is_not_live() {
        assert!(is_live_room(&json!({"status": 2})));
        assert!(is_live_room(&json!({"status": 2, "finish_time": 0})));
        assert!(!is_live_room(
            &json!({"status": 2, "finish_time": 1724300000})
        ));
        assert!(!is_live_room(&json!({"status": 4})));
    }

    #[test]
    fn a_reflow_stream_url_is_a_replay() {
        assert!(is_replay_url(
            "https://pull.example/stage/x.flv?rtm_expr_tag=reflow_room_info&sign=1"
        ));
        assert!(!is_replay_url("https://pull.example/stage/x.flv?sign=1"));
    }
}

#[cfg(test)]
mod live_session_key_tests {
    use super::live_session_key_from_room;
    use serde_json::json;

    #[test]
    fn takes_the_room_id_from_the_room_payload() {
        let room = json!({"id_str": "7412345678901234567", "status": 2});

        assert_eq!(
            live_session_key_from_room(&room).as_deref(),
            Some("7412345678901234567")
        );
    }

    /// A key that is absent or blank must stay `None`: continuation falls back to the clock
    /// window, and an empty-string key would match every other keyless session.
    #[test]
    fn a_missing_or_blank_room_id_yields_no_key() {
        assert_eq!(live_session_key_from_room(&json!({"status": 2})), None);
        assert_eq!(live_session_key_from_room(&json!({"id_str": ""})), None);
    }

    #[test]
    fn the_key_carries_no_url_cookie_or_signature() {
        let room = json!({
            "id_str": "7412345678901234567",
            "stream_url": {"flv_pull_url": {"FULL_HD1": "https://pull.example/live.flv?sign=deadbeef"}},
        });

        let key = live_session_key_from_room(&room).unwrap();
        assert!(!key.contains("://") && !key.contains("sign") && !key.contains('='));
    }
}

#[cfg(test)]
mod h5_recheck_tests {
    use super::{
        h5_recheck_web_rid, is_live_room, live_session_key_from_room, web_room, web_says_ended,
        web_user_sec_uid,
    };
    use serde_json::json;

    #[test]
    fn replay_room_triggers_web_recheck() {
        assert_eq!(
            h5_recheck_web_rid(
                Some(&json!({"status": 2, "finish_time": 1761600000})),
                Some("897251368473"),
                None,
            ),
            Some("897251368473".into())
        );
    }

    #[test]
    fn non_live_status_with_owner_triggers_recheck() {
        assert_eq!(
            h5_recheck_web_rid(Some(&json!({"status": 4})), Some("897251368473"), None),
            Some("897251368473".into())
        );
    }

    #[test]
    fn live_h5_room_skips_recheck() {
        assert_eq!(
            h5_recheck_web_rid(
                Some(&json!({"status": 2, "finish_time": 0})),
                Some("897251368473"),
                None,
            ),
            None
        );
    }

    #[test]
    fn missing_or_empty_owner_web_rid_skips_recheck() {
        let room = json!({"status": 2, "finish_time": 1761600000});
        assert_eq!(h5_recheck_web_rid(Some(&room), None, None), None);
        assert_eq!(h5_recheck_web_rid(Some(&room), Some(""), None), None);
    }

    #[test]
    fn already_queried_web_rid_is_not_rechecked() {
        let room = json!({"status": 2, "finish_time": 1761600000});
        assert_eq!(
            h5_recheck_web_rid(Some(&room), Some("897251368473"), Some("897251368473")),
            None
        );
        assert_eq!(
            h5_recheck_web_rid(Some(&room), Some("897251368473"), Some("111")),
            Some("897251368473".into())
        );
    }

    #[test]
    fn missing_h5_room_with_owner_triggers_recheck() {
        assert_eq!(
            h5_recheck_web_rid(None, Some("897251368473"), None),
            Some("897251368473".into())
        );
    }

    #[test]
    fn web_user_sec_uid_validates_expected_streamer() {
        let resp = json!({"data": {"user": {"sec_uid": "MS4w_abc"}}});
        assert_eq!(
            web_user_sec_uid(Some("MS4w_abc"), &resp).unwrap(),
            Some("MS4w_abc".to_string())
        );
        assert!(web_user_sec_uid(Some("MS4w_other"), &resp).is_err());
        assert_eq!(
            web_user_sec_uid(None, &resp).unwrap(),
            Some("MS4w_abc".to_string())
        );
        assert_eq!(
            web_user_sec_uid(Some("MS4w_abc"), &json!({"data": {}})).unwrap(),
            None
        );
        assert_eq!(
            web_user_sec_uid(
                Some("MS4w_abc"),
                &json!({"data": {"user": {"sec_uid": ""}}})
            )
            .unwrap(),
            None
        );
    }

    #[test]
    fn web_says_ended_requires_missing_user_and_ended_prompt() {
        assert!(web_says_ended(&json!({"data": {"prompts": "直播已结束"}})));
        assert!(!web_says_ended(
            &json!({"data": {"user": {}, "prompts": "直播已结束"}})
        ));
        assert!(!web_says_ended(&json!({"data": {}})));
    }

    #[test]
    fn web_room_extracts_first_room_or_none() {
        let resp = json!({"data": {"data": [{"id_str": "1", "status": 2}]}});
        assert_eq!(web_room(&resp), Some(json!({"id_str": "1", "status": 2})));
        assert_eq!(web_room(&json!({"data": {"data": []}})), None);
        assert_eq!(web_room(&json!({})), None);
    }

    #[test]
    fn milkyway_stale_h5_replay_recovers_active_web_room() {
        let h5_room = json!({
            "id_str": "7701000000000000001",
            "status": 2,
            "finish_time": 1761600000,
            "owner": {"web_rid": "897251368473"}
        });
        assert_eq!(
            h5_recheck_web_rid(Some(&h5_room), Some("897251368473"), None).as_deref(),
            Some("897251368473")
        );

        let web = json!({
            "data": {
                "user": {"sec_uid": "MS4w_milkyway"},
                "data": [{"id_str": "7701604493632211762", "status": 2}]
            }
        });
        assert_eq!(
            web_user_sec_uid(Some("MS4w_milkyway"), &web)
                .unwrap()
                .as_deref(),
            Some("MS4w_milkyway")
        );
        let room = web_room(&web).unwrap();
        assert!(is_live_room(&room));
        assert_eq!(
            live_session_key_from_room(&room).as_deref(),
            Some("7701604493632211762")
        );
    }

    #[test]
    fn web_recheck_still_rejects_replay_room() {
        let web = json!({
            "data": {
                "data": [{"id_str": "1", "status": 2, "finish_time": 1761600000}]
            }
        });
        let room = web_room(&web).unwrap();
        assert!(!is_live_room(&room));
    }
}
