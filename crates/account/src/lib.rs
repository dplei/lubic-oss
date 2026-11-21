//! B 站账号侧：扫码登录、令牌校验与刷新、preupload、提交稿件。
//!
//! 整个工程里只有这个 crate 碰 cookie / access_token。上传 crate 只拿 preupload 的结果（凭证），
//! 录制 crate 完全不知道它。第 3 步它会整体挪进总控。
//!
//! 登录信息沿用 biliup 的 `cookies.json` 格式（`cookie_info` / `token_info` / `platform`）。
//! 存在哪由调用方定：本 crate 只拿一份 JSON，令牌刷新后经保存回调交回去。

use md5::{Digest, Md5};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tracing::{info, warn};

const WEB_UA: &str = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36";
const APP_UA: &str = "Mozilla/5.0 BiliDroid/7.80.0 (bbcallen@gmail.com) os/android model/MI 6 mobi_app/android build/7800300 channel/bili innerVer/7800310 osVer/13 network/2";
/// 多久向 B 站确认一次令牌是否该刷新。
const REFRESH_CHECK_EVERY: Duration = Duration::from_secs(6 * 3600);

/// (appkey, appsec)，与 biliup 的 `AppKeyStore` 相同。
const TV: (&str, &str) = ("4409e2ce8ffd12b8", "59b43e04ad6965f34319062b478f83dd");
const ANDROID: (&str, &str) = ("783bbb7264451d82", "2653583c8873dea268ab9386918b1d65");

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("http: {0}")]
    Http(#[from] reqwest::Error),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("bilibili: {0}")]
    Api(String),
}

pub type Result<T> = std::result::Result<T, Error>;

/// 提交失败要分清「肯定没投上」和「不知道投没投上」：后者自动重试可能投出重复稿件。
#[derive(Debug, thiserror::Error)]
pub enum SubmitError {
    /// 请求没发出去（连不上等），可以放心重试。
    #[error("not sent: {0}")]
    NotSent(String),
    /// B 站明确拒绝（code != 0），没有产生稿件。
    #[error("rejected {code}: {message}")]
    Rejected { code: i64, message: String },
    /// 请求已发出但没拿到可解析的结果，稿件可能已经生成。
    #[error("unknown outcome: {0}")]
    Unknown(String),
}

/// 一个分P：`filename` 是 UPOS 文件名（`Ticket::filename()`）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Video {
    pub title: String,
    pub filename: String,
    #[serde(default)]
    pub desc: String,
}

/// 稿件。字段名与 B 站投稿接口一致，配置文件里的投稿模板也直接反序列化成它。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Studio {
    pub title: String,
    /// 分区，171 = 电子竞技。
    pub tid: u16,
    /// 新版人选分区，与旧 `tid` 独立；旧模板不发送此字段。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub human_type2: Option<u16>,
    /// 标签，逗号分隔。
    pub tag: String,
    /// 1 自制，2 转载（转载必须填 `source`）。
    pub copyright: u8,
    pub source: String,
    pub desc: String,
    /// 封面图片 URL（`Account::cover_up` 的结果），空则由 B 站截帧。
    pub cover: String,
    /// 1 = 仅自己可见。
    pub is_only_self: u8,
    /// 1 = 投稿不推送粉丝动态（biliup 模板里 `extra_fields = {"no_disturbance": 1}`，已在生产验证）。
    pub no_disturbance: u8,
    pub videos: Vec<Video>,
}

impl Default for Studio {
    fn default() -> Self {
        Self {
            title: String::new(),
            tid: 171,
            human_type2: None,
            tag: String::new(),
            copyright: 1,
            source: String::new(),
            desc: String::new(),
            cover: String::new(),
            is_only_self: 0,
            no_disturbance: 0,
            videos: Vec::new(),
        }
    }
}

impl Studio {
    /// 投稿接口要的完整 JSON：模板字段之外的开关全用 biliup 的默认值。
    fn payload(&self) -> Value {
        let mut v = serde_json::to_value(self).expect("studio serializes");
        let defaults = json!({
            "dynamic": "", "desc_format_id": 0, "subtitle": {"open": 0, "lan": ""},
            "open_subtitle": false, "interactive": 0, "dolby": 0, "lossless_music": 0,
            "no_reprint": 0, "charging_pay": 0, "up_selection_reply": false,
            "up_close_reply": false, "up_close_danmu": false,
        });
        for (k, d) in defaults.as_object().unwrap() {
            v[k] = d.clone();
        }
        v
    }
}

/// 投稿走哪个接口。两个接口的 21566 频控分开计（2026-10-30 实测：APP 刚撞 21566，8 分钟后网页接口照常投出）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Via {
    App,
    Web,
}

/// 投稿成功的结果。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Submitted {
    pub aid: u64,
    pub bvid: String,
}

/// 刷新令牌后保存新的登录信息。
pub type Save = Box<dyn Fn(&Value) + Send + Sync>;

/// 一份登录信息。第一次用到时向 B 站确认令牌，需要时刷新并经 `save` 交回。
pub struct Account {
    save: Save,
    client: reqwest::Client,
    info: Mutex<Value>,
    checked: Mutex<Option<Instant>>,
}

/// 登录信息里的 B 站用户 id。
pub fn mid(info: &Value) -> Option<u64> {
    info["token_info"]["mid"].as_u64()
}

impl Account {
    pub fn new(info: Value, save: Save) -> Self {
        Self {
            save,
            client: reqwest::Client::builder()
                .user_agent(WEB_UA)
                .timeout(Duration::from_secs(60))
                .build()
                .expect("http client"),
            info: Mutex::new(info),
            checked: Mutex::new(None),
        }
    }

    fn info(&self) -> Value {
        self.info.lock().unwrap().clone()
    }

    fn keys(info: &Value) -> (&'static str, &'static str) {
        match info["platform"].as_str() {
            Some("BiliTV") => TV,
            _ => ANDROID,
        }
    }

    /// 距上次确认超过 [`REFRESH_CHECK_EVERY`] 就问一次 B 站要不要刷新，要就刷新并写回文件。
    pub async fn refresh_if_due(&self) -> Result<()> {
        if self
            .checked
            .lock()
            .unwrap()
            .is_some_and(|t| t.elapsed() < REFRESH_CHECK_EVERY)
        {
            return Ok(());
        }
        let info = self.info();
        let keys = Self::keys(&info);
        let token = |k: &str| {
            info["token_info"][k]
                .as_str()
                .unwrap_or_default()
                .to_string()
        };
        let query = signed(
            keys,
            vec![
                ("access_key", token("access_token")),
                ("actionKey", "appkey".into()),
            ],
        );
        // query 里有令牌，错误里去掉 URL 再往外抛。
        let res: Value = self
            .client
            .get(format!(
                "https://passport.bilibili.com/x/passport-login/oauth2/info?{query}"
            ))
            .send()
            .await
            .map_err(reqwest::Error::without_url)?
            .json()
            .await
            .map_err(reqwest::Error::without_url)?;
        let Some(refresh) = res["data"]["refresh"].as_bool() else {
            return Err(Error::Api(format!(
                "token invalid, login again: {}",
                res["message"]
            )));
        };
        if refresh {
            let body = signed(
                keys,
                vec![
                    ("access_key", token("access_token")),
                    ("actionKey", "appkey".into()),
                    ("refresh_token", token("refresh_token")),
                ],
            );
            let res: Value = self
                .client
                .post("https://passport.bilibili.com/x/passport-login/oauth2/refresh_token")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(body)
                .send()
                .await?
                .json()
                .await?;
            let mut fresh = res["data"].clone();
            if fresh["cookie_info"].is_null() {
                return Err(Error::Api(format!("refresh failed: {}", res["message"])));
            }
            fresh["platform"] = info["platform"].clone();
            (self.save)(&fresh);
            *self.info.lock().unwrap() = fresh;
            info!("bilibili token refreshed");
        }
        *self.checked.lock().unwrap() = Some(Instant::now());
        Ok(())
    }

    fn cookie_header(&self) -> String {
        self.info()["cookie_info"]["cookies"]
            .as_array()
            .map(|cookies| {
                cookies
                    .iter()
                    .map(|c| {
                        format!(
                            "{}={}",
                            c["name"].as_str().unwrap_or(""),
                            c["value"].as_str().unwrap_or("")
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("; ")
            })
            .unwrap_or_default()
    }

    fn cookie(&self, name: &str) -> Option<String> {
        self.info()["cookie_info"]["cookies"]
            .as_array()?
            .iter()
            .find(|c| c["name"] == name)?["value"]
            .as_str()
            .map(String::from)
    }

    /// 带 cookie 的 web 接口 GET，`code != 0` 算错。
    async fn web_get(&self, url: &str) -> Result<Value> {
        self.refresh_if_due().await?;
        let res: Value = self
            .client
            .get(url)
            .header("Cookie", self.cookie_header())
            .send()
            .await?
            .json()
            .await?;
        if res["code"] != 0 {
            return Err(Error::Api(format!("{url}: {res}")));
        }
        Ok(res["data"].clone())
    }

    /// 账号昵称。
    pub async fn nickname(&self) -> Result<String> {
        let data = self
            .web_get("https://api.bilibili.com/x/web-interface/nav")
            .await?;
        Ok(data["uname"].as_str().unwrap_or_default().to_string())
    }

    /// 投稿预处理信息，其中 `typelist` 是分区列表（同 biliup `archive_pre`）。
    pub async fn archive_pre(&self) -> Result<Value> {
        self.web_get("https://member.bilibili.com/x/vupre/web/archive/pre")
            .await
    }

    /// `type_mode = 2` 的新版人选分区。
    pub async fn human_types(&self) -> Result<Value> {
        self.web_get("https://member.bilibili.com/x/vupre/web/archive/human/type2/list")
            .await
    }

    /// 新版页面仍携带旧 tid，由 B 站根据已上传的视频预测。
    async fn predict_tid(&self, studio: &Studio) -> Result<u16> {
        self.refresh_if_due().await?;
        let csrf = self
            .cookie("bili_jct")
            .ok_or_else(|| Error::Api("no bili_jct cookie, login again".into()))?;
        let video = studio
            .videos
            .first()
            .ok_or_else(|| Error::Api("no uploaded video for partition prediction".into()))?;
        let res: Value = self
            .client
            .post("https://member.bilibili.com/x/vupre/web/archive/types/predict")
            .query(&[("csrf", csrf)])
            .header("Cookie", self.cookie_header())
            .form(&[("title", &studio.title), ("filename", &video.filename)])
            .send()
            .await?
            .json()
            .await?;
        predicted_tid(&res)
    }

    /// 上传封面（JPEG），返回图片 URL（同 biliup `cover_up`）。
    pub async fn cover_up(&self, jpeg: &[u8]) -> Result<String> {
        use base64::Engine;
        self.refresh_if_due().await?;
        let csrf = self
            .cookie("bili_jct")
            .ok_or_else(|| Error::Api("no bili_jct cookie, login again".into()))?;
        let cover = format!(
            "data:image/jpeg;base64,{}",
            base64::engine::general_purpose::STANDARD.encode(jpeg)
        );
        let res: Value = self
            .client
            .post("https://member.bilibili.com/x/vu/web/cover/up")
            .header("Cookie", self.cookie_header())
            .form(&[("cover", cover), ("csrf", csrf)])
            .send()
            .await?
            .json()
            .await?;
        res["data"]["url"]
            .as_str()
            .map(String::from)
            .ok_or_else(|| Error::Api(format!("cover upload: {res}")))
    }

    /// 合集列表（同 biliup `list_seasons`）：`seasons[i].season.{id,title}`，
    /// `seasons[i].sections.sections[j].{id,title}`，其中 `sections[j].id` 是 section_id。
    pub async fn seasons(&self) -> Result<Value> {
        self.web_get(
            "https://member.bilibili.com/x2/creative/web/seasons?pn=1&ps=30&order=mtime&sort=desc&draft=1",
        )
        .await
    }

    /// 新建合集（创作中心 `season/add`），返回合集 id。`cover` 是 [`Account::cover_up`] 返回的图片 URL。
    /// 新合集要经 B 站人工审核。
    pub async fn create_season(&self, title: &str, cover: &str) -> Result<i64> {
        self.refresh_if_due().await?;
        let csrf = self
            .cookie("bili_jct")
            .ok_or_else(|| Error::Api("no bili_jct cookie, login again".into()))?;
        let res: Value = self
            .client
            .post("https://member.bilibili.com/x2/creative/web/season/add")
            .header("Cookie", self.cookie_header())
            .form(&[
                ("title", title),
                ("desc", ""),
                ("cover", cover),
                ("season_price", "0"),
                ("csrf", &csrf),
            ])
            .send()
            .await?
            .json()
            .await?;
        if res["code"] != 0 {
            return Err(Error::Api(format!("create season: {res}")));
        }
        res["data"]
            .as_i64()
            .ok_or_else(|| Error::Api(format!("create season: {res}")))
    }

    /// 把稿件加入合集的一个分区（同 biliup `add_archive_to_season`）。
    /// 标题和首 P 的 cid 走创作中心的 view 接口取：审核中的稿件也能取到。
    pub async fn add_to_season(&self, section_id: i64, aid: u64) -> Result<()> {
        let view = self
            .web_get(&format!(
                "https://member.bilibili.com/x/vupre/web/archive/view?aid={aid}"
            ))
            .await?;
        let episode = episode(&view, aid)?;
        let csrf = self
            .cookie("bili_jct")
            .ok_or_else(|| Error::Api("no bili_jct cookie, login again".into()))?;
        let res: Value = self
            .client
            .post(format!(
                "https://member.bilibili.com/x2/creative/web/season/section/episodes/add?csrf={csrf}"
            ))
            .header("Cookie", self.cookie_header())
            .json(&json!({"sectionId": section_id, "episodes": [episode], "csrf": csrf}))
            .send()
            .await?
            .json()
            .await?;
        if res["code"] != 0 {
            return Err(Error::Api(format!("add to season: {res}")));
        }
        Ok(())
    }

    /// 为一个 `size` 字节的文件申请 UPOS 上传凭证。`line` 是线路（upcdn），如 `tx`。
    /// 返回 preupload 的原始 JSON，上传侧自己反序列化成它的凭证类型。
    pub async fn preupload(&self, line: &str, name: &str, size: u64) -> Result<Value> {
        self.refresh_if_due().await?;
        let res: Value = self
            .client
            .get(format!(
                "https://member.bilibili.com/preupload?zone=cs&upcdn={line}&probe_version=20221109"
            ))
            .header("Cookie", self.cookie_header())
            .query(&[
                ("name", name),
                ("r", "upos"),
                ("profile", "ugcupos/bup"),
                ("ssl", "0"),
                ("version", "2.14.0"),
                ("build", "2140000"),
                ("size", &size.to_string()),
            ])
            .send()
            .await?
            .json()
            .await?;
        if res["OK"] != 1 {
            return Err(Error::Api(format!("preupload rejected: {res}")));
        }
        Ok(res)
    }

    /// 提交稿件。走 APP 接口时被 [`WEB_FALLBACKS`] 里的码拒绝，改走网页接口再提交一次。
    pub async fn submit(
        &self,
        studio: &Studio,
        via: Via,
    ) -> std::result::Result<Submitted, SubmitError> {
        let mut prepared;
        let studio = if studio.human_type2.is_some() && studio.tid == 0 {
            prepared = studio.clone();
            prepared.tid = self
                .predict_tid(studio)
                .await
                .map_err(|e| SubmitError::NotSent(e.to_string()))?;
            &prepared
        } else {
            studio
        };
        if via == Via::Web || studio.human_type2.is_some() {
            return self.submit_web(studio).await;
        }
        match self.submit_app(studio).await {
            Err(SubmitError::Rejected { code, message }) if WEB_FALLBACKS.contains(&code) => {
                warn!(code, message, "app submit rejected, retrying via web");
                self.submit_web(studio).await
            }
            other => other,
        }
    }

    /// 用网页接口提交稿件（要 cookie 里的 csrf）。请求体与 APP 接口相同（同 biliup `submit_by_web`）。
    async fn submit_web(&self, studio: &Studio) -> std::result::Result<Submitted, SubmitError> {
        let csrf = self
            .cookie("bili_jct")
            .ok_or_else(|| SubmitError::NotSent("no bili_jct cookie, login again".into()))?;
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        let sent = self
            .client
            .post(format!(
                "https://member.bilibili.com/x/vu/web/add/v3?ts={ts}&csrf={csrf}"
            ))
            .header("Cookie", self.cookie_header())
            .json(&studio.payload())
            .send()
            .await;
        submit_response(sent).await
    }

    /// 用 APP 接口提交稿件（biliup 的默认接口，只需 access_token，不需要 csrf）。
    async fn submit_app(&self, studio: &Studio) -> std::result::Result<Submitted, SubmitError> {
        self.refresh_if_due()
            .await
            .map_err(|e| SubmitError::NotSent(e.to_string()))?;
        let info = self.info();
        let query = signed(
            TV,
            vec![
                (
                    "access_key",
                    info["token_info"]["access_token"]
                        .as_str()
                        .unwrap_or_default()
                        .into(),
                ),
                ("build", "7800300".into()),
                ("c_locale", "zh-Hans_CN".into()),
                ("channel", "bili".into()),
                ("disable_rcmd", "0".into()),
                ("mobi_app", "android".into()),
                ("platform", "android".into()),
                ("s_locale", "zh-Hans_CN".into()),
                (
                    "statistics",
                    r#""appId":1,"platform":3,"version":"7.80.0","abtest":"""#.into(),
                ),
            ],
        );
        let sent = self
            .client
            .post(format!("https://member.bilibili.com/x/vu/app/add?{query}"))
            .header("user-agent", APP_UA)
            .json(&studio.payload())
            .send()
            .await;
        submit_response(sent).await
    }
}

/// APP 接口回这些码时改走网页接口重投。都是预检失败、稿件还没生成，重投不会出重复稿：
/// 21071：没填 mission_id 时 APP 接口按第一个标签自动参加活动，转载稿因此被拒「转载类型稿件不支持活动参加」；
/// 网页接口不自动参加活动（biliup #1801）。
const WEB_FALLBACKS: [i64; 1] = [21071];

fn predicted_tid(res: &Value) -> Result<u16> {
    if res["code"] == 0
        && let Some(id) = res["data"][0]["id"].as_u64()
        && let Ok(id @ 1..) = u16::try_from(id)
    {
        return Ok(id);
    }
    Err(Error::Api(
        "partition prediction returned no valid tid".into(),
    ))
}

async fn submit_response(
    sent: reqwest::Result<reqwest::Response>,
) -> std::result::Result<Submitted, SubmitError> {
    let res = match sent {
        Ok(res) => res,
        Err(e) if e.is_connect() || e.is_builder() => {
            return Err(SubmitError::NotSent(e.without_url().to_string()));
        }
        Err(e) => return Err(SubmitError::Unknown(e.without_url().to_string())),
    };
    let res: Value = res
        .json()
        .await
        .map_err(|e| SubmitError::Unknown(e.without_url().to_string()))?;
    parse_submit(&res)
}

fn parse_submit(res: &Value) -> std::result::Result<Submitted, SubmitError> {
    match res["code"].as_i64() {
        Some(0) => Ok(Submitted {
            aid: res["data"]["aid"].as_u64().unwrap_or_default(),
            bvid: res["data"]["bvid"].as_str().unwrap_or_default().into(),
        }),
        Some(code) => Err(SubmitError::Rejected {
            code,
            message: res["message"].as_str().unwrap_or_default().into(),
        }),
        None => Err(SubmitError::Unknown(res.to_string())),
    }
}

/// 由 archive/view 的 data 拼出合集里的一集。
fn episode(view: &Value, aid: u64) -> Result<Value> {
    let cid = view["videos"][0]["cid"]
        .as_u64()
        .ok_or_else(|| Error::Api(format!("no cid in archive view: {view}")))?;
    Ok(json!({
        "aid": aid,
        "cid": cid,
        "title": view["archive"]["title"].as_str().unwrap_or_default(),
        "charging_pay": 0,
    }))
}

/// 加 appkey、ts 后按键排序、urlencode，再追加 `sign = md5(query + appsec)`。
fn signed((key, sec): (&str, &str), mut params: Vec<(&str, String)>) -> String {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    params.push(("appkey", key.into()));
    params.push(("ts", ts.to_string()));
    params.sort();
    let query = form_urlencoded::Serializer::new(String::new())
        .extend_pairs(&params)
        .finish();
    let sign = format!("{:x}", Md5::digest(format!("{query}{sec}")));
    format!("{query}&sign={sign}")
}

/// 扫码登录的一次轮询结果。
#[derive(Debug)]
pub enum Poll {
    /// 还没扫。
    Waiting,
    /// 扫了还没在手机上确认。
    Scanned,
    /// 登录成功：登录信息（存起来给 [`Account::new`]）。
    Done(Value),
    /// 二维码过期（3 分钟）。
    Expired,
}

/// TV 端扫码登录第一步（同 biliup `login_by_qrcode`）：返回 (二维码内容 URL, auth_code)。
pub async fn qr_start() -> Result<(String, String)> {
    let res: Value = reqwest::Client::new()
        .post("https://passport.bilibili.com/x/passport-tv-login/qrcode/auth_code")
        .header("content-type", "application/x-www-form-urlencoded")
        .body(signed(TV, vec![("local_id", "0".into())]))
        .send()
        .await?
        .json()
        .await?;
    match (
        res["data"]["url"].as_str(),
        res["data"]["auth_code"].as_str(),
    ) {
        (Some(url), Some(code)) => Ok((url.into(), code.into())),
        _ => Err(Error::Api(format!("auth_code: {res}"))),
    }
}

/// 扫码登录第二步：问一次 B 站扫没扫。
pub async fn qr_poll(auth_code: &str) -> Result<Poll> {
    let params = vec![("local_id", "0".into()), ("auth_code", auth_code.into())];
    let res: Value = reqwest::Client::new()
        .post("https://passport.bilibili.com/x/passport-tv-login/qrcode/poll")
        .header("content-type", "application/x-www-form-urlencoded")
        .body(signed(TV, params))
        .send()
        .await?
        .json()
        .await?;
    parse_poll(res)
}

fn parse_poll(res: Value) -> Result<Poll> {
    match res["code"].as_i64() {
        Some(0) => {
            let mut info = res["data"].clone();
            info["platform"] = "BiliTV".into();
            Ok(Poll::Done(info))
        }
        Some(86039) => Ok(Poll::Waiting),
        Some(86090) => Ok(Poll::Scanned),
        Some(86038) => Ok(Poll::Expired),
        _ => Err(Error::Api(format!("qrcode poll: {}", res["message"]))),
    }
}

/// 把二维码内容画成 SVG。
pub fn qr_svg(url: &str) -> Result<String> {
    let code = qrcode::QrCode::new(url).map_err(|e| Error::Api(e.to_string()))?;
    Ok(code
        .render::<qrcode::render::svg::Color>()
        .min_dimensions(300, 300)
        .build())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sign_is_over_sorted_urlencoded_query() {
        let q = signed(("k", "s"), vec![("z", "a b".into()), ("b", "\"x\"".into())]);
        let (query, sign) = q.rsplit_once("&sign=").unwrap();
        assert!(query.starts_with("appkey=k&b=%22x%22&ts="), "{query}");
        assert!(query.ends_with("&z=a+b"));
        assert_eq!(sign, format!("{:x}", Md5::digest(format!("{query}s"))));
    }

    #[test]
    fn partition_prediction_requires_a_successful_valid_tid() {
        assert_eq!(
            predicted_tid(&json!({"code": 0, "data": [{"id": 65}]})).unwrap(),
            65
        );
        for response in [
            json!({"code": -1, "data": [{"id": 65}]}),
            json!({"code": 0, "data": []}),
            json!({"code": 0, "data": [{"id": 0}]}),
            json!({"code": 0, "data": [{"id": 65536}]}),
        ] {
            assert!(predicted_tid(&response).is_err());
        }
    }

    #[test]
    fn new_partition_is_kept_in_the_submit_payload() {
        let studio: Studio =
            serde_json::from_value(json!({"tid": 65, "human_type2": 1008})).unwrap();
        let payload = studio.payload();
        assert_eq!(payload["tid"], 65);
        assert_eq!(payload["human_type2"], 1008);
        assert!(Studio::default().payload().get("human_type2").is_none());
    }

    #[test]
    fn studio_template_defaults_and_payload() {
        let studio: Studio =
            serde_json::from_str(r#"{"title":"t","is_only_self":1,"no_disturbance":1}"#).unwrap();
        assert_eq!((studio.tid, studio.copyright), (171, 1));
        let p = studio.payload();
        assert_eq!(p["is_only_self"], 1);
        assert_eq!(p["no_disturbance"], 1);
        assert_eq!(p["cover"], "");
        assert_eq!(p["dolby"], 0);
        assert_eq!(p["videos"], json!([]));
    }

    #[test]
    fn submit_outcomes() {
        let ok = json!({"code":0,"data":{"aid":1,"bvid":"BV1"}});
        assert_eq!(parse_submit(&ok).unwrap().bvid, "BV1");
        assert!(matches!(
            parse_submit(&json!({"code":21070,"message":"slow"})),
            Err(SubmitError::Rejected { code: 21070, .. })
        ));
        // 连投撞上的「网络繁忙」：明确拒绝、没有生成稿件，可以安全重投。
        assert!(matches!(
            parse_submit(&json!({"code":69800,"message":"网络繁忙 请稍后再试"})),
            Err(SubmitError::Rejected { code: 69800, .. })
        ));
        assert!(matches!(
            parse_submit(&json!({"oops":1})),
            Err(SubmitError::Unknown(_))
        ));
    }

    #[test]
    fn season_episode_from_archive_view() {
        let view = json!({"archive": {"title": "t"}, "videos": [{"cid": 9}, {"cid": 10}]});
        assert_eq!(
            episode(&view, 1).unwrap(),
            json!({"aid": 1, "cid": 9, "title": "t", "charging_pay": 0})
        );
        assert!(episode(&json!({"archive": {}, "videos": []}), 1).is_err());
    }

    #[test]
    fn qrcode_poll_states() {
        assert!(matches!(
            parse_poll(json!({"code":86039})),
            Ok(Poll::Waiting)
        ));
        assert!(matches!(
            parse_poll(json!({"code":86090})),
            Ok(Poll::Scanned)
        ));
        assert!(matches!(
            parse_poll(json!({"code":86038})),
            Ok(Poll::Expired)
        ));
        let Ok(Poll::Done(info)) = parse_poll(json!({"code":0,"data":{"token_info":{"mid":42}}}))
        else {
            panic!()
        };
        assert_eq!(
            (mid(&info), info["platform"].as_str()),
            (Some(42), Some("BiliTV"))
        );
        assert!(parse_poll(json!({"code":-3,"message":"bad"})).is_err());
    }
}
