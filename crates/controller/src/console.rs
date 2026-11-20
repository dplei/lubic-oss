//! 控制台：服务层（`impl App` 里的校验与读写）+ 一层薄的 JSON 接口 + 管理员会话。
//!
//! 页面是一个静态单页（`assets/console.html`，在 `/`），数据全走这里的 `/api/*`。
//! `/api/*` 只认会话 cookie，`/v1/*` 只认 agent token，互不通用。接口清单见 `docs/design/config-db.md`。

use crate::{App, db, now_ms};
use argon2::password_hash::rand_core::{OsRng, RngCore};
use argon2::{PasswordHash, PasswordVerifier};
use axum::extract::{Path, Request, State};
use axum::http::{HeaderMap, StatusCode, Uri, header};
use axum::middleware::{self, Next};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use lubic::downloader::live::builtin_plugins;
use lubic_account::Poll;
use lubic_proto::Status;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, warn};

const COOKIE: &str = "lubic_session";
const SESSION_MS: i64 = 30 * 24 * 3600 * 1000;
/// 扫码登录的二维码 3 分钟有效。
const QR_MS: i64 = 3 * 60_000;
/// 「已投」列表保留最近 7 天。
const SUBMITTED_WINDOW_MS: i64 = 7 * 24 * 3600 * 1000;

/// 服务层错误，各自对应一个 HTTP 状态码；`Bad`/`Conflict` 的文字直接给人看。
#[derive(Debug, PartialEq)]
pub enum Error {
    Bad(String),
    Unauthorized,
    NotFound,
    Conflict(String),
    Internal(String),
}

impl From<rusqlite::Error> for Error {
    fn from(e: rusqlite::Error) -> Self {
        match e.sqlite_error_code() {
            Some(rusqlite::ErrorCode::ConstraintViolation) => Error::Conflict("还在被使用".into()),
            _ => Error::Internal(e.to_string()),
        }
    }
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let (code, msg) = match self {
            Error::Bad(m) => (StatusCode::BAD_REQUEST, m),
            Error::Unauthorized => (StatusCode::UNAUTHORIZED, "需要登录".into()),
            Error::NotFound => (StatusCode::NOT_FOUND, "不存在".into()),
            Error::Conflict(m) => (StatusCode::CONFLICT, m),
            Error::Internal(m) => {
                error!(error = m, "console request failed");
                (StatusCode::INTERNAL_SERVER_ERROR, m)
            }
        };
        (code, Json(json!({ "error": msg }))).into_response()
    }
}

pub type Result<T> = std::result::Result<T, Error>;

fn bad<T>(msg: impl Into<String>) -> Result<T> {
    Err(Error::Bad(msg.into()))
}

/// 房间列表里的一项：配置加运行状态。
#[derive(Debug, Serialize)]
pub struct RoomView {
    #[serde(flatten)]
    pub room: db::Room,
    /// 分给了哪台机器。
    pub agent: Option<String>,
    /// 正交接给哪台。
    pub handoff_to: Option<String>,
    /// 这一场还没下播（有分P在录或在下播宽限里）。
    pub recording: bool,
    /// 这一场的开播时刻、直播标题与各分P状态；没在录为空。
    pub session: Option<SessionView>,
    /// 最近一场已结束直播的开录时刻（unix 毫秒）；没有已结束场次为空。
    pub last_live_started_at: Option<i64>,
    /// 上传过封面背景。
    pub cover_bg: bool,
}

#[derive(Debug, Serialize)]
pub struct SessionView {
    pub started_at: i64,
    pub title: String,
    /// 各分P，按开录先后。
    pub parts: Vec<PartView>,
}

#[derive(Debug, PartialEq, Serialize)]
pub struct PartView {
    /// `state` 另有 `uploading` 在传。
    #[serde(flatten)]
    pub part: db::SessionPart,
    /// 在传的进度（0–1），按 agent 心跳里报的已传字节算。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub progress: Option<f64>,
}

/// 一台机器：`machines` 表里的一行加运行状态。
#[derive(Debug, Serialize)]
pub struct Machine {
    pub name: String,
    /// 是否参与调度（停用后不再分新房间，在录房间交接迁走）。
    pub enabled: bool,
    /// 备注。
    pub note: String,
    /// 创建时刻（unix 毫秒）。
    pub created_at: i64,
    pub online: bool,
    /// 最近一次来访（unix 毫秒），从没来过为空。
    pub last_seen: Option<i64>,
    /// 最近一次报的状态：硬盘水位、是否停录、待传积压等。
    pub status: Status,
    pub limit: Option<db::Limit>,
    /// 滚动更新的排空标记。
    pub drain: Option<db::DrainMark>,
    /// 在它上面更新失败过的提交与时刻，最近的在前（最多 10 条）。
    pub failed_commits: Vec<(String, i64)>,
    /// 分给它的房间（不含交接中的）。
    pub rooms: Vec<String>,
    /// 在线时正在传的分段：(房间, 进度 0–1)。
    pub uploading: Vec<(String, f64)>,
    /// 让出网络到这一刻（unix 毫秒），没在让出（含已到点）为空。
    pub yield_until: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct LoginStart {
    pub login_id: String,
    /// 二维码内容。
    pub url: String,
    /// 画好的二维码。
    pub svg: String,
}

#[derive(Debug, PartialEq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum LoginState {
    Waiting,
    Scanned,
    Done { account_id: i64 },
    Expired,
    Failed { error: String },
}

/// 投稿页：账号闸、待投与最近已投。
#[derive(Debug, Serialize)]
pub struct SubmitsView {
    pub accounts: Vec<AccountGate>,
    pub pending: Vec<PendingView>,
    pub submitted: Vec<db::SubmittedArchive>,
}

/// 一个账号的投稿闸（内存里的 `Gate`，重启清零）。
#[derive(Debug, Serialize)]
pub struct AccountGate {
    pub id: i64,
    pub name: String,
    /// 下次最早可投（unix 毫秒）；没有限制或已过期为空。
    pub submit_after: Option<i64>,
    /// 连续撞 21566 的次数。
    pub strikes: u32,
}

/// 待投的一份稿件加它现在卡在哪。
#[derive(Debug, Serialize)]
pub struct PendingView {
    #[serde(flatten)]
    pub archive: db::PendingArchive,
    /// `uploading`/`hold`/`backoff`/`probe`/`paused`/`interval`/`ready`/`submitting`/`attention`。
    pub state: &'static str,
    /// 状态附带的时刻：重试 / 试探 / 最早可投。
    pub at: Option<i64>,
    /// 它正在传的那段分P的进度（0–1）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub progress: Option<f64>,
}

/// 算一份待投稿件的状态与附带时刻，规则与 [`App::submit_ready`] 一致。
/// 账号在 21566 停投中时，按 id 升序第一份没在退避里的可投稿件是试探；`probed` 表示这个账号前面已经有了。
fn pending_state(
    phase: &str,
    parts_unfinished: i64,
    retry_at: Option<i64>,
    gate: Option<(i64, u32)>,
    probed: bool,
    now: i64,
) -> (&'static str, Option<i64>) {
    if phase == db::SUBMITTING {
        return ("submitting", None);
    }
    if phase == db::ATTENTION {
        return ("attention", None);
    }
    if parts_unfinished > 0 {
        return ("uploading", None);
    }
    let (submit_after, strikes) = gate
        .filter(|(after, _)| *after > now)
        .map_or((None, 0), |(after, strikes)| (Some(after), strikes));
    if let Some(at) = retry_at.filter(|at| *at > now) {
        return ("backoff", Some(submit_after.map_or(at, |s| at.max(s))));
    }
    if let Some(after) = submit_after {
        if strikes > 0 {
            return if probed {
                ("paused", None)
            } else {
                ("probe", Some(after))
            };
        }
        return ("interval", Some(after));
    }
    ("ready", None)
}

fn random_hex(bytes: usize) -> String {
    let mut buf = vec![0u8; bytes];
    OsRng.fill_bytes(&mut buf);
    buf.iter().map(|b| format!("{b:02x}")).collect()
}

impl App {
    /// 在线机器心跳里报的在传分段：part_id → 进度；失联机器的旧心跳不算。
    fn uploading_now(&self) -> Result<std::collections::HashMap<String, f64>> {
        let now = now_ms();
        let mut uploading = std::collections::HashMap::new();
        let machines = db::machines(&self.db())?;
        for m in &machines {
            if let Some(s) = self.online(&m.name, now, false)? {
                for (id, done, size) in s.uploading {
                    uploading.insert(id, done as f64 / size.max(1) as f64);
                }
            }
        }
        Ok(uploading)
    }

    pub fn room_views(&self) -> Result<Vec<RoomView>> {
        let uploading = self.uploading_now()?;
        let conn = self.db();
        let bgs = db::cover_bg_rooms(&conn)?;
        let mut out = Vec::new();
        for room in db::rooms(&conn)?.into_iter().filter(|r| !r.deleted) {
            let session =
                db::current_session(&conn, &room.name)?.map(|(started_at, title, parts)| {
                    let parts = parts
                        .into_iter()
                        .map(|mut part| {
                            let progress = uploading
                                .get(&part.part_id)
                                .copied()
                                .filter(|_| part.state == "closed");
                            if progress.is_some() {
                                part.state = "uploading".into();
                            }
                            PartView { part, progress }
                        })
                        .collect();
                    SessionView {
                        started_at,
                        title,
                        parts,
                    }
                });
            out.push(RoomView {
                agent: db::assignment(&conn, &room.name)?.map(|(a, _)| a),
                handoff_to: db::handoff(&conn, &room.name)?.map(|h| h.to),
                recording: session.is_some(),
                session,
                last_live_started_at: db::last_live_started_at(&conn, &room.name)?,
                cover_bg: bgs.contains(&room.name),
                room,
            });
        }
        Ok(out)
    }

    pub fn get_room(&self, name: &str) -> Result<db::Room> {
        match db::room(&self.db(), name)? {
            Some(r) if !r.deleted => Ok(r),
            _ => Err(Error::NotFound),
        }
    }

    /// 新建（`create`）或整行替换。改动不碰正在录的这一场：它用分配时的参数快照。
    pub fn save_room(&self, mut room: db::Room, create: bool) -> Result<()> {
        room.name = room.name.trim().to_string();
        room.url = room.url.trim().to_string();
        // 显示名去掉首尾空白，空的就当没填（回退房间名）。
        room.display_name = room
            .display_name
            .map(|n| n.trim().to_string())
            .filter(|n| !n.is_empty());
        // 文件名模板空着就用默认值（同 biliup）。
        room.filename = match room.filename.trim() {
            "" => db::DEFAULT_FILENAME.into(),
            f => f.into(),
        };
        if room.name.is_empty()
            || room.name.contains(['/', '\\'])
            || room.name.chars().any(char::is_control)
        {
            return bad("房间名不能为空，也不能含 / \\ 或控制字符");
        }
        if !builtin_plugins().iter().any(|p| p.matches(&room.url)) {
            return bad(format!("不认识的直播间地址：{}", room.url));
        }
        if let Some(q) = &room.douyin_quality
            && !["origin", "uhd", "hd", "sd", "ld", "md"].contains(&q.as_str())
        {
            return bad(format!("没有这一档抖音画质：{q}"));
        }
        if room.tail_upload && room.split_size_mb.is_none() {
            return bad("边录边传必须设置按大小切段");
        }
        let conn = self.db();
        for (i, a) in room.agents.iter().enumerate() {
            if db::machine(&conn, a)?.is_none() {
                return bad(format!("没有这台机器：{a}"));
            }
            if room.agents[..i].contains(a) {
                return bad(format!("机器重复：{a}"));
            }
        }
        let Some(tpl) = db::template(&conn, room.template_id)? else {
            return bad("模板不存在");
        };
        room.tag = db::tags("", &room.tag).join(",");
        let n = db::tags(&tpl.tag, &room.tag).len();
        if n > db::TAG_MAX {
            return bad(format!(
                "模板标签加房间标签共 {n} 个（重复的只算一次），B 站一份稿件最多 {} 个",
                db::TAG_MAX
            ));
        }
        let existing = db::room(&conn, &room.name)?.filter(|r| !r.deleted);
        match (create, existing) {
            (true, Some(_)) => return Err(Error::Conflict("已有同名房间".into())),
            (false, None) => return Err(Error::NotFound),
            _ => {}
        }
        db::save_room(&conn, &room)?;
        info!(room = room.name, create, "room saved");
        Ok(())
    }

    /// 删除：不再检测下一场；在录的这一场照常录完、投稿。
    pub fn delete_room(&self, name: &str) -> Result<()> {
        if !db::delete_room(&self.db(), name)? {
            return Err(Error::NotFound);
        }
        info!(room = name, "room deleted");
        Ok(())
    }

    /// 设置（`Some`，须是 [`crate::cover::background`] 认的 JPEG）或去掉房间的封面背景，下次投稿起生效。
    pub fn set_cover_bg(&self, name: &str, jpeg: Option<&[u8]>) -> Result<()> {
        self.get_room(name)?;
        if let Some(j) = jpeg {
            crate::cover::background(j).map_err(Error::Bad)?;
        }
        db::set_cover_bg(&self.db(), name, jpeg)?;
        info!(
            room = name,
            set = jpeg.is_some(),
            "cover background changed"
        );
        Ok(())
    }

    /// 封面预览：用房间现在的显示名、背景和此刻的时间生成。
    pub fn cover_preview(&self, name: &str) -> Result<Vec<u8>> {
        let room = self.get_room(name)?;
        let bg = db::cover_bg(&self.db(), name)?;
        let at = chrono::Local::now()
            .format(crate::cover::TIME_FORMAT)
            .to_string();
        Ok(crate::cover::render(
            bg.as_deref(),
            &[room.display_name(), &at],
        ))
    }

    pub fn order_rooms(&self, names: &[String]) -> Result<()> {
        Ok(db::order_rooms(&mut self.db(), names)?)
    }

    /// 立即结束本场，返回结束了几份稿件。
    pub fn end_room(&self, name: &str) -> Result<usize> {
        self.get_room(name)?;
        let n = db::end_room(&self.db(), name, now_ms())?;
        info!(room = name, archives = n, "ended from console");
        Ok(n)
    }

    pub fn get_template(&self, id: i64) -> Result<db::Template> {
        db::template(&self.db(), id)?.ok_or(Error::NotFound)
    }

    /// 新建（`create`，返回新 id）或按 `t.id` 整行替换。还没投的稿件投稿时读最新模板。
    pub fn save_template(&self, mut t: db::Template, create: bool) -> Result<i64> {
        t.name = t.name.trim().to_string();
        if t.name.is_empty() {
            return bad("模板名不能为空");
        }
        if t.title.trim().is_empty() {
            return bad("标题不能为空");
        }
        if t.human_type2 == Some(0) || (t.tid == 0 && t.human_type2.is_none()) {
            return bad("请选择有效分区");
        }
        match t.copyright {
            1 => {}
            2 if t.source.trim().is_empty() => return bad("转载必须填来源"),
            2 => {}
            _ => return bad("版权只能是 1（自制）或 2（转载）"),
        }
        let tags = db::tags(&t.tag, "");
        if tags.len() > db::TAG_MAX {
            return bad(format!("标签最多 {} 个", db::TAG_MAX));
        }
        t.tag = tags.join(",");
        let conn = self.db();
        if !db::accounts(&conn)?.iter().any(|a| a.id == t.account_id) {
            return bad("账号不存在");
        }
        for r in db::rooms(&conn)? {
            let n = db::tags(&t.tag, &r.tag).len();
            if r.template_id == t.id && !r.deleted && n > db::TAG_MAX {
                return bad(format!(
                    "房间 {} 还追加了标签，合起来共 {n} 个，B 站一份稿件最多 {} 个",
                    r.name,
                    db::TAG_MAX
                ));
            }
        }
        let unique = |r: rusqlite::Error| match Error::from(r) {
            Error::Conflict(_) => Error::Conflict("已有同名模板".into()),
            e => e,
        };
        if create {
            return db::insert_template(&conn, &t).map_err(unique);
        }
        if !db::update_template(&conn, &t).map_err(unique)? {
            return Err(Error::NotFound);
        }
        Ok(t.id)
    }

    pub fn delete_template(&self, id: i64) -> Result<()> {
        match db::delete_template(&self.db(), id) {
            Ok(true) => Ok(()),
            Ok(false) => Err(Error::NotFound),
            Err(e) => Err(match Error::from(e) {
                Error::Conflict(_) => {
                    Error::Conflict("还有房间在用这个模板（含已删除、还没投完的）".into())
                }
                e => e,
            }),
        }
    }

    pub fn accounts(&self) -> Result<Vec<db::AccountInfo>> {
        Ok(db::accounts(&self.db())?)
    }

    pub fn rename_account(&self, id: i64, name: &str) -> Result<()> {
        let name = name.trim();
        if name.is_empty() {
            return bad("账号名称不能为空");
        }
        if !db::rename_account(&self.db(), id, name)? {
            return Err(Error::NotFound);
        }
        info!(id, name, "upload account renamed");
        Ok(())
    }

    pub fn delete_account(&self, id: i64) -> Result<()> {
        let deleted = db::delete_account(&self.db(), id).map_err(|e| match Error::from(e) {
            Error::Conflict(_) => Error::Conflict("还有模板在用这个账号".into()),
            e => e,
        })?;
        if !deleted {
            return Err(Error::NotFound);
        }
        self.bilis.lock().unwrap().remove(&id);
        Ok(())
    }

    /// 按账号返回分区模式、旧分区树和新版人选分区。
    pub async fn types(&self, account: i64) -> Result<Value> {
        let bili = self.bili(account).map_err(|_| Error::NotFound)?;
        let pre = bili.archive_pre().await.map_err(Error::Internal)?;
        let mode = pre["type_mode"].as_u64().unwrap_or(0);
        let human = if mode == 2 {
            let data = bili.human_types().await.map_err(Error::Internal)?;
            data["type_list"]
                .as_array()
                .cloned()
                .ok_or_else(|| Error::Internal("新版分区列表格式有误".into()))?
        } else {
            Vec::new()
        };
        Ok(serde_json::json!({
            "type_mode": mode, "typelist": pre["typelist"], "human_type_list": human,
        }))
    }

    /// 合集与它下面的分区（`seasons` 接口原样的 data，同 biliup `/bili/seasons`），
    /// 外加 `can_create`：账号能不能新建合集，取投稿预处理里的 `season`（创作等级不够时为 false），取不到当作不能。
    pub async fn seasons(&self, account: i64) -> Result<Value> {
        let bili = self.bili(account).map_err(|_| Error::NotFound)?;
        let (list, pre) = tokio::join!(bili.seasons(), bili.archive_pre());
        let mut data = list.map_err(Error::Internal)?;
        data["can_create"] = pre.is_ok_and(|p| p["season"] == true).into();
        Ok(data)
    }

    /// 新建合集，返回它的分区 id（section_id）。标题去掉首尾空白；账号下已有同名合集就不建，
    /// 直接返回它的第一个分区。封面是只有合集名一行的自动封面。
    pub async fn create_season(&self, account: i64, title: &str) -> Result<i64> {
        let title = title.trim();
        if title.is_empty() {
            return Err(Error::Bad("合集标题不能为空".into()));
        }
        let bili = self.bili(account).map_err(|_| Error::NotFound)?;
        let list = bili.seasons().await.map_err(Error::Internal)?;
        if let Some(id) = first_section(&list, |s| s["title"].as_str() == Some(title)) {
            return Ok(id);
        }
        let url = bili
            .cover_up(&crate::cover::render(None, &[title]))
            .await
            .map_err(Error::Internal)?;
        let id = bili
            .create_season(title, &url)
            .await
            .map_err(Error::Internal)?;
        let list = bili.seasons().await.map_err(Error::Internal)?;
        first_section(&list, |s| s["id"].as_i64() == Some(id)).ok_or_else(|| {
            Error::Internal(format!(
                "合集 {id} 已建好，但列表里没找到它的分区，去创作中心看看"
            ))
        })
    }

    pub async fn start_login(&self) -> Result<LoginStart> {
        let (url, code) = self.qr.start().await.map_err(Error::Internal)?;
        let svg = lubic_account::qr_svg(&url).map_err(|e| Error::Internal(e.to_string()))?;
        let login_id = random_hex(16);
        let now = now_ms();
        let mut logins = self.logins.lock().unwrap();
        logins.retain(|_, (_, at)| now - *at < QR_MS);
        logins.insert(login_id.clone(), (code, now));
        Ok(LoginStart { login_id, url, svg })
    }

    /// 问一次 B 站扫没扫；成功就把账号落库（同一 `mid` 是更新）。
    pub async fn poll_login(&self, login_id: &str) -> Result<LoginState> {
        let pending = self.logins.lock().unwrap().get(login_id).cloned();
        let Some((code, at)) = pending else {
            return Err(Error::NotFound);
        };
        let done = |s| {
            self.logins.lock().unwrap().remove(login_id);
            Ok(s)
        };
        if now_ms() - at >= QR_MS {
            return done(LoginState::Expired);
        }
        let login = match self.qr.poll(&code).await {
            Ok(Poll::Waiting) => return Ok(LoginState::Waiting),
            Ok(Poll::Scanned) => return Ok(LoginState::Scanned),
            Ok(Poll::Expired) => return done(LoginState::Expired),
            Err(error) => return done(LoginState::Failed { error }),
            Ok(Poll::Done(login)) => login,
        };
        let Some(mid) = lubic_account::mid(&login) else {
            return done(LoginState::Failed {
                error: "登录信息里没有 mid".into(),
            });
        };
        let name = self.qr.nickname(&login).await.unwrap_or_else(|e| {
            warn!(mid, error = e, "fetching nickname failed");
            mid.to_string()
        });
        let id = db::save_account(&self.db(), &name, mid, &login, now_ms())?;
        self.bilis.lock().unwrap().remove(&id);
        info!(id, name, mid, "upload account logged in");
        done(LoginState::Done { account_id: id })
    }

    pub fn machines(&self) -> Result<Vec<Machine>> {
        let now = now_ms();
        let rows = db::machines(&self.db())?;
        let mut out = Vec::new();
        for m in rows {
            // `self.db()` 的锁在整条语句结束才放，所以分开取，别写进同一个结构体字面量。
            let status = self.online(&m.name, now, false)?;
            let online = status.is_some();
            let conn = self.db();
            let mut uploading = Vec::new();
            for (id, done, size) in status.map(|s| s.uploading).unwrap_or_default() {
                if let Some((_, room)) = db::part_owner(&conn, &id)? {
                    uploading.push((room, done as f64 / size.max(1) as f64));
                }
            }
            let seen = db::agent(&conn, &m.name)?;
            out.push(Machine {
                online,
                uploading,
                last_seen: seen.as_ref().map(|(t, _)| *t),
                status: seen.map(|(_, s)| s).unwrap_or_default(),
                limit: db::limit(&conn, &m.name)?,
                drain: db::drain(&conn, &m.name)?,
                failed_commits: db::failed_commits(&conn, &m.name)?,
                rooms: db::rooms_of(&conn, &m.name)?,
                name: m.name,
                enabled: m.enabled,
                note: m.note,
                created_at: m.created_at,
                yield_until: m.yield_until.filter(|&t| t > now),
            });
        }
        Ok(out)
    }

    /// 新增一台机器，返回 (name, token)；`add_to_rooms` 为真时追加到所有未删除房间优先列表末尾。
    pub fn add_machine(
        &self,
        name: &str,
        note: &str,
        add_to_rooms: bool,
    ) -> Result<(String, String)> {
        let name = name.trim();
        if !(1..=32).contains(&name.len())
            || !name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
        {
            return bad("机器名只能用 1–32 个字母、数字、- 或 _");
        }
        let note = note.trim();
        let token = random_hex(32);
        let mut conn = self.db();
        let tx = conn.transaction()?;
        db::insert_machine(&tx, name, &token, note, now_ms()).map_err(|e| {
            match Error::from(e) {
                Error::Conflict(_) => Error::Conflict("已有同名机器".into()),
                e => e,
            }
        })?;
        if add_to_rooms {
            db::add_machine_to_rooms(&tx, name)?;
        }
        tx.commit()?;
        drop(conn);
        self.changed.notify_waiters();
        info!(machine = name, add_to_rooms, "machine added");
        Ok((name.to_string(), token))
    }

    /// 停用/启用机器或修改备注。
    pub fn update_machine(
        &self,
        name: &str,
        enabled: Option<bool>,
        note: Option<String>,
    ) -> Result<()> {
        let note = note.as_deref().map(str::trim);
        if !db::update_machine(&self.db(), name, enabled, note)? {
            return Err(Error::NotFound);
        }
        if let Some(enabled) = enabled {
            self.changed.notify_waiters();
            if enabled {
                info!(machine = name, "machine enabled");
            } else {
                info!(machine = name, "machine disabled");
            }
        }
        if note.is_some() {
            info!(machine = name, "machine note changed");
        }
        Ok(())
    }

    /// 让出网络到 `until`（unix 毫秒，须在未来 24 小时内），`None` 立即结束（`load-balance.md` §3）。
    pub fn set_yield(&self, name: &str, until: Option<i64>) -> Result<()> {
        if let Some(t) = until {
            let now = now_ms();
            if t <= now || t - now > 24 * 3600 * 1000 {
                return bad("让出网络的结束时刻须在未来 24 小时内");
            }
        }
        if !db::set_yield(&self.db(), name, until)? {
            return Err(Error::NotFound);
        }
        self.changed.notify_waiters();
        info!(machine = name, until, "machine yield network changed");
        Ok(())
    }

    /// 查看机器的 token。
    pub fn machine_token(&self, name: &str) -> Result<String> {
        Ok(db::machine(&self.db(), name)?.ok_or(Error::NotFound)?.token)
    }

    /// 重置机器的 token，返回新 token，旧 token 立即失效。
    pub fn reset_machine_token(&self, name: &str) -> Result<String> {
        let token = random_hex(32);
        if !db::set_machine_token(&self.db(), name, &token)? {
            return Err(Error::NotFound);
        }
        info!(machine = name, "machine token reset");
        Ok(token)
    }

    /// 移除或强制移除一台机器。
    pub fn remove_machine(&self, name: &str, force: bool) -> Result<()> {
        let now = now_ms();
        let status = self.online(name, now, false)?;
        let mut conn = self.db();
        let tx = conn.transaction()?;
        let Some(m) = db::machine(&tx, name)? else {
            return Err(Error::NotFound);
        };
        let mut reasons = Vec::new();
        if !force {
            if m.enabled {
                reasons.push("还没停用".into());
            }
            let rooms = db::rooms_of(&tx, name)?;
            if !rooms.is_empty() {
                reasons.push(format!(
                    "还分配着 {} 个房间：{}",
                    rooms.len(),
                    rooms.join(", ")
                ));
            }
            let handoffs = db::handoffs_of(&tx, name)?;
            if !handoffs.is_empty() {
                reasons.push(format!(
                    "还在交接 {} 个房间：{}",
                    handoffs.len(),
                    handoffs.join(", ")
                ));
            }
            let unfinished = db::unfinished_parts(&tx, name)?;
            if unfinished > 0 {
                reasons.push(format!("还有 {unfinished} 个分段没传完"));
            }
            if let Some(s) = &status
                && (s.queued_parts > 0 || s.outbox > 0)
            {
                reasons.push(format!(
                    "机器上还有 {} 个分段待传、{} 条事件未上报",
                    s.queued_parts, s.outbox
                ));
            }
        } else if status.is_some() {
            return Err(Error::Conflict("机器在线，不能强制移除".into()));
        }
        for room in db::rooms(&tx)?.into_iter().filter(|r| !r.deleted) {
            if room.agents.len() == 1 && room.agents[0] == name {
                reasons.push(format!("房间 {} 只剩这一台优先机器", room.name));
            }
        }
        if !reasons.is_empty() {
            return Err(Error::Conflict(reasons.join("；")));
        }
        if force {
            db::drop_agent_work(&tx, name)?;
            for id in db::lose_parts(&tx, name)? {
                warn!(
                    machine = name,
                    part = id,
                    "part marked lost by force removal"
                );
            }
        }
        db::delete_machine(&tx, name)?;
        tx.commit()?;
        drop(conn);
        self.changed.notify_waiters();
        if force {
            info!(machine = name, "machine force-removed");
        } else {
            info!(machine = name, "machine removed");
        }
        Ok(())
    }

    /// 投稿页数据：账号投稿闸、待投稿件与最近 7 天已投稿件。
    pub fn submits(&self) -> Result<SubmitsView> {
        let now = now_ms();
        let uploading = self.uploading_now()?;
        let (accounts, pending, submitted, progress) = {
            let conn = self.db();
            let mut progress = std::collections::HashMap::new();
            for (id, p) in uploading {
                if let Some((archive, _)) = db::part_owner(&conn, &id)? {
                    progress.insert(archive, p);
                }
            }
            (
                db::accounts(&conn)?,
                db::pending_archives(&conn)?,
                db::submitted_since(&conn, now - SUBMITTED_WINDOW_MS)?,
                progress,
            )
        };
        let gates: std::collections::HashMap<i64, (i64, u32)> = self
            .gates
            .lock()
            .unwrap()
            .iter()
            .map(|(id, g)| (*id, (g.submit_after, g.submit_strikes)))
            .collect();
        let retry: std::collections::HashMap<i64, i64> = self
            .retry
            .lock()
            .unwrap()
            .iter()
            .map(|(id, (_, at))| (*id, *at))
            .collect();
        let accounts = accounts
            .into_iter()
            .map(|a| {
                let (after, strikes) = gates.get(&a.id).copied().unwrap_or((0, 0));
                AccountGate {
                    id: a.id,
                    name: a.name,
                    submit_after: (after > now).then_some(after),
                    strikes,
                }
            })
            .collect();
        let mut probed = std::collections::HashSet::new();
        let pending = pending
            .into_iter()
            .map(|archive| {
                let acc = archive.account_id;
                let hold = archive.hold_until.filter(|h| {
                    archive.phase == db::ENDED && archive.parts_unfinished == 0 && *h > now
                });
                let (state, at) = match hold {
                    Some(h) => ("hold", Some(h)),
                    None => pending_state(
                        &archive.phase,
                        archive.parts_unfinished,
                        retry.get(&archive.id).copied(),
                        acc.and_then(|acc| gates.get(&acc).copied()),
                        acc.is_some_and(|acc| probed.contains(&acc)),
                        now,
                    ),
                };
                if state == "probe" {
                    probed.extend(acc);
                }
                let progress = progress
                    .get(&archive.id)
                    .copied()
                    .filter(|_| state == "uploading");
                PendingView {
                    archive,
                    state,
                    at,
                    progress,
                }
            })
            .collect();
        Ok(SubmitsView {
            accounts,
            pending,
            submitted,
        })
    }

    /// 清掉一份退避稿件的重试等待，下个检测周期即重投（不跳过账号闸）。
    pub fn retry_archive(&self, id: i64) -> Result<()> {
        if !db::archive_exists(&self.db(), id)? {
            return Err(Error::NotFound);
        }
        let now = now_ms();
        let mut retry = self.retry.lock().unwrap();
        if retry.get(&id).is_none_or(|(_, at)| *at <= now) {
            return Err(Error::Conflict("这份稿件不在退避中".into()));
        }
        retry.remove(&id);
        drop(retry);
        info!(id, "retry from console");
        Ok(())
    }

    /// 丢弃一份下播后还没投的稿件：不投稿，没传的分P agent 申请凭证时被拒、删掉本地文件。
    pub fn discard_archive(&self, id: i64) -> Result<()> {
        if !db::archive_exists(&self.db(), id)? {
            return Err(Error::NotFound);
        }
        if !db::discard_archive(&self.db(), id)? {
            return Err(Error::Conflict("只能丢弃已结束、还没提交的稿件".into()));
        }
        self.retry.lock().unwrap().remove(&id);
        info!(id, "discarded from console");
        Ok(())
    }

    /// 校验管理员密码，成功返回新会话的 token。失败固定等 1 秒，挡暴力试密码。
    pub async fn admin_login(&self, password: &str) -> Result<String> {
        let ok = self.config.admin_password_hash.as_deref().is_some_and(|h| {
            PasswordHash::new(h).is_ok_and(|h| {
                argon2::Argon2::default()
                    .verify_password(password.as_bytes(), &h)
                    .is_ok()
            })
        });
        if !ok {
            tokio::time::sleep(Duration::from_secs(1)).await;
            warn!("console login failed");
            return Err(Error::Unauthorized);
        }
        let token = random_hex(32);
        db::create_session(&self.db(), &token, now_ms() + SESSION_MS)?;
        info!("console login");
        Ok(token)
    }

    fn session(&self, headers: &HeaderMap) -> Option<String> {
        headers
            .get_all(header::COOKIE)
            .iter()
            .filter_map(|v| v.to_str().ok())
            .flat_map(|v| v.split(';'))
            .find_map(|kv| {
                kv.trim()
                    .strip_prefix(&format!("{COOKIE}="))
                    .map(String::from)
            })
    }
}

/// 合集列表里第一个满足 `pick(&item["season"])` 的合集的第一个分区 id。
fn first_section(data: &Value, pick: impl Fn(&Value) -> bool) -> Option<i64> {
    data["seasons"]
        .as_array()?
        .iter()
        .find(|item| pick(&item["season"]))?["sections"]["sections"][0]["id"]
        .as_i64()
}

/// `/api/*` 的鉴权：没配管理员密码一律 403，没有有效会话 401。
pub async fn require_session(State(app): State<Arc<App>>, req: Request, next: Next) -> Response {
    if app.config.admin_password_hash.is_none() {
        return StatusCode::FORBIDDEN.into_response();
    }
    let valid = app
        .session(req.headers())
        .is_some_and(|t| db::session_valid(&app.db(), &t, now_ms()).unwrap_or(false));
    if !valid {
        return Error::Unauthorized.into_response();
    }
    next.run(req).await
}

pub fn routes(app: Arc<App>) -> Router<Arc<App>> {
    Router::new()
        .route("/api/rooms", get(list_rooms).post(create_room))
        .route("/api/rooms/order", post(order_rooms))
        .route(
            "/api/rooms/{name}",
            get(get_room).put(put_room).delete(delete_room),
        )
        .route("/api/rooms/{name}/end", post(end_room))
        .route(
            "/api/rooms/{name}/cover",
            get(cover_preview).put(put_cover_bg).delete(delete_cover_bg),
        )
        .route("/api/templates", get(list_templates).post(create_template))
        .route(
            "/api/templates/{id}",
            get(get_template).put(put_template).delete(delete_template),
        )
        .route("/api/accounts", get(list_accounts))
        .route(
            "/api/accounts/{id}",
            put(rename_account).delete(delete_account),
        )
        .route("/api/accounts/{id}/types", get(types))
        .route(
            "/api/accounts/{id}/seasons",
            get(seasons).post(create_season),
        )
        .route("/api/accounts/login", post(start_login))
        .route("/api/accounts/login/{id}", get(poll_login))
        .route("/api/machines", get(machines).post(create_machine))
        .route(
            "/api/machines/{name}",
            put(put_machine).delete(delete_machine),
        )
        .route("/api/machines/{name}/yield", put(put_machine_yield))
        .route(
            "/api/machines/{name}/token",
            get(get_machine_token).post(reset_machine_token),
        )
        .route("/api/submits", get(submits))
        .route("/api/archives/{id}/retry", post(retry_archive))
        .route("/api/archives/{id}/discard", post(discard_archive))
        .route("/api/logout", post(logout))
        .route_layer(middleware::from_fn_with_state(app, require_session))
        .route("/api/login", post(login))
        .route("/", get(page))
        .route("/favicon.svg", get(favicon))
}

/// 控制台页面本身不用登录：没有会话时它自己显示登录表单。
/// Logo 内联进页面，红灯 `#rec` 闪不闪由页面按「有没有房间在录」决定。
async fn page() -> Html<String> {
    let logo = without_style(include_str!("../../../docs/brand/icon.svg")).replacen(
        "<svg ",
        "<svg aria-hidden=\"true\" ",
        1,
    );
    Html(
        include_str!("../assets/console.html")
            .replace("{{commit}}", &commit_link(lubic_proto::COMMIT))
            .replace("{{logo}}", &logo)
            .replace(
                "{{pushed_at}}",
                &push_time(option_env!("GIT_PUSHED_AT").unwrap_or("")),
            ),
    )
}

fn commit_link(commit: &str) -> String {
    if commit.len() == 40 && commit.bytes().all(|b| b.is_ascii_hexdigit()) {
        format!(
            "<a href=\"https://github.com/dplei/lubic/commit/{commit}\" target=\"_blank\" rel=\"noopener noreferrer\" aria-describedby=\"tip\">{}</a>",
            crate::short(commit)
        )
    } else {
        "<span tabindex=\"0\">(none)</span>".into()
    }
}

/// GitHub push 事件里的 unix 秒数，固定显示北京时间。
fn push_time(value: &str) -> String {
    value
        .parse::<i64>()
        .ok()
        .and_then(|secs| chrono::DateTime::from_timestamp(secs, 0))
        .map(|at| {
            format!(
                "推送到 GitHub：{}（北京时间）",
                at.with_timezone(&chrono::FixedOffset::east_opt(8 * 3600).unwrap())
                    .format("%Y-%m-%d %H:%M:%S")
            )
        })
        .unwrap_or_else(|| "未记录 GitHub 推送时间".into())
}

/// 标签页图标不闪（Chrome/Safari 本来也只取首帧），红灯常亮。
async fn favicon() -> impl IntoResponse {
    (
        [
            (header::CONTENT_TYPE, "image/svg+xml"),
            (header::CACHE_CONTROL, "max-age=86400"),
        ],
        without_style(include_str!("../../../docs/brand/favicon.svg")),
    )
}

/// 去掉品牌 SVG 自带的闪烁样式（`docs/brand/to_svg.py` 生成的那段 `<style>`）。
fn without_style(svg: &str) -> String {
    match (svg.find("<style>"), svg.find("</style>")) {
        (Some(a), Some(b)) => format!("{}{}", &svg[..a], &svg[b + "</style>".len()..]),
        _ => svg.to_string(),
    }
}

type S = State<Arc<App>>;

async fn list_rooms(State(app): S) -> Result<Json<Vec<RoomView>>> {
    app.room_views().map(Json)
}

async fn create_room(State(app): S, Json(room): Json<db::Room>) -> Result<StatusCode> {
    app.save_room(room, true).map(|_| StatusCode::CREATED)
}

async fn get_room(State(app): S, Path(name): Path<String>) -> Result<Json<db::Room>> {
    app.get_room(&name).map(Json)
}

async fn put_room(
    State(app): S,
    Path(name): Path<String>,
    Json(room): Json<db::Room>,
) -> Result<StatusCode> {
    app.save_room(db::Room { name, ..room }, false)
        .map(|_| StatusCode::NO_CONTENT)
}

async fn delete_room(State(app): S, Path(name): Path<String>) -> Result<StatusCode> {
    app.delete_room(&name).map(|_| StatusCode::NO_CONTENT)
}

async fn order_rooms(State(app): S, Json(names): Json<Vec<String>>) -> Result<StatusCode> {
    app.order_rooms(&names).map(|_| StatusCode::NO_CONTENT)
}

async fn end_room(State(app): S, Path(name): Path<String>) -> Result<Json<Value>> {
    app.end_room(&name).map(|n| Json(json!({ "ended": n })))
}

async fn cover_preview(State(app): S, Path(name): Path<String>) -> Result<Response> {
    let jpeg = app.cover_preview(&name)?;
    Ok((
        [
            (header::CONTENT_TYPE, "image/jpeg"),
            (header::CACHE_CONTROL, "no-store"),
        ],
        jpeg,
    )
        .into_response())
}

/// 请求体就是 JPEG 本身：页面已裁成 1146×717 并压好，远小于 axum 默认 2 MB 的上限。
async fn put_cover_bg(
    State(app): S,
    Path(name): Path<String>,
    body: axum::body::Bytes,
) -> Result<StatusCode> {
    app.set_cover_bg(&name, Some(&body))
        .map(|_| StatusCode::NO_CONTENT)
}

async fn delete_cover_bg(State(app): S, Path(name): Path<String>) -> Result<StatusCode> {
    app.set_cover_bg(&name, None)
        .map(|_| StatusCode::NO_CONTENT)
}

async fn list_templates(State(app): S) -> Result<Json<Vec<db::Template>>> {
    Ok(Json(db::templates(&app.db())?))
}

async fn create_template(
    State(app): S,
    Json(t): Json<db::Template>,
) -> Result<(StatusCode, Json<Value>)> {
    let id = app.save_template(t, true)?;
    Ok((StatusCode::CREATED, Json(json!({ "id": id }))))
}

async fn get_template(State(app): S, Path(id): Path<i64>) -> Result<Json<db::Template>> {
    app.get_template(id).map(Json)
}

async fn put_template(
    State(app): S,
    Path(id): Path<i64>,
    Json(t): Json<db::Template>,
) -> Result<StatusCode> {
    app.save_template(db::Template { id, ..t }, false)
        .map(|_| StatusCode::NO_CONTENT)
}

async fn delete_template(State(app): S, Path(id): Path<i64>) -> Result<StatusCode> {
    app.delete_template(id).map(|_| StatusCode::NO_CONTENT)
}

async fn list_accounts(State(app): S) -> Result<Json<Vec<db::AccountInfo>>> {
    app.accounts().map(Json)
}

#[derive(Deserialize)]
struct RenameAccount {
    name: String,
}

async fn rename_account(
    State(app): S,
    Path(id): Path<i64>,
    Json(req): Json<RenameAccount>,
) -> Result<StatusCode> {
    app.rename_account(id, &req.name)
        .map(|_| StatusCode::NO_CONTENT)
}

async fn delete_account(State(app): S, Path(id): Path<i64>) -> Result<StatusCode> {
    app.delete_account(id).map(|_| StatusCode::NO_CONTENT)
}

async fn types(State(app): S, Path(id): Path<i64>) -> Result<Json<Value>> {
    app.types(id).await.map(Json)
}

async fn seasons(State(app): S, Path(id): Path<i64>) -> Result<Json<Value>> {
    app.seasons(id).await.map(Json)
}

#[derive(Deserialize)]
struct NewSeason {
    title: String,
}

async fn create_season(
    State(app): S,
    Path(id): Path<i64>,
    Json(b): Json<NewSeason>,
) -> Result<Json<Value>> {
    app.create_season(id, &b.title)
        .await
        .map(|id| Json(json!({ "section_id": id })))
}

async fn start_login(State(app): S) -> Result<Json<LoginStart>> {
    app.start_login().await.map(Json)
}

async fn poll_login(State(app): S, Path(id): Path<String>) -> Result<Json<LoginState>> {
    app.poll_login(&id).await.map(Json)
}

async fn machines(State(app): S) -> Result<Json<Vec<Machine>>> {
    app.machines().map(Json)
}

#[derive(Deserialize)]
struct CreateMachine {
    name: String,
    #[serde(default)]
    note: String,
    #[serde(default)]
    add_to_rooms: bool,
}

#[derive(Deserialize)]
struct UpdateMachine {
    enabled: Option<bool>,
    note: Option<String>,
}

async fn create_machine(
    State(app): S,
    Json(req): Json<CreateMachine>,
) -> Result<(StatusCode, Json<Value>)> {
    let (name, token) = app.add_machine(&req.name, &req.note, req.add_to_rooms)?;
    Ok((
        StatusCode::CREATED,
        Json(json!({ "name": name, "token": token })),
    ))
}

async fn put_machine(
    State(app): S,
    Path(name): Path<String>,
    Json(req): Json<UpdateMachine>,
) -> Result<StatusCode> {
    app.update_machine(&name, req.enabled, req.note)
        .map(|_| StatusCode::NO_CONTENT)
}

#[derive(Deserialize)]
struct MachineYield {
    until: Option<i64>,
}

async fn put_machine_yield(
    State(app): S,
    Path(name): Path<String>,
    Json(req): Json<MachineYield>,
) -> Result<StatusCode> {
    app.set_yield(&name, req.until)
        .map(|_| StatusCode::NO_CONTENT)
}

async fn get_machine_token(State(app): S, Path(name): Path<String>) -> Result<Json<Value>> {
    app.machine_token(&name)
        .map(|t| Json(json!({ "token": t })))
}

async fn reset_machine_token(State(app): S, Path(name): Path<String>) -> Result<Json<Value>> {
    app.reset_machine_token(&name)
        .map(|t| Json(json!({ "token": t })))
}

async fn delete_machine(State(app): S, Path(name): Path<String>, uri: Uri) -> Result<StatusCode> {
    let force = uri
        .query()
        .is_some_and(|q| q.split('&').any(|kv| kv == "force=1"));
    app.remove_machine(&name, force)
        .map(|_| StatusCode::NO_CONTENT)
}

async fn submits(State(app): S) -> Result<Json<SubmitsView>> {
    app.submits().map(Json)
}

async fn retry_archive(State(app): S, Path(id): Path<i64>) -> Result<StatusCode> {
    app.retry_archive(id).map(|_| StatusCode::NO_CONTENT)
}

async fn discard_archive(State(app): S, Path(id): Path<i64>) -> Result<StatusCode> {
    app.discard_archive(id).map(|_| StatusCode::NO_CONTENT)
}

#[derive(Deserialize)]
struct Password {
    password: String,
}

async fn login(State(app): S, Json(p): Json<Password>) -> Response {
    if app.config.admin_password_hash.is_none() {
        return StatusCode::FORBIDDEN.into_response();
    }
    match app.admin_login(&p.password).await {
        Ok(token) => {
            let cookie = format!(
                "{COOKIE}={token}; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age={}",
                SESSION_MS / 1000
            );
            (StatusCode::NO_CONTENT, [(header::SET_COOKIE, cookie)]).into_response()
        }
        Err(e) => e.into_response(),
    }
}

async fn logout(State(app): S, headers: HeaderMap) -> Result<Response> {
    if let Some(t) = app.session(&headers) {
        db::delete_session(&app.db(), &t)?;
    }
    let clear = format!("{COOKIE}=; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age=0");
    Ok((StatusCode::NO_CONTENT, [(header::SET_COOKIE, clear)]).into_response())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::{config, db_room, test_apps};
    use argon2::password_hash::{PasswordHasher, SaltString};
    use reqwest::Method;

    /// 起真的 HTTP 服务；管理员密码 `pw`（哈希参数调到最小，测试快）。
    async fn serve() -> (String, Arc<App>, crate::tests::FakeQr) {
        let params = argon2::Params::new(8, 1, 1, None).unwrap();
        let hasher =
            argon2::Argon2::new(argon2::Algorithm::Argon2id, argon2::Version::V0x13, params);
        let hash = hasher
            .hash_password(b"pw", &SaltString::generate(&mut OsRng))
            .unwrap()
            .to_string();
        let mut config = config();
        config.admin_password_hash = Some(hash);
        let (app, _, qr) = test_apps(config);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(axum::serve(listener, crate::router(app.clone())).into_future());
        (base, app, qr)
    }

    struct Client {
        base: String,
        http: reqwest::Client,
        cookie: Option<String>,
    }

    impl Client {
        async fn call(
            &self,
            method: Method,
            path: &str,
            body: Option<Value>,
        ) -> (StatusCode, Value) {
            let mut req = self.http.request(method, format!("{}{path}", self.base));
            if let Some(c) = &self.cookie {
                req = req.header("cookie", c);
            }
            if let Some(b) = body {
                req = req.json(&b);
            }
            let res = req.send().await.unwrap();
            let status = res.status();
            (status, res.json().await.unwrap_or(Value::Null))
        }

        async fn login(&mut self, password: &str) -> StatusCode {
            let res = self
                .http
                .post(format!("{}/api/login", self.base))
                .json(&json!({ "password": password }))
                .send()
                .await
                .unwrap();
            if let Some(c) = res.headers().get("set-cookie") {
                let c = c.to_str().unwrap();
                assert!(
                    c.contains("HttpOnly") && c.contains("SameSite=Strict"),
                    "{c}"
                );
                self.cookie = Some(c.split(';').next().unwrap().to_string());
            }
            res.status()
        }
    }

    #[tokio::test]
    async fn console_needs_a_session_separate_from_agent_tokens() {
        let (base, _, _) = serve().await;
        let mut c = Client {
            base: base.clone(),
            http: reqwest::Client::new(),
            cookie: None,
        };
        assert_eq!(
            c.call(Method::GET, "/api/rooms", None).await.0,
            StatusCode::UNAUTHORIZED
        );
        // agent token 进不了控制台。
        let res = c
            .http
            .get(format!("{base}/api/rooms"))
            .bearer_auth("ta")
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(c.login("wrong").await, StatusCode::UNAUTHORIZED);
        assert_eq!(c.login("pw").await, StatusCode::NO_CONTENT);
        assert_eq!(
            c.call(Method::GET, "/api/rooms", None).await.0,
            StatusCode::OK
        );
        // 会话 cookie 进不了 agent 接口。
        let res = c
            .http
            .post(format!("{base}{}", lubic_proto::WATCH))
            .header("cookie", c.cookie.clone().unwrap())
            .json(&lubic_proto::Watch::default())
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
        // 登出后会话失效。
        assert_eq!(
            c.call(Method::POST, "/api/logout", None).await.0,
            StatusCode::NO_CONTENT
        );
        assert_eq!(
            c.call(Method::GET, "/api/rooms", None).await.0,
            StatusCode::UNAUTHORIZED
        );
    }

    #[tokio::test]
    async fn console_without_admin_password_is_disabled() {
        let (app, _, _) = test_apps(config());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(axum::serve(listener, crate::router(app)).into_future());
        let http = reqwest::Client::new();
        let res = http
            .post(format!("{base}/api/login"))
            .json(&json!({"password": ""}))
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::FORBIDDEN);
        let res = http.get(format!("{base}/api/rooms")).send().await.unwrap();
        assert_eq!(res.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn account_names_can_be_changed_without_changing_login_or_templates() {
        let (base, app, _) = serve().await;
        let mut c = Client {
            base,
            http: reqwest::Client::new(),
            cookie: None,
        };
        let path = "/api/accounts/1";
        let body = json!({"name": "  回放投稿号  ", "mid": 99, "login": {}});
        assert_eq!(
            c.call(Method::PUT, path, Some(body.clone())).await.0,
            StatusCode::UNAUTHORIZED
        );
        c.login("pw").await;
        let login = db::account_login(&app.db(), 1).unwrap();
        assert_eq!(
            c.call(Method::PUT, path, Some(body)).await.0,
            StatusCode::NO_CONTENT
        );
        let (_, accounts) = c.call(Method::GET, "/api/accounts", None).await;
        assert_eq!(
            accounts[0],
            json!({"id": 1, "name": "回放投稿号", "mid": 1})
        );
        assert_eq!(db::account_login(&app.db(), 1).unwrap(), login);
        assert_eq!(app.get_template(1).unwrap().account_id, 1);
        assert_eq!(
            c.call(Method::PUT, path, Some(json!({"name": "  "})))
                .await
                .0,
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            c.call(
                Method::PUT,
                "/api/accounts/99",
                Some(json!({"name": "不存在"}))
            )
            .await
            .0,
            StatusCode::NOT_FOUND
        );

        let refreshed = json!({"token_info": {"mid": 1}, "refresh": "new"});
        assert_eq!(
            db::save_account(&app.db(), "新的平台昵称", 1, &refreshed, now_ms()).unwrap(),
            1
        );
        assert_eq!(
            app.accounts().unwrap()[0].name,
            "回放投稿号",
            "重新扫码保留自定义名称"
        );
        assert_eq!(db::account_login(&app.db(), 1).unwrap(), Some(refreshed));
    }

    #[tokio::test]
    async fn rooms_can_be_created_paused_and_pause_and_resume_via_api() {
        let (base, app, _) = serve().await;
        let mut c = Client {
            base,
            http: reqwest::Client::new(),
            cookie: None,
        };
        c.login("pw").await;
        let mut room = json!({
            "name": "paused", "url": "https://live.bilibili.com/9", "template_id": 1,
            "agents": [],
        });
        assert_eq!(
            c.call(Method::POST, "/api/rooms", Some(room.clone()))
                .await
                .0,
            StatusCode::CREATED
        );
        for agents in [json!(["a"]), json!([])] {
            room["agents"] = agents.clone();
            assert_eq!(
                c.call(Method::PUT, "/api/rooms/paused", Some(room.clone()))
                    .await
                    .0,
                StatusCode::NO_CONTENT
            );
            assert_eq!(
                c.call(Method::GET, "/api/rooms/paused", None).await.1["agents"],
                agents
            );
        }
        assert!(db_room(&app, "paused").agents.is_empty());
    }

    #[tokio::test]
    async fn cover_background_upload_preview_and_removal() {
        let (base, app, _qr) = serve().await;
        let mut c = Client {
            base,
            http: reqwest::Client::new(),
            cookie: None,
        };
        c.login("pw").await;
        let send = |method: Method, path: &str, body: Vec<u8>| {
            c.http
                .request(method, format!("{}{path}", c.base))
                .header("cookie", c.cookie.clone().unwrap())
                .body(body)
                .send()
        };
        let jpeg = |w, h| {
            let mut out = Vec::new();
            image::codecs::jpeg::JpegEncoder::new(&mut out)
                .encode_image(&image::RgbImage::from_pixel(
                    w,
                    h,
                    image::Rgb([200, 200, 200]),
                ))
                .unwrap();
            out
        };
        let has_bg = |rooms: &Value| {
            rooms
                .as_array()
                .unwrap()
                .iter()
                .any(|r| r["name"] == "r" && r["cover_bg"] == true)
        };
        let black = send(Method::GET, "/api/rooms/r/cover", vec![])
            .await
            .unwrap();
        assert_eq!(black.headers()["content-type"], "image/jpeg");
        let black = black.bytes().await.unwrap();

        let res = send(Method::PUT, "/api/rooms/r/cover", jpeg(100, 100))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        let res = send(Method::PUT, "/api/rooms/nope/cover", jpeg(1146, 717))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
        let res = send(Method::PUT, "/api/rooms/r/cover", jpeg(1146, 717))
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NO_CONTENT);
        assert!(has_bg(&c.call(Method::GET, "/api/rooms", None).await.1));
        let with_bg = send(Method::GET, "/api/rooms/r/cover", vec![])
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let px = image::load_from_memory(&with_bg)
            .unwrap()
            .to_rgb8()
            .get_pixel(5, 5)
            .0[0];
        assert!((190..=210).contains(&px), "背景保留原始亮度: {px}");
        assert!(db::cover_bg(&app.db(), "r").unwrap().is_some());

        let res = send(Method::DELETE, "/api/rooms/r/cover", vec![])
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NO_CONTENT);
        assert!(!has_bg(&c.call(Method::GET, "/api/rooms", None).await.1));
        let again = send(Method::GET, "/api/rooms/r/cover", vec![])
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(
            image::load_from_memory(&again)
                .unwrap()
                .to_rgb8()
                .get_pixel(5, 5)
                .0,
            [0, 0, 0]
        );
        assert_ne!(black, with_bg);
    }

    #[tokio::test]
    async fn room_tags_append_to_the_template_and_cap_at_ten() {
        let (base, app, _qr) = serve().await;
        let mut c = Client {
            base,
            http: reqwest::Client::new(),
            cookie: None,
        };
        c.login("pw").await;
        let (_, mut tpl) = c.call(Method::GET, "/api/templates/1", None).await;
        let mut put_tpl = |tag: &str| {
            tpl["tag"] = tag.into();
            tpl.clone()
        };
        let body = put_tpl("a,b,c,d,e,f");
        assert_eq!(
            c.call(Method::PUT, "/api/templates/1", Some(body)).await.0,
            StatusCode::NO_CONTENT
        );
        let room = |name: &str, tag: &str| {
            json!({"name": name, "url": "https://live.bilibili.com/9", "template_id": 1,
                   "agents": ["a"], "tag": tag})
        };
        // 和模板重复的 e 只算一次：6 + 3 = 9，去掉空白后存下。
        assert_eq!(
            c.call(Method::POST, "/api/rooms", Some(room("x", " e, x,y ,z,")))
                .await
                .0,
            StatusCode::CREATED
        );
        assert_eq!(db_room(&app, "x").tag, "e,x,y,z");
        let (code, v) = c
            .call(Method::POST, "/api/rooms", Some(room("y", "u,v,w,x,y")))
            .await;
        assert_eq!(code, StatusCode::BAD_REQUEST);
        assert!(v["error"].as_str().unwrap().contains("共 11 个"), "{v}");
        // 模板加标签会把房间 x 撑过上限。
        let body = put_tpl("a,b,c,d,e,f,g,h");
        let (code, v) = c.call(Method::PUT, "/api/templates/1", Some(body)).await;
        assert_eq!(code, StatusCode::BAD_REQUEST);
        assert!(v["error"].as_str().unwrap().contains("房间 x"), "{v}");
        let body = put_tpl("1,2,3,4,5,6,7,8,9,10,11");
        let (code, v) = c.call(Method::PUT, "/api/templates/1", Some(body)).await;
        assert_eq!(code, StatusCode::BAD_REQUEST);
        assert!(v["error"].as_str().unwrap().contains("最多 10 个"), "{v}");
    }

    #[tokio::test]
    async fn rooms_templates_and_accounts_crud_with_validation() {
        let (base, app, qr) = serve().await;
        let mut c = Client {
            base,
            http: reqwest::Client::new(),
            cookie: None,
        };
        c.login("pw").await;
        let room = json!({
            "name": "新房间", "url": "https://live.bilibili.com/9", "template_id": 1,
            "agents": ["b", "a"], "split_size_mb": 200, "tail_upload": true,
        });
        assert_eq!(
            c.call(Method::POST, "/api/rooms", Some(room.clone()))
                .await
                .0,
            StatusCode::CREATED
        );
        assert_eq!(
            c.call(Method::POST, "/api/rooms", Some(room.clone()))
                .await
                .0,
            StatusCode::CONFLICT
        );
        // 校验：地址、机器、边录边传、模板。
        let bad = |k: &str, v: Value| {
            let mut r = room.clone();
            r["name"] = "x".into();
            r[k] = v;
            r
        };
        for (body, want) in [
            (bad("url", "https://example.com/1".into()), "直播间地址"),
            (bad("agents", json!(["zz"])), "没有这台机器"),
            (bad("agents", json!(["a", "a"])), "机器重复"),
            (bad("split_size_mb", Value::Null), "边录边传"),
            (bad("douyin_quality", "4k".into()), "抖音画质"),
            (bad("template_id", 99.into()), "模板不存在"),
            (bad("name", "a/b".into()), "房间名"),
        ] {
            let (code, v) = c.call(Method::POST, "/api/rooms", Some(body)).await;
            assert_eq!(code, StatusCode::BAD_REQUEST);
            assert!(v["error"].as_str().unwrap().contains(want), "{v}");
        }
        // 列表带运行状态，按顺序；改顺序、改内容、删除。
        let (_, list) = c.call(Method::GET, "/api/rooms", None).await;
        let names: Vec<_> = list
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r["name"].as_str().unwrap())
            .collect();
        assert_eq!(names, ["r", "r2", "新房间"]);
        assert_eq!(list[0]["recording"], false);
        assert_eq!(list[0]["agent"], Value::Null);
        c.call(
            Method::POST,
            "/api/rooms/order",
            Some(json!(["新房间", "r"])),
        )
        .await;
        let (_, list) = c.call(Method::GET, "/api/rooms", None).await;
        assert_eq!(list[0]["name"], "新房间");
        let mut edited = room.clone();
        edited["split_time"] = 600.into();
        let path = format!("/api/rooms/{}", urlencoding("新房间"));
        assert_eq!(
            c.call(Method::PUT, &path, Some(edited)).await.0,
            StatusCode::NO_CONTENT
        );
        assert_eq!(db_room(&app, "新房间").split_time, Some(600));
        // 封面名去首尾空白；空的当没填。
        let mut named = room.clone();
        named["display_name"] = " Secret（王者组排） ".into();
        c.call(Method::PUT, &path, Some(named.clone())).await;
        assert_eq!(
            c.call(Method::GET, &path, None).await.1["display_name"],
            "Secret（王者组排）"
        );
        named["display_name"] = "  ".into();
        c.call(Method::PUT, &path, Some(named)).await;
        assert_eq!(db_room(&app, "新房间").display_name, None);
        assert_eq!(
            db_room(&app, "新房间").filename,
            db::DEFAULT_FILENAME,
            "没给模板用默认值"
        );
        let mut custom = room.clone();
        custom["filename"] = "{streamer}_%H{colon}%M".into();
        custom["season_section_id"] = 11.into();
        c.call(Method::PUT, &path, Some(custom)).await;
        let (_, got) = c.call(Method::GET, &path, None).await;
        assert_eq!(got["filename"], "{streamer}_%H{colon}%M");
        assert_eq!(got["season_section_id"], 11, "合集跟房间走");
        c.call(Method::PUT, &path, Some(room.clone())).await;
        assert_eq!(db_room(&app, "新房间").season_section_id, None);
        assert_eq!(
            c.call(Method::DELETE, &path, None).await.0,
            StatusCode::NO_CONTENT
        );
        assert_eq!(
            c.call(Method::GET, &path, None).await.0,
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            c.call(Method::POST, "/api/rooms/r/end", None).await.1["ended"],
            0
        );

        // 模板：转载必须有来源；还有房间在用的不能删。
        let tpl = json!({"name": "t2", "account_id": 1, "title": "{name}", "copyright": 2});
        assert_eq!(
            c.call(Method::POST, "/api/templates", Some(tpl.clone()))
                .await
                .0,
            StatusCode::BAD_REQUEST
        );
        let mut tpl = tpl;
        tpl["source"] = "{url}".into();
        let (code, v) = c
            .call(Method::POST, "/api/templates", Some(tpl.clone()))
            .await;
        assert_eq!(code, StatusCode::CREATED);
        let id = v["id"].as_i64().unwrap();
        let path = format!("/api/templates/{id}");
        let mut edited = tpl.clone();
        edited["tid"] = 17.into();
        assert_eq!(
            c.call(Method::PUT, &path, Some(edited)).await.0,
            StatusCode::NO_CONTENT
        );
        assert_eq!(c.call(Method::GET, &path, None).await.1["tid"], 17);
        assert_eq!(
            c.call(Method::POST, "/api/templates", Some(tpl)).await.0,
            StatusCode::CONFLICT
        );
        assert_eq!(
            c.call(Method::DELETE, "/api/templates/1", None).await.0,
            StatusCode::CONFLICT
        );
        assert_eq!(
            c.call(Method::DELETE, &format!("/api/templates/{id}"), None)
                .await
                .0,
            StatusCode::NO_CONTENT
        );

        // 账号：扫码 → 已扫 → 成功落库；同一 mid 再扫是更新；还有模板在用的不能删。
        qr.0.lock()
            .unwrap()
            .extend([Poll::Scanned, Poll::Done(json!({"token_info": {"mid": 5}}))]);
        let (_, start) = c.call(Method::POST, "/api/accounts/login", None).await;
        assert!(start["svg"].as_str().unwrap().starts_with("<?xml"));
        let poll = format!(
            "/api/accounts/login/{}",
            start["login_id"].as_str().unwrap()
        );
        assert_eq!(c.call(Method::GET, &poll, None).await.1["state"], "scanned");
        let (_, done) = c.call(Method::GET, &poll, None).await;
        assert_eq!(done["state"], "done");
        assert_eq!(
            c.call(Method::GET, &poll, None).await.0,
            StatusCode::NOT_FOUND,
            "用过即删"
        );
        let (_, accounts) = c.call(Method::GET, "/api/accounts", None).await;
        assert_eq!(accounts[1]["name"], "up5");
        assert!(accounts[1].get("login").is_none(), "不回 cookie");
        assert_eq!(
            c.call(Method::GET, "/api/accounts/1/types", None).await.1["typelist"][0]["id"],
            171
        );
        assert_eq!(
            c.call(Method::GET, "/api/accounts/1/seasons", None).await.1["seasons"][0]["sections"]
                ["sections"][0]["id"],
            11
        );
        assert_eq!(
            c.call(Method::GET, "/api/accounts/1/seasons", None).await.1["can_create"],
            true
        );
        let (code, v) = c
            .call(
                Method::POST,
                "/api/accounts/1/seasons",
                Some(json!({"title": "新合集"})),
            )
            .await;
        assert_eq!(
            (code, v["section_id"].as_i64()),
            (StatusCode::OK, Some(1000))
        );
        assert_eq!(
            c.call(
                Method::POST,
                "/api/accounts/1/seasons",
                Some(json!({"title": " "})),
            )
            .await
            .0,
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            c.call(
                Method::POST,
                "/api/accounts/99/seasons",
                Some(json!({"title": "x"})),
            )
            .await
            .0,
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            c.call(Method::GET, "/api/accounts/99/seasons", None)
                .await
                .0,
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            c.call(Method::DELETE, "/api/accounts/1", None).await.0,
            StatusCode::CONFLICT
        );
        let new = done["account_id"].as_i64().unwrap();
        assert_eq!(
            c.call(Method::DELETE, &format!("/api/accounts/{new}"), None)
                .await
                .0,
            StatusCode::NO_CONTENT
        );
    }

    #[tokio::test]
    async fn machines_show_online_limit_disk_and_rooms() {
        let (app, _, _) = test_apps(config());
        let now = now_ms();
        let status = Status {
            disk_usage: 0.815,
            ..Default::default()
        };
        db::touch_agent(&app.db(), "a", now, &status).unwrap();
        let limit = db::Limit {
            since: now,
            until: now + 1,
            reason: "upload".into(),
            need_bps: 1.0,
        };
        db::set_limit(&app.db(), "b", &limit).unwrap();
        let task = db_room(&app, "r").task();
        db::assign(&app.db(), "r", "a", "s", now, &task).unwrap();
        let m = app.machines().unwrap();
        let names: Vec<_> = m.iter().map(|m| (&*m.name, m.online, m.enabled)).collect();
        assert_eq!(
            names,
            [("a", true, true), ("b", false, true), ("c", false, true)]
        );
        assert_eq!(
            (m[0].status.disk_usage, &*m[0].rooms),
            (0.815, &["r".to_string()][..])
        );
        assert_eq!(m[1].limit.as_ref().unwrap().reason, "upload");
        assert_eq!(m[1].last_seen, None);
    }

    #[test]
    fn commit_link_uses_the_full_sha_and_handles_local_builds() {
        let sha = "d7d35240de77ac7d309a5f27e4ba9b85561e31c6";
        let link = commit_link(sha);
        assert!(link.contains(&format!(
            "href=\"https://github.com/dplei/lubic/commit/{sha}\""
        )));
        assert!(link.contains(">d7d35240</a>"));
        assert!(link.contains("target=\"_blank\" rel=\"noopener noreferrer\""));
        for value in ["", "dev", "<script>", "d7d3524"] {
            assert_eq!(commit_link(value), "<span tabindex=\"0\">(none)</span>");
        }
    }

    #[test]
    fn push_time_is_beijing_time_and_handles_missing_values() {
        assert_eq!(
            push_time("0"),
            "推送到 GitHub：1970-01-01 08:00:00（北京时间）"
        );
        for value in ["", "invalid", "9223372036854775807"] {
            assert_eq!(push_time(value), "未记录 GitHub 推送时间");
        }
    }

    #[tokio::test]
    async fn page_is_served_without_a_session() {
        let (base, _, _) = serve().await;
        let res = reqwest::get(format!("{base}/")).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let html = res.text().await.unwrap();
        assert!(html.contains("lubic 控制台") && !html.contains("{{commit}}"));
        assert!(!html.contains("{{pushed_at}}"));
        assert!(html.contains(&push_time(option_env!("GIT_PUSHED_AT").unwrap_or(""))));
        assert!(
            html.contains("id=\"rec\"")
                && !html.contains("{{logo}}")
                && !html.contains("@keyframes rec{")
        );
        let res = reqwest::get(format!("{base}/favicon.svg")).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(res.headers()[header::CONTENT_TYPE], "image/svg+xml");
        let svg = res.text().await.unwrap();
        assert!(svg.contains("id=\"rec\"") && !svg.contains("<style>"));
    }

    #[test]
    fn room_view_shows_the_current_session_and_its_parts() {
        use db::tests::{closed, opened, uploaded};
        let (app, _, _) = test_apps(config());
        let events = [
            opened("p1", "s", 1000),
            closed("p1", "s", 2000),
            uploaded("p1"),
            opened("p2", "s", 2000),
            closed("p2", "s", 3000),
            opened("p3", "s", 3000),
        ];
        db::apply_all(&mut app.db(), "a", &events).unwrap();
        let views = app.room_views().unwrap();
        let s = views[0].session.as_ref().unwrap();
        assert!(views[0].recording);
        assert_eq!((s.started_at, &*s.title), (1000, "直播"));
        let states: Vec<_> = s.parts.iter().map(|p| &*p.part.state).collect();
        assert_eq!(states, ["uploaded", "closed", "open"]);

        // 在线机器报 p2 在传：p2 变在传并带进度；失联机器的旧心跳不算。
        let status = |part: &str| Status {
            uploading: vec![(part.into(), 1, 4)],
            ..Default::default()
        };
        db::touch_agent(&app.db(), "a", now_ms(), &status("p2")).unwrap();
        db::touch_agent(&app.db(), "b", 0, &status("p1")).unwrap();
        let views = app.room_views().unwrap();
        let parts = &views[0].session.as_ref().unwrap().parts;
        assert_eq!(
            (&*parts[1].part.state, parts[1].progress),
            ("uploading", Some(0.25))
        );
        assert_eq!(parts[0].part.state, "uploaded");
        assert!(views[1].session.is_none());
    }

    #[test]
    fn room_view_tracks_last_ended_live_started_at() {
        use db::tests::{closed, opened};
        let (app, _, _) = test_apps(config());
        let view = |room: &str| {
            app.room_views()
                .unwrap()
                .into_iter()
                .find(|v| v.room.name == room)
                .unwrap()
        };

        let r = view("r");
        assert_eq!(r.last_live_started_at, None);
        assert_eq!(
            serde_json::to_value(&r).unwrap()["last_live_started_at"],
            Value::Null
        );

        db::apply(&app.db(), "a", &opened("p1", "s1", 1000)).unwrap();
        let r = view("r");
        assert_eq!(r.session.as_ref().unwrap().started_at, 1000);
        assert_eq!(r.last_live_started_at, None);
        db::apply(&app.db(), "a", &closed("p1", "s1", 2000)).unwrap();
        let r = view("r");
        assert!(r.recording && r.session.is_some());
        assert_eq!(r.last_live_started_at, None);

        db::end_room(&app.db(), "r", 3000).unwrap();
        let r = view("r");
        assert!(!r.recording);
        assert!(r.session.is_none());
        assert_eq!(r.last_live_started_at, Some(1000));

        for phase in [db::SUBMITTED, db::ATTENTION, db::SUBMITTING] {
            app.db()
                .execute("UPDATE archives SET phase = ?1 WHERE id = 1", [phase])
                .unwrap();
            assert_eq!(view("r").last_live_started_at, Some(1000), "{phase}");
        }

        db::apply(&app.db(), "a", &opened("p2", "s2", 5000)).unwrap();
        let r = view("r");
        assert_eq!(r.session.as_ref().unwrap().started_at, 5000);
        assert_eq!(r.last_live_started_at, Some(1000));
        db::end_room(&app.db(), "r", 6000).unwrap();
        let r = view("r");
        assert!(r.session.is_none());
        assert_eq!(r.last_live_started_at, Some(5000));

        app.db()
            .execute(
                "INSERT INTO archives(room, session, title, started_at, ended_at, phase)
                 VALUES ('r', 's0', '补录', 500, 900, ?1)",
                [db::ENDED],
            )
            .unwrap();
        assert_eq!(view("r").last_live_started_at, Some(5000));

        app.db()
            .execute(
                "INSERT INTO archives(room, session, title, started_at, ended_at, phase)
                 VALUES ('r2', 's9', '别家', 9000, 9500, ?1)",
                [db::ENDED],
            )
            .unwrap();
        assert_eq!(view("r").last_live_started_at, Some(5000));
        assert_eq!(view("r2").last_live_started_at, Some(9000));

        let mut paused = db_room(&app, "r");
        paused.agents.clear();
        db::save_room(&app.db(), &paused).unwrap();
        let r = view("r");
        assert!(r.room.agents.is_empty());
        assert_eq!(r.last_live_started_at, Some(5000));
    }

    fn urlencoding(s: &str) -> String {
        s.bytes().map(|b| format!("%{b:02X}")).collect()
    }

    #[test]
    fn add_machine_validates_name_and_optionally_appends_to_active_rooms() {
        let (app, _, _) = test_apps(config());
        for bad_name in ["", "   ", &"x".repeat(33), "a b", "a/b", "机器"] {
            assert!(matches!(
                app.add_machine(bad_name, "", false),
                Err(Error::Bad(_))
            ));
        }
        assert_eq!(
            app.add_machine("a", "", false),
            Err(Error::Conflict("已有同名机器".into()))
        );

        db::delete_room(&app.db(), "r2").unwrap();
        let (name, token) = app.add_machine(" node_1-a ", " 备注 ", true).unwrap();
        assert_eq!(name, "node_1-a");
        assert_eq!(token.len(), 64);
        assert_eq!(app.machine_token("node_1-a").unwrap(), token);
        assert_eq!(
            db::machine(&app.db(), "node_1-a").unwrap().unwrap().note,
            "备注"
        );
        assert_eq!(db_room(&app, "r").agents, ["a", "b", "node_1-a"]);
        assert_eq!(db_room(&app, "r2").agents, ["c", "a"], "已删除的房间不追加");
    }

    #[test]
    fn remove_machine_checks_preconditions_and_succeeds_when_idle() {
        use crate::tests::assign;
        use db::tests::closed;

        let conflict = |res: Result<()>| match res {
            Err(Error::Conflict(msg)) => msg,
            other => panic!("expected Conflict, got {other:?}"),
        };

        // 1. 未停用。
        let (app, _, _) = test_apps(config());
        assert!(conflict(app.remove_machine("b", false)).contains("还没停用"));

        // 2a. 分配着房间。
        let (app, _, _) = test_apps(config());
        app.update_machine("b", Some(false), None).unwrap();
        assign(&app, "r", "b", 100);
        assert!(conflict(app.remove_machine("b", false)).contains("还分配着 1 个房间：r"));

        // 2b. 交接中。
        let (app, _, _) = test_apps(config());
        app.update_machine("b", Some(false), None).unwrap();
        db::start_handoff(&app.db(), "r", "a", "b", 100).unwrap();
        assert!(conflict(app.remove_machine("b", false)).contains("还在交接 1 个房间：r"));

        // 3a. 有 closed 分段没传完。
        let (app, _, _) = test_apps(config());
        app.update_machine("b", Some(false), None).unwrap();
        db::apply(&app.db(), "b", &closed("p1", "s", 100)).unwrap();
        assert!(conflict(app.remove_machine("b", false)).contains("还有 1 个分段没传完"));

        // 3b. 在线心跳 queued_parts 非 0。
        let (app, _, _) = test_apps(config());
        app.update_machine("b", Some(false), None).unwrap();
        let st = Status {
            queued_parts: 2,
            outbox: 1,
            ..Default::default()
        };
        db::touch_agent(&app.db(), "b", now_ms(), &st).unwrap();
        assert!(
            conflict(app.remove_machine("b", false))
                .contains("机器上还有 2 个分段待传、1 条事件未上报")
        );

        // 4. 房间只剩这一台优先机器（r2 先改成只有 c）。
        let (app, _, _) = test_apps(config());
        let mut r2 = db_room(&app, "r2");
        r2.agents = vec!["c".into()];
        db::save_room(&app.db(), &r2).unwrap();
        app.update_machine("c", Some(false), None).unwrap();
        assert_eq!(
            conflict(app.remove_machine("c", false)),
            "房间 r2 只剩这一台优先机器"
        );

        // 满足全部条件：停用后移除 b，房间 r 的列表变成 [a]，同名能再加回来。
        app.update_machine("b", Some(false), None).unwrap();
        app.remove_machine("b", false).unwrap();
        assert_eq!(db_room(&app, "r").agents, ["a"]);
        assert_eq!(app.remove_machine("b", false), Err(Error::NotFound));
        let (added, _) = app.add_machine("b", "re-added", false).unwrap();
        assert_eq!(added, "b");
    }

    #[test]
    fn force_remove_rejects_online_machine_and_settles_parts_when_offline() {
        use crate::tests::{assign, seen};
        use db::tests::{closed, ended, opened, uploaded};

        let (app, _, _) = test_apps(config());
        seen(&app, "b", now_ms(), false);
        match app.remove_machine("b", true) {
            Err(Error::Conflict(msg)) => assert!(msg.contains("机器在线"), "{msg}"),
            other => panic!("expected Conflict, got {other:?}"),
        }

        // b 离线：分配与交接被撤，open/closed 分P标为 lost，已结束稿件变为 ready。
        let (app, _, _) = test_apps(config());
        assign(&app, "r", "b", 100);
        db::start_handoff(&app.db(), "r2", "c", "b", 100).unwrap();
        db::apply_all(
            &mut app.db(),
            "a",
            &[closed("pa", "s", 100), uploaded("pa")],
        )
        .unwrap();
        db::apply_all(
            &mut app.db(),
            "b",
            &[
                opened("pb1", "s", 110),
                closed("pb2", "s", 120),
                ended("s", 200),
            ],
        )
        .unwrap();
        assert!(db::ready(&app.db(), i64::MAX).unwrap().is_empty());

        app.remove_machine("b", true).unwrap();

        assert_eq!(db::assignment(&app.db(), "r").unwrap(), None);
        assert_eq!(db::handoff(&app.db(), "r2").unwrap(), None);
        assert_eq!(db::ready(&app.db(), i64::MAX).unwrap(), [1]);
        assert_eq!(db_room(&app, "r").agents, ["a"]);
    }

    #[tokio::test]
    async fn machines_http_endpoints_enforce_session_and_hide_token_in_list() {
        use crate::tests::seen;
        let (base, app, _) = serve().await;
        let mut c = Client {
            base,
            http: reqwest::Client::new(),
            cookie: None,
        };
        assert_eq!(
            c.call(Method::GET, "/api/machines", None).await.0,
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            c.call(Method::POST, "/api/machines", Some(json!({"name": "d"})))
                .await
                .0,
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            c.call(Method::DELETE, "/api/machines/a", None).await.0,
            StatusCode::UNAUTHORIZED
        );

        c.login("pw").await;
        let (code, list) = c.call(Method::GET, "/api/machines", None).await;
        assert_eq!(code, StatusCode::OK);
        let first = &list.as_array().unwrap()[0];
        assert!(first.get("token").is_none());
        assert_eq!(first["enabled"], true);
        assert_eq!(first["note"], "");
        assert!(first.get("created_at").unwrap().is_i64());

        let (code, created) = c
            .call(
                Method::POST,
                "/api/machines",
                Some(json!({"name": "d", "note": "new"})),
            )
            .await;
        assert_eq!(code, StatusCode::CREATED);
        assert_eq!(created["name"], "d");
        let token = created["token"].as_str().unwrap();
        assert_eq!(token.len(), 64);
        assert_eq!(
            c.call(Method::GET, "/api/machines/d/token", None).await.1["token"],
            token
        );
        let (_, reset) = c.call(Method::POST, "/api/machines/d/token", None).await;
        assert_ne!(reset["token"].as_str().unwrap(), token);
        assert_eq!(
            c.call(
                Method::PUT,
                "/api/machines/d",
                Some(json!({"enabled": false, "note": "off"}))
            )
            .await
            .0,
            StatusCode::NO_CONTENT
        );
        assert_eq!(
            c.call(Method::DELETE, "/api/machines/d", None).await.0,
            StatusCode::NO_CONTENT
        );

        seen(&app, "a", now_ms(), false);
        assert_eq!(
            c.call(Method::DELETE, "/api/machines/a?force=1", None)
                .await
                .0,
            StatusCode::CONFLICT
        );
    }

    /// `PUT /api/machines/{name}/yield`：未来 24 小时内才收，`null` 结束，未登录 401；列表里过期的回 null。
    #[tokio::test]
    async fn machine_yield_endpoint_validates_until_and_lists_it() {
        const HOUR: i64 = 3600 * 1000;
        let (base, app, _) = serve().await;
        let mut c = Client {
            base,
            http: reqwest::Client::new(),
            cookie: None,
        };
        async fn put(c: &Client, name: &str, until: Value) -> StatusCode {
            let path = format!("/api/machines/{name}/yield");
            let body = json!({ "until": until });
            c.call(Method::PUT, &path, Some(body)).await.0
        }
        assert_eq!(
            put(&c, "a", json!(now_ms() + HOUR)).await,
            StatusCode::UNAUTHORIZED
        );
        c.login("pw").await;
        for until in [now_ms() - 1, now_ms() + 24 * HOUR + 60_000] {
            assert_eq!(
                put(&c, "a", json!(until)).await,
                StatusCode::BAD_REQUEST,
                "{until}"
            );
        }
        assert_eq!(
            put(&c, "nope", json!(now_ms() + HOUR)).await,
            StatusCode::NOT_FOUND
        );
        let until = now_ms() + HOUR;
        assert_eq!(put(&c, "a", json!(until)).await, StatusCode::NO_CONTENT);
        let yield_of = |list: &Value, name: &str| {
            let m = list.as_array().unwrap().iter().find(|m| m["name"] == name);
            m.unwrap()["yield_until"].clone()
        };
        let list = c.call(Method::GET, "/api/machines", None).await.1;
        assert_eq!(yield_of(&list, "a"), json!(until));
        assert_eq!(yield_of(&list, "b"), Value::Null);
        assert!(db::yielding(&app.db(), "a", now_ms()).unwrap());
        // 已到点的不再显示。
        db::set_yield(&app.db(), "b", Some(now_ms() - 1)).unwrap();
        let list = c.call(Method::GET, "/api/machines", None).await.1;
        assert_eq!(yield_of(&list, "b"), Value::Null);
        assert_eq!(put(&c, "a", Value::Null).await, StatusCode::NO_CONTENT);
        assert!(!db::yielding(&app.db(), "a", now_ms()).unwrap());
    }

    #[tokio::test]
    async fn create_season_makes_one_with_a_title_cover() {
        let (app, fakes, _) = test_apps(config());
        let fake = fakes.get(1);
        assert_eq!(app.create_season(1, "  房间 A ").await, Ok(1000));
        assert_eq!(
            *fake.seasons_created.lock().unwrap(),
            [("房间 A".into(), "https://cover/1".into())]
        );
        assert_eq!(
            fake.covers.lock().unwrap().last().unwrap(),
            &crate::cover::render(None, &["房间 A"])
        );
    }

    #[tokio::test]
    async fn seasons_say_whether_account_can_create() {
        let (app, fakes, _) = test_apps(config());
        assert_eq!(app.seasons(1).await.unwrap()["can_create"], true);
        *fakes.get(1).no_season.lock().unwrap() = true;
        assert_eq!(app.seasons(1).await.unwrap()["can_create"], false);
    }

    #[tokio::test]
    async fn create_season_picks_existing_same_title() {
        let (app, fakes, _) = test_apps(config());
        let fake = fakes.get(1);
        assert_eq!(app.create_season(1, "合集").await, Ok(11));
        assert!(fake.seasons_created.lock().unwrap().is_empty());
        assert!(fake.covers.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn create_season_rejects_empty_title() {
        let (app, fakes, _) = test_apps(config());
        let fake = fakes.get(1);
        assert!(matches!(
            app.create_season(1, "  ").await,
            Err(Error::Bad(_))
        ));
        assert!(fake.seasons_created.lock().unwrap().is_empty());
        assert!(fake.covers.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn create_season_unknown_account() {
        let (app, _, _) = test_apps(config());
        assert_eq!(app.create_season(99, "x").await, Err(Error::NotFound));
    }

    #[tokio::test]
    async fn create_season_reports_missing_section() {
        let (app, fakes, _) = test_apps(config());
        let fake = fakes.get(1);
        *fake.created_season_hidden.lock().unwrap() = true;
        assert!(matches!(
            app.create_season(1, "房间 A").await,
            Err(Error::Internal(_))
        ));
        assert_eq!(fake.seasons_created.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn submits_need_a_session() {
        let (base, _, _) = serve().await;
        let c = Client {
            base,
            http: reqwest::Client::new(),
            cookie: None,
        };
        assert_eq!(
            c.call(Method::GET, "/api/submits", None).await.0,
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            c.call(Method::POST, "/api/archives/1/retry", None).await.0,
            StatusCode::UNAUTHORIZED
        );
    }

    #[tokio::test]
    async fn submits_show_where_each_archive_is_stuck() {
        use db::tests::{closed, ended, opened, uploaded};
        let (base, app, _) = serve().await;
        let mut c = Client {
            base,
            http: reqwest::Client::new(),
            cookie: None,
        };
        c.login("pw").await;

        let login2 = json!({"token_info": {"mid": 2}});
        let acc2 = db::save_account(&app.db(), "acc2", 2, &login2, 0).unwrap();

        const H: i64 = 3_600_000;
        // 1: 在录（recording），不出现在 pending。
        db::apply(&app.db(), "a", &opened("p0", "s0", 10)).unwrap();

        // 2: ended 且还有未传分P → uploading；lost 不计入 total。
        db::apply_all(
            &mut app.db(),
            "a",
            &[
                closed("p1", "s1", H + 20),
                uploaded("p1"),
                closed("p2", "s1", H + 21),
                closed("p3", "s1", H + 22),
                lubic_proto::AgentEvent::Lost {
                    part_id: "p3".into(),
                },
                ended("s1", H + 25),
            ],
        )
        .unwrap();

        // 各场相隔一小时，超出下播宽限，不会并成一份。
        let make_ready = |app: &App, session: &str, part: &str| {
            let t = session[1..].parse::<i64>().unwrap() * H;
            db::apply_all(
                &mut app.db(),
                "a",
                &[
                    closed(part, session, t + 30),
                    uploaded(part),
                    ended(session, t + 35),
                ],
            )
            .unwrap();
        };
        // 3: submitting
        make_ready(&app, "s2", "p4");
        db::set_phase(&app.db(), 3, db::SUBMITTING, None, None).unwrap();

        // 4: attention
        make_ready(&app, "s3", "p5");
        db::set_phase(
            &app.db(),
            4,
            db::ATTENTION,
            Some("submit outcome unknown"),
            None,
        )
        .unwrap();

        // 5: ready（账号 1 无闸）
        make_ready(&app, "s4", "p6");

        // 6: interval（钉住账号 2，strikes 0、submit_after 在未来）
        make_ready(&app, "s5", "p7");
        app.db()
            .execute("UPDATE archives SET account_id = ?1 WHERE id = 6", [acc2])
            .unwrap();
        let future = now_ms() + 60_000;
        app.gates.lock().unwrap().insert(
            acc2,
            crate::Gate {
                submit_after: future,
                submit_strikes: 0,
                ..Default::default()
            },
        );

        let (code, body) = c.call(Method::GET, "/api/submits", None).await;
        assert_eq!(code, StatusCode::OK);
        let pending = body["pending"].as_array().unwrap();
        let summary: Vec<_> = pending
            .iter()
            .map(|p| {
                (
                    p["id"].as_i64().unwrap(),
                    p["state"].as_str().unwrap(),
                    p["at"].as_i64(),
                )
            })
            .collect();
        assert_eq!(
            summary,
            [
                (2, "uploading", None),
                (3, "submitting", None),
                (4, "attention", None),
                (5, "ready", None),
                (6, "interval", Some(future)),
            ]
        );
        assert_eq!(
            (
                pending[0]["parts_uploaded"].as_i64(),
                pending[0]["parts_total"].as_i64()
            ),
            (Some(1), Some(2))
        );
        assert!(pending[0].get("parts_unfinished").is_none());
        assert!(pending[0].get("progress").is_none());

        // 在线机器报 p2 在传：稿件 2 带上当前进度，机器行带上房间与进度。
        let status = Status {
            uploading: vec![("p2".into(), 1, 4)],
            ..Default::default()
        };
        db::touch_agent(&app.db(), "a", now_ms(), &status).unwrap();
        let (_, body) = c.call(Method::GET, "/api/submits", None).await;
        assert_eq!(body["pending"][0]["progress"], 0.25);
        let m = app.machines().unwrap();
        assert_eq!(m[0].uploading, [("r".to_string(), 0.25)]);
        assert_eq!(pending[2]["note"], "submit outcome unknown");
    }

    #[test]
    fn paused_account_probes_the_first_archive_not_in_backoff() {
        use db::tests::{closed, ended, uploaded};
        let (app, _, _) = test_apps(config());
        // 各场相隔一小时，超出下播宽限，不会并成一份。
        for (i, (s, p)) in [("s1", "p1"), ("s2", "p2"), ("s3", "p3")]
            .into_iter()
            .enumerate()
        {
            let t = i as i64 * 3_600_000;
            db::apply_all(
                &mut app.db(),
                "a",
                &[closed(p, s, t + 10), uploaded(p), ended(s, t + 11)],
            )
            .unwrap();
        }
        let now = now_ms();
        let submit_after = now + 120_000;
        app.gates.lock().unwrap().insert(
            1,
            crate::Gate {
                submit_after,
                submit_strikes: 1,
                ..Default::default()
            },
        );
        app.retry.lock().unwrap().insert(1, (1, now + 30_000));

        let v = app.submits().unwrap();
        assert_eq!(
            (v.accounts[0].strikes, v.accounts[0].submit_after),
            (1, Some(submit_after))
        );
        let states: Vec<_> = v
            .pending
            .iter()
            .map(|p| (p.archive.id, p.state, p.at))
            .collect();
        assert_eq!(
            states,
            [
                (1, "backoff", Some(submit_after)),
                (2, "probe", Some(submit_after)),
                (3, "paused", None),
            ]
        );
    }

    #[test]
    fn expired_gate_shows_no_submit_after() {
        use db::tests::{closed, ended, uploaded};
        let (app, _, _) = test_apps(config());
        db::apply_all(
            &mut app.db(),
            "a",
            &[closed("p1", "s1", 10), uploaded("p1"), ended("s1", 11)],
        )
        .unwrap();
        app.gates.lock().unwrap().insert(
            1,
            crate::Gate {
                submit_after: now_ms() - 1,
                submit_strikes: 2,
                ..Default::default()
            },
        );
        let v = app.submits().unwrap();
        assert_eq!(v.accounts[0].submit_after, None);
        assert_eq!((v.pending[0].state, v.pending[0].at), ("ready", None));
    }

    #[test]
    fn submitted_list_keeps_seven_days_newest_first() {
        use db::tests::{closed, ended, uploaded};
        let (app, _, _) = test_apps(config());
        let now = now_ms();
        let day = 24 * 3600 * 1000;
        let recent_at = now - 3600 * 1000;
        let ended_two_days = now - 2 * day;
        let old_at = now - 8 * day;
        for (i, (s, p, end_t, sub_t, bvid)) in [
            ("s1", "p1", recent_at - 10, Some(recent_at), "BV1"),
            ("s2", "p2", ended_two_days, None, "BV2"),
            ("s3", "p3", old_at - 10, Some(old_at), "BV3"),
        ]
        .into_iter()
        .enumerate()
        {
            let id = i as i64 + 1;
            db::apply_all(
                &mut app.db(),
                "a",
                &[closed(p, s, end_t - 10), uploaded(p), ended(s, end_t)],
            )
            .unwrap();
            db::submitted(&app.db(), id, bvid, id as u64, None, sub_t.unwrap_or(0)).unwrap();
            if sub_t.is_none() {
                app.db()
                    .execute(
                        "UPDATE archives SET submitted_at = NULL WHERE id = ?1",
                        [id],
                    )
                    .unwrap();
            }
        }
        let v = app.submits().unwrap();
        let got: Vec<_> = v
            .submitted
            .iter()
            .map(|s| (s.id, &*s.bvid, s.submitted_at))
            .collect();
        assert_eq!(got, [(1, "BV1", recent_at), (2, "BV2", ended_two_days)]);
    }

    #[tokio::test]
    async fn retry_clears_only_the_archive_backoff() {
        use db::tests::{closed, ended, uploaded};
        let (app, fakes, _) = test_apps(config());
        let fake = fakes.get(1);
        for (i, (s, p)) in [("s1", "p1"), ("s2", "p2")].into_iter().enumerate() {
            let t = i as i64 * 3_600_000;
            db::apply_all(
                &mut app.db(),
                "a",
                &[closed(p, s, t + 10), uploaded(p), ended(s, t + 11)],
            )
            .unwrap();
        }
        assert_eq!(
            retry_archive(State(app.clone()), Path(99)).await,
            Err(Error::NotFound)
        );
        assert_eq!(
            retry_archive(State(app.clone()), Path(1)).await,
            Err(Error::Conflict("这份稿件不在退避中".into()))
        );

        app.retry.lock().unwrap().insert(1, (1, now_ms() + 60_000));
        assert_eq!(
            retry_archive(State(app.clone()), Path(1)).await,
            Ok(StatusCode::NO_CONTENT)
        );
        assert!(!app.retry.lock().unwrap().contains_key(&1));
        app.submit_ready().await.unwrap();
        assert_eq!(fake.submitted.lock().unwrap().len(), 1);

        let pause_until = now_ms() + 120_000;
        app.gates.lock().unwrap().insert(
            1,
            crate::Gate {
                submit_after: pause_until,
                submit_strikes: 1,
                ..Default::default()
            },
        );
        app.retry.lock().unwrap().insert(2, (1, now_ms() + 60_000));
        assert_eq!(
            retry_archive(State(app.clone()), Path(2)).await,
            Ok(StatusCode::NO_CONTENT)
        );
        assert_eq!(app.gates.lock().unwrap()[&1].submit_after, pause_until);
        app.submit_ready().await.unwrap();
        assert_eq!(fake.submitted.lock().unwrap().len(), 1);
        let v = app.submits().unwrap();
        assert_eq!(
            (v.pending[0].archive.id, v.pending[0].state, v.pending[0].at),
            (2, "probe", Some(pause_until))
        );
    }
}
