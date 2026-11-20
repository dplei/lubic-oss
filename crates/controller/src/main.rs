//! 总控：检测开播 → 按优先机器列表分配 agent → 替每个分P preupload 下发凭证 → 下播后一次性投稿。
//!
//! 投稿账号的 cookie 只在这里。agent 经 HTTP 长轮询主动连过来（协议见 `lubic-proto`），
//! 配置与状态存 SQLite（见 `db.rs`），控制台接口见 `console.rs`。

mod console;
mod cover;
mod db;
#[cfg(test)]
mod demo;
mod trial;

use async_trait::async_trait;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{Local, TimeZone};
use clap::{Parser, Subcommand};
use lubic::downloader::live::{LivePlugin, LiveRequest, LiveStatus, builtin_plugins};
use lubic_account::{Account, Poll, Save, Studio, SubmitError, Submitted, Via, Video};
use lubic_proto::{
    Ack, Assignments, Drain, DrainRequest, PROBE_PREFIX, Report, RoomTask, SKIP_WINDOW, Status,
    TRIAL_PREFIX, TicketRequest, Version, Watch,
};
use rusqlite::Connection;
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;
use tracing::{error, info, warn};

#[derive(Parser)]
#[command(version = if lubic_proto::COMMIT.is_empty() { "dev" } else { lubic_proto::COMMIT })]
struct Cli {
    /// 配置文件（TOML）。
    #[arg(short, long, default_value = "lubic-controller.toml")]
    config: PathBuf,
    /// 日志目录：除 stdout 外再按天写 `controller.YYYY-MM-DD.log`，保留 14 天。不配只输出到 stdout。
    #[arg(long)]
    log_dir: Option<PathBuf>,
    /// 不真投稿：只打印要投的稿件，稿件标为 submitted、bvid 记 `dry-run`。
    #[arg(long)]
    dry_run: bool,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// 扫码登录 B 站，把投稿账号存进数据库（同一账号再登录是更新）。
    Login {
        /// 二维码图片输出路径。
        #[arg(default_value = "qrcode.svg")]
        qrcode: PathBuf,
    },
    /// 手动场末：把房间没结束的稿件标为结束，分P传完后投稿。总控、agent 运行中都能用。
    End { room: String },
    /// 打印取回已上传原片的 curl 命令（含凭证，别贴进日志）。凭证签发后约 5 天有效。
    Upos {
        /// `part_id`，或录制路径的一部分（如 `某房间 2026-11-01 13:04`）。
        query: String,
    },
    /// 从标准输入读一行密码，打印配置里 `admin_password_hash` 要填的哈希。
    HashPassword,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    /// 只听本机，公网经反向代理加 TLS 转进来。
    #[serde(default = "default_listen")]
    listen: SocketAddr,
    /// SQLite 数据库：房间、模板、投稿账号的 cookie 与运行状态都在里面，别入库。
    db: PathBuf,
    /// 控制台管理员密码的 argon2 哈希（`hash-password` 子命令生成）。不配则控制台不可用。
    admin_password_hash: Option<String>,
    /// agent 多久没来就算失联：不再分给它，已分给它的在播房间改派。
    #[serde(default = "default_offline_after", deserialize_with = "duration")]
    offline_after: Duration,
    /// 开播检测间隔。
    #[serde(default = "default_poll", deserialize_with = "duration")]
    poll_interval: Duration,
    /// 受限标记最长保留多久：首次标记保留它的 1/3，到期复查不达标依次续 2/3、1 倍，之后每次 1 倍；
    /// 解除后这么久内又受限，接着上次的档位往上走。只为实测时能缩短才可配。
    #[serde(default = "default_limited_for", deserialize_with = "duration")]
    limited_for: Duration,
    /// 总开关：先让备用机故障试拉、健康了才迁异常房间（`docs/design/handoff-health.md` §4）。默认关闭：
    /// 关着时走到要试拉这一步只打一条 `health.trial` 日志，其余行为不变。
    #[serde(default)]
    health_trial: bool,
    /// 钉钉机器人 webhook：要人处理的事（目前是投稿被拒 21588）发一条消息。不配就只打日志。
    dingtalk_webhook: Option<String>,
    /// 机器人开了加签时填 `SEC…`。
    dingtalk_secret: Option<String>,
    /// agent 名 → token。只在库升到迁移 9 时导入一次，之后在控制台管理（见 `docs/design/machines.md`）。
    #[serde(default)]
    agents: HashMap<String, AgentConfig>,
    /// 以下三项只在首次启动时导入数据库，之后以数据库为准、不再读（见 `docs/design/config-db.md`）。
    /// biliup 格式的登录信息文件。
    cookies: Option<PathBuf>,
    /// 房间没配 `agents` 时用的优先机器列表。
    #[serde(default)]
    default_agents: Vec<String>,
    #[serde(default)]
    rooms: Vec<TomlRoom>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AgentConfig {
    token: String,
}

fn default_listen() -> SocketAddr {
    "127.0.0.1:7070".parse().unwrap()
}
fn default_offline_after() -> Duration {
    Duration::from_secs(120)
}
fn default_poll() -> Duration {
    Duration::from_secs(30)
}
fn default_limited_for() -> Duration {
    Duration::from_secs(3600)
}

/// 录制实时倍率低于它就判受限（agent 报的是 2 分钟内各 30 秒子窗口的最高值，低于它即持续低了 2 分钟）。
const RECORD_RATE_MIN: f64 = 0.9;
/// [`SKIP_WINDOW`] 内时间戳前跳至少这么多次、且跳过至少 [`SKIP_MS_MIN`]（一半以上内容没收到）就判受限。
/// 要求多次是因为主播断流重推一般只跳一次；CDN 一阵一阵给数据时每 20 秒左右就跳一次（2026-10-30 云服务器 B）。
const SKIP_JUMPS_MIN: u32 = 3;
const SKIP_MS_MIN: u64 = 30_000;

/// 一个房间最近 [`SKIP_WINDOW`] 的跳过量够判受限（正常录制与试拉同一口径）。
fn skips_bad(skipped_ms: u64, jumps: u32) -> bool {
    jumps >= SKIP_JUMPS_MIN && skipped_ms >= SKIP_MS_MIN
}

fn failures_bad(attempts: u32, video_stalled: bool) -> bool {
    attempts >= 2 || video_stalled
}

fn failure_reason(attempts: u32, video_stalled: bool) -> String {
    if video_stalled {
        "video stalled".into()
    } else {
        format!("empty attempts {attempts}")
    }
}

/// 丢内容的受限原因，以 `record` 开头：`move_if_limited` 据此记 `rate_moved`，源站慢的例外同样适用。
fn skipped_reason(prefix: &str, skipped_ms: u64) -> String {
    format!(
        "{prefix}skipped {}s/{}s",
        skipped_ms / 1000,
        SKIP_WINDOW.as_secs()
    )
}

/// 试拉的结果键（`probe:<房间名>`）。
fn is_probe(room: &str) -> bool {
    room.starts_with(PROBE_PREFIX)
}
/// 试拉或故障试拉的键（`probe:`、`trial:`）：不是这台正式在录的房间，整机与房间的判定都不算它。
fn is_side(room: &str) -> bool {
    is_probe(room) || room.starts_with(TRIAL_PREFIX)
}
/// 频繁短连接（`handoff-health.md` §3）：这么久内至少 [`SHORT_CONN_MIN`] 次不足 [`SHORT_CONN_MAX_MS`] 的连接。
const SHORT_CONN_WINDOW_MS: i64 = 2 * 60_000;
const SHORT_CONN_MAX_MS: u64 = 30_000;
const SHORT_CONN_MIN: i64 = 3;
/// 回差：已达到门槛的房间，窗口内还有这么多次短连接就不算回落。一次滑出窗口、又来一次时不会在门槛上反复触发。
const SHORT_CONN_KEEP: i64 = 2;
/// 持续无有效视频（`handoff-health.md` §3）：平台确认在播时连续这么久没写出有效视频。
const NO_VIDEO_MS: i64 = 60_000;
/// 跨连接内容缺口（`handoff-health.md` §3）：这么久内至少 [`GAP_SEAMS_MIN`] 个测得的重连接缝、累计缺失至少 [`GAP_MS_MIN`]。
const GAP_WINDOW_MS: i64 = 60_000;
const GAP_SEAMS_MIN: i64 = 2;
const GAP_MS_MIN: i64 = 10_000;
/// 故障试拉的健康观察时间（`handoff-health.md` §4.1）：原机基本录不到内容（持续没有有效视频、连续两次空录、
/// 视频停滞、倍率低于 [`TRIAL_FAST_RATE`]）时备用机只观察 [`TRIAL_FAST_WINDOW_MS`]，其余观察 [`TRIAL_SLOW_WINDOW_MS`]。
const TRIAL_FAST_RATE: f64 = 0.4;
const TRIAL_FAST_WINDOW_MS: i64 = 20_000;
const TRIAL_SLOW_WINDOW_MS: i64 = 60_000;
/// 观察期间媒体推进至少达到实时时长的这个比例。
const TRIAL_RATE_MIN: f64 = 0.9;
/// 首包突发：连上时 CDN 一次给出几秒 GOP 缓存（实测约 6 秒）。从第一次有媒体起过这么久才开始算推进，
/// 逐秒采样下一两秒内就给完了。
const TRIAL_BURST_SETTLE_MS: u64 = 2_000;
/// 从下发故障试拉起，最多这么久完成连接和健康验证（§4.2）。
const TRIAL_TIMEOUT_MS: i64 = 90_000;
/// 试拉没成后多久再试（§4.4）。判为源站问题或源站无流的从它起每次翻倍，到 [`TRIAL_SOURCE_RECHECK_MAX_MS`] 封顶。
const TRIAL_RECHECK_MS: i64 = 2 * 60_000;
const TRIAL_SOURCE_RECHECK_MAX_MS: i64 = 10 * 60_000;
/// 整机升级（§5.2）：同一台机器上未到期的房间级限制达到这么多条，就标整机受限。
const TRIAL_ESCALATE_ROOMS: usize = 2;

/// 最近 10 分钟有这么大比例在传、还跟不上产出，就判受限。
const UPLOAD_BUSY_MIN: f64 = 0.9;
/// 上传速度低于产出的这个比例才算跟不上：两者差不多时只是刚好传得过来，B 站偶发 5xx 重试就会让它略低于产出
/// （2026-10-24 线上云服务器 B 以 1537913 < 1538596 B/s 被判受限、换机）。
const UPLOAD_KEEP_UP_MIN: f64 = 0.9;
/// 新机写出第一个关键帧后至少等这么久；旧机也要录到新机起点之后这么久才收尾：
/// 两台各自的 CDN 延迟与连上时的 GOP 缓存不同，按 lag 估算会有几秒误差，留 10 秒保证重叠不为负。
const HANDOFF_OVERLAP_MS: i64 = 10_000;
/// 两台都报了推流时刻（码流 SEI）时，旧机录到新机起点之后这么久就收尾。两边是同一个时钟，
/// 误差只有各机对时的偏差（毫秒级），这几秒只防旧机最后一个 GOP 收尾、心跳在路上这点出入。
const HANDOFF_LIVE_OVERLAP_MS: i64 = 5_000;
/// 有交接在等它收尾时，旧机的长轮询最多挂这么久：每次回来都带上它此刻录到了哪，总控才能及时放行，
/// 不必等下个检测周期（2026-10-24 线上按 30 秒周期等，接缝重叠了约 63 秒）。
const HANDOFF_WATCH_HOLD: Duration = Duration::from_secs(2);
/// 旧机最多再录这么久：等它录到新机的起点，但它卡住了也不能一直等。
const HANDOFF_MAX_WAIT_MS: i64 = 10 * 60_000;
/// 迁回优先机器时，在当前分P到切段时长前这么久发起：留给新机连上、写出关键帧与切点下发（实测十几秒），
/// 检测周期 30 秒也能赶上几轮。
const MOVE_BACK_LEAD_MS: i64 = 2 * 60_000;
/// 交接目标这么久还没写出第一个关键帧，就放弃它。
const HANDOFF_TIMEOUT_MS: i64 = 3 * 60_000;
/// 边录边传的分段时长上限：凭证开录时签发、50 分钟内要传完（实测至少 60 分钟有效）。
const TAIL_SPLIT_MAX: Duration = Duration::from_secs(45 * 60);
/// 总控检测到稿件的那一场已不在播（下播或换了场次）这么久、也没有分P在写，就替 agent 结束它。
/// 比 agent 的下播宽限（5 分钟）多留一倍：正常情况下 agent 自己先报场次结束。
const ORPHAN_AFTER_MS: i64 = 10 * 60_000;
/// 放行后这么久内没收到新提交的心跳，判这次更新失败（同脚本的自检时限）。
const DRAIN_CONFIRM_MS: i64 = 10 * 60_000;
/// 放行后这么久还收到旧提交的心跳，说明脚本回滚了：放行那一刻旧进程可能还在路上的那次心跳不算。
const DRAIN_STALE_MS: i64 = 2 * 60_000;
/// 房间因录制信号迁走后这段时间内，接手的机器又因它慢了，先让另一台交叉试拉，分清是这台还是源站慢。
const SLOW_SOURCE_MS: i64 = 60 * 60_000;
/// 交叉试拉这么久还没出结果（没连上、倍率没攒够、试拉机失联），就放弃，按源站慢处理。
const CROSS_CHECK_TIMEOUT_MS: i64 = 5 * 60_000;
/// 交叉试拉判为源站慢后这么久内不再试拉、不标记；之后仍慢就再试一次。
const SOURCE_SLOW_RECHECK_MS: i64 = 10 * 60_000;
/// 受限标记到期前这么久开始试拉：录制倍率要攒满 2 分钟才报，留出连上的时间。
const PROBE_LEAD_MS: i64 = 3 * 60_000;

/// TOML 里的旧房间格式，只给首次导入用。
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TomlRoom {
    /// 房间名：文件名里的 `{name}`、投稿模板的 `{name}`。
    name: String,
    url: String,
    /// 优先机器列表，缺省用 `default_agents`。
    agents: Option<Vec<String>>,
    #[serde(default, deserialize_with = "opt_duration")]
    split_time: Option<Duration>,
    split_size_mb: Option<u64>,
    /// 下播宽限期，缺省 5 分钟。
    #[serde(default, deserialize_with = "opt_duration")]
    grace: Option<Duration>,
    /// B 站画质 qn（10000 原画）。
    bili_qn: Option<u32>,
    /// 边录边传：必须配 `split_size_mb`，`split_time` 封顶 [`TAIL_SPLIT_MAX`]。
    #[serde(default)]
    tail_upload: bool,
    /// 投稿模板，字段同 B 站投稿接口。`title`/`desc`/`source` 支持 `{name}`、`{title}`、`{url}`
    /// 与开播时间的 strftime。
    upload: Studio,
}

impl TomlRoom {
    fn import(&self, default_agents: &[String]) -> (db::Room, Studio) {
        let room = db::Room {
            name: self.name.clone(),
            url: self.url.clone(),
            template_id: 0,
            agents: self
                .agents
                .clone()
                .unwrap_or_else(|| default_agents.to_vec()),
            split_time: self.split_time.map(|d| d.as_secs()),
            split_size_mb: self.split_size_mb,
            grace: self.grace.map(|d| d.as_secs()),
            bili_qn: self.bili_qn,
            douyin_quality: None,
            tail_upload: self.tail_upload,
            display_name: None,
            filename: db::DEFAULT_FILENAME.into(),
            season_section_id: None,
            tag: String::new(),
            deleted: false,
        };
        (room, self.upload.clone())
    }
}

impl db::Room {
    /// 显示名：封面第一行与文件名的 `{streamer}`，没填用房间名。
    fn display_name(&self) -> &str {
        self.display_name.as_deref().unwrap_or(&self.name)
    }

    fn task(&self) -> RoomTask {
        let secs = |s: Option<u64>| s.map(Duration::from_secs);
        RoomTask {
            name: self.name.clone(),
            url: self.url.clone(),
            // 边录边传在开录时就拿凭证，分段时长加收尾上传要撑在凭证有效期内。
            split_time: match (self.tail_upload, secs(self.split_time)) {
                (true, t) => Some(t.map_or(TAIL_SPLIT_MAX, |t| t.min(TAIL_SPLIT_MAX))),
                (false, t) => t,
            },
            split_size_mb: self.split_size_mb,
            grace: secs(self.grace),
            bili_qn: self.bili_qn,
            douyin_quality: self.douyin_quality.clone(),
            filename: self.filename.clone(),
            streamer: self.display_name().to_string(),
            tail_upload: self.tail_upload,
        }
    }
}

fn duration<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Duration, D::Error> {
    let s = String::deserialize(d)?;
    humantime::parse_duration(&s).map_err(serde::de::Error::custom)
}

fn opt_duration<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<Duration>, D::Error> {
    duration(d).map(Some)
}

impl Config {
    fn load(path: &Path) -> Result<Self, String> {
        let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
        Self::parse(&text).map_err(|e| format!("{}: {e}", path.display()))
    }

    fn parse(text: &str) -> Result<Self, String> {
        let config: Self = toml::from_str(text).map_err(|e| e.to_string())?;
        for room in &config.rooms {
            if room.tail_upload && room.split_size_mb.is_none() {
                return Err(format!(
                    "room {}: tail_upload needs split_size_mb",
                    room.name
                ));
            }
        }
        Ok(config)
    }

    /// 首次迁移时导入的旧配置。
    fn import(&self) -> Result<db::Import, String> {
        let login = match &self.cookies {
            None => None,
            Some(path) => {
                let read = std::fs::read(path).map_err(|e| e.to_string());
                let parsed =
                    read.and_then(|b| serde_json::from_slice(&b).map_err(|e| e.to_string()));
                Some(parsed.map_err(|e| format!("{}: {e}", path.display()))?)
            }
        };
        let rooms = self
            .rooms
            .iter()
            .map(|r| r.import(&self.default_agents))
            .collect();
        Ok(db::Import { login, rooms })
    }

    fn open_db(&self) -> Result<Connection, String> {
        let mut machines: Vec<(String, String)> = self
            .agents
            .iter()
            .map(|(k, v)| (k.clone(), v.token.clone()))
            .collect();
        machines.sort_by(|a, b| a.0.cmp(&b.0));
        db::open(&self.db, &machines, || self.import())
            .map_err(|e| format!("{}: {e}", self.db.display()))
    }
}

/// 一个投稿账号：申请上传凭证、上传封面、投稿。测试里换成假的。
#[async_trait]
trait Bili: Send + Sync {
    async fn preupload(&self, line: &str, name: &str, size: u64) -> Result<Value, String>;
    async fn submit(&self, studio: &Studio, via: Via) -> Result<Submitted, SubmitError>;
    async fn cover_up(&self, jpeg: &[u8]) -> Result<String, String>;
    /// 投稿预处理信息（含分区列表）。
    async fn archive_pre(&self) -> Result<Value, String>;
    async fn human_types(&self) -> Result<Value, String>;
    /// 合集列表（B 站原样的 data）。
    async fn seasons(&self) -> Result<Value, String>;
    async fn add_to_season(&self, section_id: i64, aid: u64) -> Result<(), String>;
    /// 新建合集，返回合集 id；`cover` 是已上传封面的 URL。
    async fn create_season(&self, title: &str, cover: &str) -> Result<i64, String>;
}

#[async_trait]
impl Bili for Account {
    async fn preupload(&self, line: &str, name: &str, size: u64) -> Result<Value, String> {
        Account::preupload(self, line, name, size)
            .await
            .map_err(|e| e.to_string())
    }

    async fn submit(&self, studio: &Studio, via: Via) -> Result<Submitted, SubmitError> {
        Account::submit(self, studio, via).await
    }

    async fn cover_up(&self, jpeg: &[u8]) -> Result<String, String> {
        Account::cover_up(self, jpeg)
            .await
            .map_err(|e| e.to_string())
    }

    async fn archive_pre(&self) -> Result<Value, String> {
        Account::archive_pre(self).await.map_err(|e| e.to_string())
    }

    async fn human_types(&self) -> Result<Value, String> {
        Account::human_types(self).await.map_err(|e| e.to_string())
    }

    async fn seasons(&self) -> Result<Value, String> {
        Account::seasons(self).await.map_err(|e| e.to_string())
    }

    async fn add_to_season(&self, section_id: i64, aid: u64) -> Result<(), String> {
        Account::add_to_season(self, section_id, aid)
            .await
            .map_err(|e| e.to_string())
    }

    async fn create_season(&self, title: &str, cover: &str) -> Result<i64, String> {
        Account::create_season(self, title, cover)
            .await
            .map_err(|e| e.to_string())
    }
}

/// 扫码登录（不属于任何账号）。测试里换成假的。
#[async_trait]
trait QrLogin: Send + Sync {
    /// 返回 (二维码内容, auth_code)。
    async fn start(&self) -> Result<(String, String), String>;
    async fn poll(&self, auth_code: &str) -> Result<Poll, String>;
    async fn nickname(&self, login: &Value) -> Result<String, String>;
}

struct BiliLogin;

#[async_trait]
impl QrLogin for BiliLogin {
    async fn start(&self) -> Result<(String, String), String> {
        lubic_account::qr_start().await.map_err(|e| e.to_string())
    }

    async fn poll(&self, auth_code: &str) -> Result<Poll, String> {
        lubic_account::qr_poll(auth_code)
            .await
            .map_err(|e| e.to_string())
    }

    async fn nickname(&self, login: &Value) -> Result<String, String> {
        Account::new(login.clone(), Box::new(|_| {}))
            .nickname()
            .await
            .map_err(|e| e.to_string())
    }
}

/// `--dry-run`：凭证照常申请、封面照常上传（都不产生稿件），投稿和加入合集只打印。
struct DryRun(Account);

#[async_trait]
impl Bili for DryRun {
    async fn preupload(&self, line: &str, name: &str, size: u64) -> Result<Value, String> {
        Bili::preupload(&self.0, line, name, size).await
    }

    async fn cover_up(&self, jpeg: &[u8]) -> Result<String, String> {
        Bili::cover_up(&self.0, jpeg).await
    }

    async fn archive_pre(&self) -> Result<Value, String> {
        Bili::archive_pre(&self.0).await
    }

    async fn human_types(&self) -> Result<Value, String> {
        Bili::human_types(&self.0).await
    }

    async fn seasons(&self) -> Result<Value, String> {
        Bili::seasons(&self.0).await
    }

    async fn add_to_season(&self, section_id: i64, aid: u64) -> Result<(), String> {
        info!(section_id, aid, "dry run, not adding to season");
        Ok(())
    }

    async fn create_season(&self, title: &str, _cover: &str) -> Result<i64, String> {
        info!(title, "dry run, not creating season");
        Err("dry run, not creating season".into())
    }

    async fn submit(&self, studio: &Studio, via: Via) -> Result<Submitted, SubmitError> {
        info!(
            ?via,
            studio = serde_json::to_string(studio).unwrap(),
            "dry run, not submitting"
        );
        Ok(Submitted {
            aid: 0,
            bvid: "dry-run".into(),
        })
    }
}

/// 由登录信息和保存回调造一个账号。
type MakeBili = Box<dyn Fn(Value, Save) -> Arc<dyn Bili> + Send + Sync>;

struct App {
    config: Config,
    /// `Arc`：账号刷新令牌时经保存回调写回 `accounts.login`。
    db: Arc<Mutex<Connection>>,
    make_bili: MakeBili,
    /// 账号 id → 账号，按需建。登录信息更新或账号删除时丢掉。
    bilis: Mutex<HashMap<i64, Arc<dyn Bili>>>,
    qr: Box<dyn QrLogin>,
    /// 进行中的扫码登录：登录 id → (auth_code, 发起时刻)。只在内存里。
    logins: Mutex<HashMap<String, (String, i64)>>,
    /// 分配变了：叫醒挂着的 `/v1/watch`。
    changed: Notify,
    /// 刚因录制信号标记了受限：叫醒检测周期立刻跑一轮、发起交接，不等下一个 `poll_interval`。
    tick_now: Notify,
    /// 启动时刻：刚启动时 agent 还没来得及报到，不能据此判它们失联。
    started: i64,
    /// 提交失败的退避（稿件 id → 失败次数, 下次可重试时刻），只在内存里。
    retry: Mutex<HashMap<i64, (u32, i64)>>,
    /// 加入合集失败的重试（稿件 id → 失败次数, 下次可重试时刻），只在内存里；待加入的合集在库里。
    season_retry: Mutex<HashMap<i64, (u32, i64)>>,
    /// 房间 → 本轮交接已失败过的目标，分配一变就清。只在内存里。
    tried: Mutex<HashMap<String, Vec<String>>>,
    /// 房间 → 最近一次因录制信号被迁走（源站慢的例外与交叉试拉用）。只在内存里。
    rate_moved: Mutex<HashMap<String, Moved>>,
    /// agent → (受限解除的时刻, 那次连续标记了多久)，递增标记时长用。只在内存里。
    // ponytail: 重启后忘掉，最坏就是重新从最短的一档起。
    released: Mutex<HashMap<String, (i64, i64)>>,
    /// (房间, agent) → (房间级限制解除的时刻, 那次连续标记了多久)，同 `released`。只在内存里。
    room_released: Mutex<HashMap<(String, String), (i64, i64)>>,
    /// 已达到门槛的健康信号：(agent, 房间, 信号)，只在状态变化时打日志用。只在内存里，重启后再打一次。
    health: Mutex<std::collections::HashSet<(String, String, &'static str)>>,
    /// (agent, 房间) → 这台机器上该房间达到门槛的信号，每次心跳整台重算（[`App::note_troubles`]）。只在内存里。
    troubles: Mutex<HashMap<(String, String), trial::Trouble>>,
    /// 房间 → 进行中的故障试拉。只在内存里。
    trials: Mutex<HashMap<String, trial::Trial>>,
    /// 房间 → 候选机：故障试拉转正时让它丢弃暂存内容（§6.2），交接结束就清。只在内存里。
    trial_drops: Mutex<HashMap<String, String>>,
    /// 房间 → 故障试拉的进度，原机恢复就清。只在内存里。
    rechecks: Mutex<HashMap<String, trial::Recheck>>,
    /// 房间 → 总控最近一次检测：在播、不在播，检测失败为 `None`。兜底 no_video 用，只在内存里。
    detections: Mutex<HashMap<String, Option<bool>>>,
    /// (agent, 房间) → 总控第一次发现这台没报该房间 `record_video` 的时刻。兜底 no_video 用，只在内存里，
    /// 重启后重新计时。
    unreported: Mutex<HashMap<(String, String), i64>>,
    /// 按账号的限频闸：preupload（B 站 601「上传过快」按账号算，所有 agent 共用）与投稿间隔。
    gates: Mutex<HashMap<i64, Gate>>,
    /// 稿件 id → 总控第一次看到它那一场已不在播的时刻（孤儿稿件兜底用）。只在内存里，重启后重新计时。
    stale: Mutex<HashMap<i64, i64>>,
    /// 本程序的提交号（[`lubic_proto::COMMIT`]）；测试里改它。
    commit: String,
    /// 收到停止信号：检测周期不再认领新投稿，进行中的那次返回后退出。
    stop: tokio::sync::watch::Sender<bool>,
}

/// 一个房间最近一次因录制信号被迁走，见 [`App::cross_check`]。
#[derive(Clone, Debug, Default)]
struct Moved {
    at: i64,
    from: String,
    /// 正在替它交叉试拉的机器与发起时刻。
    check: Option<(String, i64)>,
    /// 交叉试拉判为源站慢：这一刻前不再试拉、不标记。
    slow_until: i64,
}

/// 两次 preupload 至少隔这么久。
const PREUPLOAD_INTERVAL_MS: i64 = 2_000;
const COOLDOWN_FIRST_MS: i64 = 60_000;
const COOLDOWN_MAX_MS: i64 = 30 * 60_000;
/// 同一账号两次投稿至少隔这么久，再随机加最多 [`SUBMIT_JITTER_MS`]。实测连投相隔 0.2 秒回 69800
/// 「网络繁忙」；相隔 1 分钟连投三份，第三份就撞 21566。
const SUBMIT_INTERVAL_MS: i64 = 60_000;
const SUBMIT_JITTER_MS: i64 = 60_000;
/// 投稿撞 21566「投稿过于频繁」后停用这个接口：15 分钟起、翻倍、2 小时封顶，改走另一个接口；
/// 两个接口都停用时整个账号停投，期间只放一份试探。
/// 每份稿件各自重试时，几份排队稿件合起来每小时撞几十次，频控一直解不开（2026-10-29 实测十小时以上）。
const SUBMIT_COOLDOWN_FIRST_MS: i64 = 15 * 60_000;
const SUBMIT_COOLDOWN_MAX_MS: i64 = 120 * 60_000;
const SUBMIT_TOO_FREQUENT: i64 = 21566;
/// 投稿被拒 21588「视频时长不足 1 秒」：有分P太短，重投还是一样，停下交给人（2026-11-20 稿件 182）。
const PART_TOO_SHORT: i64 = 21588;

/// 一个账号的 preupload 限频闸，照 biliup 的 601 闸（dplei/biliup ceab215f）：请求之间留最小间隔；
/// 碰到 601 就冷却这个账号（60 秒起、翻倍、30 分钟封顶），冷却期内直接拒绝、不再去碰 B 站；
/// 冷却结束只放一个请求试探，成功才恢复。
// ponytail: 只在内存里，总控重启后冷却清零（最多再撞一次 601 重新冷却）；要跨重启再落 SQLite。
#[derive(Debug, Default, Clone)]
struct Gate {
    /// 下一个请求最早可以在这时发出。
    next_at: i64,
    /// 冷却到这时为止。
    until: i64,
    /// 连续撞 601 的次数，决定下次冷却多久。
    strikes: u32,
    /// 冷却后的试探请求还没回来。
    probing: bool,
    /// 这个账号下次投稿最早在这时（上次投稿请求返回后 [`SUBMIT_INTERVAL_MS`]，两个接口都停投时更久）。
    submit_after: i64,
    /// 两个接口都在停投时，较少的那个连续撞 21566 次数；有接口能用时为 0。给控制台看。
    submit_strikes: u32,
    /// APP、网页接口各自停投到何时、连续撞 21566 的次数（决定下次停投多久）。
    vias: [(i64, u32); 2],
}

/// 投稿接口按这个顺序挑：APP 是主力，网页接口只在 APP 停投时顶上。
const VIAS: [Via; 2] = [Via::App, Via::Web];

impl Gate {
    /// 放行返回要先等多久（毫秒），拒绝返回冷却到何时。
    fn admit(&mut self, now: i64) -> Result<i64, i64> {
        if now < self.until || self.probing {
            return Err(self.until);
        }
        self.probing = self.strikes > 0;
        let at = self.next_at.max(now);
        self.next_at = at + PREUPLOAD_INTERVAL_MS;
        Ok(at - now)
    }

    /// 一次 preupload 的结果；撞了 601 返回冷却到何时。
    fn done(&mut self, now: i64, rate_limited: bool) -> Option<i64> {
        self.probing = false;
        if !rate_limited {
            self.strikes = 0;
            return None;
        }
        // 同一批在途请求一起撞上，只算一次。
        if now >= self.until {
            self.strikes += 1;
            let cool = (COOLDOWN_FIRST_MS << (self.strikes - 1).min(5)).min(COOLDOWN_MAX_MS);
            self.until = now + cool;
        }
        Some(self.until)
    }
}

/// 请求被取消（agent 断开、handler 被丢弃）时也要放开试探位，否则闸会永远关着。
struct InFlight<'a>(&'a Mutex<HashMap<i64, Gate>>, i64);

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        self.0.lock().unwrap().entry(self.1).or_default().probing = false;
    }
}

/// preupload 的错误是不是「上传过快」限频。错误码不固定：实测先回 601，冷却中再请求回 406，提示文字相同。
fn rate_limited(error: &str) -> bool {
    ["\"code\":601", "\"code\":406", "上传视频过快"]
        .iter()
        .any(|k| error.contains(k))
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

type HttpError = (StatusCode, String);

fn internal(e: impl std::fmt::Display) -> HttpError {
    error!(error = %e, "request failed");
    (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
}

struct Availability {
    tier: Option<u8>,
    move_away: Option<String>,
    track_rate_move: bool,
    handoff_target: bool,
    cross_probe_candidate: bool,
    send_cross_probe: bool,
    send_recovery_probe: bool,
    probe_machine_limit_due: bool,
    failure_trial_origin: bool,
    failure_trial_candidate: Option<TrialCandidate>,
    continue_failure_trial: bool,
    hold_uploads: bool,
}

enum TrialCandidate {
    Unrestricted,
    RoomLimited { since: i64 },
}

enum Pick {
    Start,
    Handoff,
}

/// 钉钉加签：`timestamp\nsecret` 以 secret 做 HMAC-SHA256，Base64 后 URL 编码。
fn dingtalk_sign(ts: i64, secret: &str) -> String {
    use base64::Engine;
    use hmac::Mac;
    let mut mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(format!("{ts}\n{secret}").as_bytes());
    base64::engine::general_purpose::STANDARD
        .encode(mac.finalize().into_bytes())
        .replace('+', "%2B")
        .replace('/', "%2F")
        .replace('=', "%3D")
}

impl App {
    fn db(&self) -> std::sync::MutexGuard<'_, Connection> {
        self.db.lock().unwrap()
    }

    /// 按 `Authorization: Bearer <token>` 认出 agent。
    fn auth(&self, headers: &HeaderMap) -> Result<String, HttpError> {
        let unauthorized = || (StatusCode::UNAUTHORIZED, "unknown agent token".into());
        let token = headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .ok_or_else(unauthorized)?;
        db::machine_by_token(&self.db(), token)
            .map_err(internal)?
            .ok_or_else(unauthorized)
    }

    /// 这台 agent 该录的房间，按房间列表顺序，参数是分配时的快照。
    fn desired(&self, agent: &str) -> db::Result<Vec<RoomTask>> {
        db::assigned_tasks(&self.db(), agent)
    }

    /// 账号 id 对应的账号，第一次用到时按库里的登录信息建。
    fn bili(&self, id: i64) -> Result<Arc<dyn Bili>, String> {
        if let Some(b) = self.bilis.lock().unwrap().get(&id) {
            return Ok(b.clone());
        }
        let login = db::account_login(&self.db(), id)
            .map_err(|e| e.to_string())?
            .ok_or_else(|| format!("upload account {id} not found"))?;
        let db = self.db.clone();
        let save: Save = Box::new(move |login| {
            if let Err(e) = db::set_login(&db.lock().unwrap(), id, login) {
                error!(account = id, error = %e, "failed to save refreshed login");
            }
        });
        let b = (self.make_bili)(login, save);
        self.bilis.lock().unwrap().insert(id, b.clone());
        Ok(b)
    }

    /// 最近 `offline_after` 内来过。刚启动时给每台一个 `offline_after` 的宽限。
    fn online(
        &self,
        agent: &str,
        now: i64,
        grace: bool,
    ) -> db::Result<Option<lubic_proto::Status>> {
        Ok(self.online_status(db::agent(&self.db(), agent)?, now, grace))
    }

    fn online_status(&self, seen: Option<(i64, Status)>, now: i64, grace: bool) -> Option<Status> {
        let since = |t: i64| if grace { t.max(self.started) } else { t };
        let limit = self.config.offline_after.as_millis() as i64;
        match seen {
            Some((t, status)) if now - since(t) < limit => Some(status),
            None if grace && now - self.started < limit => Some(Default::default()),
            _ => None,
        }
    }

    /// 同一时刻的机器能力。选机按真实心跳；`room` 用于开播、交接和故障试拉的房间级限制。
    fn availability(&self, agent: &str, now: i64, room: Option<&str>) -> db::Result<Availability> {
        let conn = self.db();
        let enabled = db::machine_enabled(&conn, agent)?;
        let status = self.online_status(db::agent(&conn, agent)?, now, false);
        let limit = db::limit(&conn, agent)?;
        let drain = db::drain(&conn, agent)?;
        let yielding = db::yielding(&conn, agent, now)?;
        let room_limit = match room {
            Some(room) => db::room_limit(&conn, room, agent)?,
            None => None,
        };

        // 开播允许到期待验证的房间级限制；交接仍须等验证通过（handoff-health.md §7）。
        let can_record = enabled && status.as_ref().is_some_and(|s| !s.paused);
        let tier = if !can_record {
            None
        } else if yielding {
            Some(4)
        } else if limit.is_some() || room_limit.as_ref().is_some_and(|l| now < l.until) {
            Some(3)
        } else if drain.is_some() {
            Some(2)
        } else {
            Some(1)
        };

        let move_away = if !enabled {
            Some("disabled".into())
        } else if let Some(l) = &limit {
            Some(l.reason.clone())
        } else if let Some(d) = &drain {
            Some(format!("draining for {}", short(&d.commit)))
        } else if yielding {
            Some("yielding network".into())
        } else {
            None
        };

        let cross_probe_candidate = can_record && drain.is_none() && !yielding;
        let continue_failure_trial = cross_probe_candidate && limit.is_none();
        let failure_trial_candidate = (continue_failure_trial
            && status.is_some_and(|s| s.trial_support))
        .then(|| match &room_limit {
            Some(l) => TrialCandidate::RoomLimited { since: l.since },
            None => TrialCandidate::Unrestricted,
        });
        let send_recovery_probe = enabled && !yielding;
        Ok(Availability {
            tier,
            move_away,
            track_rate_move: enabled
                && limit
                    .as_ref()
                    .is_some_and(|l| l.reason.starts_with("record")),
            handoff_target: continue_failure_trial && room_limit.is_none(),
            cross_probe_candidate,
            // 已选好交叉试拉机后，下发时只因让出网络而拦住（load-balance.md §3）。
            send_cross_probe: !yielding,
            send_recovery_probe,
            probe_machine_limit_due: send_recovery_probe
                && limit
                    .as_ref()
                    .is_some_and(|l| now >= l.until - PROBE_LEAD_MS),
            failure_trial_origin: limit.is_none(),
            failure_trial_candidate,
            continue_failure_trial,
            hold_uploads: yielding,
        })
    }

    /// 按检测结果调整一个房间的分配，返回是否改了。
    /// `live`：`Some(Some(场次键))` 在播，`Some(None)` 不在播，`None` 检测失败（什么也不做）。
    fn schedule(
        &self,
        room: &db::Room,
        live: Option<Option<String>>,
        now: i64,
    ) -> db::Result<bool> {
        let current = db::assignment(&self.db(), &room.name)?;
        match live {
            None => Ok(false),
            Some(Some(session)) => {
                // 交接在心跳里完成时用分配里的场次（见 `finish_handoffs_from`），所以随检测更新。
                if current.is_some() {
                    db::set_assignment_session(&self.db(), &room.name, &session)?;
                }
                let handoff = db::handoff(&self.db(), &room.name)?;
                if let Some((agent, _)) = &current {
                    if self.online(agent, now, true)?.is_some() {
                        return self.move_if_limited(room, agent, handoff, &session, now);
                    }
                    // 失联优先于受限交接：交接目标在线就直接交给它，不另挑第三台。
                    if let Some(h) = handoff
                        && self.online(&h.to, now, false)?.is_some()
                    {
                        self.assign(room, &h.to, &session, now)?;
                        warn!(
                            room = room.name,
                            from = agent,
                            to = h.to,
                            "assigned agent unreachable, handed to the handoff target"
                        );
                        return Ok(true);
                    }
                    // 正在故障试拉：候选机在线就直接改派给它，暂存内容转正（任务沿用这一场的快照，
                    // 与下发的试拉相同）；不在线就取消试拉，照常改派。
                    let trial = self.trials.lock().unwrap().remove(&room.name);
                    if let Some(t) = trial
                        && self.online(&t.to, now, false)?.is_some()
                    {
                        self.assign(room, &t.to, &session, now)?;
                        warn!(
                            room = room.name,
                            from = agent,
                            to = t.to,
                            "assigned agent unreachable, handed to the failure trial candidate"
                        );
                        return Ok(true);
                    }
                }
                // 删除或暂停的房间：在录的这一场照常，没在录就不再分配。控制台结束过的这一场同理。
                if current.is_none()
                    && (room.deleted
                        || room.agents.is_empty()
                        || db::skipped(&self.db(), &room.name, &session)?)
                {
                    return Ok(false);
                }
                let exclude: Vec<&str> = current.iter().map(|(a, _)| a.as_str()).collect();
                // 全部候选都受限时仍分给受限机：录总比不录好（handoff.md 取舍 2）。排空中的同理，
                // 宁可推迟更新也不漏录，且排在受限机之前（它录得好好的，只是等着更新）。
                // 让出网络的机器最后兜底（load-balance.md §1）。
                let Some(name) = self.pick(room, &exclude, now, Pick::Start)? else {
                    warn!(room = room.name, candidates = ?room.agents, "live but no agent available");
                    return Ok(false);
                };
                self.assign(room, &name, &session, now)?;
                // 待验证的机器开录即解除：录制本身就是验证，再出问题走正常检测。
                let pending = db::room_limit(&self.db(), &room.name, &name)?;
                if pending.is_some_and(|l| now >= l.until) {
                    db::clear_room_limit(&self.db(), &room.name, &name)?;
                    info!(
                        room = room.name,
                        agent = name,
                        "room limit pending verification cleared, recording here again"
                    );
                }
                match &current {
                    Some((old, _)) => {
                        warn!(
                            room = room.name,
                            from = old,
                            to = name,
                            "assigned agent unreachable, reassigned"
                        )
                    }
                    None => info!(room = room.name, agent = name, session, "live, assigned"),
                }
                Ok(true)
            }
            Some(None) => {
                // 下播且 agent 已报完这一场（宽限期满）才撤。刚分配的给 5 分钟，等 agent 报上第一个分段，
                // 免得一次误判把正要开录的任务撤掉。
                let Some((agent, assigned_at)) = current else {
                    return Ok(false);
                };
                if db::recording(&self.db(), &room.name)? || now - assigned_at < 5 * 60 * 1000 {
                    return Ok(false);
                }
                db::unassign(&self.db(), &room.name)?;
                self.tried.lock().unwrap().remove(&room.name);
                info!(room = room.name, agent, "offline, unassigned");
                Ok(true)
            }
        }
    }

    /// 开播先比档位，交接只挑可交接的机器。同档取负载（[`db::load`]）最小的，相等时取优先列表里靠前的。
    fn pick(
        &self,
        room: &db::Room,
        exclude: &[&str],
        now: i64,
        purpose: Pick,
    ) -> db::Result<Option<String>> {
        let mut best: Option<((u8, i64), &String)> = None;
        for name in &room.agents {
            if exclude.contains(&name.as_str()) {
                continue;
            }
            let availability = self.availability(name, now, Some(&room.name))?;
            let tier = match purpose {
                Pick::Start => availability.tier,
                Pick::Handoff => availability.handoff_target.then_some(1),
            };
            let Some(tier) = tier else {
                continue;
            };
            let rank = (tier, db::load(&self.db(), name, &room.name)?);
            if best.is_none_or(|(b, _)| rank < b) {
                best = Some((rank, name));
            }
        }
        Ok(best.map(|(_, name)| name.clone()))
    }

    fn assign(&self, room: &db::Room, agent: &str, session: &str, now: i64) -> db::Result<()> {
        db::assign(&self.db(), &room.name, agent, session, now, &room.task())?;
        self.tried.lock().unwrap().remove(&room.name);
        Ok(())
    }

    /// 分配给的 agent 在线：推进进行中的交接；没有交接而它停用、受限、在排空、在让出网络，或已不在房间的优先列表里，
    /// 就发起一次。原机好好的不因「更靠前的机器可用」迁回（`load-balance.md` §2）。返回分配或交接是否变了。
    fn move_if_limited(
        &self,
        room: &db::Room,
        agent: &str,
        handoff: Option<db::Handoff>,
        session: &str,
        now: i64,
    ) -> db::Result<bool> {
        if let Some(h) = handoff {
            return match h.started_at {
                Some(_) => self.finish_handoff(room, &h, session, now),
                None if now - h.requested_at >= HANDOFF_TIMEOUT_MS
                    || self.online(&h.to, now, false)?.is_none() =>
                {
                    db::delete_handoff(&self.db(), &room.name)?;
                    self.tried
                        .lock()
                        .unwrap()
                        .entry(room.name.clone())
                        .or_default()
                        .push(h.to.clone());
                    warn!(
                        room = room.name,
                        to = h.to,
                        "handoff target did not start recording, giving up on it"
                    );
                    Ok(true)
                }
                None => Ok(false),
            };
        }
        let availability = self.availability(agent, now, None)?;
        let Some((reason, at_split)) = self.move_reason(room, agent, availability.move_away) else {
            return Ok(false);
        };
        let tried = self
            .tried
            .lock()
            .unwrap()
            .get(&room.name)
            .cloned()
            .unwrap_or_default();
        let mut exclude: Vec<&str> = tried.iter().map(String::as_str).collect();
        exclude.push(agent);
        let Some(to) = self.pick(room, &exclude, now, Pick::Handoff)? else {
            // 没有可交接的机器：留在原机继续录，下个周期再看。
            return Ok(false);
        };
        if at_split && !self.time_to_move_back(room, agent, now)? {
            return Ok(false);
        }
        db::start_handoff(&self.db(), &room.name, agent, &to, now)?;
        if availability.track_rate_move {
            let moved = Moved {
                at: now,
                from: agent.to_string(),
                ..Default::default()
            };
            self.rate_moved
                .lock()
                .unwrap()
                .insert(room.name.clone(), moved);
        }
        info!(room = room.name, from = agent, to, reason, "handing off");
        Ok(true)
    }

    /// 在录 `room` 的 `agent` 为什么该迁走：`None` 不迁；`Some((原因, 对准切段))`，对准切段的只有
    /// 原机好好的、只是已不在优先列表里这一种（见 [`Self::time_to_move_back`]）。
    fn move_reason(
        &self,
        room: &db::Room,
        agent: &str,
        move_away: Option<String>,
    ) -> Option<(String, bool)> {
        if let Some(reason) = move_away {
            return Some((reason, false));
        }
        if !room.agents.iter().any(|a| a == agent) {
            return Some(("no longer in the room's agent list".into(), true));
        }
        None
    }

    /// 原机好好的、只是已不在优先列表里：换走。按时间切段的房间等当前分P录到切段前
    /// [`MOVE_BACK_LEAD_MS`] 再交，让旧机在切点的收尾顶替这次切段，不多出一个短分P。
    fn time_to_move_back(&self, room: &db::Room, agent: &str, now: i64) -> db::Result<bool> {
        let Some(split) = room.task().split_time else {
            return Ok(true);
        };
        let since = db::open_part_since(&self.db(), &room.name, agent)?;
        Ok(since.is_some_and(|t| now - t >= split.as_millis() as i64 - MOVE_BACK_LEAD_MS))
    }

    /// 新机已开录的交接：旧机录过了新机的起点就把分配改成新机，旧机停录。返回是否完成。
    fn finish_handoff(
        &self,
        room: &db::Room,
        h: &db::Handoff,
        session: &str,
        now: i64,
    ) -> db::Result<bool> {
        let Some(started) = h.started_at else {
            return Ok(false);
        };
        let caught_up = match self.caught_up_live(room, h)? {
            Some(caught_up) => caught_up,
            None => now - started >= HANDOFF_OVERLAP_MS && self.caught_up(room, h)?,
        };
        if !caught_up && now - started < HANDOFF_MAX_WAIT_MS {
            return Ok(false);
        }
        self.assign(room, &h.to, session, now)?;
        info!(
            room = room.name,
            from = h.from,
            to = h.to,
            start_lag = h.start_lag,
            start_live = h.start_live,
            waited_ms = now - started,
            "handoff done, old agent stops"
        );
        Ok(true)
    }

    /// 这台 agent 心跳时推进从它交接出去的房间：旧机录过新机起点就立刻放行，不等检测周期。返回分配是否变了。
    fn finish_handoffs_from(&self, agent: &str, now: i64) -> db::Result<bool> {
        let handoffs = db::started_handoffs_from(&self.db(), agent)?;
        let mut changed = false;
        for (name, session, h) in handoffs {
            let Some(room) = db::room(&self.db(), &name)? else {
                continue;
            };
            changed |= self.finish_handoff(&room, &h, &session, now)?;
        }
        Ok(changed)
    }

    /// 按推流时刻（码流 SEI，见 `PaceCount::live_lag_ms`）判断旧机是否已录到新机起点之后 [`HANDOFF_LIVE_OVERLAP_MS`]。
    /// 旧机心跳时刻 `t` 写到的内容落后直播 `live_lag`，录到了直播的 `t − live_lag`；各机都对 NTP，可以和新机起点直接比。
    /// 不依赖 CDN 节点延迟：两台拉到的节点差几秒、几分钟都一样准。任一边没有推流时刻（码流没有 SEI、旧机没在拉流）返回 `None`，
    /// 退回按 lag 估算。
    fn caught_up_live(&self, room: &db::Room, h: &db::Handoff) -> db::Result<Option<bool>> {
        let Some(start) = h.start_live else {
            return Ok(None);
        };
        let Some((seen, status)) = db::agent(&self.db(), &h.from)? else {
            return Ok(None);
        };
        let lag = status
            .record_live_lags
            .iter()
            .find(|(r, _)| *r == room.name)
            .map(|(_, lag)| *lag);
        Ok(lag.map(|lag| seen - lag >= start + HANDOFF_LIVE_OVERLAP_MS))
    }

    /// 旧机是否已录到新机起点之后 [`HANDOFF_OVERLAP_MS`]：受限的旧机往往落后直播几十秒，只按墙钟等会留下空洞。
    /// 比的是「录到了直播的哪一刻」：旧机心跳时刻 `t` 落后 `lag旧`，录到约 `t − lag旧`；新机起点约 `started_at − lag新`。
    /// 不比 CDN 时间戳：实测同一场直播，有的连接是绝对时间轴、有的从 0 起，两条相对时间轴差几十秒时根本分不出来。
    /// `started_at` 是总控收到上报的时刻，比真正写出第一个关键帧晚，所以只会多等。
    /// 旧机没在拉流（没报 lag）：它已不产出，重连后又从直播当前开始录，等也没用，放行。
    fn caught_up(&self, room: &db::Room, h: &db::Handoff) -> db::Result<bool> {
        let Some((seen, status)) = db::agent(&self.db(), &h.from)? else {
            return Ok(true);
        };
        let old_lag = status.record_lags.iter().find(|(r, _)| *r == room.name);
        let (Some(started), Some((_, old_lag))) = (h.started_at, old_lag) else {
            return Ok(true);
        };
        Ok(seen - old_lag >= started - h.start_lag.unwrap_or(0) + HANDOFF_OVERLAP_MS)
    }

    /// 结束 agent 没报场次结束的稿件（如交接时两台的场次键不同，旧机停录时不报结束）。
    /// `live` 同 [`App::schedule`]；检测失败不计时。稿件那一场已不在播持续 [`ORPHAN_AFTER_MS`]、
    /// 且没有分P在写，才结束它。返回结束了几份。
    fn close_orphans(
        &self,
        room: &str,
        live: Option<&Option<String>>,
        now: i64,
    ) -> db::Result<usize> {
        let Some(current) = live else {
            return Ok(0);
        };
        let archives = db::recording_archives(&self.db(), room)?;
        let mut closed = 0;
        for (id, session) in archives {
            if current.as_deref() == Some(session.as_str()) {
                self.stale.lock().unwrap().remove(&id);
                continue;
            }
            let since = *self.stale.lock().unwrap().entry(id).or_insert(now);
            if now - since < ORPHAN_AFTER_MS || db::has_open_parts(&self.db(), id)? {
                continue;
            }
            if db::end_archive(&self.db(), id, now)? {
                warn!(
                    room,
                    id,
                    session,
                    current = current.as_deref().unwrap_or("(offline)"),
                    "agent never reported this session's end, ended it"
                );
                closed += 1;
            }
            self.stale.lock().unwrap().remove(&id);
        }
        Ok(closed)
    }

    /// 更新脚本问能不能换到 `commit`（`docs/design/auto-update.md`「总控侧」）。同一时间只放一台。
    fn drain(&self, agent: &str, commit: &str, now: i64) -> db::Result<Drain> {
        let wait = |reason: String| Ok(Drain::Wait { reason });
        if commit.is_empty() {
            return wait("target has no commit (local build), not rolling".into());
        }
        let current = db::agent(&self.db(), agent)?.map(|(_, s)| s.commit);
        if current.as_deref() == Some(commit) {
            db::delete_drain(&self.db(), agent)?;
            return Ok(Drain::Done);
        }
        if self.commit != commit {
            return wait(format!(
                "controller is at {}, update it first",
                short(&self.commit)
            ));
        }
        if db::commit_failed(&self.db(), commit)? {
            return wait(format!(
                "{} failed on some agent, rolling paused",
                short(commit)
            ));
        }
        // 先取出来：写在 `for` 头里，锁会一直持有到循环结束。
        let drains = db::drains(&self.db())?;
        for d in drains {
            if d.agent == agent {
                continue;
            }
            // 别台排的是旧目标：它的脚本早换了目标（或不再问），这条已作废。
            // ponytail: 同一目标的排空停在那（如那台钉了 IMAGE 不再问）只能等人删，或等下一个提交。
            if d.commit != commit && d.released_at.is_none() {
                db::delete_drain(&self.db(), &d.agent)?;
                continue;
            }
            return wait(format!("{} is draining", d.agent));
        }
        db::start_drain(&self.db(), agent, commit, now)?;
        let rooms = db::assigned_tasks(&self.db(), agent)?;
        if !rooms.is_empty() {
            let names: Vec<_> = rooms.iter().map(|r| r.name.as_str()).collect();
            return wait(format!("still recording {}", names.join(", ")));
        }
        db::release_drain(&self.db(), agent, now)?;
        info!(
            agent,
            commit = short(commit),
            "drained, released for update"
        );
        Ok(Drain::Go)
    }

    /// 判放行了的更新成没成：只看心跳里的提交号。失败就记下这个提交，滚动停在它上面。
    fn settle_drains(&self, now: i64) -> db::Result<()> {
        // 先取出来：写在 `for` 头里，锁会一直持有到循环结束。
        let drains = db::drains(&self.db())?;
        for d in drains {
            let Some(released) = d.released_at else {
                continue;
            };
            let seen = db::agent(&self.db(), &d.agent)?;
            let reason = match &seen {
                Some((_, s)) if s.commit == d.commit => {
                    info!(agent = d.agent, commit = short(&d.commit), "agent updated");
                    db::delete_drain(&self.db(), &d.agent)?;
                    continue;
                }
                Some((t, s)) if *t - released >= DRAIN_STALE_MS => {
                    format!("came back at {}", short(&s.commit))
                }
                _ if now - released >= DRAIN_CONFIRM_MS => {
                    "no heartbeat with the new commit".into()
                }
                _ => continue,
            };
            warn!(
                agent = d.agent,
                commit = short(&d.commit),
                reason,
                "update failed, rolling paused for this commit"
            );
            db::fail_commit(&self.db(), &d.commit, &d.agent, now)?;
            db::delete_drain(&self.db(), &d.agent)?;
        }
        Ok(())
    }

    fn stopping(&self) -> bool {
        *self.stop.borrow()
    }

    /// 发一条钉钉消息，不等结果；没配 webhook 就什么都不做。
    fn notify(&self, text: String) {
        let Some(mut url) = self.config.dingtalk_webhook.clone() else {
            return;
        };
        if let Some(secret) = &self.config.dingtalk_secret {
            let ts = now_ms();
            url = format!("{url}&timestamp={ts}&sign={}", dingtalk_sign(ts, secret));
        }
        let body =
            serde_json::json!({"msgtype": "text", "text": {"content": format!("lubic {text}")}});
        tokio::spawn(async move {
            let sent = reqwest::Client::new()
                .post(&url)
                .header("content-type", "application/json")
                .body(body.to_string())
                .timeout(Duration::from_secs(10))
                .send()
                .await;
            match sent {
                Ok(r) => match r.text().await {
                    Ok(t) if t.contains(r#""errcode":0"#) => {}
                    res => warn!(response = ?res, "dingtalk rejected the message"),
                },
                Err(e) => warn!(error = %e, "dingtalk send failed"),
            }
        });
    }

    /// 每次心跳带来新状态时判一次受限：未标记的看两个信号，已标记的到期复查上传速度。
    fn judge(&self, agent: &str, s: &Status, now: i64) -> db::Result<()> {
        let max = self.config.limited_for.as_millis() as i64;
        // 20 / 40 / 60 分钟递增，封顶 60（默认 1h 时）。一次波动误判只隔离 20 分钟。
        let step = max / 3;
        let next = |held: i64| (held + step).min(max);
        let limit = db::limit(&self.db(), agent)?;
        if let Some(limit) = limit {
            if now < limit.until {
                return Ok(());
            }
            // 先看它自己的上传速度，再看到期前试拉的结果；都没有样本就按时解除。
            let until = now + next(now - limit.since);
            if let Some(u) = s.upload_bps.filter(|u| *u < limit.need_bps) {
                warn!(
                    agent,
                    upload_bps = u,
                    need_bps = limit.need_bps,
                    "agent still limited, extended"
                );
                db::set_limit(&self.db(), agent, &db::Limit { until, ..limit })?;
                return Ok(());
            }
            let probe = self.probe_verdict(agent, s, now)?;
            if let Some(Err(reason)) = probe {
                warn!(agent, reason, "agent still limited, extended");
                let l = db::Limit {
                    until,
                    reason,
                    ..limit
                };
                db::set_limit(&self.db(), agent, &l)?;
                return Ok(());
            }
            let probe = if probe.is_some() { "ok" } else { "none" };
            info!(agent, upload_bps = ?s.upload_bps, probe, "agent no longer limited");
            self.released
                .lock()
                .unwrap()
                .insert(agent.to_string(), (now, (now - limit.since).min(max)));
            db::clear_limit(&self.db(), agent)?;
            return Ok(());
        }
        let rated: Vec<(&String, f64)> = s
            .record_rates
            .iter()
            .filter(|(r, v)| *v < RECORD_RATE_MIN && !is_side(r))
            .map(|(r, v)| (r, *v))
            .collect();
        let skipped: Vec<(&String, u64)> = s
            .record_skips
            .iter()
            .filter(|(r, ms, jumps)| skips_bad(*ms, *jumps) && !is_side(r))
            .map(|(r, ms, _)| (r, *ms))
            .collect();
        let failed: Vec<(&String, u32, bool)> = s
            .record_failures
            .iter()
            .filter(|(r, n, stalled)| failures_bad(*n, *stalled) && !is_side(r))
            .map(|(r, n, stalled)| (r, *n, *stalled))
            .collect();
        let mut slow: Vec<&String> = rated
            .iter()
            .map(|x| x.0)
            .chain(skipped.iter().map(|x| x.0))
            .chain(failed.iter().map(|x| x.0))
            .collect();
        slow.sort();
        slow.dedup();
        let mut counted = Vec::new();
        for room in slow {
            // 开关开着时录制类信号不标整机：先让备用机故障试拉，只迁异常房间（`trial.rs`），交叉试拉也由它取代。
            if !self.config.health_trial && self.cross_check(agent, room, now)? {
                counted.push(room);
            }
        }
        let slowest = rated
            .iter()
            .filter(|(r, _)| counted.contains(r))
            .map(|x| x.1)
            .reduce(f64::min);
        let most_skipped = skipped
            .iter()
            .filter(|(r, _)| counted.contains(r))
            .map(|x| x.1)
            .max();
        let failure = failed
            .iter()
            .filter(|(r, _, _)| counted.contains(r))
            .max_by_key(|x| (x.2, x.1));
        let (reason, need_bps) = if let Some((_, attempts, stalled)) = failure {
            (
                format!("record {}", failure_reason(*attempts, *stalled)),
                s.produce_bps,
            )
        } else {
            match (slowest, most_skipped, s.upload_bps) {
                // 受限时的产出也被压低了，按倍率还原成码率作为恢复门槛。
                (Some(r), _, _) => (
                    format!("record rate {r:.2}"),
                    s.produce_bps / r.clamp(0.1, 1.0),
                ),
                // 丢内容同理，按收到的比例还原。
                (None, Some(ms), _) => (
                    skipped_reason("record ", ms),
                    s.produce_bps
                        / (1.0 - ms as f64 / SKIP_WINDOW.as_millis() as f64).clamp(0.1, 1.0),
                ),
                (None, None, Some(u))
                    if s.upload_busy >= UPLOAD_BUSY_MIN
                        && u < s.produce_bps * UPLOAD_KEEP_UP_MIN =>
                {
                    (
                        format!(
                            "upload {:.1} Mbps < produce {:.1} Mbps",
                            u * 8e-6,
                            s.produce_bps * 8e-6
                        ),
                        s.produce_bps,
                    )
                }
                _ => return Ok(()),
            }
        };
        warn!(agent, reason, "agent limited");
        if reason.starts_with("record") {
            // 录制慢每多等一秒就多丢一秒内容。notify_one 留 permit：检测周期正忙时，跑完接着再跑一轮。
            self.tick_now.notify_one();
        }
        let hold = match self.released.lock().unwrap().get(agent) {
            Some(&(at, held)) if now - at < max => next(held),
            _ => step,
        };
        let limit = db::Limit {
            since: now,
            until: now + hold,
            reason,
            need_bps,
        };
        db::set_limit(&self.db(), agent, &limit)
    }

    /// 健康信号（`handoff-health.md` §3）的门槛判断。第一期只打日志：信号达到或回落门槛时各打一条结构化日志，
    /// 不标记、不交接、不试拉。返回这次的变化 (房间, 信号, 是否达到门槛)，测试用。
    ///
    /// 日志字段：`signal` 信号名，`count` 计入的次数，`total_ms` 这些次数的合计毫秒：
    /// `short_connections` 为写出过有效视频的短连接次数与时长之和，`empty` 另报同窗口内不足 30 秒的空连接数（不计门槛）；`no_video` 次数记 1，`total_ms` 为已持续多久没写出视频；
    /// `reconnect_gaps` 为测得的接缝数（重叠也算）与缺失之和。
    ///
    /// no_video 另有总控兜底（[`App::unreported_no_video`]）：agent 查平台失败、误判不在播或根本没起录时，
    /// 它不报 `record_video`，光靠自报覆盖不到「agent 在线但录制没有进展」（§2）。
    fn health(&self, agent: &str, s: &Status, now: i64) -> Vec<(String, &'static str, bool)> {
        // (房间, 信号) → (次数, 合计毫秒)
        let mut hits: HashMap<(String, &'static str), (i64, i64)> = HashMap::new();
        // 房间 → 不足 30 秒的空连接数：只随 short_connections 日志报，不计门槛（由 no_video 与 record_failures 覆盖）。
        let mut empty: HashMap<&str, i64> = HashMap::new();
        for (room, ago, ms, video) in &s.record_connections {
            if !is_side(room) && (*ago as i64) < SHORT_CONN_WINDOW_MS && *ms < SHORT_CONN_MAX_MS {
                if !video {
                    *empty.entry(room).or_default() += 1;
                    continue;
                }
                let hit = hits.entry((room.clone(), "short_connections")).or_default();
                hit.0 += 1;
                hit.1 += *ms as i64;
            }
        }
        {
            let state = self.health.lock().unwrap();
            hits.retain(|(room, signal), (count, _)| {
                let held = state.contains(&(agent.to_string(), room.clone(), *signal));
                *count
                    >= if held {
                        SHORT_CONN_KEEP
                    } else {
                        SHORT_CONN_MIN
                    }
            });
        }
        let mut gaps: HashMap<(String, &'static str), (i64, i64)> = HashMap::new();
        for (room, ago, ms) in &s.record_gaps {
            if !is_side(room) && (*ago as i64) < GAP_WINDOW_MS {
                let hit = gaps.entry((room.clone(), "reconnect_gaps")).or_default();
                hit.0 += 1;
                hit.1 += *ms as i64;
            }
        }
        hits.extend(
            gaps.into_iter()
                .filter(|(_, (seams, ms))| *seams >= GAP_SEAMS_MIN && *ms >= GAP_MS_MIN),
        );
        for (room, video, since) in &s.record_video {
            // 一帧都没写出过、或宽限后刚重新确认在播：从确认在播起算。
            let quiet = video.map_or(*since, |v| v.min(*since)) as i64;
            if !is_side(room) && quiet >= NO_VIDEO_MS {
                hits.insert((room.clone(), "no_video"), (1, quiet));
            }
        }
        let keep = self.unreported_no_video(agent, s, now, &mut hits);
        let mut state = self.health.lock().unwrap();
        let mut changes = Vec::new();
        for ((room, signal), (count, total_ms)) in &hits {
            if !state.insert((agent.to_string(), room.clone(), signal)) {
                continue;
            }
            changes.push((room.clone(), *signal, true));
            if *signal == "no_video_paused" {
                // 停录的机器本来就不录，第三期另行处理；这里只注明原因，不算 no_video。
                info!(
                    event_name = "health.signal",
                    outcome = "suppressed",
                    reason_code = "agent_paused",
                    agent,
                    room,
                    signal = "no_video",
                    total_ms,
                    "recording health signal suppressed, agent paused"
                );
            } else {
                let empty = empty.get(room.as_str()).copied().unwrap_or(0);
                warn!(
                    event_name = "health.signal",
                    outcome = "triggered",
                    agent,
                    room,
                    signal,
                    count,
                    total_ms,
                    empty,
                    "recording health signal reached threshold"
                );
            }
        }
        state.retain(|(a, room, signal)| {
            if a != agent
                || hits.contains_key(&(room.clone(), *signal))
                || (keep.contains(room) && signal.starts_with("no_video"))
            {
                return true;
            }
            if *signal == "no_video_paused" {
                info!(
                    event_name = "health.signal",
                    outcome = "cleared",
                    reason_code = "agent_paused",
                    agent,
                    room,
                    signal = "no_video",
                    "recording health signal back below threshold"
                );
            } else {
                info!(
                    event_name = "health.signal",
                    outcome = "cleared",
                    agent,
                    room,
                    signal,
                    "recording health signal back below threshold"
                );
            }
            changes.push((room.clone(), *signal, false));
            false
        });
        changes.sort();
        changes
    }

    fn detected(&self, room: &str, live: Option<bool>) {
        self.detections
            .lock()
            .unwrap()
            .insert(room.to_string(), live);
    }

    /// 总控兜底的 no_video：房间分给这台（交接收尾中的不算）、总控最近一次检测在播，而这台没报它的
    /// `record_video`，从分配与第一次发现没报里较晚的那个起满 [`NO_VIDEO_MS`] 就记一次。旧 agent
    /// （`health_reports` 为否）本来就不报，不兜底；停录的机器记成 `no_video_paused`，只注明原因。
    /// 返回总控检测失败的房间：这些房间的 no_video 状态与计时都保持不变。
    fn unreported_no_video(
        &self,
        agent: &str,
        s: &Status,
        now: i64,
        hits: &mut HashMap<(String, &'static str), (i64, i64)>,
    ) -> std::collections::HashSet<String> {
        let mut keep = std::collections::HashSet::new();
        let rooms = if s.health_reports {
            self.fallback_rooms(agent).unwrap_or_else(|e| {
                error!(agent, error = %e, "reading assignments for health failed");
                Vec::new()
            })
        } else {
            Vec::new()
        };
        let detections = self.detections.lock().unwrap();
        let mut unreported = self.unreported.lock().unwrap();
        let mut still = std::collections::HashSet::new();
        for (room, assigned_at) in rooms {
            if s.record_video.iter().any(|(r, ..)| *r == room) {
                continue;
            }
            match detections.get(&room).copied().flatten() {
                Some(false) => continue,
                None => {
                    keep.insert(room.clone());
                }
                Some(true) => {
                    let key = (agent.to_string(), room.clone());
                    let since = *unreported.entry(key).or_insert(now);
                    let quiet = now - since.max(assigned_at);
                    if quiet >= NO_VIDEO_MS {
                        let signal = if s.paused {
                            "no_video_paused"
                        } else {
                            "no_video"
                        };
                        hits.insert((room.clone(), signal), (1, quiet));
                    }
                }
            }
            still.insert(room);
        }
        unreported.retain(|(a, room), _| a != agent || still.contains(room));
        keep
    }

    /// 分给这台的房间与分配时刻，去掉已交接给别人、这台在收尾的。
    fn fallback_rooms(&self, agent: &str) -> db::Result<Vec<(String, i64)>> {
        let conn = self.db();
        let handing: Vec<_> = db::started_handoffs_from(&conn, agent)?
            .into_iter()
            .map(|(room, ..)| room)
            .collect();
        let mut rooms = Vec::new();
        for room in db::rooms_of(&conn, agent)? {
            if handing.contains(&room) {
                continue;
            }
            if let Some((_, at)) = db::assignment(&conn, &room)? {
                rooms.push((room, at));
            }
        }
        Ok(rooms)
    }

    /// `agent` 录 `room` 慢了，这个慢算不算它的：房间刚因录制信号从别的机器迁来时，先让另一台（优先原机，
    /// 受限也行）交叉试拉。试拉达标说明慢的是这台，算；试拉也慢、找不到能试拉的机器或等不到结果，判源站慢，
    /// 不算，[`SOURCE_SLOW_RECHECK_MS`] 后仍慢再试。还在等试拉也不算。
    /// 由来：2026-11-01 某房间从云服务器 B 迁到云服务器 A 后，云服务器 A 连的 CDN 节点只给 0.1~0.2 倍，
    /// 旧规则一律判源站慢，丢了约 16 分钟；迁回云服务器 B 立刻正常。
    fn cross_check(&self, agent: &str, room: &str, now: i64) -> db::Result<bool> {
        let Some(mut m) = self.rate_moved.lock().unwrap().get(room).cloned() else {
            return Ok(true);
        };
        if now - m.at >= SLOW_SOURCE_MS {
            return Ok(true);
        }
        if now < m.slow_until {
            warn!(
                agent,
                room, "slow again, source was cross-checked slow; not marking"
            );
            return Ok(false);
        }
        let mut verdict = None;
        let before = m.check.clone();
        match m.check.clone() {
            None => match self.prober(room, agent, &m.from, now)? {
                Some(p) => {
                    info!(
                        agent,
                        room,
                        prober = p,
                        "slow right after a move, cross-checking"
                    );
                    m.check = Some((p, now));
                }
                None => {
                    warn!(
                        agent,
                        room, "slow right after a move, nobody to cross-check; not marking"
                    );
                    m.slow_until = now + SOURCE_SLOW_RECHECK_MS;
                    verdict = Some(false);
                }
            },
            Some((p, since)) => {
                let key = format!("{PROBE_PREFIX}{room}");
                let st = self.online(&p, now, false)?;
                let failed = st.as_ref().is_some_and(|s| {
                    s.record_failures
                        .iter()
                        .any(|(k, n, stalled)| *k == key && failures_bad(*n, *stalled))
                });
                let rate = st.as_ref().and_then(|s| {
                    let r = s.record_rates.iter().find(|(k, _)| *k == key)?.1;
                    let skips = s
                        .record_skips
                        .iter()
                        .any(|(k, ms, jumps)| *k == key && skips_bad(*ms, *jumps));
                    Some((r, skips))
                });
                match rate {
                    Some((r, false)) if r >= RECORD_RATE_MIN && !failed => {
                        info!(
                            agent,
                            room,
                            prober = p,
                            rate = r,
                            "cross-check ok, this agent is the slow one"
                        );
                        // 原机当初也因这个房间受限：它现在拉得好，解除，房间才迁得回去。上传受限不动。
                        if db::limit(&self.db(), &p)?
                            .is_some_and(|l| !l.reason.starts_with("upload"))
                        {
                            info!(agent = p, "agent no longer limited, cross-check ok");
                            db::clear_limit(&self.db(), &p)?;
                        }
                        verdict = Some(true);
                    }
                    Some((r, _)) => {
                        warn!(
                            agent,
                            room,
                            prober = p,
                            rate = r,
                            "cross-check slow too, source is slow; not marking"
                        );
                        verdict = Some(false);
                    }
                    None if failed => {
                        warn!(
                            agent,
                            room,
                            prober = p,
                            "cross-check has no effective video either; not marking"
                        );
                        verdict = Some(false);
                    }
                    None if st.is_none() || now - since >= CROSS_CHECK_TIMEOUT_MS => {
                        warn!(
                            agent,
                            room,
                            prober = p,
                            "cross-check got no result; not marking"
                        );
                        verdict = Some(false);
                    }
                    None => {}
                }
                if let Some(v) = verdict {
                    m.check = None;
                    if !v {
                        m.slow_until = now + SOURCE_SLOW_RECHECK_MS;
                    }
                }
            }
        }
        let changed = m.check != before;
        self.rate_moved.lock().unwrap().insert(room.to_string(), m);
        if changed {
            // 试拉开始或结束：叫醒试拉机的长轮询。
            self.changed.notify_waiters();
        }
        Ok(verdict.unwrap_or(false))
    }

    /// 替 `recorder` 交叉试拉 `room` 的机器：原机优先，再按优先列表；在线、启用、没停录、没排空、没在让出网络，受限也行。
    fn prober(
        &self,
        room: &str,
        recorder: &str,
        from: &str,
        now: i64,
    ) -> db::Result<Option<String>> {
        let Some(r) = db::room(&self.db(), room)? else {
            return Ok(None);
        };
        for name in std::iter::once(from).chain(r.agents.iter().map(String::as_str)) {
            if name != recorder && self.availability(name, now, None)?.cross_probe_candidate {
                return Ok(Some(name.to_string()));
            }
        }
        Ok(None)
    }

    /// 到期前试拉的结论：`None` 没有结论（没试拉、倍率还没攒够，或录这个房间的那台也慢、分不清是源站），
    /// `Some(Err(原因))` 试拉仍不达标，`Some(Ok)` 达标。
    fn probe_verdict(
        &self,
        agent: &str,
        s: &Status,
        now: i64,
    ) -> db::Result<Option<Result<(), String>>> {
        let rate = s.record_rates.iter().find(|(k, _)| is_probe(k));
        let failure = s
            .record_failures
            .iter()
            .find(|(k, n, stalled)| is_probe(k) && failures_bad(*n, *stalled));
        let Some(key) = rate
            .map(|(key, _)| key)
            .or_else(|| failure.map(|(key, _, _)| key))
        else {
            return Ok(None);
        };
        let room = &key[PROBE_PREFIX.len()..];
        let skipped = |st: &Status, k: &str| {
            st.record_skips
                .iter()
                .find(|(r, ms, jumps)| r == k && skips_bad(*ms, *jumps))
                .map(|(_, ms, _)| *ms)
        };
        // 先取出来：`if let` 链里的临时锁会一直持有到整条判断结束，`online` 再拿就死锁。
        let recorder = db::assignment(&self.db(), room)?;
        if let Some((other, _)) = recorder
            && other != agent
            && let Some(o) = self.online(&other, now, false)?
            && (o
                .record_rates
                .iter()
                .any(|(r, v)| r == room && *v < RECORD_RATE_MIN)
                || skipped(&o, room).is_some()
                || o.record_failures
                    .iter()
                    .any(|(k, n, stalled)| k == room && failures_bad(*n, *stalled)))
        {
            warn!(
                agent,
                room, "probe inconclusive, the room is slow on its recorder too"
            );
            return Ok(None);
        }
        if let Some((_, attempts, stalled)) = failure {
            return Ok(Some(Err(format!(
                "probe record {}",
                failure_reason(*attempts, *stalled)
            ))));
        }
        let Some((_, rate)) = rate else {
            return Ok(None);
        };
        Ok(Some(if *rate < RECORD_RATE_MIN {
            Err(format!("probe record rate {rate:.2}"))
        } else if let Some(ms) = skipped(s, key) {
            Err(skipped_reason("probe ", ms))
        } else {
            Ok(())
        }))
    }

    /// 受限标记快到期时让这台试拉的房间：优先列表含它、此刻在别的机器上录的第一个房间。
    fn probe_for(&self, agent: &str, now: i64) -> db::Result<Option<RoomTask>> {
        let availability = self.availability(agent, now, None)?;
        if !availability.send_cross_probe {
            return Ok(None);
        }
        // 交叉试拉优先：它在等这台的结果决定迁不迁。
        let checks: Vec<String> = self
            .rate_moved
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, m)| m.check.as_ref().is_some_and(|(p, _)| p == agent))
            .map(|(r, _)| r.clone())
            .collect();
        for room in checks {
            // 先取出来：`if let` 链里的临时锁会一直持有到整条判断结束，再拿就死锁。
            let elsewhere =
                db::assignment(&self.db(), &room)?.is_some_and(|(other, _)| other != agent);
            let r = db::room(&self.db(), &room)?;
            if let Some(r) = r
                && elsewhere
            {
                return Ok(Some(r.task()));
            }
        }
        if !availability.send_recovery_probe {
            return Ok(None);
        }
        if availability.probe_machine_limit_due {
            let rooms = db::rooms(&self.db())?;
            for room in rooms {
                if room.deleted || !room.agents.iter().any(|a| a == agent) {
                    continue;
                }
                if db::assignment(&self.db(), &room.name)?.is_some_and(|(other, _)| other != agent)
                {
                    return Ok(Some(room.task()));
                }
            }
        }
        // 房间级限制的恢复验证：试拉的就是受限的那个房间，它得正在别的机器上录。
        let limits = db::room_limits_of(&self.db(), agent)?;
        for l in limits {
            if now < l.until - PROBE_LEAD_MS {
                continue;
            }
            let elsewhere =
                db::assignment(&self.db(), &l.room)?.is_some_and(|(other, _)| other != agent);
            let room = db::room(&self.db(), &l.room)?;
            if let Some(room) = room
                && elsewhere
            {
                return Ok(Some(room.task()));
            }
        }
        Ok(None)
    }

    /// 受限标记再续多久：已连续标记 `held` 之后的下一档（20 / 40 / 60 分钟，默认 `limited_for` 为 1h 时）。
    fn next_hold(&self, held: i64) -> i64 {
        let max = self.config.limited_for.as_millis() as i64;
        (held + max / 3).min(max)
    }

    /// 每次心跳复查这台机器上到期的房间级限制（`handoff-health.md` §7）：只认到期前试拉的结果，
    /// 达标解除，不达标按递增档位续期，没有结果（房间没在播、试拉名额被占）保持待验证。
    fn judge_room_limits(&self, agent: &str, s: &Status, now: i64) -> db::Result<()> {
        let limits = db::room_limits_of(&self.db(), agent)?;
        for l in limits {
            let key = format!("{PROBE_PREFIX}{}", l.room);
            let probed = s.record_rates.iter().any(|(k, _)| *k == key)
                || s.record_failures.iter().any(|(k, ..)| *k == key);
            if now < l.until || !probed {
                continue;
            }
            match self.probe_verdict(agent, s, now)? {
                None => {}
                Some(Ok(())) => {
                    info!(
                        agent,
                        room = l.room,
                        "room limit verified by probe, cleared"
                    );
                    let max = self.config.limited_for.as_millis() as i64;
                    self.room_released.lock().unwrap().insert(
                        (l.room.clone(), l.agent.clone()),
                        (now, (now - l.since).min(max)),
                    );
                    db::clear_room_limit(&self.db(), &l.room, agent)?;
                }
                Some(Err(reason)) => {
                    warn!(agent, room = l.room, reason, "room still limited, extended");
                    let until = now + self.next_hold(now - l.since);
                    db::set_room_limit(&self.db(), &db::RoomLimit { until, reason, ..l })?;
                }
            }
        }
        Ok(())
    }

    /// 提交所有分P已齐的稿件。先标「提交中」落库再发请求；结果未知的交给人，绝不自动重投。
    async fn submit_ready(&self) -> db::Result<()> {
        let now = now_ms();
        let ready = db::ready(&self.db(), now)?;
        for id in ready {
            if self.stopping() {
                break;
            }
            if self
                .retry
                .lock()
                .unwrap()
                .get(&id)
                .is_some_and(|(_, at)| now < *at)
            {
                continue;
            }
            let archive = db::archive(&self.db(), id)?;
            if archive.parts.is_empty() {
                warn!(
                    room = archive.room,
                    id, "archive has no uploaded part, dropping it"
                );
                db::delete_archive(&self.db(), id)?;
                continue;
            }
            let target = self.submit_target(&archive);
            let (room, tpl, bili) = match target {
                Ok(t) => t,
                Err(e) => {
                    error!(room = archive.room, id, error = e, "cannot submit");
                    db::set_phase(&self.db(), id, db::ATTENTION, Some(&e), None)?;
                    continue;
                }
            };
            let account = archive.account_id.unwrap_or(tpl.account_id);
            if self.gates.lock().unwrap().get(&account).is_some_and(|g| {
                now < g.submit_after
                    || (tpl.human_type2.is_some() && now < g.vias[Via::Web as usize].0)
            }) {
                continue; // 同账号刚投过，下个周期再投
            }
            let mut studio = render(&room, &tpl, &archive);
            let bg = db::cover_bg(&self.db(), &room.name)?;
            studio.cover = upload_cover(&*bili, &room, &archive, bg.as_deref()).await;
            if !db::begin_submit(&self.db(), id)? {
                continue; // 准备期间在控制台丢弃了
            }
            info!(
                id,
                title = studio.title,
                parts = studio.videos.len(),
                "submitting"
            );
            let via = if studio.human_type2.is_some() {
                Via::Web
            } else {
                let gates = self.gates.lock().unwrap();
                let vias = gates.get(&account).map_or([(0, 0); 2], |g| g.vias);
                let now = now_ms();
                VIAS.into_iter()
                    .find(|v| vias[*v as usize].0 <= now)
                    .unwrap_or(Via::App)
            };
            let submitted = bili.submit(&studio, via).await;
            let too_frequent = matches!(
                submitted,
                Err(SubmitError::Rejected {
                    code: SUBMIT_TOO_FREQUENT,
                    ..
                })
            );
            {
                let mut gates = self.gates.lock().unwrap();
                let gate = gates.entry(account).or_default();
                let now = now_ms();
                let (until, strikes) = &mut gate.vias[via as usize];
                if too_frequent {
                    *strikes += 1;
                    *until = now
                        + (SUBMIT_COOLDOWN_FIRST_MS << (*strikes - 1).min(5))
                            .min(SUBMIT_COOLDOWN_MAX_MS);
                    warn!(
                        account,
                        ?via,
                        until = *until,
                        "submit endpoint too frequent, pausing it"
                    );
                } else {
                    *strikes = 0;
                }
                let jitter = std::hash::BuildHasher::hash_one(
                    &std::collections::hash_map::RandomState::new(),
                    id,
                ) % (SUBMIT_JITTER_MS as u64 + 1);
                gate.submit_after = now + SUBMIT_INTERVAL_MS + jitter as i64;
                gate.submit_strikes = 0;
                if gate.vias.iter().all(|(until, _)| *until > now) {
                    let reopen = gate.vias.iter().map(|(until, _)| *until).min().unwrap();
                    gate.submit_after = gate.submit_after.max(reopen);
                    gate.submit_strikes = gate.vias.iter().map(|(_, s)| *s).min().unwrap();
                    warn!(
                        account,
                        wait_ms = gate.submit_after - now,
                        "account submits too frequently via every endpoint, pausing it"
                    );
                }
            }
            match submitted {
                Ok(done) => {
                    info!(
                        id,
                        bvid = done.bvid,
                        aid = done.aid,
                        title = studio.title,
                        "submitted"
                    );
                    db::submitted(
                        &self.db(),
                        id,
                        &done.bvid,
                        done.aid,
                        room.season_section_id,
                        now_ms(),
                    )?;
                    self.retry.lock().unwrap().remove(&id);
                }
                Err(
                    e @ SubmitError::Rejected {
                        code: PART_TOO_SHORT,
                        ..
                    },
                ) => {
                    error!(id, error = %e, "a part is too short, stopped; needs a human");
                    db::set_phase(&self.db(), id, db::ATTENTION, Some(&e.to_string()), None)?;
                    self.retry.lock().unwrap().remove(&id);
                    self.notify(format!(
                        "稿件 {id}「{}」投稿被拒，已停投：{e}。要先摘掉太短的分P再投。",
                        studio.title
                    ));
                }
                Err(e @ (SubmitError::NotSent(_) | SubmitError::Rejected { .. })) => {
                    db::set_phase(&self.db(), id, db::ENDED, Some(&e.to_string()), None)?;
                    if too_frequent {
                        continue; // 账号已停投；稿件本身不退避，频控一解就按顺序投
                    }
                    let mut retry = self.retry.lock().unwrap();
                    let failures = retry.get(&id).map_or(0, |r| r.0) + 1;
                    let wait = (30_000i64 << (failures - 1).min(5)).min(600_000);
                    retry.insert(id, (failures, now + wait));
                    warn!(id, error = %e, wait_ms = wait, "submit failed, will retry");
                }
                Err(e) => {
                    error!(id, error = %e, "submit outcome unknown; check the archive list before resubmitting");
                    db::set_phase(&self.db(), id, db::ATTENTION, Some(&e.to_string()), None)?;
                }
            }
        }
        Ok(())
    }

    /// 把已投稿的稿件加入房间配的合集（同 biliup：最多 5 次、隔 30 秒）。待加入的记在库里，
    /// 总控重启后接着加；次数只在内存里，重启后重新数。都失败只记错误，去创作中心手动加。
    async fn add_to_seasons(&self) -> db::Result<()> {
        let now = now_ms();
        let pending = db::season_pending(&self.db())?;
        for (id, account, aid, section_id) in pending {
            if self
                .season_retry
                .lock()
                .unwrap()
                .get(&id)
                .is_some_and(|(_, at)| now < *at)
            {
                continue;
            }
            let added = match account {
                Some(account) => match self.bili(account) {
                    Ok(bili) => bili.add_to_season(section_id, aid).await,
                    Err(e) => Err(e),
                },
                None => Err("archive has no upload account".into()),
            };
            let mut retry = self.season_retry.lock().unwrap();
            match added {
                Ok(()) => {
                    info!(id, aid, section_id, "added to season");
                    retry.remove(&id);
                }
                Err(e) => {
                    let failures = retry.get(&id).map_or(0, |r| r.0) + 1;
                    if failures < SEASON_TRIES {
                        warn!(
                            id,
                            aid,
                            section_id,
                            error = e,
                            "adding to season failed, will retry"
                        );
                        retry.insert(id, (failures, now + SEASON_RETRY_MS));
                        continue;
                    }
                    error!(
                        id,
                        aid,
                        section_id,
                        error = e,
                        "adding to season failed, giving up; add it in the creator center"
                    );
                    retry.remove(&id);
                }
            }
            drop(retry);
            db::season_done(&self.db(), id)?;
        }
        Ok(())
    }

    /// 投稿要的房间、模板与账号：账号用稿件开稿时钉住的那个。
    fn submit_target(
        &self,
        archive: &db::Archive,
    ) -> Result<(db::Room, db::Template, Arc<dyn Bili>), String> {
        let room = db::room(&self.db(), &archive.room)
            .map_err(|e| e.to_string())?
            .ok_or("room not in config")?;
        let tpl = db::template(&self.db(), room.template_id)
            .map_err(|e| e.to_string())?
            .ok_or("no upload template")?;
        let bili = self.bili(archive.account_id.unwrap_or(tpl.account_id))?;
        Ok((room, tpl, bili))
    }
}

const SEASON_TRIES: u32 = 5;
const SEASON_RETRY_MS: i64 = 30_000;

/// 生成并上传自动封面（垫上房间的封面背景，没有就黑底），返回图片 URL；失败只记警告、返回空（由 B 站截帧），不挡投稿。
async fn upload_cover(
    bili: &dyn Bili,
    room: &db::Room,
    archive: &db::Archive,
    bg: Option<&[u8]>,
) -> String {
    let started = local(archive.started_at)
        .format(cover::TIME_FORMAT)
        .to_string();
    let jpeg = cover::render(bg, &[room.display_name(), &started]);
    match bili.cover_up(&jpeg).await {
        Ok(url) => url,
        Err(e) => {
            warn!(
                room = archive.room,
                error = e,
                "cover upload failed, submitting without it"
            );
            String::new()
        }
    }
}

fn local(ms: i64) -> chrono::DateTime<Local> {
    Local
        .timestamp_millis_opt(ms)
        .single()
        .unwrap_or_else(Local::now)
}

async fn watch(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Json(req): Json<Watch>,
) -> Result<Json<Assignments>, HttpError> {
    let agent = app.auth(&headers)?;
    let now = now_ms();
    db::touch_agent(&app.db(), &agent, now, &req.status).map_err(internal)?;
    app.judge(&agent, &req.status, now).map_err(internal)?;
    app.judge_room_limits(&agent, &req.status, now)
        .map_err(internal)?;
    app.health(&agent, &req.status, now);
    app.note_troubles(&agent, &req.status);
    app.trial_step(now).map_err(internal)?;
    if app.finish_handoffs_from(&agent, now).map_err(internal)? {
        app.changed.notify_waiters();
    }
    let mut have = req.rooms;
    have.sort();
    let mut have_cuts = req.cuts;
    have_cuts.sort();
    let handing_off = !db::started_handoffs_from(&app.db(), &agent)
        .map_err(internal)?
        .is_empty();
    let hold = if handing_off {
        HANDOFF_WATCH_HOLD
    } else {
        lubic_proto::WATCH_HOLD
    };
    let deadline = tokio::time::Instant::now() + hold;
    let mut stop = app.stop.subscribe();
    // 旧 agent 不回显暂停状态：只比这次长轮询开头的值。
    let mut held = req.hold_uploads;
    loop {
        let changed = app.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        let rooms = app.desired(&agent).map_err(internal)?;
        let cuts = db::cuts_for(&app.db(), &agent).map_err(internal)?;
        let (probe, trial) = app.side_tasks(&agent, now_ms()).map_err(internal)?;
        let mut names: Vec<_> = rooms.iter().map(|r| r.name.clone()).collect();
        names.sort();
        let cut_names: Vec<_> = cuts.iter().map(|(r, _)| r.clone()).collect();
        let probe_changed = probe.as_ref().map(|t| &t.name) != req.probe.as_ref()
            || trial.as_ref().map(|t| &t.name) != req.trial.as_ref();
        let reply = Assignments {
            rooms,
            cuts,
            probe,
            trial,
            trial_drop: app.trial_drop_for(&agent).map_err(internal)?,
            hold_uploads: app
                .availability(&agent, now_ms(), None)
                .map_err(internal)?
                .hold_uploads,
        };
        // agent 的暂停状态和应下发的不同（开关变了，控制台改完会叫醒这里）：立刻下发，不等长轮询挂满。
        let hold_changed = *held.get_or_insert(reply.hold_uploads) != reply.hold_uploads;
        if names != have || cut_names != have_cuts || probe_changed || hold_changed {
            return Ok(Json(reply));
        }
        tokio::select! {
            _ = changed => {}
            _ = tokio::time::sleep_until(deadline) => return Ok(Json(reply)),
            // 总控要退出：不挂满长轮询，免得拖住优雅退出。
            _ = stop.wait_for(|s| *s) => return Ok(Json(reply)),
        }
    }
}

async fn report(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Json(req): Json<Report>,
) -> Result<Json<Ack>, HttpError> {
    let agent = app.auth(&headers)?;
    let events: Vec<_> = req.events.iter().map(|e| e.event.clone()).collect();
    db::apply_all(&mut app.db(), &agent, &events).map_err(internal)?;
    if events
        .iter()
        .any(|e| matches!(e, lubic_proto::AgentEvent::Recording { .. }))
    {
        app.changed.notify_waiters();
    }
    let acked = req.events.iter().map(|e| e.seq).max().unwrap_or(0);
    Ok(Json(Ack { acked }))
}

async fn ticket(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Json(req): Json<TicketRequest>,
) -> Result<Json<Value>, HttpError> {
    let agent = app.auth(&headers)?;
    if db::part_discarded(&app.db(), &req.part_id).map_err(internal)? {
        return Err((StatusCode::GONE, lubic_proto::DISCARDED.into()));
    }
    let account = db::ticket_account(&app.db(), &req.part_id, &req.room)
        .map_err(internal)?
        .ok_or((
            StatusCode::BAD_REQUEST,
            format!("room {:?} has no upload account", req.room),
        ))?;
    let bili = app.bili(account).map_err(internal)?;
    let admitted = app
        .gates
        .lock()
        .unwrap()
        .entry(account)
        .or_default()
        .admit(now_ms());
    match admitted {
        Ok(wait) => tokio::time::sleep(Duration::from_millis(wait as u64)).await,
        Err(until) => {
            let left = (until - now_ms()).max(0) / 1000;
            return Err((
                StatusCode::TOO_MANY_REQUESTS,
                format!("preupload cooling down after 601, {left}s left"),
            ));
        }
    }
    let in_flight = InFlight(&app.gates, account);
    let res = bili.preupload(&req.line, &req.file_name, req.size).await;
    drop(in_flight);
    let cooling = {
        let limited = res.as_ref().err().is_some_and(|e| rate_limited(e));
        let mut gates = app.gates.lock().unwrap();
        gates.entry(account).or_default().done(now_ms(), limited)
    };
    if let Some(until) = cooling {
        warn!(
            account,
            cooldown_s = (until - now_ms()) / 1000,
            "preupload rate limited, this account's tickets paused"
        );
    }
    match res {
        Ok(v) => {
            if let Err(e) = db::save_upos_ticket(&app.db(), &req.part_id, &v, now_ms()) {
                warn!(part_id = req.part_id, error = %e, "saving upos ticket failed");
            }
            info!(
                agent,
                account,
                part_id = req.part_id,
                size = req.size,
                line = req.line,
                "ticket issued"
            );
            Ok(Json(v))
        }
        Err(e) => {
            warn!(agent, part_id = req.part_id, error = e, "preupload failed");
            Err((StatusCode::BAD_GATEWAY, e))
        }
    }
}

async fn drain(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Json(req): Json<DrainRequest>,
) -> Result<Json<Drain>, HttpError> {
    let agent = app.auth(&headers)?;
    let now = now_ms();
    app.settle_drains(now).map_err(internal)?;
    let res = app.drain(&agent, &req.commit, now).map_err(internal)?;
    if let Drain::Wait { reason } = &res {
        info!(agent, commit = short(&req.commit), reason, "update waits");
    }
    Ok(Json(res))
}

async fn version() -> Json<Version> {
    Json(Version {
        commit: lubic_proto::COMMIT.into(),
    })
}

/// 提交号前 8 位，日志与等待原因里用。
fn short(commit: &str) -> &str {
    match commit {
        "" => "(none)",
        c => c.get(..8).unwrap_or(c),
    }
}

fn router(app: Arc<App>) -> Router {
    Router::new()
        .route(lubic_proto::WATCH, post(watch))
        .route(lubic_proto::REPORT, post(report))
        .route(lubic_proto::TICKET, post(ticket))
        .route(lubic_proto::DRAIN, post(drain))
        .route(lubic_proto::VERSION, get(version))
        .merge(console::routes(app.clone()))
        .with_state(app)
}

/// 检测一个房间：`Some(Some(场次键))` 在播，`Some(None)` 不在播，`None` 检测失败。
async fn detect(
    plugins: &[Arc<dyn LivePlugin>],
    client: &reqwest::Client,
    room: &db::Room,
) -> Option<Option<String>> {
    let Some(plugin) = plugins.iter().find(|p| p.matches(&room.url)) else {
        error!(url = room.url, "no plugin matches this url");
        return None;
    };
    let req = LiveRequest {
        client: client.clone(),
        url: room.url.clone(),
        name: room.name.clone(),
        options: Default::default(),
        credentials: Default::default(),
    };
    match plugin.check_stream(req).await {
        Ok(LiveStatus::Live { stream }) => Some(Some(stream.live_session_key.unwrap_or_default())),
        Ok(LiveStatus::Offline) => Some(None),
        Err(e) => {
            warn!(room = room.name, error = %e, "check stream failed");
            None
        }
    }
}

/// 常驻：每个检测周期从库里读房间、检测、调整分配、提交分P已齐的稿件。
/// 已删除的房间只在还有分配时检测（为了下播后撤销），之后真删掉。
async fn tick_loop(app: Arc<App>) {
    let plugins = builtin_plugins();
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .expect("http client");
    let mut tick = tokio::time::interval(app.config.poll_interval);
    let mut stop = app.stop.subscribe();
    loop {
        tokio::select! {
            _ = tick.tick() => {}
            _ = app.tick_now.notified() => {}
            _ = stop.wait_for(|s| *s) => return,
        }
        let mut changed = false;
        let rooms = match active_rooms(&app) {
            Ok(r) => r,
            Err(e) => {
                error!(error = %e, "reading rooms failed");
                continue;
            }
        };
        for room in &rooms {
            if app.stopping() {
                return;
            }
            let live = detect(&plugins, &client, room).await;
            app.detected(&room.name, live.as_ref().map(Option::is_some));
            if let Err(e) = app.close_orphans(&room.name, live.as_ref(), now_ms()) {
                error!(room = room.name, error = %e, "closing orphaned archives failed");
            }
            match app.schedule(room, live, now_ms()) {
                Ok(c) => changed |= c,
                Err(e) => error!(room = room.name, error = %e, "schedule failed"),
            }
        }
        if changed {
            app.changed.notify_waiters();
        }
        if let Err(e) = app.trial_step(now_ms()) {
            error!(error = %e, "failure trials failed");
        }
        if let Err(e) = app.settle_drains(now_ms()) {
            error!(error = %e, "settling drains failed");
        }
        if let Err(e) = app.submit_ready().await {
            error!(error = %e, "submit failed");
        }
        if app.stopping() {
            return;
        }
        if let Err(e) = app.add_to_seasons().await {
            error!(error = %e, "adding to seasons failed");
        }
        if let Err(e) = db::purge_rooms(&app.db()) {
            error!(error = %e, "purging deleted rooms failed");
        }
    }
}

fn active_rooms(app: &App) -> db::Result<Vec<db::Room>> {
    let conn = app.db();
    let mut rooms = db::rooms(&conn)?;
    rooms.retain(|r| !r.deleted || db::assignment(&conn, &r.name).is_ok_and(|a| a.is_some()));
    Ok(rooms)
}

const TITLE_MAX: usize = 80;

fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        s.chars().take(max - 3).chain("...".chars()).collect()
    }
}

/// 套投稿模板：`{name}`、`{title}`、`{url}` 与开播时间的 strftime；分P标题取录像文件名。
fn render(room: &db::Room, tpl: &db::Template, archive: &db::Archive) -> Studio {
    let started = local(archive.started_at);
    let fill = |t: &str| {
        let t = t
            .replace("{name}", &archive.room.replace('%', "%%"))
            .replace("{title}", &archive.title.replace('%', "%%"))
            .replace("{url}", &room.url.replace('%', "%%"));
        let mut out = String::new();
        // 模板里有非法的 strftime 占位符时原样保留，不让一次格式错误挡住投稿。
        if std::fmt::Write::write_fmt(&mut out, format_args!("{}", started.format(&t))).is_err() {
            return t;
        }
        out
    };
    let videos = archive
        .parts
        .iter()
        .map(|(path, filename)| Video {
            title: truncate(
                Path::new(path)
                    .file_stem()
                    .and_then(|n| n.to_str())
                    .unwrap_or("P"),
                TITLE_MAX,
            ),
            filename: filename.clone(),
            desc: String::new(),
        })
        .collect();
    Studio {
        title: truncate(&fill(&tpl.title), TITLE_MAX),
        desc: fill(&tpl.desc),
        source: fill(&tpl.source),
        // 控制台保存时已拦住超过上限的组合，这里截断只防万一，免得整份稿件被拒。
        tag: db::tags(&tpl.tag, &room.tag)
            .into_iter()
            .take(db::TAG_MAX)
            .collect::<Vec<_>>()
            .join(","),
        videos,
        ..tpl.studio()
    }
}

fn app(config: Config, conn: Connection, make_bili: MakeBili, qr: Box<dyn QrLogin>) -> Arc<App> {
    Arc::new(App {
        config,
        db: Arc::new(Mutex::new(conn)),
        make_bili,
        bilis: Mutex::default(),
        qr,
        logins: Mutex::default(),
        changed: Notify::new(),
        tick_now: Notify::new(),
        started: now_ms(),
        retry: Mutex::default(),
        season_retry: Mutex::default(),
        tried: Mutex::default(),
        rate_moved: Mutex::default(),
        released: Mutex::default(),
        room_released: Mutex::default(),
        health: Mutex::default(),
        troubles: Mutex::default(),
        trials: Mutex::default(),
        trial_drops: Mutex::default(),
        rechecks: Mutex::default(),
        detections: Mutex::default(),
        unreported: Mutex::default(),
        gates: Mutex::default(),
        stale: Mutex::default(),
        commit: lubic_proto::COMMIT.into(),
        stop: tokio::sync::watch::Sender::new(false),
    })
}

async fn run(config: Config, dry_run: bool) -> Result<(), String> {
    let conn = config.open_db()?;
    if !config.rooms.is_empty() || config.cookies.is_some() {
        info!(
            "rooms/cookies in the TOML are only imported on first start; the database is authoritative"
        );
    }
    if !config.agents.is_empty() {
        warn!(
            "[agents] in the TOML was imported into the database once and is no longer read; manage machines in the console"
        );
    }
    if config.admin_password_hash.is_none() {
        warn!("admin_password_hash not set, console API is disabled");
    }
    for (id, room) in db::interrupted_submits(&conn).map_err(|e| e.to_string())? {
        error!(
            id,
            room,
            "controller stopped while submitting; check the archive list, then set phase to 'ended' to resubmit or 'submitted'"
        );
    }
    let make_bili: MakeBili = if dry_run {
        Box::new(|login, save| Arc::new(DryRun(Account::new(login, save))))
    } else {
        Box::new(|login, save| Arc::new(Account::new(login, save)))
    };
    let listen = config.listen;
    let app = app(config, conn, make_bili, Box::new(BiliLogin));
    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .map_err(|e| format!("{listen}: {e}"))?;
    info!(%listen, dry_run, commit = short(lubic_proto::COMMIT), "controller listening");
    let ticks = tokio::spawn(tick_loop(app.clone()));
    let stopping = app.clone();
    let served = axum::serve(listener, router(app))
        .with_graceful_shutdown(async move {
            lubic::stop_signal().await;
            info!("stopping, waiting for the submit in progress");
            stopping.stop.send_replace(true);
        })
        .await
        .map_err(|e| e.to_string());
    let _ = ticks.await;
    served
}

fn end(config: Config, room: String) -> Result<(), String> {
    let conn = config.open_db()?;
    let n = db::end_room(&conn, &room, now_ms()).map_err(|e| e.to_string())?;
    info!(
        room,
        archives = n,
        "marked ended; the running controller submits once every part is uploaded"
    );
    Ok(())
}

fn upos(config: Config, query: &str) -> Result<(), String> {
    let conn = config.open_db()?;
    let found = db::upos_tickets(&conn, query, now_ms()).map_err(|e| e.to_string())?;
    if found.is_empty() {
        return Err(format!("no uploaded part within 5 days matches {query:?}"));
    }
    for t in found {
        let name = Path::new(&t.path)
            .file_name()
            .unwrap_or_default()
            .to_string_lossy();
        let hours = (now_ms() - t.issued_at) / 3_600_000;
        println!(
            "# {} {} bytes, ticket {hours}h old",
            t.part_id,
            t.size.unwrap_or(-1)
        );
        println!(
            "curl -fL --retry 3 -H 'X-Upos-Auth: {}' '{}' -o '{name}'",
            t.auth, t.url
        );
    }
    Ok(())
}

/// 命令行扫码登录：二维码写到 `svg`，扫码确认后账号存进库。
async fn login(config: Config, svg: &Path) -> Result<(), String> {
    let conn = config.open_db()?;
    let qr = BiliLogin;
    let (url, code) = qr.start().await?;
    std::fs::write(svg, lubic_account::qr_svg(&url).map_err(|e| e.to_string())?)
        .map_err(|e| format!("{}: {e}", svg.display()))?;
    info!(svg = %svg.display(), "scan the QR code with the Bilibili app");
    for _ in 0..180 {
        tokio::time::sleep(Duration::from_secs(1)).await;
        match qr.poll(&code).await? {
            Poll::Waiting | Poll::Scanned => {}
            Poll::Expired => break,
            Poll::Done(login) => {
                let mid = lubic_account::mid(&login).ok_or("login has no mid")?;
                let name = qr
                    .nickname(&login)
                    .await
                    .unwrap_or_else(|_| mid.to_string());
                let id = db::save_account(&conn, &name, mid, &login, now_ms())
                    .map_err(|e| e.to_string())?;
                info!(id, name, mid, "upload account saved");
                return Ok(());
            }
        }
    }
    Err("qrcode expired".into())
}

fn hash_password() -> Result<(), String> {
    use argon2::password_hash::{PasswordHasher, SaltString, rand_core::OsRng};
    let mut password = String::new();
    std::io::stdin()
        .read_line(&mut password)
        .map_err(|e| e.to_string())?;
    let password = password.trim_end_matches(['\r', '\n']);
    if password.is_empty() {
        return Err("empty password".into());
    }
    let salt = SaltString::generate(&mut OsRng);
    let hash = argon2::Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map_err(|e| e.to_string())?;
    println!("{hash}");
    Ok(())
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    if let Err(e) = lubic::logging::init(cli.log_dir.as_deref(), "controller") {
        eprintln!("{e}");
        std::process::exit(1);
    }
    let result = match Config::load(&cli.config) {
        Err(e) => Err(e),
        Ok(config) => match cli.command {
            None => run(config, cli.dry_run).await,
            Some(Command::End { room }) => end(config, room),
            Some(Command::Login { qrcode }) => login(config, &qrcode).await,
            Some(Command::Upos { query }) => upos(config, &query),
            Some(Command::HashPassword) => hash_password(),
        },
    };
    if let Err(e) = result {
        error!("{e}");
        std::process::exit(1);
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use db::tests::{closed, ended, mem, opened, uploaded};
    use lubic_proto::{AgentEvent, Envelope, Status};

    /// 假的投稿账号，按登录信息里的 mid 区分。
    #[derive(Default)]
    pub(crate) struct Fake {
        pub mid: u64,
        pub submitted: Mutex<Vec<Studio>>,
        pub next: Mutex<Option<SubmitError>>,
        /// 每次投稿走的接口。
        pub vias: Mutex<Vec<Via>>,
        pub preuploads: Mutex<u32>,
        /// preupload 回 601。
        pub limited: Mutex<bool>,
        pub cover_fails: Mutex<bool>,
        /// 上传过的封面。
        pub covers: Mutex<Vec<Vec<u8>>>,
        /// 加入合集的调用 (section_id, aid)。
        pub seasons_added: Mutex<Vec<(i64, u64)>>,
        /// 接下来几次加入合集失败。
        pub season_fails: Mutex<u32>,
        /// 新建合集的调用 (title, cover)。
        pub seasons_created: Mutex<Vec<(String, String)>>,
        /// 新建的合集不出现在合集列表里。
        pub created_season_hidden: Mutex<bool>,
        /// 账号没有建合集的权限（投稿预处理的 `season` 为 false）。
        pub no_season: Mutex<bool>,
        pub type_mode: Mutex<u8>,
    }

    #[async_trait]
    impl Bili for Fake {
        async fn preupload(&self, line: &str, name: &str, size: u64) -> Result<Value, String> {
            *self.preuploads.lock().unwrap() += 1;
            if *self.limited.lock().unwrap() {
                return Err(r#"preupload rejected: {"OK":0,"code":601}"#.into());
            }
            Ok(serde_json::json!({ "line": line, "name": name, "size": size, "mid": self.mid }))
        }

        async fn cover_up(&self, jpeg: &[u8]) -> Result<String, String> {
            assert!(jpeg.starts_with(&[0xff, 0xd8]), "jpeg");
            self.covers.lock().unwrap().push(jpeg.to_vec());
            if *self.cover_fails.lock().unwrap() {
                return Err("cover rejected".into());
            }
            Ok(format!("https://cover/{}", self.mid))
        }

        async fn archive_pre(&self) -> Result<Value, String> {
            Ok(serde_json::json!({
                "typelist": [{ "id": 171, "name": "电子竞技" }],
                "type_mode": *self.type_mode.lock().unwrap(),
                "season": !*self.no_season.lock().unwrap(),
            }))
        }

        async fn human_types(&self) -> Result<Value, String> {
            Ok(serde_json::json!({"type_list": [{"id": 1008, "name": "游戏"}]}))
        }

        async fn seasons(&self) -> Result<Value, String> {
            let mut list = vec![serde_json::json!({
                "season": { "id": 1, "title": "合集" },
                "sections": { "sections": [{ "id": 11, "title": "正片" }] },
            })];
            if !*self.created_season_hidden.lock().unwrap() {
                for (i, (title, _)) in self.seasons_created.lock().unwrap().iter().enumerate() {
                    let i = i as i64;
                    list.push(serde_json::json!({
                        "season": { "id": 100 + i, "title": title },
                        "sections": { "sections": [{ "id": 1000 + i, "title": "正片" }] },
                    }));
                }
            }
            Ok(serde_json::json!({ "seasons": list }))
        }

        async fn add_to_season(&self, section_id: i64, aid: u64) -> Result<(), String> {
            let mut fails = self.season_fails.lock().unwrap();
            if *fails > 0 {
                *fails -= 1;
                return Err("season rejected".into());
            }
            self.seasons_added.lock().unwrap().push((section_id, aid));
            Ok(())
        }

        async fn create_season(&self, title: &str, cover: &str) -> Result<i64, String> {
            let mut created = self.seasons_created.lock().unwrap();
            let id = 100 + created.len() as i64;
            created.push((title.into(), cover.into()));
            Ok(id)
        }

        async fn submit(&self, studio: &Studio, via: Via) -> Result<Submitted, SubmitError> {
            self.vias.lock().unwrap().push(via);
            if let Some(e) = self.next.lock().unwrap().take() {
                return Err(e);
            }
            self.submitted.lock().unwrap().push(studio.clone());
            Ok(Submitted {
                aid: 1,
                bvid: "BV1".into(),
            })
        }
    }

    /// 每个 mid 一个假账号。
    #[derive(Default)]
    pub(crate) struct Fakes(Mutex<HashMap<u64, Arc<Fake>>>);

    impl Fakes {
        pub fn get(&self, mid: u64) -> Arc<Fake> {
            let mut m = self.0.lock().unwrap();
            m.entry(mid)
                .or_insert_with(|| {
                    Arc::new(Fake {
                        mid,
                        ..Default::default()
                    })
                })
                .clone()
        }
    }

    /// 假的扫码：按顺序吐出预设的轮询结果，吐完一直是「未扫」。
    #[derive(Clone, Default)]
    pub(crate) struct FakeQr(pub Arc<Mutex<std::collections::VecDeque<Poll>>>);

    #[async_trait]
    impl QrLogin for FakeQr {
        async fn start(&self) -> Result<(String, String), String> {
            Ok(("https://qr/1".into(), "code".into()))
        }

        async fn poll(&self, _: &str) -> Result<Poll, String> {
            Ok(self.0.lock().unwrap().pop_front().unwrap_or(Poll::Waiting))
        }

        async fn nickname(&self, login: &Value) -> Result<String, String> {
            Ok(format!("up{}", login["token_info"]["mid"]))
        }
    }

    pub(crate) fn config() -> Config {
        toml::from_str(
            r#"
            db = "c.db"
            cookies = "c.json"
            default_agents = ["a", "b"]
            [agents.a]
            token = "ta"
            [agents.b]
            token = "tb"
            [agents.c]
            token = "tc"
            [[rooms]]
            name = "r"
            url = "https://live.bilibili.com/6"
            split_time = "2m"
            [rooms.upload]
            title = "{name}-{title}-%Y"
            source = "{url}"
            copyright = 2
            [[rooms]]
            name = "r2"
            url = "https://live.bilibili.com/7"
            agents = ["c", "a"]
            [rooms.upload]
            "#,
        )
        .unwrap()
    }

    pub(crate) fn test_apps(config: Config) -> (Arc<App>, Arc<Fakes>, FakeQr) {
        let fakes = Arc::new(Fakes::default());
        let f = fakes.clone();
        let make: MakeBili =
            Box::new(move |login, _| f.get(lubic_account::mid(&login).unwrap()) as Arc<dyn Bili>);
        let qr = FakeQr::default();
        let mut app = app(config, mem(), make, Box::new(qr.clone()));
        Arc::get_mut(&mut app).unwrap().started = 0;
        (app, fakes, qr)
    }

    /// 跳过投稿间隔：同账号接着投下一份。
    fn skip_submit_spacing(app: &App) {
        for g in app.gates.lock().unwrap().values_mut() {
            g.submit_after = 0;
        }
    }

    /// 账号 1 的假实现。
    fn test_app() -> (Arc<App>, Arc<Fake>) {
        let (app, fakes, _) = test_apps(config());
        let fake = fakes.get(1);
        (app, fake)
    }

    pub(crate) fn db_room(app: &App, name: &str) -> db::Room {
        db::room(&app.db(), name).unwrap().unwrap()
    }

    /// 按房间当前配置分配。
    pub(crate) fn assign(app: &App, room: &str, agent: &str, at: i64) {
        let task = db_room(app, room).task();
        db::assign(&app.db(), room, agent, "s", at, &task).unwrap();
    }

    pub(crate) fn seen(app: &App, agent: &str, at: i64, paused: bool) {
        let status = Status {
            paused,
            ..Default::default()
        };
        db::touch_agent(&app.db(), agent, at, &status).unwrap();
    }

    fn assigned(app: &App, room: &str) -> Option<String> {
        db::assignment(&app.db(), room).unwrap().map(|(a, _)| a)
    }

    const MIN: i64 = 60_000;
    const HOUR: i64 = 60 * MIN;

    #[test]
    fn tail_upload_needs_a_size_split_and_caps_the_split_time() {
        let base = r#"
            db = "c.db"
            cookies = "c.json"
            agents = {}
            [[rooms]]
            name = "r"
            url = "u"
            tail_upload = true
            [rooms.upload]
            "#;
        let e = Config::parse(base).err().unwrap();
        assert!(e.contains("split_size_mb"), "{e}");
        let c = Config::parse(&base.replace("tail_upload", "split_size_mb = 200\ntail_upload"))
            .unwrap();
        let t = c.rooms[0].import(&[]).0.task();
        assert_eq!(t.split_time, Some(TAIL_SPLIT_MAX), "缺省也封顶");
        assert!(t.tail_upload);
        let c = Config::parse(&base.replace(
            "tail_upload = true",
            "split_size_mb = 200\nsplit_time = \"1h\"\ntail_upload = true",
        ))
        .unwrap();
        assert_eq!(
            c.rooms[0].import(&[]).0.task().split_time,
            Some(TAIL_SPLIT_MAX)
        );
        // 没开的房间不动。
        let (app, _) = test_app();
        assert_eq!(
            db_room(&app, "r").task().split_time,
            Some(Duration::from_secs(120))
        );
    }

    #[test]
    fn assigns_first_available_agent_by_priority_and_reassigns_when_unreachable() {
        let (app, _) = test_app();
        let room = &db_room(&app, "r");
        let live = || Some(Some("s".to_string()));
        let t = 100 * MIN;
        // a 停录了（硬盘满），b 在线：分给 b。
        seen(&app, "a", t, true);
        seen(&app, "b", t, false);
        assert!(app.schedule(room, live(), t).unwrap());
        assert_eq!(assigned(&app, "r").as_deref(), Some("b"));
        assert!(!app.schedule(room, live(), t + MIN).unwrap());
        // 检测失败什么也不做。
        assert!(!app.schedule(room, None, t + 3 * MIN).unwrap());
        // b 失联 2 分钟以上、a 恢复：改派给 a。
        seen(&app, "a", t + 3 * MIN, false);
        assert!(app.schedule(room, live(), t + 3 * MIN).unwrap());
        assert_eq!(assigned(&app, "r").as_deref(), Some("a"));
        // 房间自己的列表优先于默认列表；c 从没来过，跳过它。
        let r2 = &db_room(&app, "r2");
        assert!(app.schedule(r2, live(), t + 3 * MIN).unwrap());
        assert_eq!(assigned(&app, "r2").as_deref(), Some("a"));
        // 谁都不在：不分配。
        let (app, _) = test_app();
        assert!(!app.schedule(&db_room(&app, "r"), live(), t).unwrap());
    }

    #[test]
    fn empty_agents_keep_the_current_session_then_pause_until_reselected() {
        let (app, _) = test_app();
        let t = 100 * MIN;
        seen(&app, "a", t, false);
        let mut room = db_room(&app, "r");
        assert!(app.schedule(&room, Some(Some("s".into())), t).unwrap());
        db::apply(&app.db(), "a", &opened("p1", "s", t)).unwrap();
        let task = app.desired("a").unwrap();

        room.agents.clear();
        app.save_room(room, false).unwrap();
        let room = db_room(&app, "r");
        assert!(
            !app.schedule(&room, Some(Some("s".into())), t + MIN)
                .unwrap()
        );
        assert_eq!(app.desired("a").unwrap(), task, "当前这一场继续录");
        assert!(
            !app.schedule(&room, Some(None), t + 10 * MIN).unwrap(),
            "下播宽限内继续录"
        );

        db::apply(&app.db(), "a", &ended("s", t + 10 * MIN)).unwrap();
        assert!(app.schedule(&room, Some(None), t + 11 * MIN).unwrap());
        assert!(app.desired("a").unwrap().is_empty());
        seen(&app, "a", t + 12 * MIN, false);
        assert!(
            !app.schedule(&room, Some(Some("next".into())), t + 12 * MIN)
                .unwrap()
        );
        assert_eq!(assigned(&app, "r"), None, "下一场不再分配");

        let mut room = room;
        room.agents.push("a".into());
        app.save_room(room, false).unwrap();
        assert!(
            app.schedule(&db_room(&app, "r"), Some(Some("next".into())), t + 12 * MIN)
                .unwrap()
        );
        assert_eq!(assigned(&app, "r").as_deref(), Some("a"));
    }

    #[test]
    fn restart_grace_keeps_assignments_while_agents_report_back() {
        let (app, _) = test_app();
        let t = 100 * MIN;
        seen(&app, "a", t - 10 * MIN, false); // 总控停了 10 分钟
        seen(&app, "b", t, false);
        assign(&app, "r", "a", t - 10 * MIN);
        let mut app = app;
        Arc::get_mut(&mut app).unwrap().started = t;
        let room = &db_room(&app, "r");
        assert!(!app.schedule(room, Some(Some("s".into())), t + MIN).unwrap());
        assert_eq!(assigned(&app, "r").as_deref(), Some("a"));
        // 宽限过后 a 仍没来：改派。
        seen(&app, "b", t + 3 * MIN, false);
        assert!(
            app.schedule(room, Some(Some("s".into())), t + 3 * MIN)
                .unwrap()
        );
        assert_eq!(assigned(&app, "r").as_deref(), Some("b"));
    }

    #[test]
    fn unassigns_only_after_the_session_ended_and_not_right_after_assigning() {
        let (app, _) = test_app();
        let room = &db_room(&app, "r");
        let t = 100 * MIN;
        seen(&app, "a", t, false);
        app.schedule(room, Some(Some("s".into())), t).unwrap();
        // 刚分配就检测到不在播：先不撤。
        assert!(!app.schedule(room, Some(None), t + MIN).unwrap());
        // agent 在录（宽限期内）：不撤。
        db::apply(&app.db(), "a", &opened("p1", "s", t)).unwrap();
        assert!(!app.schedule(room, Some(None), t + 10 * MIN).unwrap());
        // 这一场报完了：撤。
        db::apply(&app.db(), "a", &ended("s", t + 10 * MIN)).unwrap();
        assert!(app.schedule(room, Some(None), t + 11 * MIN).unwrap());
        assert_eq!(assigned(&app, "r"), None);
    }

    fn status(rates: &[(&str, f64)], produce: f64, upload: Option<f64>, busy: f64) -> Status {
        Status {
            record_rates: rates.iter().map(|(r, v)| (r.to_string(), *v)).collect(),
            produce_bps: produce,
            upload_bps: upload,
            upload_busy: busy,
            ..Default::default()
        }
    }

    fn limited(app: &App, agent: &str) -> Option<db::Limit> {
        db::limit(&app.db(), agent).unwrap()
    }

    fn rooms_of(app: &App, agent: &str) -> Vec<String> {
        app.desired(agent)
            .unwrap()
            .into_iter()
            .map(|t| t.name)
            .collect()
    }

    fn recording(app: &App, agent: &str, room: &str) {
        let ev = AgentEvent::Recording {
            room: room.into(),
            lag_ms: Some(500),
            start_live_ms: None,
        };
        db::apply(&app.db(), agent, &ev).unwrap();
    }

    fn skips(list: &[(&str, u64, u32)]) -> Vec<(String, u64, u32)> {
        list.iter()
            .map(|(r, ms, n)| (r.to_string(), *ms, *n))
            .collect()
    }

    fn empty_connections(mut s: Status, list: &[(&str, u64, u64)]) -> Status {
        let empty = list
            .iter()
            .map(|(r, ago, ms)| (r.to_string(), *ago, *ms, false));
        s.record_connections.extend(empty);
        s
    }

    /// 都是写出过有效视频的连接；空连接用 [`empty_connections`] 另加。
    fn connections(list: &[(&str, u64, u64)]) -> Status {
        Status {
            record_connections: list
                .iter()
                .map(|(r, ago, ms)| (r.to_string(), *ago, *ms, true))
                .collect(),
            ..Default::default()
        }
    }

    /// 健康信号日志与总控其余日志一样用英文文案。
    #[test]
    fn health_signal_logs_are_in_english() {
        #[derive(Clone, Default)]
        struct Buf(Arc<Mutex<Vec<u8>>>);
        impl std::io::Write for Buf {
            fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().write(b)
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let t = 100 * MIN;
        let (app, _) = test_app();
        let buf = Buf::default();
        let writer = buf.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(move || writer.clone())
            .with_ansi(false)
            .finish();
        let ago = |s: u64| s * 1000;
        let short = empty_connections(
            connections(&[("r", ago(5), 1), ("r", ago(6), 1), ("r", ago(7), 1)]),
            &[("r", ago(8), 1)],
        );
        tracing::subscriber::with_default(subscriber, || {
            app.health("a", &short, t);
            app.health("a", &Status::default(), t);
        });
        let out = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
        let lines: Vec<_> = out
            .lines()
            .filter(|l| l.contains("health.signal"))
            .collect();
        assert_eq!(lines.len(), 2, "{out}");
        assert!(lines.iter().all(|l| l.is_ascii()), "{out}");
        assert!(
            lines[0].contains("count=3") && lines[0].contains("empty=1"),
            "空连接分开计数：{out}"
        );
    }

    /// 2 分钟内 3 次不足 30 秒的连接才达到门槛；只在状态变化时报，不标记受限、不交接。
    #[test]
    fn short_connections_are_logged_on_change_only() {
        let t = 100 * MIN;
        let (app, _) = test_app();
        seen(&app, "a", t, false);
        seen(&app, "b", t, false);
        assign(&app, "r", "a", t);
        let ago = |s: u64| s * 1000;
        for (list, why) in [
            (
                vec![("r", ago(10), 5_000), ("r", ago(20), 5_000)],
                "只有 2 次",
            ),
            (
                vec![
                    ("r", ago(10), 5_000),
                    ("r", ago(20), 5_000),
                    ("r", ago(30), 30_000),
                ],
                "30 秒不算短",
            ),
            (
                vec![
                    ("r", ago(10), 5_000),
                    ("r", ago(20), 5_000),
                    ("r", ago(121), 5_000),
                ],
                "超出 2 分钟",
            ),
            (
                vec![
                    ("probe:r", ago(1), 1),
                    ("probe:r", ago(2), 1),
                    ("probe:r", ago(3), 1),
                ],
                "试拉不算",
            ),
        ] {
            assert!(app.health("a", &connections(&list), t).is_empty(), "{why}");
        }
        let empty = empty_connections(
            connections(&[("r", ago(10), 5_000), ("r", ago(20), 5_000)]),
            &[("r", ago(1), 1), ("r", ago(2), 1), ("r", ago(3), 1)],
        );
        assert!(
            app.health("a", &empty, t).is_empty(),
            "空连接不计门槛，由 no_video 与 record_failures 覆盖"
        );
        let short_list = [
            ("r", ago(5), 24_000),
            ("r", ago(40), 5_000),
            ("r", ago(100), 3_000),
        ];
        let short = connections(&short_list);
        assert_eq!(
            app.health("a", &short, t),
            [("r".into(), "short_connections", true)]
        );
        assert!(
            app.health("a", &short, t + 1_000).is_empty(),
            "同一状态不重复报"
        );
        assert!(
            app.health("b", &Status::default(), t).is_empty(),
            "别的机器不受影响"
        );
        // 回差：达到门槛后，窗口里还剩 2 次不算回落（一次滑出窗口又来一次就会反复触发），降到 1 次才回落。
        assert!(
            app.health("a", &connections(&short_list[..2]), t + 1_500)
                .is_empty()
        );
        assert_eq!(
            app.health("a", &connections(&short_list[..1]), t + 2_000),
            [("r".into(), "short_connections", false)]
        );
        assert!(
            app.health("a", &connections(&short_list[..2]), t + 2_500)
                .is_empty(),
            "回落后 2 次不触发"
        );
        app.judge("a", &short, t).unwrap();
        assert!(limited(&app, "a").is_none(), "第一期不标记");
        app.schedule(&db_room(&app, "r"), Some(Some("s".into())), t)
            .unwrap();
        assert!(
            db::handoff(&app.db(), "r").unwrap().is_none(),
            "第一期不交接"
        );
    }

    /// 平台确认在播、从最近一次有效视频与确认在播的起点里较晚的那个起，满 60 秒没写出视频才达到门槛；
    /// 一帧都没写出过的从确认在播起算。
    #[test]
    fn no_video_for_a_minute_while_live_is_logged_on_change_only() {
        let t = 100 * MIN;
        let (app, _) = test_app();
        let video = |list: &[(&str, Option<u64>, u64)]| Status {
            record_video: list
                .iter()
                .map(|(r, v, s)| (r.to_string(), *v, *s))
                .collect(),
            ..Default::default()
        };
        let ago = |s: u64| s * 1000;
        for (list, why) in [
            (vec![("r", Some(ago(59)), ago(600))], "59 秒前还有视频"),
            (vec![("r", None, ago(59))], "确认在播才 59 秒"),
            (vec![("r", Some(ago(600)), ago(30))], "宽限后刚重新确认在播"),
            (vec![("probe:r", None, ago(600))], "试拉不算"),
        ] {
            assert!(app.health("a", &video(&list), t).is_empty(), "{why}");
        }
        let stuck = video(&[("r", None, ago(60)), ("q", Some(ago(90)), ago(600))]);
        assert_eq!(
            app.health("a", &stuck, t),
            [
                ("q".into(), "no_video", true),
                ("r".into(), "no_video", true)
            ]
        );
        assert!(
            app.health("a", &stuck, t + 10_000).is_empty(),
            "同一状态不重复报"
        );
        let resumed = video(&[("r", Some(0), ago(60)), ("q", Some(ago(90)), ago(600))]);
        assert_eq!(
            app.health("a", &resumed, t),
            [("r".into(), "no_video", false)]
        );
        app.judge("a", &stuck, t).unwrap();
        assert!(limited(&app, "a").is_none(), "第一期不标记");
    }

    /// 总控兜底：房间分给这台、总控自己检测在播，而这台（会报健康字段的新 agent）没报它的 `record_video`，
    /// 从分配与总控第一次发现没报里较晚的那个起满 60 秒，也算 no_video。
    #[test]
    fn no_video_falls_back_to_the_controllers_own_detection() {
        let t = 100 * MIN;
        let (app, _) = test_app();
        let new = Status {
            health_reports: true,
            ..Default::default()
        };
        let paused = Status {
            paused: true,
            ..new.clone()
        };
        let reported = Status {
            record_video: vec![("r".into(), Some(0), 10 * MIN as u64)],
            ..new.clone()
        };
        seen(&app, "b", t, false);
        assign(&app, "r", "a", t);
        app.detected("r", Some(true));
        assert!(app.health("a", &new, t).is_empty());
        assert!(
            app.health("a", &new, t + 59_000).is_empty(),
            "分配不足 60 秒"
        );
        assert_eq!(
            app.health("a", &new, t + 60_000),
            [("r".into(), "no_video", true)]
        );
        assert!(
            app.health("a", &new, t + 70_000).is_empty(),
            "同一状态不重复报"
        );
        app.detected("r", None);
        assert!(
            app.health("a", &new, t + 80_000).is_empty(),
            "检测失败：不触发也不清掉"
        );
        app.detected("r", Some(false));
        assert_eq!(
            app.health("a", &new, t + 90_000),
            [("r".into(), "no_video", false)],
            "总控认为不在播"
        );
        app.detected("r", Some(true));
        assert!(
            app.health("a", &reported, t + 100_000).is_empty(),
            "这台报着视频"
        );
        assert!(
            app.health("a", &new, t + 110_000).is_empty(),
            "录了很久才不报的，从发现没报起算"
        );
        assert!(app.health("a", &new, t + 169_000).is_empty());
        assert_eq!(
            app.health("a", &new, t + 170_000),
            [("r".into(), "no_video", true)]
        );
        app.health("a", &reported, t + 180_000);
        assert!(
            app.health("a", &Status::default(), t + 10 * MIN).is_empty(),
            "旧 agent 不报 record_video，不兜底"
        );
        assert!(app.health("a", &paused, t + 10 * MIN).is_empty());
        assert_eq!(
            app.health("a", &paused, t + 11 * MIN),
            [("r".into(), "no_video_paused", true)],
            "停录的机器不报 no_video，只注明原因"
        );
        assert_eq!(
            app.health("a", &reported, t + 12 * MIN),
            [("r".into(), "no_video_paused", false)]
        );
        assert!(app.health("a", &new, t + 13 * MIN).is_empty());
        db::start_handoff(&app.db(), "r", "a", "b", t + 13 * MIN).unwrap();
        app.db()
            .execute("UPDATE handoffs SET started_at = ?1", [t + 13 * MIN])
            .unwrap();
        assert!(
            app.health("a", &new, t + 15 * MIN).is_empty(),
            "交接收尾中的旧机不兜底"
        );
        assert!(
            app.health("b", &new, t + 15 * MIN).is_empty(),
            "交接目标没分配到它"
        );
        app.judge("a", &new, t + 15 * MIN).unwrap();
        assert!(limited(&app, "a").is_none(), "只打日志");
    }

    /// 60 秒内至少 2 个测得的接缝（重叠也算一个）、累计缺失至少 10 秒才达到门槛。
    #[test]
    fn reconnect_gaps_are_logged_on_change_only() {
        let t = 100 * MIN;
        let (app, _) = test_app();
        let gaps = |list: &[(&str, u64, u64)]| Status {
            record_gaps: list
                .iter()
                .map(|(r, at, ms)| (r.to_string(), *at, *ms))
                .collect(),
            ..Default::default()
        };
        let ago = |s: u64| s * 1000;
        for (list, why) in [
            (vec![("r", ago(5), 30_000)], "只有 1 个接缝"),
            (
                vec![("r", ago(5), 5_000), ("r", ago(10), 4_999)],
                "累计不足 10 秒",
            ),
            (
                vec![("r", ago(5), 9_000), ("r", ago(60), 9_000)],
                "超出 60 秒",
            ),
            (
                vec![("probe:r", ago(1), 9_000), ("probe:r", ago(2), 9_000)],
                "试拉不算",
            ),
        ] {
            assert!(app.health("a", &gaps(&list), t).is_empty(), "{why}");
        }
        let lossy = gaps(&[("r", ago(5), 0), ("r", ago(30), 10_000)]);
        assert_eq!(
            app.health("a", &lossy, t),
            [("r".into(), "reconnect_gaps", true)]
        );
        assert!(
            app.health("a", &lossy, t + 1_000).is_empty(),
            "同一状态不重复报"
        );
        assert_eq!(
            app.health("a", &Status::default(), t + 2_000),
            [("r".into(), "reconnect_gaps", false)]
        );
        app.judge("a", &lossy, t).unwrap();
        assert!(limited(&app, "a").is_none(), "第一期不标记");
    }

    /// 故障试拉的原始数（`trial:<房间>`）与到期前试拉一样，不进整机与房间的判定（§6.1.1 约定 4）。
    #[test]
    fn trial_keys_are_excluded_from_machine_and_room_judgement() {
        let t = 100 * MIN;
        let (app, _) = test_app();
        let k = || "trial:r".to_string();
        let s = Status {
            record_rates: vec![(k(), 0.1)],
            record_skips: vec![(k(), 50_000, 5)],
            record_failures: vec![(k(), 2, true)],
            record_connections: (1..=3).map(|i| (k(), i * 1000, 1, true)).collect(),
            record_video: vec![(k(), None, 10 * MIN as u64)],
            record_gaps: vec![(k(), 1_000, 9_000), (k(), 2_000, 9_000)],
            produce_bps: 1000.0,
            ..Default::default()
        };
        app.judge("a", &s, t).unwrap();
        assert!(limited(&app, "a").is_none(), "试拉的原始数不标记整机");
        assert!(app.health("a", &s, t).is_empty(), "也不触发房间信号");
    }

    #[test]
    fn empty_retries_and_video_stalls_trigger_a_handoff_without_rate_samples() {
        let t = 100 * MIN;
        for (failures, stalled) in [(2, false), (1, true)] {
            let (app, _) = test_app();
            seen(&app, "a", t, false);
            seen(&app, "b", t, false);
            assign(&app, "r", "a", t);
            let s: Status = serde_json::from_value(serde_json::json!({
                "record_failures": [["r", failures, stalled]]
            }))
            .unwrap();
            app.judge("a", &s, t).unwrap();
            assert!(limited(&app, "a").is_some());
            assert!(
                app.schedule(&db_room(&app, "r"), Some(Some("s".into())), t)
                    .unwrap()
            );
            assert_eq!(assigned(&app, "r").as_deref(), Some("a"));
            assert_eq!(db::handoff(&app.db(), "r").unwrap().unwrap().to, "b");
        }
    }

    #[test]
    fn one_empty_attempt_or_a_probe_does_not_move_rooms_and_single_machine_keeps_recording() {
        let t = 100 * MIN;
        let (app, _) = test_app();
        for (room, attempts, stalled) in [("r", 1, false), ("probe:r", 2, true)] {
            let s = Status {
                record_failures: vec![(room.into(), attempts, stalled)],
                ..Default::default()
            };
            app.judge("a", &s, t).unwrap();
            assert!(limited(&app, "a").is_none());
        }
        seen(&app, "a", t, false);
        assign(&app, "r", "a", t);
        let s = Status {
            record_failures: vec![("r".into(), 2, false)],
            ..Default::default()
        };
        app.judge("a", &s, t).unwrap();
        let mut room = db_room(&app, "r");
        room.agents = vec!["a".into()];
        assert!(!app.schedule(&room, Some(Some("s".into())), t).unwrap());
        assert_eq!(assigned(&app, "r").as_deref(), Some("a"));
        assert!(db::handoff(&app.db(), "r").unwrap().is_none());
    }

    #[test]
    fn video_failures_participate_in_cross_checks_and_probe_verdicts() {
        let t = 100 * MIN;
        for good in [false, true] {
            let (app, _) = test_app();
            seen(&app, "a", t, false);
            seen(&app, "b", t, false);
            assign(&app, "r", "a", t);
            app.rate_moved.lock().unwrap().insert(
                "r".into(),
                Moved {
                    at: t,
                    from: "b".into(),
                    ..Default::default()
                },
            );
            let fault = Status {
                record_failures: vec![("r".into(), 2, false)],
                ..Default::default()
            };
            app.judge("a", &fault, t).unwrap();
            assert!(limited(&app, "a").is_none());
            let probe = Status {
                // 即使旧倍率样本是健康的，新失败信号仍必须优先。
                record_rates: vec![("probe:r".into(), 1.0)],
                record_failures: if good {
                    vec![]
                } else {
                    vec![("probe:r".into(), 1, true)]
                },
                ..Default::default()
            };
            db::touch_agent(&app.db(), "b", t + 1_000, &probe).unwrap();
            app.judge("a", &fault, t + 1_000).unwrap();
            assert_eq!(limited(&app, "a").is_some(), good);
        }
        let (app, _) = test_app();
        let probe = Status {
            record_failures: vec![("probe:r".into(), 2, false)],
            ..Default::default()
        };
        assert!(matches!(
            app.probe_verdict("a", &probe, t).unwrap(),
            Some(Err(_))
        ));
        assign(&app, "r", "b", t);
        let source = Status {
            record_failures: vec![("r".into(), 1, true)],
            ..Default::default()
        };
        db::touch_agent(&app.db(), "b", t, &source).unwrap();
        assert_eq!(
            app.probe_verdict("a", &probe, t).unwrap(),
            None,
            "两台都没有有效视频，不能认定是本机问题"
        );
    }

    /// 丢内容：60 秒里跳 3 次、跳过 30 秒才标记；试拉键与刚迁来的房间不算。
    #[test]
    fn judges_limits_from_skipped_content() {
        let t = 100 * MIN;
        let judge = |app: &App, rates: &[(&str, f64)], sk: &[(&str, u64, u32)]| {
            let s = Status {
                record_skips: skips(sk),
                ..status(rates, 1000.0, None, 0.0)
            };
            app.judge("a", &s, t).unwrap();
            limited(app, "a")
        };
        let (app, _) = test_app();
        assert_eq!(judge(&app, &[], &[("r", 40_000, 2)]), None, "只跳了 2 次");
        assert_eq!(judge(&app, &[], &[("r", 20_000, 3)]), None, "只丢了 20 秒");
        assert_eq!(
            judge(&app, &[("probe:r", 0.1)], &[("probe:r", 50_000, 5)]),
            None,
            "试拉的结果不触发标记"
        );
        let l = judge(&app, &[], &[("r", 30_000, 3)]).unwrap();
        assert_eq!(l.reason, "record skipped 30s/60s");
        assert_eq!(l.need_bps, 2000.0);
        // 录制信号一标记就叫醒检测周期去交接。
        let woke = std::pin::pin!(app.tick_now.notified());
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(std::future::Future::poll(woke, &mut cx).is_ready());
        // 倍率也低时按倍率。
        let (app, _) = test_app();
        let l = judge(&app, &[("r", 0.5)], &[("r", 30_000, 3)]).unwrap();
        assert!(l.reason.starts_with("record rate"), "{}", l.reason);
        // 刚因录制信号迁来的房间：没有能交叉试拉的机器，按源站慢，不标记。
        let (app, _) = test_app();
        let moved = Moved {
            at: t - 10 * MIN,
            from: "b".into(),
            ..Default::default()
        };
        app.rate_moved.lock().unwrap().insert("r".into(), moved);
        assert_eq!(judge(&app, &[], &[("r", 50_000, 4)]), None);
    }

    /// 到期时：上传慢照旧续期；否则看试拉，不达标续期，达标或没有结论解除。
    #[test]
    fn the_probe_decides_whether_a_hold_ends() {
        let t = 100 * MIN;
        let end = t + 60 * MIN;
        let check = |rates: &[(&str, f64)], sk: &[(&str, u64, u32)], upload: Option<f64>| {
            let (app, _) = test_app();
            app.judge("a", &status(&[("r", 0.5)], 400.0, None, 0.0), t)
                .unwrap();
            let s = Status {
                record_skips: skips(sk),
                ..status(rates, 0.0, upload, 0.5)
            };
            app.judge("a", &s, end).unwrap();
            (app.clone(), limited(&app, "a"))
        };
        let (_, l) = check(&[("probe:r", 0.95)], &[], None);
        assert_eq!(l, None, "试拉达标、没有上传样本：解除");
        let (_, l) = check(&[("probe:r", 0.5)], &[], None);
        let l = l.unwrap();
        assert_eq!(
            (l.reason.as_str(), l.until),
            ("probe record rate 0.50", end + 60 * MIN)
        );
        assert_eq!(l.need_bps, 800.0, "门槛不变");
        let (_, l) = check(&[("probe:r", 0.95)], &[("probe:r", 40_000, 3)], None);
        assert_eq!(l.unwrap().reason, "probe skipped 40s/60s");
        let (_, l) = check(&[], &[], None);
        assert_eq!(l, None, "没有试拉：照旧按时解除");
        let (_, l) = check(&[("probe:r", 0.95)], &[], Some(100.0));
        assert!(
            l.unwrap().reason.starts_with("record rate"),
            "上传慢：照旧续期"
        );

        // 录这个房间的 b 也慢：分不清是源站，不算试拉的结论。
        let (app, _) = test_app();
        app.judge("a", &status(&[("r", 0.5)], 400.0, None, 0.0), t)
            .unwrap();
        assign(&app, "r", "b", t);
        db::touch_agent(&app.db(), "b", end, &status(&[("r", 0.5)], 0.0, None, 0.0)).unwrap();
        app.judge("a", &status(&[("probe:r", 0.5)], 0.0, None, 0.0), end)
            .unwrap();
        assert_eq!(limited(&app, "a"), None);
    }

    /// 到期前 3 分钟起，让受限机试拉一个在别处录、优先列表含它的房间。
    #[test]
    fn probes_a_room_recorded_elsewhere_before_the_hold_ends() {
        let (app, _) = test_app();
        let t = 100 * MIN;
        let until = t + 60 * MIN;
        let probe = |at: i64| app.probe_for("a", at).unwrap().map(|r| r.name);
        assign(&app, "r", "b", t);
        assert_eq!(probe(until - MIN), None, "没受限");
        let limit = db::Limit {
            since: t,
            until,
            reason: "record rate 0.5".into(),
            need_bps: 0.0,
        };
        db::set_limit(&app.db(), "a", &limit).unwrap();
        assert_eq!(probe(until - 4 * MIN), None, "离到期还远");
        assert_eq!(probe(until - 3 * MIN).as_deref(), Some("r"));
        // r2 的列表是 [c, a]，在 c 上录也可以试拉；r 分给 a 自己就不算。
        assign(&app, "r", "a", t);
        assert_eq!(probe(until - MIN), None, "分给自己、r2 没分配");
        assign(&app, "r2", "c", t);
        assert_eq!(probe(until - MIN).as_deref(), Some("r2"));
        // c 的列表里没有它。
        assert_eq!(app.probe_for("b", until - MIN).unwrap(), None);
        db::update_machine(&app.db(), "a", Some(false), None).unwrap();
        assert_eq!(probe(until - MIN), None, "停用");
    }

    fn room_limit(app: &App, room: &str, agent: &str) -> Option<db::RoomLimit> {
        db::room_limit(&app.db(), room, agent).unwrap()
    }

    fn limit_room(app: &App, room: &str, agent: &str, since: i64, until: i64) {
        let l = db::RoomLimit {
            room: room.into(),
            agent: agent.into(),
            since,
            until,
            reason: "no_video".into(),
        };
        db::set_room_limit(&app.db(), &l).unwrap();
    }

    /// 房间级限制只影响这个房间与这台机器的组合（C3）：分配与交接都跳过它，别的房间照常用这台；
    /// 全部候选都受限时仍分给它。到期没验证的保持待验证，下次开播照常分配、开录即解除（C2）。
    #[test]
    fn a_room_limit_only_keeps_that_room_off_that_machine() {
        let live = || Some(Some("s".to_string()));
        let t = 100 * MIN;
        let (app, _) = test_app();
        seen(&app, "a", t, false);
        seen(&app, "b", t, false);
        limit_room(&app, "r", "a", t, t + 20 * MIN);
        assert!(app.schedule(&db_room(&app, "r"), live(), t).unwrap());
        assert_eq!(assigned(&app, "r").as_deref(), Some("b"), "r 跳过 a");
        assert!(app.schedule(&db_room(&app, "r2"), live(), t).unwrap());
        assert_eq!(assigned(&app, "r2").as_deref(), Some("a"), "r2 照常用 a");
        assert!(limited(&app, "a").is_none(), "不是整机限制");

        // 只有 a 在线：录总比不录好，限制保留。
        let (app, _) = test_app();
        seen(&app, "a", t, false);
        limit_room(&app, "r", "a", t, t + 20 * MIN);
        assert!(app.schedule(&db_room(&app, "r"), live(), t).unwrap());
        assert_eq!(assigned(&app, "r").as_deref(), Some("a"));
        assert!(room_limit(&app, "r", "a").is_some());

        // 到期没验证（待验证）：r 在 b 上录着，不迁回 a。
        let (app, _) = test_app();
        seen(&app, "a", t, false);
        seen(&app, "b", t, false);
        limit_room(&app, "r", "a", t - 30 * MIN, t - 10 * MIN);
        let mut room = db_room(&app, "r");
        room.split_time = None;
        assign(&app, "r", "b", t);
        assert!(!app.schedule(&room, live(), t).unwrap(), "待验证不迁回");
        assert!(room_limit(&app, "r", "a").is_some());
        // 下次开播照常分给 a，开录即解除。
        db::unassign(&app.db(), "r").unwrap();
        assert!(app.schedule(&room, live(), t + MIN).unwrap());
        assert_eq!(assigned(&app, "r").as_deref(), Some("a"));
        assert_eq!(room_limit(&app, "r", "a"), None);
        // 解除后也不迁回（load-balance.md §2）。
        let (app, _) = test_app();
        seen(&app, "a", t, false);
        seen(&app, "b", t, false);
        assign(&app, "r", "b", t);
        assert!(!app.schedule(&room, live(), t).unwrap());
        assert_eq!(db::handoff(&app.db(), "r").unwrap(), None);
    }

    /// 房间级限制的恢复只靠试拉验证：到期前 3 分钟起让受限机试拉这个房间，到期时达标解除、
    /// 不达标按 20→40→60 分钟续期、没有结果保持待验证。
    #[test]
    fn a_room_limit_ends_only_after_a_probe_verifies_it() {
        let t = 100 * MIN;
        let until = t + 20 * MIN;
        let (app, _) = test_app();
        seen(&app, "b", t, false);
        assign(&app, "r", "b", t);
        limit_room(&app, "r", "a", t, until);
        let probe = |at: i64| app.probe_for("a", at).unwrap().map(|r| r.name);
        assert_eq!(probe(until - 4 * MIN), None, "离到期还远");
        assert_eq!(probe(until - 3 * MIN).as_deref(), Some("r"));
        let judge = |rates: &[(&str, f64)], at: i64| {
            app.judge_room_limits("a", &status(rates, 0.0, None, 0.0), at)
                .unwrap();
            room_limit(&app, "r", "a")
        };
        assert!(
            judge(&[("probe:r", 1.0)], until - 1).is_some(),
            "达标也不提前解除"
        );
        let l = judge(&[("probe:r", 0.5)], until).unwrap();
        assert_eq!(
            (l.since, l.until, l.reason.as_str()),
            (t, until + 40 * MIN, "probe record rate 0.50"),
            "不达标：续 40 分钟"
        );
        let end = until + 40 * MIN;
        assert!(judge(&[], end + HOUR).is_some(), "没有试拉结果：待验证");
        assert!(
            judge(&[("probe:r2", 1.0)], end + HOUR).is_some(),
            "别的房间的试拉不算"
        );
        assert_eq!(probe(end + HOUR).as_deref(), Some("r"), "待验证时继续试拉");
        assert_eq!(judge(&[("probe:r", 1.0)], end + HOUR), None, "达标：解除");
        assert!(limited(&app, "a").is_none());

        // 房间不在别处录就没有验证条件。
        limit_room(&app, "r", "a", t, until);
        db::unassign(&app.db(), "r").unwrap();
        assert_eq!(probe(until - MIN), None);
    }

    #[test]
    fn judges_limits_from_either_signal_and_rechecks_upload_speed_when_the_hold_ends() {
        let (app, _) = test_app();
        let t = 100 * MIN;
        // 倍率正常、上传空闲（传得过来）：不标记。
        app.judge("a", &status(&[("r", 0.95)], 1000.0, Some(500.0), 0.3), t)
            .unwrap();
        assert_eq!(limited(&app, "a"), None);
        // 一直在传、只比产出略慢：刚好传得过来，不标记。
        app.judge("a", &status(&[], 1000.0, Some(950.0), 0.95), t)
            .unwrap();
        assert_eq!(limited(&app, "a"), None);
        // 一直在传还是慢于产出：标记，门槛是产出速度。
        app.judge("a", &status(&[], 1000.0, Some(500.0), 0.95), t)
            .unwrap();
        let l = limited(&app, "a").unwrap();
        assert_eq!((l.until, l.need_bps), (t + 20 * MIN, 1000.0));
        assert!(l.reason.starts_with("upload"), "{}", l.reason);
        // 录制倍率低：门槛按倍率还原成码率。
        app.judge("b", &status(&[("r", 0.5)], 400.0, None, 0.0), t)
            .unwrap();
        let l = limited(&app, "b").unwrap();
        assert_eq!(l.need_bps, 800.0);
        assert!(l.reason.starts_with("record"), "{}", l.reason);

        // 标记期间什么都不变，哪怕上传已经很快。
        app.judge("a", &status(&[], 0.0, Some(5000.0), 0.5), t + 10 * MIN)
            .unwrap();
        assert!(limited(&app, "a").is_some());
        // 到期仍慢：依次续 40、60 分钟，封顶 60；之后达标：解除。
        let slow = status(&[], 0.0, Some(600.0), 1.0);
        app.judge("a", &slow, t + 20 * MIN).unwrap();
        assert_eq!(limited(&app, "a").unwrap().until, t + 60 * MIN);
        app.judge("a", &slow, t + 60 * MIN).unwrap();
        assert_eq!(limited(&app, "a").unwrap().until, t + 120 * MIN);
        app.judge("a", &slow, t + 120 * MIN).unwrap();
        assert_eq!(limited(&app, "a").unwrap().until, t + 180 * MIN);
        app.judge("a", &status(&[], 0.0, Some(1200.0), 1.0), t + 180 * MIN)
            .unwrap();
        assert_eq!(limited(&app, "a"), None);
        // 解除后 1 小时内又受限：接着最高一档，60 分钟。
        let upload = status(&[], 1000.0, Some(500.0), 0.95);
        app.judge("a", &upload, t + 200 * MIN).unwrap();
        assert_eq!(limited(&app, "a").unwrap().until, t + 260 * MIN);
        // 没有上传样本（没有积压）：按时解除。
        app.judge("b", &status(&[], 0.0, None, 0.0), t + 20 * MIN)
            .unwrap();
        assert_eq!(limited(&app, "b"), None);
        // 只标过 20 分钟就解除，1 小时内再受限续到 40 分钟；稳定满 1 小时后再受限，从 20 分钟起。
        app.judge("b", &upload, t + 30 * MIN).unwrap();
        assert_eq!(limited(&app, "b").unwrap().until, t + 70 * MIN);
        app.judge("b", &status(&[], 0.0, None, 0.0), t + 70 * MIN)
            .unwrap();
        app.judge("b", &upload, t + 130 * MIN).unwrap();
        assert_eq!(limited(&app, "b").unwrap().until, t + 150 * MIN);
    }

    #[test]
    fn rolling_update_drains_one_agent_at_a_time_and_pauses_on_a_failed_commit() {
        let (mut app, _) = test_app();
        Arc::get_mut(&mut app).unwrap().commit = "new".into();
        let room = &db_room(&app, "r");
        let live = || Some(Some("s".to_string()));
        let at = |app: &App, agent: &str, t: i64, commit: &str| {
            let status = Status {
                commit: commit.into(),
                ..Default::default()
            };
            db::touch_agent(&app.db(), agent, t, &status).unwrap();
        };
        let wait = |d: Drain| match d {
            Drain::Wait { reason } => reason,
            other => panic!("expected wait, got {other:?}"),
        };
        let t = 100 * MIN;
        at(&app, "a", t, "old");
        at(&app, "b", t, "old");
        assert!(app.schedule(room, live(), t).unwrap());
        assert_eq!(assigned(&app, "r").as_deref(), Some("a"));

        assert!(wait(app.drain("a", "newer", t).unwrap()).contains("controller is at new"));
        assert!(wait(app.drain("a", "", t).unwrap()).contains("no commit"));
        assert!(wait(app.drain("a", "new", t).unwrap()).contains("still recording r"));
        assert!(wait(app.drain("b", "new", t).unwrap()).contains("a is draining"));

        // 排空中的 a 按受限同一套流程把 r 交给 b。
        assert!(app.schedule(room, live(), t).unwrap());
        assert_eq!(db::handoff(&app.db(), "r").unwrap().unwrap().to, "b");
        recording(&app, "b", "r");
        let started = db::handoff(&app.db(), "r")
            .unwrap()
            .unwrap()
            .started_at
            .unwrap();
        at(&app, "a", started, "old");
        at(&app, "b", started, "old");
        assert!(
            app.schedule(room, live(), started + HANDOFF_OVERLAP_MS)
                .unwrap()
        );
        assert_eq!(assigned(&app, "r").as_deref(), Some("b"));

        let t = started + HANDOFF_OVERLAP_MS;
        assert_eq!(app.drain("a", "new", t).unwrap(), Drain::Go);
        assert_eq!(
            app.drain("a", "new", t + 1).unwrap(),
            Drain::Go,
            "再问还是 go"
        );
        // 放行那一刻旧进程还在路上的心跳不算失败。
        at(&app, "a", t + 1000, "old");
        app.settle_drains(t + 1000).unwrap();
        assert!(db::drain(&app.db(), "a").unwrap().is_some());
        at(&app, "a", t + MIN, "new");
        app.settle_drains(t + MIN).unwrap();
        assert_eq!(db::drain(&app.db(), "a").unwrap(), None);
        assert_eq!(app.drain("a", "new", t + MIN).unwrap(), Drain::Done);

        // b 名下有 r，先排空：r 交回 a。这里直接撤掉分配，看失败判定。
        db::unassign(&app.db(), "r").unwrap();
        assert_eq!(app.drain("b", "new", t + MIN).unwrap(), Drain::Go);
        at(&app, "b", t + MIN + DRAIN_STALE_MS, "old");
        app.settle_drains(t + MIN + DRAIN_STALE_MS).unwrap();
        assert_eq!(db::drain(&app.db(), "b").unwrap(), None);
        assert!(wait(app.drain("b", "new", t + 2 * MIN).unwrap()).contains("failed"));

        // 失联不报心跳：放行 10 分钟后同样判失败。
        let c = db::failed_commits(&app.db(), "b").unwrap();
        assert_eq!(c, vec![("new".to_string(), t + MIN + DRAIN_STALE_MS)]);
        db::start_drain(&app.db(), "c", "x", t).unwrap();
        db::release_drain(&app.db(), "c", t).unwrap();
        app.settle_drains(t + DRAIN_CONFIRM_MS - 1).unwrap();
        assert!(db::drain(&app.db(), "c").unwrap().is_some());
        app.settle_drains(t + DRAIN_CONFIRM_MS).unwrap();
        assert_eq!(db::drain(&app.db(), "c").unwrap(), None);
        assert!(db::commit_failed(&app.db(), "x").unwrap());
    }

    /// 稿件的阶段，按场次键查。
    fn phase(app: &App, session: &str) -> String {
        app.db()
            .query_row(
                "SELECT phase FROM archives WHERE session = ?1",
                [session],
                |r| r.get(0),
            )
            .unwrap()
    }

    /// 2026-10-25 线上：a 录着 s1 时主播重开成 s2，交接后 b 按 s2 开录，a 停录不报 s1 结束。
    /// 在下播宽限内，b 的分P并回 s1 那份稿件（场次键改成 s2），总控不再替它结束。
    #[test]
    fn handoff_across_sessions_within_grace_continues_the_same_archive() {
        let (app, _) = test_app();
        let t = 100 * MIN;
        for ev in [opened("p1", "s1", t), closed("p1", "s1", t), uploaded("p1")] {
            db::apply(&app.db(), "a", &ev).unwrap();
        }
        db::apply(&app.db(), "b", &opened("p2", "s2", t)).unwrap();
        let now_live = Some(Some("s2".to_string()));
        assert_eq!(
            app.close_orphans("r", now_live.as_ref(), t + ORPHAN_AFTER_MS)
                .unwrap(),
            0
        );
        assert_eq!(phase(&app, "s2"), db::RECORDING);
        assert_eq!(db::recording_archives(&app.db(), "r").unwrap().len(), 1);
    }

    /// 2026-11-10 线上：小虞姬断流 11 分钟后重开播，抖音换了场次键。总控 10 分钟替 agent
    /// 结束了稿件并立刻投稿，重开的一场另投一份。现在：期限（最后收尾 + 房间宽限）内不投，
    /// 重开的分P并回原稿件；超过宽限再开播才是新稿件。
    #[test]
    fn reconnect_within_room_grace_joins_the_ended_archive_and_holds_its_submit() {
        let (app, _) = test_app();
        app.db()
            .execute("UPDATE rooms SET grace = 1800 WHERE name = 'r'", [])
            .unwrap();
        let t = 100 * MIN;
        let offline = Some(None);
        for ev in [
            opened("p1", "s1", t),
            closed("p1", "s1", t + 10 * MIN),
            uploaded("p1"),
        ] {
            db::apply(&app.db(), "a", &ev).unwrap();
        }
        app.close_orphans("r", offline.as_ref(), t + 10 * MIN)
            .unwrap();
        assert_eq!(
            app.close_orphans("r", offline.as_ref(), t + 20 * MIN)
                .unwrap(),
            1
        );
        assert!(db::ready(&app.db(), t + 21 * MIN).unwrap().is_empty());

        db::apply(&app.db(), "a", &opened("p2", "s2", t + 21 * MIN)).unwrap();
        assert_eq!(phase(&app, "s2"), db::RECORDING);
        for ev in [
            closed("p2", "s2", t + 50 * MIN),
            uploaded("p2"),
            ended("s2", t + 50 * MIN),
        ] {
            db::apply(&app.db(), "a", &ev).unwrap();
        }
        assert!(db::ready(&app.db(), t + 80 * MIN - 1).unwrap().is_empty());
        assert_eq!(db::ready(&app.db(), t + 80 * MIN).unwrap(), [1]);
        assert_eq!(db::archive(&app.db(), 1).unwrap().parts.len(), 2);

        db::apply(&app.db(), "a", &opened("p3", "s3", t + 80 * MIN + 1)).unwrap();
        assert_eq!(
            db::recording_archives(&app.db(), "r").unwrap(),
            [(2, "s3".into())]
        );
    }

    #[test]
    fn orphan_timer_waits_for_open_parts_ignores_failed_checks_and_resets_when_live_again() {
        let (app, _) = test_app();
        let t = 100 * MIN;
        let offline = Some(None);
        let s1 = Some(Some("s1".to_string()));
        db::apply(&app.db(), "a", &opened("p1", "s1", t)).unwrap();

        // 检测失败不起计时；下播后起计时，分P还在写就不结束。
        app.close_orphans("r", None, t).unwrap();
        app.close_orphans("r", offline.as_ref(), t + MIN).unwrap();
        assert_eq!(
            app.close_orphans("r", offline.as_ref(), t + MIN + ORPHAN_AFTER_MS)
                .unwrap(),
            0
        );
        // 又在播同一场：计时清零。
        app.close_orphans("r", s1.as_ref(), t + 20 * MIN).unwrap();
        db::apply(&app.db(), "a", &closed("p1", "s1", t + 20 * MIN)).unwrap();
        app.close_orphans("r", offline.as_ref(), t + 21 * MIN)
            .unwrap();
        assert_eq!(
            app.close_orphans("r", offline.as_ref(), t + 30 * MIN)
                .unwrap(),
            0
        );
        assert_eq!(
            app.close_orphans("r", offline.as_ref(), t + 31 * MIN)
                .unwrap(),
            1
        );
        assert_eq!(phase(&app, "s1"), db::ENDED);
    }

    /// 控制台结束本场：撤掉分配让 agent 停录，同一场不再分配，下一场照常。
    #[test]
    fn ended_from_console_stops_recording_and_skips_only_that_session() {
        let (app, _) = test_app();
        let room = &db_room(&app, "r");
        let t = 100 * MIN;
        seen(&app, "a", t, false);
        assert!(app.schedule(room, Some(Some("s1".into())), t).unwrap());
        db::apply(&app.db(), "a", &opened("p1", "s1", t)).unwrap();
        assert_eq!(db::end_room(&app.db(), "r", t + 1).unwrap(), 1);
        assert_eq!(assigned(&app, "r"), None);
        assert!(
            !app.schedule(room, Some(Some("s1".into())), t + MIN)
                .unwrap()
        );
        assert_eq!(assigned(&app, "r"), None);
        seen(&app, "a", t + 2 * MIN, false);
        assert!(
            app.schedule(room, Some(Some("s2".into())), t + 2 * MIN)
                .unwrap()
        );
        assert_eq!(assigned(&app, "r").as_deref(), Some("a"));
    }

    /// 丢弃的稿件：没传的分P拿不到上传票据（410），不投稿。
    #[tokio::test]
    async fn discarded_archive_gets_no_ticket_and_is_never_submitted() {
        let (app, fake) = test_app();
        db::apply_all(
            &mut app.db(),
            "a",
            &[
                opened("p1", "s", 1),
                closed("p1", "s", 2),
                uploaded("p1"),
                opened("p2", "s", 3),
                closed("p2", "s", 4),
                ended("s", 5),
            ],
        )
        .unwrap();
        let id = db::pending_archives(&app.db()).unwrap()[0].id;
        assert!(db::discard_archive(&app.db(), id).unwrap());
        assert!(!db::discard_archive(&app.db(), id).unwrap(), "只丢一次");
        let req = TicketRequest {
            room: "r".into(),
            part_id: "p2".into(),
            file_name: "f.flv".into(),
            size: 1,
            line: "tx".into(),
        };
        let mut h = HeaderMap::new();
        h.insert("authorization", "Bearer ta".parse().unwrap());
        let err = super::ticket(State(app.clone()), h, Json(req))
            .await
            .unwrap_err();
        assert_eq!(err, (StatusCode::GONE, lubic_proto::DISCARDED.into()));
        db::apply(&app.db(), "a", &uploaded("p2")).unwrap();
        app.submit_ready().await.unwrap();
        assert!(fake.submitted.lock().unwrap().is_empty());
        assert!(db::pending_archives(&app.db()).unwrap().is_empty());
    }

    /// 交接在旧机心跳里完成，用的是分配里记的场次：要随检测更新，不能停在第一次分配时的那场。
    #[test]
    fn assignment_follows_the_detected_session() {
        let (app, _) = test_app();
        let room = &db_room(&app, "r");
        let t = 100 * MIN;
        seen(&app, "a", t, false);
        assert!(app.schedule(room, Some(Some("s1".into())), t).unwrap());
        app.schedule(room, Some(Some("s2".into())), t + MIN)
            .unwrap();
        let session: String = app
            .db()
            .query_row(
                "SELECT session FROM assignments WHERE room = 'r'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(session, "s2");
    }

    #[test]
    fn draining_agent_is_picked_before_a_limited_one_and_only_as_a_last_resort() {
        let (app, _) = test_app();
        let room = &db_room(&app, "r");
        let t = 100 * MIN;
        seen(&app, "a", t, false);
        seen(&app, "b", t, false);
        db::start_drain(&app.db(), "a", "new", t).unwrap();
        assert_eq!(
            app.pick(room, &[], t, Pick::Handoff).unwrap().as_deref(),
            Some("b")
        );
        let limit = db::Limit {
            since: t,
            until: t + MIN,
            reason: "upload".into(),
            need_bps: 1.0,
        };
        db::set_limit(&app.db(), "b", &limit).unwrap();
        assert!(app.schedule(room, Some(Some("s".into())), t).unwrap());
        assert_eq!(assigned(&app, "r").as_deref(), Some("a"));
    }

    #[test]
    fn limited_agent_hands_off_after_the_new_one_writes_a_keyframe() {
        let (app, _) = test_app();
        let room = &db_room(&app, "r");
        let live = || Some(Some("s".to_string()));
        let t = 100 * MIN;
        seen(&app, "a", t, false);
        seen(&app, "b", t, false);
        assert!(app.schedule(room, live(), t).unwrap());
        assert_eq!(assigned(&app, "r").as_deref(), Some("a"));
        app.judge("a", &status(&[("r", 0.5)], 400.0, None, 0.0), t)
            .unwrap();

        // 发起交接：两台都录，分配还在 a。
        assert!(app.schedule(room, live(), t + MIN / 2).unwrap());
        assert_eq!(
            (rooms_of(&app, "a"), rooms_of(&app, "b")),
            (vec!["r".into()], vec!["r".into()])
        );
        assert!(
            !app.schedule(room, live(), t + MIN).unwrap(),
            "b 还没写出关键帧"
        );
        // 不是交接目标的 agent 报 Recording 不算数。
        recording(&app, "c", "r");
        assert_eq!(
            db::handoff(&app.db(), "r").unwrap().unwrap().started_at,
            None
        );
        recording(&app, "b", "r");
        let started = db::handoff(&app.db(), "r")
            .unwrap()
            .unwrap()
            .started_at
            .unwrap();
        // started_at 是总控的真实时钟，心跳跟上它。
        seen(&app, "a", started, false);
        seen(&app, "b", started, false);
        // 重叠等待内不动。
        assert!(
            !app.schedule(room, live(), started + HANDOFF_OVERLAP_MS - 1)
                .unwrap()
        );
        // 旧机落后：b 报第一个关键帧时落后 0.5 秒，起点约是直播的 started − 500。
        // a 在 at 时刻落后 lag，录到 at − lag；要过了起点再 10 秒才收尾。
        let old_at = |lag: i64, at: i64| {
            let status = Status {
                record_lags: vec![("r".into(), lag)],
                ..Default::default()
            };
            db::touch_agent(&app.db(), "a", at, &status).unwrap();
            seen(&app, "b", at, false);
            app.schedule(room, live(), at).unwrap()
        };
        let at = started + HANDOFF_OVERLAP_MS;
        // at − lag ≥ started − 500 + 10_000，即 at 时落后不超过 500 毫秒才收尾。
        assert!(!old_at(501, at), "落后太多，还没录到起点之后 10 秒");
        assert!(old_at(501, at + 1));
        assert_eq!(assigned(&app, "r").as_deref(), Some("b"));
        assert!(rooms_of(&app, "a").is_empty());
        assert_eq!(db::handoff(&app.db(), "r").unwrap(), None);

        // 标记期间与解除后都不迁回（load-balance.md §2）。
        let later = started + 2 * MIN;
        seen(&app, "a", later, false);
        seen(&app, "b", later, false);
        assert!(!app.schedule(room, live(), later).unwrap());
        db::clear_limit(&app.db(), "a").unwrap();
        assert!(!app.schedule(room, live(), later + MIN).unwrap());
        assert_eq!(assigned(&app, "r").as_deref(), Some("b"));
    }

    #[test]
    fn preupload_gate_spaces_requests_and_cools_down_after_601_with_one_probe() {
        let mut g = Gate::default();
        let t = 1_000_000;
        // 最小间隔：同一时刻来三个，依次等 0、2、4 秒。
        assert_eq!(
            [g.admit(t), g.admit(t), g.admit(t)],
            [Ok(0), Ok(2_000), Ok(4_000)]
        );
        // 撞 601：冷却 60 秒，期间一律拒绝；同批另一个也撞上，不重复加码。
        assert_eq!(g.done(t + 5_000, true), Some(t + 65_000));
        assert_eq!(g.done(t + 6_000, true), Some(t + 65_000));
        assert_eq!(g.admit(t + 30_000), Err(t + 65_000));
        // 冷却结束只放一个试探，它回来之前其余拒绝。
        assert_eq!(g.admit(t + 65_000), Ok(0));
        assert!(g.admit(t + 70_000).is_err());
        // 试探又撞上：冷却翻倍。
        assert_eq!(g.done(t + 66_000, true), Some(t + 186_000));
        // 再试探成功：恢复正常。
        assert_eq!(g.admit(t + 186_000), Ok(0));
        assert_eq!(g.done(t + 187_000, false), None);
        assert_eq!(g.admit(t + 190_000), Ok(0));
        assert_eq!(g.admit(t + 190_000), Ok(2_000));
        // 试探请求被取消：守卫放开试探位。
        g.done(t + 200_000, true);
        let gates = Mutex::new(HashMap::from([(1, g)]));
        let admit = |at| gates.lock().unwrap().get_mut(&1).unwrap().admit(at);
        assert_eq!(admit(t + 1_000_000), Ok(0));
        drop(InFlight(&gates, 1));
        assert_eq!(admit(t + 1_000_001), Ok(1_999));
        // 冷却封顶 30 分钟。
        let mut g = Gate::default();
        for _ in 0..10 {
            g.until = 0;
            g.done(t, true);
        }
        assert_eq!(g.until - t, COOLDOWN_MAX_MS);
        assert!(rate_limited(
            r#"bilibili: preupload rejected: {"OK":0,"code":601,"info":"您上传视频过快"}"#
        ));
        assert!(rate_limited(
            r#"bilibili: preupload rejected: {"OK":0,"code":406,"info":"您上传视频过快，请您稍作休息后再继续"}"#
        ));
        assert!(!rate_limited("bilibili: preupload rejected: {\"OK\":0}"));
    }

    #[test]
    fn old_agent_catch_up_compares_live_moments_by_lag() {
        let (app, _) = test_app();
        let room = &db_room(&app, "r");
        // 新机在总控时刻 100_000 报了第一个关键帧，当时落后直播 2 秒：起点约是直播的 98_000，
        // 旧机要录到 108_000 才收尾。
        let h = db::Handoff {
            from: "a".into(),
            to: "b".into(),
            requested_at: 0,
            started_at: Some(100_000),
            start_lag: Some(2_000),
            start_live: None,
        };
        // 旧机在 `seen` 时刻心跳，报了 lag。
        let report = |seen: i64, lag: Option<i64>| {
            let status = Status {
                record_lags: lag.map(|l| ("r".into(), l)).into_iter().collect(),
                ..Default::default()
            };
            db::touch_agent(&app.db(), "a", seen, &status).unwrap();
        };
        // 130_000 时落后 40 秒，只录到 90_000。
        report(130_000, Some(40_000));
        assert!(!app.caught_up(room, &h).unwrap());
        // 150_000 时落后 42 秒，录到 108_000：正好够。
        report(150_000, Some(42_000));
        assert!(app.caught_up(room, &h).unwrap());
        report(150_000, Some(42_001));
        assert!(!app.caught_up(room, &h).unwrap());
        // 新机没报 lag：按 0 算，更保守。
        let mut no_lag = h.clone();
        no_lag.start_lag = None;
        report(150_000, Some(40_001));
        assert!(!app.caught_up(room, &no_lag).unwrap());
        // 旧机没在拉流（没报 lag）、或从没来过：它已不产出，放行。
        report(150_000, None);
        assert!(app.caught_up(room, &h).unwrap());
        let mut stranger = h.clone();
        stranger.from = "nobody".into();
        assert!(app.caught_up(room, &stranger).unwrap());
    }

    /// 两台都有推流时刻（码流 SEI）：按它判断，不受两台 CDN 节点延迟不同的影响（2026-10-24 线上按 lag 估算、
    /// 等检测周期，接缝重叠约 63 秒）。旧机每次心跳都推进，录过新机起点 5 秒就放行，不等 10 秒的保底重叠与检测周期。
    #[test]
    fn old_agent_stops_on_its_heartbeat_once_past_the_new_start_by_live_moments() {
        let (app, _) = test_app();
        let room = &db_room(&app, "r");
        let live = || Some(Some("s".to_string()));
        let t = now_ms();
        seen(&app, "a", t, false);
        seen(&app, "b", t, false);
        assert!(app.schedule(room, live(), t).unwrap());
        app.judge("a", &status(&[("r", 0.5)], 400.0, None, 0.0), t)
            .unwrap();
        assert!(app.schedule(room, live(), t).unwrap());
        // 新机写出的第一帧是直播的 START；它自己的 lag 估算偏得离谱也无所谓。
        const START: i64 = 1_792_839_450_000;
        let ev = AgentEvent::Recording {
            room: "r".into(),
            lag_ms: Some(-300_000),
            start_live_ms: Some(START),
        };
        db::apply(&app.db(), "b", &ev).unwrap();
        let h = db::handoff(&app.db(), "r").unwrap().unwrap();
        assert_eq!(h.start_live, Some(START));
        let started = h.started_at.unwrap();
        assert!(
            !app.finish_handoffs_from("b", started).unwrap(),
            "只推进从它交接出去的"
        );
        // 旧机在 at 时刻心跳，写到的内容落后直播 live_lag，录到了直播的 at − live_lag。按 lag 估算它远没录到。
        let heartbeat = |at: i64, live_lag: Option<i64>| {
            let status = Status {
                record_lags: vec![("r".into(), 600_000)],
                record_live_lags: live_lag.map(|l| ("r".into(), l)).into_iter().collect(),
                ..Default::default()
            };
            db::touch_agent(&app.db(), "a", at, &status).unwrap();
            app.finish_handoffs_from("a", at).unwrap()
        };
        let at = started + 1_000;
        assert!(!heartbeat(
            at,
            Some(at - START - HANDOFF_LIVE_OVERLAP_MS + 1)
        ));
        // 旧机没有推流时刻：退回 lag 估算，它落后 10 分钟，不放行。
        assert!(!heartbeat(at, None));
        assert_eq!(assigned(&app, "r").as_deref(), Some("a"));
        assert!(heartbeat(at, Some(at - START - HANDOFF_LIVE_OVERLAP_MS)));
        assert_eq!(assigned(&app, "r").as_deref(), Some("b"));
        assert_eq!(db::handoff(&app.db(), "r").unwrap(), None);
        assert!(rooms_of(&app, "a").is_empty());
    }

    #[test]
    fn handoff_failures_keep_the_old_agent_or_promote_the_target() {
        let (app, _) = test_app();
        let room = &db_room(&app, "r");
        let live = || Some(Some("s".to_string()));
        let t = 100 * MIN;
        seen(&app, "a", t, false);
        seen(&app, "b", t, false);
        assign(&app, "r", "a", t);
        app.judge("a", &status(&[], 1000.0, Some(10.0), 1.0), t)
            .unwrap();
        assert!(app.schedule(room, live(), t).unwrap());

        // b 一直没写出关键帧：超时放弃它；列表里没别人了，留在 a，也不再找 b。
        seen(&app, "a", t + 3 * MIN, false);
        seen(&app, "b", t + 3 * MIN, false);
        assert!(app.schedule(room, live(), t + 3 * MIN).unwrap());
        assert_eq!(db::handoff(&app.db(), "r").unwrap(), None);
        assert!(rooms_of(&app, "b").is_empty());
        assert!(!app.schedule(room, live(), t + 3 * MIN + 1).unwrap());
        assert_eq!(assigned(&app, "r").as_deref(), Some("a"));

        // 换个房间：交接中旧机失联，直接交给目标，不另挑。
        let r2 = &db_room(&app, "r2"); // agents = ["c", "a"]
        assign(&app, "r2", "a", t);
        seen(&app, "c", t + 3 * MIN, false);
        assert!(app.schedule(r2, live(), t + 3 * MIN).unwrap());
        assert_eq!(db::handoff(&app.db(), "r2").unwrap().unwrap().to, "c");
        seen(&app, "c", t + 6 * MIN, false); // a 3 分钟没来
        assert!(app.schedule(r2, live(), t + 6 * MIN).unwrap());
        assert_eq!(assigned(&app, "r2").as_deref(), Some("c"));
        assert_eq!(db::handoff(&app.db(), "r2").unwrap(), None);
    }

    #[test]
    fn all_limited_still_records_and_slow_source_does_not_cascade() {
        let (app, _) = test_app();
        let room = &db_room(&app, "r");
        let live = || Some(Some("s".to_string()));
        let t = 100 * MIN;
        seen(&app, "a", t, false);
        seen(&app, "b", t, false);
        app.judge("a", &status(&[], 1000.0, Some(10.0), 1.0), t)
            .unwrap();
        app.judge("b", &status(&[], 1000.0, Some(10.0), 1.0), t)
            .unwrap();
        // 全部受限：仍按列表分给第一台在线的，不发起交接。
        assert!(app.schedule(room, live(), t).unwrap());
        assert_eq!(assigned(&app, "r").as_deref(), Some("a"));
        assert!(!app.schedule(room, live(), t + MIN).unwrap());

        // 源站慢：r 因倍率从 a 迁到 b 之后 1 小时内，b 报 r 倍率低不标记。
        let (app, _) = test_app();
        seen(&app, "a", t, false);
        seen(&app, "b", t, false);
        assign(&app, "r", "a", t);
        app.judge("a", &status(&[("r", 0.6)], 400.0, None, 0.0), t)
            .unwrap();
        assert!(app.schedule(&db_room(&app, "r"), live(), t).unwrap());
        app.judge("b", &status(&[("r", 0.6)], 400.0, None, 0.0), t + 5 * MIN)
            .unwrap();
        assert_eq!(limited(&app, "b"), None);
        // 别的房间倍率低照常标记。
        app.judge("b", &status(&[("r2", 0.6)], 400.0, None, 0.0), t + 5 * MIN)
            .unwrap();
        assert!(limited(&app, "b").is_some());
    }

    /// 刚迁来的房间在新机上又慢了：让原机交叉试拉。试拉达标就标记新机、解除原机，房间迁回；
    /// 试拉也慢判源站慢，10 分钟内不再试。
    #[test]
    fn cross_checks_a_room_that_is_slow_again_right_after_a_move() {
        let live = || Some(Some("s".to_string()));
        let t = 100 * MIN;
        let setup = || {
            let (app, _) = test_app();
            seen(&app, "a", t, false);
            seen(&app, "b", t, false);
            assign(&app, "r", "a", t);
            app.judge("a", &status(&[("r", 0.6)], 400.0, None, 0.0), t)
                .unwrap();
            // 标记拉长到 1 小时，免得到期前试拉混进来。
            let l = limited(&app, "a").unwrap();
            let l = db::Limit {
                until: t + 60 * MIN,
                ..l
            };
            db::set_limit(&app.db(), "a", &l).unwrap();
            assert!(app.schedule(&db_room(&app, "r"), live(), t).unwrap());
            // 交接完成。
            db::delete_handoff(&app.db(), "r").unwrap();
            assign(&app, "r", "b", t);
            // b 又慢：先不标记，让 a 试拉 r。
            let slow = status(&[("r", 0.2)], 400.0, None, 0.0);
            seen(&app, "a", t + 5 * MIN, false);
            app.judge("b", &slow, t + 5 * MIN).unwrap();
            assert_eq!(limited(&app, "b"), None);
            let probe = app.probe_for("a", t + 5 * MIN).unwrap().map(|r| r.name);
            assert_eq!(probe.as_deref(), Some("r"));
            (app, slow)
        };

        // 试拉达标：b 受限，a 解除，r 交接回 a，试拉停。
        let (app, slow) = setup();
        let at = t + 8 * MIN;
        db::touch_agent(
            &app.db(),
            "a",
            at,
            &status(&[("probe:r", 1.0)], 0.0, None, 0.0),
        )
        .unwrap();
        seen(&app, "b", at, false);
        app.judge("b", &slow, at).unwrap();
        assert!(limited(&app, "b").is_some());
        assert_eq!(limited(&app, "a"), None);
        assert_eq!(app.probe_for("a", at).unwrap(), None);
        assert!(app.schedule(&db_room(&app, "r"), live(), at).unwrap());
        assert_eq!(db::handoff(&app.db(), "r").unwrap().unwrap().to, "a");

        // 试拉也慢：源站慢，不标记；10 分钟内不再试拉，之后仍慢再试。
        let (app, slow) = setup();
        let at = t + 8 * MIN;
        db::touch_agent(
            &app.db(),
            "a",
            at,
            &status(&[("probe:r", 0.3)], 0.0, None, 0.0),
        )
        .unwrap();
        app.judge("b", &slow, at).unwrap();
        assert_eq!(limited(&app, "b"), None);
        assert!(limited(&app, "a").is_some(), "a 的标记不动");
        assert_eq!(app.probe_for("a", at).unwrap(), None);
        app.judge("b", &slow, at + 9 * MIN).unwrap();
        assert_eq!(app.probe_for("a", at + 9 * MIN).unwrap(), None);
        seen(&app, "a", at + 10 * MIN, false);
        app.judge("b", &slow, at + 10 * MIN).unwrap();
        let probe = app.probe_for("a", at + 10 * MIN).unwrap().map(|r| r.name);
        assert_eq!(probe.as_deref(), Some("r"));

        // 迟迟没有试拉结果：5 分钟后放弃，不标记。
        let (app, slow) = setup();
        seen(&app, "a", t + 7 * MIN, false);
        app.judge("b", &slow, t + 7 * MIN).unwrap();
        assert!(app.probe_for("a", t + 7 * MIN).unwrap().is_some(), "还在等");
        seen(&app, "a", t + 10 * MIN, false);
        app.judge("b", &slow, t + 10 * MIN).unwrap();
        assert_eq!(limited(&app, "b"), None);
        assert_eq!(app.probe_for("a", t + 10 * MIN).unwrap(), None);
    }

    #[tokio::test]
    async fn account_partition_mode_selects_the_official_list() {
        let (app, fake) = test_app();
        let old = app.types(1).await.unwrap();
        assert_eq!(old["type_mode"], 0);
        assert_eq!(old["typelist"][0]["id"], 171);
        assert_eq!(old["human_type_list"], serde_json::json!([]));
        *fake.type_mode.lock().unwrap() = 2;
        let new = app.types(1).await.unwrap();
        assert_eq!(new["type_mode"], 2);
        assert_eq!(new["human_type_list"][0]["id"], 1008);
        assert_eq!(new["typelist"][0]["id"], 171);
    }

    #[tokio::test]
    async fn new_partition_uses_web_and_respects_its_cooldown() {
        let (app, fake) = test_app();
        let mut tpl = db::template(&app.db(), 1).unwrap().unwrap();
        tpl.human_type2 = Some(1008);
        tpl.tid = 65;
        db::update_template(&app.db(), &tpl).unwrap();
        db::apply_all(
            &mut app.db(),
            "a",
            &[
                opened("p1", "s", 1),
                closed("p1", "s", 2),
                uploaded("p1"),
                ended("s", 3),
            ],
        )
        .unwrap();
        app.gates.lock().unwrap().entry(1).or_default().vias[Via::Web as usize].0 = now_ms() + HOUR;
        app.submit_ready().await.unwrap();
        assert!(fake.vias.lock().unwrap().is_empty());
        app.gates.lock().unwrap().get_mut(&1).unwrap().vias[Via::Web as usize].0 = 0;
        *fake.next.lock().unwrap() = Some(SubmitError::Rejected {
            code: SUBMIT_TOO_FREQUENT,
            message: "slow down".into(),
        });
        app.submit_ready().await.unwrap();
        assert_eq!(*fake.vias.lock().unwrap(), [Via::Web]);
        skip_submit_spacing(&app);
        app.retry.lock().unwrap().clear();
        app.submit_ready().await.unwrap();
        assert_eq!(fake.vias.lock().unwrap().len(), 1, "Web 停投期间不改投 APP");
        app.gates.lock().unwrap().get_mut(&1).unwrap().vias[Via::Web as usize].0 = 0;
        app.submit_ready().await.unwrap();
        let submitted = fake.submitted.lock().unwrap();
        assert_eq!(submitted[0].human_type2, Some(1008));
        assert_eq!(submitted[0].tid, 65);
    }

    #[tokio::test]
    async fn submits_once_when_every_part_is_uploaded_and_never_retries_unknown_outcome() {
        let (app, fake) = test_app();
        let evs = [
            opened("p1", "s", 1),
            closed("p1", "s", 2),
            uploaded("p1"),
            closed("p2", "s", 30),
            uploaded("p2"),
            ended("s", 40),
        ];
        db::apply_all(&mut app.db(), "a", &evs).unwrap();
        app.submit_ready().await.unwrap();
        app.submit_ready().await.unwrap();
        let submitted = fake.submitted.lock().unwrap().clone();
        assert_eq!(submitted.len(), 1);
        let s = &submitted[0];
        assert!(s.title.starts_with("r-直播-19"), "{}", s.title);
        assert_eq!(s.source, "https://live.bilibili.com/6");
        assert_eq!(s.cover, "https://cover/1", "自动封面");
        let names: Vec<_> = s.videos.iter().map(|v| (&*v.title, &*v.filename)).collect();
        assert_eq!(names, [("p1", "n-p1"), ("p2", "n-p2")]);

        // 明确被拒：回到 ended，退避中不重投。
        db::apply_all(
            &mut app.db(),
            "a",
            &[
                closed("q1", "s2", HOUR + 10),
                uploaded("q1"),
                ended("s2", HOUR + 11),
            ],
        )
        .unwrap();
        // 69800「网络繁忙」是明确拒绝、没有生成稿件：退避后重投，不交给人。
        *fake.next.lock().unwrap() = Some(SubmitError::Rejected {
            code: 69800,
            message: "网络繁忙 请稍后再试".into(),
        });
        skip_submit_spacing(&app);
        app.submit_ready().await.unwrap();
        app.submit_ready().await.unwrap();
        assert_eq!(db::ready(&app.db(), i64::MAX).unwrap(), [2]);
        assert_eq!(fake.submitted.lock().unwrap().len(), 1);
        // 退避到期后结果未知：交给人，不再自动提交。
        app.retry.lock().unwrap().clear();
        skip_submit_spacing(&app);
        *fake.next.lock().unwrap() = Some(SubmitError::Unknown("timeout".into()));
        app.submit_ready().await.unwrap();
        app.submit_ready().await.unwrap();
        assert!(db::ready(&app.db(), i64::MAX).unwrap().is_empty());
        assert_eq!(fake.submitted.lock().unwrap().len(), 1);
    }

    /// 与 `deploy/lubic-update.sh` 里 openssl 算的签名一致。
    #[test]
    fn dingtalk_sign_matches_openssl() {
        assert_eq!(
            dingtalk_sign(1_795_157_000_000, "SECtest"),
            "k5Ncc%2F3pqbbHq4XjoA1uP4%2F5ze8WsOZAku6vxwdTSrQ%3D"
        );
    }

    /// 21588「视频时长不足 1 秒」：某个分P太短，重投多少次都一样，停下交给人。
    #[tokio::test]
    async fn part_too_short_stops_without_retry() {
        let (app, fake) = test_app();
        db::apply_all(
            &mut app.db(),
            "a",
            &[closed("p1", "s", 10), uploaded("p1"), ended("s", 11)],
        )
        .unwrap();
        *fake.next.lock().unwrap() = Some(SubmitError::Rejected {
            code: PART_TOO_SHORT,
            message: "该视频时长不足 1 秒".into(),
        });
        app.submit_ready().await.unwrap();
        let phase: String = app
            .db()
            .query_row("SELECT phase FROM archives WHERE id = 1", [], |r| r.get(0))
            .unwrap();
        assert_eq!(phase, db::ATTENTION);
        assert!(app.retry.lock().unwrap().is_empty(), "不进退避");
        assert!(db::ready(&app.db(), i64::MAX).unwrap().is_empty());
        skip_submit_spacing(&app);
        app.submit_ready().await.unwrap();
        assert_eq!(fake.vias.lock().unwrap().len(), 1, "不再自动投");
    }

    #[tokio::test]
    async fn same_account_submits_are_spaced_and_other_accounts_do_not_wait() {
        let (app, fakes, _) = test_apps(config());
        // 各场相隔一小时，超出下播宽限，不会并成一份。
        let archive = |s: &'static str, p: &'static str| {
            let t = s[1..].parse::<i64>().unwrap() * HOUR;
            [closed(p, s, t + 10), uploaded(p), ended(s, t + 11)]
        };
        db::apply_all(&mut app.db(), "a", &archive("s1", "p1")).unwrap();
        // 之后开的稿件钉住账号 2。
        let login = serde_json::json!({"token_info": {"mid": 2}});
        assert_eq!(
            db::save_account(&app.db(), "acc2", 2, &login, 0).unwrap(),
            2
        );
        let mut tpl = db::template(&app.db(), 1).unwrap().unwrap();
        tpl.account_id = 2;
        db::update_template(&app.db(), &tpl).unwrap();
        db::apply_all(&mut app.db(), "a", &archive("s2", "p2")).unwrap();
        db::apply_all(&mut app.db(), "a", &archive("s3", "p3")).unwrap();

        let before = now_ms();
        app.submit_ready().await.unwrap();
        app.submit_ready().await.unwrap();
        let count = |mid| fakes.get(mid).submitted.lock().unwrap().len();
        assert_eq!(
            (count(1), count(2)),
            (1, 1),
            "两个账号各投一份，账号 2 的第二份要等"
        );
        assert_eq!(db::ready(&app.db(), i64::MAX).unwrap(), [3]);
        let after = app.gates.lock().unwrap()[&2].submit_after;
        assert!(
            after >= before + SUBMIT_INTERVAL_MS,
            "间隔从上次投稿返回算起"
        );
        assert!(after <= now_ms() + SUBMIT_INTERVAL_MS + SUBMIT_JITTER_MS);

        app.gates.lock().unwrap().get_mut(&2).unwrap().submit_after = now_ms() - 1;
        app.submit_ready().await.unwrap();
        assert_eq!(count(2), 2, "间隔到了接着投");
    }

    #[tokio::test]
    async fn submitted_archive_records_when_it_went_out() {
        let (app, _) = test_app();
        db::apply_all(
            &mut app.db(),
            "a",
            &[closed("p1", "s", 10), uploaded("p1"), ended("s", 11)],
        )
        .unwrap();
        let before = now_ms();
        app.submit_ready().await.unwrap();
        let after = now_ms();
        let at: Option<i64> = app
            .db()
            .query_row("SELECT submitted_at FROM archives WHERE id = 1", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert!(at.is_some_and(|t| (before..=after).contains(&t)));
    }

    #[tokio::test]
    async fn too_frequent_switches_endpoint_then_pauses_the_whole_account() {
        let (app, fake) = test_app();
        for (s, p) in [("s1", "p1"), ("s2", "p2")] {
            db::apply_all(
                &mut app.db(),
                "a",
                &[closed(p, s, 10), uploaded(p), ended(s, 11)],
            )
            .unwrap();
        }
        let too_frequent = || {
            *fake.next.lock().unwrap() = Some(SubmitError::Rejected {
                code: SUBMIT_TOO_FREQUENT,
                message: "投稿过于频繁".into(),
            })
        };
        let gate = || app.gates.lock().unwrap()[&1].clone();
        let reopen = || app.gates.lock().unwrap().get_mut(&1).unwrap().submit_after = 0;
        let vias = || fake.vias.lock().unwrap().clone();

        too_frequent();
        let before = now_ms();
        app.submit_ready().await.unwrap();
        assert_eq!(vias(), [Via::App], "先走 APP");
        assert!(
            gate().vias[0].0 >= before + SUBMIT_COOLDOWN_FIRST_MS,
            "APP 停用"
        );
        assert!(
            gate().submit_after < before + SUBMIT_COOLDOWN_FIRST_MS,
            "账号只隔普通间隔"
        );
        assert_eq!(gate().submit_strikes, 0);
        assert!(app.retry.lock().unwrap().is_empty(), "稿件本身不退避");

        reopen();
        too_frequent();
        let before = now_ms();
        app.submit_ready().await.unwrap();
        assert_eq!(vias(), [Via::App, Via::Web], "APP 停用时改走网页");
        assert!(
            gate().vias[1].0 >= before + SUBMIT_COOLDOWN_FIRST_MS,
            "网页停用"
        );
        // APP 是上一轮停的，先恢复；账号停到它恢复为止，不跟这一轮的墙钟比。
        assert_eq!(
            gate().submit_after,
            gate().vias[0].0,
            "都撞了整个账号停投，停到先恢复的接口"
        );
        assert_eq!(gate().submit_strikes, 1);
        app.submit_ready().await.unwrap();
        assert_eq!(vias().len(), 2, "停投期间另一份也不投");

        let reopen_all = || {
            let mut gates = app.gates.lock().unwrap();
            let g = gates.get_mut(&1).unwrap();
            g.submit_after = 0;
            g.vias[0].0 = 0;
            g.vias[1].0 = 0;
        };
        reopen_all();
        too_frequent();
        let before = now_ms();
        app.submit_ready().await.unwrap();
        assert_eq!(vias()[2], Via::App, "APP 先恢复就先用 APP");
        assert!(
            gate().vias[0].0 >= before + 2 * SUBMIT_COOLDOWN_FIRST_MS,
            "连续撞上翻倍"
        );

        reopen_all();
        too_frequent();
        app.gates.lock().unwrap().get_mut(&1).unwrap().vias[0].1 = 9;
        app.submit_ready().await.unwrap();
        let until = gate().vias[0].0;
        assert!(until <= now_ms() + SUBMIT_COOLDOWN_MAX_MS, "封顶");
        assert!(until > now_ms() + SUBMIT_COOLDOWN_MAX_MS - 60_000);

        reopen();
        app.submit_ready().await.unwrap();
        assert_eq!(vias()[4], Via::Web);
        assert_eq!(fake.submitted.lock().unwrap().len(), 1, "网页接口照常投");
        assert_eq!(gate().vias[1].1, 0, "投成功的接口清零");
        assert!(gate().vias[0].0 > now_ms(), "APP 还在停用");
        assert!(
            gate().submit_after < now_ms() + SUBMIT_COOLDOWN_FIRST_MS,
            "账号恢复普通间隔"
        );
    }

    #[tokio::test]
    async fn adds_submitted_archive_to_the_room_season_with_retries() {
        let (app, fake) = test_app();
        let submit = |session: &'static str, part: &'static str| {
            let app = app.clone();
            async move {
                db::apply_all(
                    &mut app.db(),
                    "a",
                    &[
                        closed(part, session, 10),
                        uploaded(part),
                        ended(session, 11),
                    ],
                )
                .unwrap();
                skip_submit_spacing(&app);
                app.submit_ready().await.unwrap();
            }
        };
        // 房间没配合集：不加。
        submit("s0", "p0").await;
        assert!(db::season_pending(&app.db()).unwrap().is_empty());

        let mut room = db::room(&app.db(), "r").unwrap().unwrap();
        room.season_section_id = Some(11);
        db::save_room(&app.db(), &room).unwrap();
        submit("s1", "p1").await;
        app.add_to_seasons().await.unwrap();
        assert_eq!(*fake.seasons_added.lock().unwrap(), [(11, 1)]);
        assert!(db::season_pending(&app.db()).unwrap().is_empty());

        // 失败：待加入留在库里（重启后接着加），隔一会儿再试；第 5 次还失败就放弃。
        *fake.season_fails.lock().unwrap() = 5;
        submit("s2", "p2").await;
        for _ in 0..4 {
            app.add_to_seasons().await.unwrap();
            app.add_to_seasons().await.unwrap(); // 间隔未到，不试
            assert_eq!(db::season_pending(&app.db()).unwrap().len(), 1);
            app.season_retry
                .lock()
                .unwrap()
                .values_mut()
                .for_each(|r| r.1 = 0);
        }
        assert_eq!(*fake.season_fails.lock().unwrap(), 1);
        app.add_to_seasons().await.unwrap();
        assert!(db::season_pending(&app.db()).unwrap().is_empty());
        assert_eq!(fake.seasons_added.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn dry_run_does_not_add_to_season() {
        let dry = DryRun(Account::new(serde_json::json!({}), Box::new(|_| {})));
        assert!(dry.add_to_season(11, 1).await.is_ok());
    }

    #[tokio::test]
    async fn archive_without_uploaded_parts_is_dropped() {
        let (app, fake) = test_app();
        let lost = AgentEvent::Lost {
            part_id: "p1".into(),
        };
        db::apply_all(
            &mut app.db(),
            "a",
            &[opened("p1", "s", 1), lost, ended("s", 2)],
        )
        .unwrap();
        app.submit_ready().await.unwrap();
        assert!(fake.submitted.lock().unwrap().is_empty());
        assert!(db::ready(&app.db(), i64::MAX).unwrap().is_empty());
    }

    /// 进入试拉窗口：没回显试拉的长轮询立即返回并带上 `probe`，回显了同一房间就挂住。
    #[tokio::test]
    async fn http_watch_returns_the_probe_and_holds_once_echoed() {
        let (app, _) = test_app();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(axum::serve(listener, router(app.clone())).into_future());
        let now = now_ms();
        assign(&app, "r", "b", now);
        let limit = db::Limit {
            since: now - 59 * MIN,
            until: now + MIN,
            reason: "record rate 0.5".into(),
            need_bps: 0.0,
        };
        db::set_limit(&app.db(), "a", &limit).unwrap();
        let client = reqwest::Client::new();
        let watch = |probe: Option<&str>| {
            client
                .post(format!("{base}{}", lubic_proto::WATCH))
                .bearer_auth("ta")
                .json(&Watch {
                    probe: probe.map(Into::into),
                    ..Default::default()
                })
                .send()
        };
        let got: Assignments = tokio::time::timeout(Duration::from_secs(2), watch(None))
            .await
            .unwrap()
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(got.probe.map(|t| t.name).as_deref(), Some("r"));
        let held = tokio::spawn(watch(Some("r")));
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!held.is_finished(), "已在试拉，挂住");
        held.abort();
    }

    /// 起一个真的 HTTP 服务，走一遍 agent 会用到的三个接口。
    #[tokio::test]
    async fn http_watch_holds_until_assignment_changes_and_report_acks() {
        let (app, _) = test_app();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(axum::serve(listener, router(app.clone())).into_future());
        let client = reqwest::Client::new();
        let post = |path: &str, token: &str, body: Value| {
            client
                .post(format!("{base}{path}"))
                .bearer_auth(token)
                .json(&body)
                .send()
        };

        let bad = post(
            lubic_proto::WATCH,
            "nope",
            serde_json::to_value(Watch::default()).unwrap(),
        );
        assert_eq!(bad.await.unwrap().status(), StatusCode::UNAUTHORIZED);

        // 期望与现状一致（都为空）：挂住，分配一变就返回。
        let held = post(
            lubic_proto::WATCH,
            "ta",
            serde_json::to_value(Watch::default()).unwrap(),
        );
        let held = tokio::spawn(held);
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!held.is_finished());
        assert!(
            db::agent(&app.db(), "a").unwrap().is_some(),
            "watch is a heartbeat"
        );
        assign(&app, "r", "a", now_ms());
        app.changed.notify_waiters();
        let got: Assignments = tokio::time::timeout(Duration::from_secs(5), held)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(got.rooms, [db_room(&app, "r").task()]);

        let report = Report {
            events: vec![
                Envelope {
                    seq: 7,
                    event: opened("p1", "s", 1),
                },
                Envelope {
                    seq: 8,
                    event: closed("p1", "s", 2),
                },
            ],
        };
        let ack: Ack = post(
            lubic_proto::REPORT,
            "tb",
            serde_json::to_value(&report).unwrap(),
        )
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
        assert_eq!(ack.acked, 8);
        let agent: String = app
            .db()
            .query_row("SELECT agent FROM parts", [], |r| r.get(0))
            .unwrap();
        assert_eq!(agent, "b");

        let req = TicketRequest {
            room: "r".into(),
            part_id: "p1".into(),
            file_name: "p1.flv".into(),
            size: 5,
            line: "tx".into(),
        };
        let v: Value = post(
            lubic_proto::TICKET,
            "ta",
            serde_json::to_value(&req).unwrap(),
        )
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
        assert_eq!(v["line"], "tx");

        // 发起 a → b 的交接：b 还没报 Recording 时 a 的长轮询挂住；b 一报 start_live，
        // report 唤醒长轮询，旧机 a 立即拿到 cut，新机 b 不带 cut；之后 a 回显 cuts 一致则挂住。
        db::start_handoff(&app.db(), "r", "a", "b", now_ms()).unwrap();
        let watch_a = |cuts: Vec<String>| {
            post(
                lubic_proto::WATCH,
                "ta",
                serde_json::to_value(Watch {
                    rooms: vec!["r".into()],
                    cuts,
                    probe: None,
                    trial: None,
                    hold_uploads: None,
                    status: Status {
                        record_lags: vec![("r".into(), 100_000)],
                        ..Default::default()
                    },
                })
                .unwrap(),
            )
        };
        let held_a = tokio::spawn(watch_a(vec![]));
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!held_a.is_finished(), "还没切点，长轮询挂住");

        const CUT: i64 = 1_792_839_450_000;
        let rec = Report {
            events: vec![Envelope {
                seq: 9,
                event: AgentEvent::Recording {
                    room: "r".into(),
                    lag_ms: Some(100),
                    start_live_ms: Some(CUT),
                },
            }],
        };
        post(
            lubic_proto::REPORT,
            "tb",
            serde_json::to_value(&rec).unwrap(),
        )
        .await
        .unwrap();
        let got_a: Assignments = tokio::time::timeout(Duration::from_secs(2), held_a)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(got_a.cuts, [("r".to_string(), CUT)]);

        // 新机 b 的 Assignments 不带 cut。
        let got_b: Assignments = post(
            lubic_proto::WATCH,
            "tb",
            serde_json::to_value(Watch::default()).unwrap(),
        )
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
        assert_eq!(got_b.rooms, [db_room(&app, "r").task()]);
        assert!(got_b.cuts.is_empty());

        // a 回显不一致（空）时立刻返回，回显一致（["r"]）时挂住不立刻返回。
        let imm: Assignments = tokio::time::timeout(Duration::from_millis(200), watch_a(vec![]))
            .await
            .unwrap()
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(imm.cuts, [("r".to_string(), CUT)]);
        let held_echo = tokio::spawn(watch_a(vec!["r".into()]));
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!held_echo.is_finished(), "回显的 cuts 一致时不立刻返回");
        held_echo.abort();
    }

    /// 现有格式的 TOML + 升级前的库（只有旧表，有稿件、有分配）→ 首次启动导入；再启动不重复、不再读 TOML。
    #[test]
    fn first_start_imports_toml_rooms_once_and_keeps_running_state() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("c.db");
        let cookies = dir.path().join("c.json");
        std::fs::write(
            &cookies,
            r#"{"token_info":{"mid":7,"access_token":"x"},"cookie_info":{}}"#,
        )
        .unwrap();
        {
            let old = Connection::open(&db_path).unwrap();
            old.execute_batch(db::SCHEMA).unwrap();
            old.execute(
                "INSERT INTO archives(room, session, title, started_at, phase) VALUES ('r', 's', 't', 1, 'ended')",
                [],
            )
            .unwrap();
            old.execute(
                "INSERT INTO assignments(room, agent, session, assigned_at) VALUES ('r', 'a', 's', 1)",
                [],
            )
            .unwrap();
        }
        let text = |title: &str| {
            format!(
                "db = {:?}\ncookies = {:?}\ndefault_agents = [\"a\", \"b\"]\n[agents.a]\ntoken = \"ta\"\n\
                 [[rooms]]\nname = \"r\"\nurl = \"https://live.bilibili.com/6\"\nsplit_time = \"2m\"\n\
                 [rooms.upload]\ntitle = \"{title}\"\ncopyright = 2\nsource = \"{{url}}\"\n\
                 [[rooms]]\nname = \"r2\"\nurl = \"https://live.bilibili.com/7\"\nagents = [\"c\"]\n[rooms.upload]\n",
                db_path, cookies
            )
        };
        let conn = Config::parse(&text("first")).unwrap().open_db().unwrap();
        let rooms = db::rooms(&conn).unwrap();
        let names: Vec<_> = rooms.iter().map(|r| (&*r.name, r.agents.clone())).collect();
        assert_eq!(
            names,
            [
                ("r", vec!["a".to_string(), "b".into()]),
                ("r2", vec!["c".into()])
            ],
            "缺省机器列表展开、顺序照 TOML"
        );
        assert_eq!(rooms[0].split_time, Some(120));
        let accounts = db::accounts(&conn).unwrap();
        assert_eq!((accounts.len(), accounts[0].mid), (1, 7));
        let tpl = db::template(&conn, rooms[0].template_id).unwrap().unwrap();
        assert_eq!((&*tpl.name, &*tpl.title, tpl.copyright), ("r", "first", 2));
        assert_eq!(tpl.account_id, accounts[0].id);
        assert_eq!(
            db::archive(&conn, 1).unwrap().account_id,
            Some(accounts[0].id)
        );
        let tasks = db::assigned_tasks(&conn, "a").unwrap();
        assert_eq!(tasks, [rooms[0].task()], "升级时在录的房间仍发给 agent");
        drop(conn);

        // 再启动：TOML 改了也不再读，不重复。
        let conn = Config::parse(&text("second")).unwrap().open_db().unwrap();
        assert_eq!(db::rooms(&conn).unwrap().len(), 2);
        assert_eq!(db::templates(&conn).unwrap().len(), 2);
        assert_eq!(
            db::template(&conn, rooms[0].template_id)
                .unwrap()
                .unwrap()
                .title,
            "first"
        );

        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&db_path).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600, "库里有 cookie，只给本用户读写");

        // 有房间却读不到 cookies：启动失败，库原样不动。
        let fresh = dir.path().join("fresh.db");
        let bad = text("x")
            .replace(&format!("{cookies:?}"), "\"/nonexistent.json\"")
            .replace(&format!("{db_path:?}"), &format!("{fresh:?}"));
        let e = Config::parse(&bad).unwrap().open_db().err().unwrap();
        assert!(e.contains("nonexistent"), "{e}");
        let v: i64 = Connection::open(&fresh)
            .unwrap()
            .pragma_query_value(None, "user_version", |r| r.get(0))
            .unwrap();
        assert_eq!(v, 0);
    }

    /// 改正在录的房间：参数、优先机器、删除都不动这一场，下一场才按新配置。
    #[test]
    fn edits_to_a_recording_room_take_effect_from_the_next_session() {
        let (app, _) = test_app();
        let live = || Some(Some("s".to_string()));
        let t = 100 * MIN;
        seen(&app, "a", t, false);
        seen(&app, "b", t, false);
        assert!(app.schedule(&db_room(&app, "r"), live(), t).unwrap());
        // 改切段时长、改优先机器为只有 b：a 照录、参数照旧，不换机。
        let mut room = db_room(&app, "r");
        room.split_time = Some(600);
        room.agents = vec!["b".into()];
        db::save_room(&app.db(), &room).unwrap();
        assert!(!app.schedule(&db_room(&app, "r"), live(), t + MIN).unwrap());
        assert_eq!(assigned(&app, "r").as_deref(), Some("a"));
        let split = |app: &App, agent: &str| app.desired(agent).unwrap()[0].split_time;
        assert_eq!(split(&app, "a"), Some(Duration::from_secs(120)));
        // 下播撤销后再开播：按新列表分给 b、用新参数。
        db::apply(&app.db(), "a", &ended("s", t)).unwrap();
        assert!(
            app.schedule(&db_room(&app, "r"), Some(None), t + 10 * MIN)
                .unwrap()
        );
        seen(&app, "a", t + 11 * MIN, false);
        seen(&app, "b", t + 11 * MIN, false);
        assert!(
            app.schedule(&db_room(&app, "r"), live(), t + 11 * MIN)
                .unwrap()
        );
        assert_eq!(assigned(&app, "r").as_deref(), Some("b"));
        assert_eq!(split(&app, "b"), Some(Duration::from_secs(600)));

        // 删除在录的房间：这一场照常，失联也改派；下播撤销后不再检测、不再分配，投完后真删掉。
        db::apply(&app.db(), "b", &opened("p1", "s2", t + 11 * MIN)).unwrap();
        room.agents = vec!["b".into(), "a".into()];
        db::save_room(&app.db(), &room).unwrap();
        db::delete_room(&app.db(), "r").unwrap();
        assert_eq!(active_rooms(&app).unwrap().len(), 2, "还有分配，照常检测");
        seen(&app, "a", t + 14 * MIN, false);
        let r = db_room(&app, "r");
        assert!(
            app.schedule(&r, live(), t + 14 * MIN).unwrap(),
            "b 失联，改派 a"
        );
        assert_eq!(assigned(&app, "r").as_deref(), Some("a"));
        db::apply(&app.db(), "b", &ended("s2", t + 15 * MIN)).unwrap();
        assert!(app.schedule(&r, Some(None), t + 20 * MIN).unwrap());
        assert!(
            !app.schedule(&r, live(), t + 21 * MIN).unwrap(),
            "删了不再分配"
        );
        assert_eq!(assigned(&app, "r"), None);
        let names: Vec<_> = active_rooms(&app)
            .unwrap()
            .into_iter()
            .map(|r| r.name)
            .collect();
        assert_eq!(names, ["r2"]);
        assert_eq!(db::purge_rooms(&app.db()).unwrap(), 0, "还有稿件没投");
        db::set_phase(&app.db(), 1, db::SUBMITTED, None, None).unwrap();
        assert_eq!(db::purge_rooms(&app.db()).unwrap(), 1);
    }

    /// 两个账号：凭证按分P钉住的账号或房间当前模板的账号选，601 只冷却撞上的那个账号，投稿用钉住的账号。
    #[tokio::test]
    async fn tickets_and_submits_follow_the_pinned_account_and_601_cools_one_account() {
        let (app, fakes, _) = test_apps(config());
        let login2 = serde_json::json!({"token_info": {"mid": 2}});
        let acc2 = db::save_account(&app.db(), "acc2", 2, &login2, 0).unwrap();
        let tpl2 = db::Template {
            name: "t2".into(),
            account_id: acc2,
            title: "x".into(),
            ..Default::default()
        };
        let tpl2 = db::insert_template(&app.db(), &tpl2).unwrap();
        let mut r2 = db_room(&app, "r2");
        r2.template_id = tpl2;
        db::save_room(&app.db(), &r2).unwrap();

        let ticket = |room: &str, part: &str| {
            let req = TicketRequest {
                room: room.into(),
                part_id: part.into(),
                file_name: "f.flv".into(),
                size: 1,
                line: "tx".into(),
            };
            let app = app.clone();
            async move {
                let mut h = HeaderMap::new();
                h.insert("authorization", "Bearer ta".parse().unwrap());
                super::ticket(State(app), h, Json(req))
                    .await
                    .map(|j| j.0["mid"].clone())
            }
        };
        assert_eq!(ticket("r2", "q1").await.unwrap(), 2);
        assert_eq!(ticket("r", "p1").await.unwrap(), 1);
        assert_eq!(
            ticket("nope", "x").await.unwrap_err().0,
            StatusCode::BAD_REQUEST
        );

        // 分P已登记在钉住账号 1 的稿件里，之后 r 的模板换成账号 2：这一场仍用账号 1。
        db::apply_all(
            &mut app.db(),
            "a",
            &[opened("p2", "s", 1), closed("p2", "s", 2)],
        )
        .unwrap();
        let mut r = db_room(&app, "r");
        r.template_id = tpl2;
        db::save_room(&app.db(), &r).unwrap();
        let skip_spacing = || app.gates.lock().unwrap().clear(); // 跳过闸的最小间隔
        skip_spacing();
        assert_eq!(ticket("r", "p2").await.unwrap(), 1);
        assert_eq!(ticket("r", "p3").await.unwrap(), 2, "新分P按当前模板");
        skip_spacing();

        // 账号 1 撞 601：它冷却，账号 2 不受影响。
        *fakes.get(1).limited.lock().unwrap() = true;
        assert_eq!(
            ticket("r", "p2").await.unwrap_err().0,
            StatusCode::BAD_GATEWAY
        );
        assert_eq!(
            ticket("r", "p2").await.unwrap_err().0,
            StatusCode::TOO_MANY_REQUESTS
        );
        assert_eq!(ticket("r2", "q2").await.unwrap(), 2);

        // 投稿用钉住的账号 1；封面上传失败照常投稿、不带封面。
        *fakes.get(1).cover_fails.lock().unwrap() = true;
        db::apply_all(&mut app.db(), "a", &[uploaded("p2"), ended("s", 3)]).unwrap();
        app.submit_ready().await.unwrap();
        let s = fakes.get(1).submitted.lock().unwrap().clone();
        assert_eq!((s.len(), &*s[0].cover), (1, ""));
        assert!(fakes.get(2).submitted.lock().unwrap().is_empty());
        assert_eq!(s[0].title, "x", "模板字段投稿时读最新的");
    }

    /// 封面第一行用房间的封面名，没填回退房间名；第二行是「YYYY-MM-DD HH点场」。
    #[tokio::test]
    async fn cover_uses_display_name_or_falls_back_to_room_name() {
        let (app, fake) = test_app();
        let started = 1_761_220_800_000; // 2025-10-23 20:00 +08:00
        let time = local(started).format(cover::TIME_FORMAT).to_string();
        assert!(time.ends_with("点场"), "{time}");
        let submit = |session: &str, part: &str| {
            let evs = [
                opened(part, session, started),
                closed(part, session, started + 1),
                uploaded(part),
                ended(session, started + 2),
            ];
            db::apply_all(&mut app.db(), "a", &evs).unwrap();
        };
        submit("s1", "p1");
        app.submit_ready().await.unwrap();
        let mut room = db_room(&app, "r");
        room.display_name = Some("Secret（王者组排）".into());
        db::save_room(&app.db(), &room).unwrap();
        // 第二场之前传了封面背景，投稿封面垫上它。
        let mut bg = Vec::new();
        image::codecs::jpeg::JpegEncoder::new(&mut bg)
            .encode_image(&image::RgbImage::from_pixel(
                cover::WIDTH,
                cover::HEIGHT,
                image::Rgb([200, 100, 50]),
            ))
            .unwrap();
        db::set_cover_bg(&app.db(), "r", Some(&bg)).unwrap();
        submit("s2", "p2");
        skip_submit_spacing(&app);
        app.submit_ready().await.unwrap();
        let covers = fake.covers.lock().unwrap().clone();
        assert_eq!(covers.len(), 2);
        assert_eq!(
            covers[0],
            cover::render(None, &["r", &time]),
            "没填封面名用房间名"
        );
        assert_eq!(
            covers[1],
            cover::render(Some(&bg), &["Secret（王者组排）", &time])
        );
        // 房间名、标题里的 {name} 不受封面名影响。
        let titles: Vec<_> = fake
            .submitted
            .lock()
            .unwrap()
            .iter()
            .map(|s| s.title.clone())
            .collect();
        assert!(titles.iter().all(|t| t.starts_with("r-")), "{titles:?}");
    }

    /// 文件名模板与显示名随录制任务经 `/v1/watch` 下发；显示名空着时 `{streamer}` 用房间名。
    #[test]
    fn watch_task_carries_the_filename_template_and_display_name() {
        let (app, _) = test_app();
        let t = 100 * MIN;
        seen(&app, "a", t, false);
        assert!(
            app.schedule(&db_room(&app, "r"), Some(Some("s".into())), t)
                .unwrap()
        );
        let task = &app.desired("a").unwrap()[0];
        assert_eq!(
            (&*task.filename, &*task.streamer),
            (db::DEFAULT_FILENAME, "r")
        );
        // 改显示名与模板：在录的这一场不变（快照），下一场才用新的。
        let mut room = db_room(&app, "r");
        room.display_name = Some("Secret（王者组排）".into());
        room.filename = "{streamer}_%H".into();
        db::save_room(&app.db(), &room).unwrap();
        assert_eq!(app.desired("a").unwrap()[0].streamer, "r");
        let next = db_room(&app, "r").task();
        assert_eq!(
            (&*next.filename, &*next.streamer),
            ("{streamer}_%H", "Secret（王者组排）")
        );
        assert_eq!(next.name, "r", "目录与标题里的 {{name}} 仍是房间名");
    }

    #[test]
    fn dynamic_machine_auth_follows_add_reset_and_remove() {
        let (app, _) = test_app();
        let bearer = |token: &str| {
            let mut h = HeaderMap::new();
            h.insert("authorization", format!("Bearer {token}").parse().unwrap());
            h
        };
        assert_eq!(
            app.auth(&HeaderMap::new()).unwrap_err().0,
            StatusCode::UNAUTHORIZED
        );
        let (_, token) = app.add_machine("d", "", false).unwrap();
        assert_eq!(app.auth(&bearer(&token)).unwrap(), "d");

        let new_token = app.reset_machine_token("d").unwrap();
        assert_ne!(new_token, token);
        assert_eq!(
            app.auth(&bearer(&token)).unwrap_err().0,
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(app.auth(&bearer(&new_token)).unwrap(), "d");

        app.update_machine("d", Some(false), None).unwrap();
        app.remove_machine("d", false).unwrap();
        assert_eq!(
            app.auth(&bearer(&new_token)).unwrap_err().0,
            StatusCode::UNAUTHORIZED
        );
    }

    #[test]
    fn added_machine_can_be_picked_when_added_to_rooms() {
        let (app, _) = test_app();
        let t = 100 * MIN;
        app.add_machine("d", "", true).unwrap();
        seen(&app, "d", t, false);
        let room = db_room(&app, "r");
        assert!(app.schedule(&room, Some(Some("s".into())), t).unwrap());
        assert_eq!(assigned(&app, "r").as_deref(), Some("d"));
    }

    #[test]
    fn disabled_machine_is_never_picked_even_in_fallback_rounds() {
        let (app, _) = test_app();
        let room = &db_room(&app, "r");
        let live = || Some(Some("s".to_string()));
        let t = 100 * MIN;
        app.update_machine("a", Some(false), None).unwrap();
        seen(&app, "a", t, false);
        seen(&app, "b", t, false);
        // a 停用、在线，b 在线 → 分给 b。
        assert!(app.schedule(room, live(), t).unwrap());
        assert_eq!(assigned(&app, "r").as_deref(), Some("b"));

        // b 受限、a 停用 → 兜底轮也不选 a，仍分给 b。
        db::unassign(&app.db(), "r").unwrap();
        app.judge("b", &status(&[], 1000.0, Some(10.0), 1.0), t)
            .unwrap();
        assert!(app.schedule(room, live(), t).unwrap());
        assert_eq!(assigned(&app, "r").as_deref(), Some("b"));

        // 只有停用的 a 在线 → 不分配。
        db::unassign(&app.db(), "r").unwrap();
        let t2 = t + 5 * MIN;
        seen(&app, "a", t2, false);
        assert!(!app.schedule(room, live(), t2).unwrap());
        assert_eq!(assigned(&app, "r"), None);
    }

    #[test]
    fn disabling_assigned_machine_hands_off_and_enabling_waits_for_a_part() {
        let (app, _) = test_app();
        let room = &db_room(&app, "r");
        let live = || Some(Some("s".to_string()));
        let t = 100 * MIN;
        seen(&app, "a", t, false);
        assert!(app.schedule(room, live(), t).unwrap());
        assert_eq!(assigned(&app, "r").as_deref(), Some("a"));

        // 停用 a，但 b 离线不可接：不产生交接、仍在 a。
        app.update_machine("a", Some(false), None).unwrap();
        assert!(!app.schedule(room, live(), t + MIN).unwrap());
        assert_eq!(db::handoff(&app.db(), "r").unwrap(), None);
        assert_eq!(assigned(&app, "r").as_deref(), Some("a"));

        // b 上线后跑一次 schedule：产生 a → b 的交接。
        seen(&app, "a", t + 2 * MIN, false);
        seen(&app, "b", t + 2 * MIN, false);
        assert!(app.schedule(room, live(), t + 2 * MIN).unwrap());
        let h = db::handoff(&app.db(), "r").unwrap().unwrap();
        assert_eq!((&*h.from, &*h.to), ("a", "b"));

        // 交接完成到 b 后启用 a，再跑 schedule：b 上没有在写的分P对不上切段，分配仍是 b、没有新交接。
        recording(&app, "b", "r");
        let started = db::handoff(&app.db(), "r")
            .unwrap()
            .unwrap()
            .started_at
            .unwrap();
        seen(&app, "a", started, false);
        seen(&app, "b", started, false);
        assert!(
            app.schedule(room, live(), started + HANDOFF_OVERLAP_MS)
                .unwrap()
        );
        assert_eq!(assigned(&app, "r").as_deref(), Some("b"));
        assert_eq!(db::handoff(&app.db(), "r").unwrap(), None);

        let later = started + 2 * MIN;
        app.update_machine("a", Some(true), None).unwrap();
        seen(&app, "a", later, false);
        seen(&app, "b", later, false);
        assert!(!app.schedule(room, live(), later).unwrap());
        assert_eq!(assigned(&app, "r").as_deref(), Some("b"));
        assert_eq!(db::handoff(&app.db(), "r").unwrap(), None);
    }
    /// 建一个优先列表为 `agents` 的房间（沿用 `r` 的其余配置）。
    fn room_on(app: &App, name: &str, agents: &[&str]) -> db::Room {
        let mut room = db_room(app, "r");
        room.name = name.into();
        room.agents = agents.iter().map(|a| a.to_string()).collect();
        db::save_room(&app.db(), &room).unwrap();
        db_room(app, name)
    }

    fn live() -> Option<Option<String>> {
        Some(Some("s".into()))
    }

    /// load-balance.md §1：同时开播按在录数分散，同数按优先列表。
    #[test]
    fn live_rooms_spread_by_load_then_by_list_order() {
        let (app, _) = test_app();
        let t = 100 * MIN;
        for a in ["a", "b", "c"] {
            seen(&app, a, t, false);
        }
        for (name, want) in [("x0", "a"), ("x1", "b"), ("x2", "c")] {
            let room = room_on(&app, name, &["a", "b", "c"]);
            assert!(app.schedule(&room, live(), t).unwrap());
            assert_eq!(assigned(&app, name).as_deref(), Some(want), "{name}");
        }

        // a 已有 1 个、b 和 c 都是 0：去 b（同数按列表顺序）。
        let (app, _) = test_app();
        for a in ["a", "b", "c"] {
            seen(&app, a, t, false);
        }
        let x0 = room_on(&app, "x0", &["a", "b", "c"]);
        assert!(app.schedule(&x0, live(), t).unwrap());
        let x1 = room_on(&app, "x1", &["a", "c", "b"]);
        assert!(app.schedule(&x1, live(), t).unwrap());
        assert_eq!(
            assigned(&app, "x1").as_deref(),
            Some("c"),
            "按这个房间自己的列表"
        );
        let x2 = room_on(&app, "x2", &["a", "b", "c"]);
        assert!(app.schedule(&x2, live(), t).unwrap());
        assert_eq!(assigned(&app, "x2").as_deref(), Some("b"));
    }

    /// 以某台为目标、还没完成的交接计入它的负载。
    #[test]
    fn pending_handoff_target_counts_as_load() {
        let (app, _) = test_app();
        let t = 100 * MIN;
        for a in ["a", "b", "c"] {
            seen(&app, a, t, false);
        }
        assign(&app, "r", "a", t);
        db::start_handoff(&app.db(), "r", "a", "b", t).unwrap();
        let room = room_on(&app, "x", &["b", "c"]);
        assert!(app.schedule(&room, live(), t).unwrap());
        assert_eq!(assigned(&app, "x").as_deref(), Some("c"));
    }

    /// 档 1 没有时依次落到排空、受限、让出的机器；停用的永不选。档位先于负载。
    #[test]
    fn falls_back_through_draining_limited_yielding_and_never_picks_disabled() {
        let (app, _) = test_app();
        let t = 100 * MIN;
        for a in ["a", "b", "c"] {
            seen(&app, a, t, false);
        }
        assert!(db::set_yield(&app.db(), "a", Some(t + HOUR)).unwrap());
        let limit = db::Limit {
            since: t,
            until: t + HOUR,
            reason: "upload".into(),
            need_bps: 1.0,
        };
        db::set_limit(&app.db(), "b", &limit).unwrap();
        db::start_drain(&app.db(), "c", "new", t).unwrap();
        // 让出的 a 已有一个房间、负载最高，也不影响档位。
        assign(&app, "r2", "c", t);
        let room = room_on(&app, "x", &["a", "b", "c"]);
        for (want, disable) in [
            (Some("c"), "c"),
            (Some("b"), "b"),
            (Some("a"), "a"),
            (None, ""),
        ] {
            assert_eq!(app.schedule(&room, live(), t).unwrap(), want.is_some());
            assert_eq!(assigned(&app, "x").as_deref(), want);
            if want.is_some() {
                db::unassign(&app.db(), "x").unwrap();
                db::update_machine(&app.db(), disable, Some(false), None).unwrap();
            }
        }
    }

    #[test]
    fn availability_keeps_distinct_rules_for_overlapping_machine_states() {
        let (app, _) = test_app();
        let t = 100 * MIN;
        let get = || app.availability("a", t, None).unwrap();

        let offline = get();
        assert_eq!(offline.tier, None);
        assert!(offline.send_recovery_probe);
        seen(&app, "a", t, true);
        let paused = get();
        assert_eq!(paused.tier, None);
        assert!(paused.move_away.is_none());
        assert!(!paused.cross_probe_candidate && paused.send_recovery_probe);

        seen(&app, "a", t, false);
        let legacy = get();
        assert_eq!(legacy.tier, Some(1));
        assert!(legacy.handoff_target && legacy.cross_probe_candidate);
        assert!(legacy.continue_failure_trial && legacy.failure_trial_candidate.is_none());

        db::start_drain(&app.db(), "a", "new", t).unwrap();
        let draining = get();
        assert_eq!(draining.tier, Some(2));
        assert_eq!(draining.move_away.as_deref(), Some("draining for new"));
        assert!(!draining.cross_probe_candidate && draining.send_cross_probe);
        db::delete_drain(&app.db(), "a").unwrap();

        db::set_limit(
            &app.db(),
            "a",
            &db::Limit {
                since: t - HOUR,
                until: t + MIN,
                reason: "record rate 0.42".into(),
                need_bps: 1.0,
            },
        )
        .unwrap();
        let limited = get();
        assert_eq!(limited.tier, Some(3));
        assert!(limited.cross_probe_candidate && limited.probe_machine_limit_due);
        assert!(limited.track_rate_move);
        assert!(!limited.failure_trial_origin && !limited.continue_failure_trial);

        db::start_drain(&app.db(), "a", "new", t).unwrap();
        db::set_yield(&app.db(), "a", Some(t + 1)).unwrap();
        let yielding = get();
        assert_eq!(yielding.tier, Some(4));
        assert_eq!(yielding.move_away.as_deref(), Some("record rate 0.42"));
        assert!(!yielding.send_cross_probe && !yielding.send_recovery_probe);
        assert!(yielding.hold_uploads && yielding.track_rate_move);
        let ended = app.availability("a", t + 1, None).unwrap();
        assert_eq!(ended.tier, Some(3));
        assert!(!ended.hold_uploads);

        db::update_machine(&app.db(), "a", Some(false), None).unwrap();
        let disabled = get();
        assert_eq!(disabled.tier, None);
        assert_eq!(disabled.move_away.as_deref(), Some("disabled"));
        assert!(disabled.hold_uploads && !disabled.track_rate_move);
        db::set_yield(&app.db(), "a", None).unwrap();
        let disabled = get();
        assert!(disabled.send_cross_probe && !disabled.send_recovery_probe);
    }

    /// load-balance.md §2：原机好好的，不因「更靠前的机器可用」迁回；原机不在列表里了才在切段前换走。
    #[test]
    fn stays_on_its_agent_and_moves_only_off_a_removed_one_near_the_split() {
        let (app, _) = test_app();
        let t = 100 * MIN;
        // a 不在线，r 分给排第二的 b。
        seen(&app, "b", t, false);
        let mut room = db_room(&app, "r");
        room.split_time = Some(3600);
        assert!(app.schedule(&room, live(), t).unwrap());
        assert_eq!(assigned(&app, "r").as_deref(), Some("b"));
        db::apply(&app.db(), "b", &opened("p1", "s", t)).unwrap();

        let at = |room: &db::Room, m: i64| {
            seen(&app, "a", t + m, false);
            seen(&app, "b", t + m, false);
            app.schedule(room, live(), t + m).unwrap()
        };
        // a 上线了，切段前也不迁回。
        assert!(!at(&room, 10 * MIN));
        assert!(!at(&room, 58 * MIN));
        assert_eq!(db::handoff(&app.db(), "r").unwrap(), None);
        // 列表里去掉 b：等到切段前 2 分钟换到 a。
        room.agents = vec!["a".into()];
        assert!(!at(&room, 58 * MIN - 1));
        assert!(at(&room, 58 * MIN));
        let h = db::handoff(&app.db(), "r").unwrap().unwrap();
        assert_eq!((&*h.from, &*h.to), ("b", "a"));

        // 不按时长切段的房间：去掉 b 就换。
        let (app, _) = test_app();
        seen(&app, "b", t, false);
        let mut room = db_room(&app, "r");
        room.split_time = None;
        assert!(app.schedule(&room, live(), t).unwrap());
        seen(&app, "a", t + MIN, false);
        seen(&app, "b", t + MIN, false);
        assert!(!app.schedule(&room, live(), t + MIN).unwrap());
        room.agents = vec!["a".into()];
        assert!(app.schedule(&room, live(), t + MIN).unwrap());
        assert_eq!(db::handoff(&app.db(), "r").unwrap().unwrap().to, "a");
    }

    /// load-balance.md §3：让出网络的机器迁走在录房间，没有目标时留着录；不当交接目标、不做试拉；到点自动恢复。
    #[test]
    fn yielding_agent_hands_off_and_is_used_only_as_a_last_resort() {
        let (app, _) = test_app();
        let t = 100 * MIN;
        let until = t + HOUR;
        let room = db_room(&app, "r");
        seen(&app, "a", t, false);
        assert!(app.schedule(&room, live(), t).unwrap());
        assert!(db::set_yield(&app.db(), "a", Some(until)).unwrap());
        let availability = app.availability("a", t, None).unwrap();
        assert_eq!(
            app.move_reason(&room, "a", availability.move_away),
            Some(("yielding network".into(), false))
        );
        // 没有交接目标：留在 a 继续录。
        assert!(!app.schedule(&room, live(), t).unwrap());
        assert_eq!(db::handoff(&app.db(), "r").unwrap(), None);
        // b 上线：交接给它。
        seen(&app, "a", t + MIN, false);
        seen(&app, "b", t + MIN, false);
        assert!(app.schedule(&room, live(), t + MIN).unwrap());
        let h = db::handoff(&app.db(), "r").unwrap().unwrap();
        assert_eq!((&*h.from, &*h.to), ("a", "b"));

        // 新开播：b 在线就不给 a，哪怕 a 负载更低；b 不在线才兜底给 a。
        let (app, _) = test_app();
        seen(&app, "a", t, false);
        seen(&app, "b", t, false);
        db::set_yield(&app.db(), "a", Some(until)).unwrap();
        assign(&app, "r2", "b", t);
        let room = db_room(&app, "r");
        assert!(app.schedule(&room, live(), t).unwrap());
        assert_eq!(assigned(&app, "r").as_deref(), Some("b"));
        db::unassign(&app.db(), "r").unwrap();
        seen(&app, "b", t - HOUR, false);
        assert!(app.schedule(&room, live(), t).unwrap());
        assert_eq!(assigned(&app, "r").as_deref(), Some("a"));

        // 受限的 b 在录：让出的 a 不当交接目标，也不当交叉试拉机、不派试拉。
        let (app, _) = test_app();
        seen(&app, "a", t, false);
        seen(&app, "b", t, false);
        db::set_yield(&app.db(), "a", Some(until)).unwrap();
        let room = db_room(&app, "r");
        assign(&app, "r", "b", t);
        let limit = db::Limit {
            since: t - HOUR,
            until: t + MIN,
            reason: "upload".into(),
            need_bps: 1.0,
        };
        db::set_limit(&app.db(), "a", &limit).unwrap();
        db::set_limit(&app.db(), "b", &limit).unwrap();
        db::clear_limit(&app.db(), "a").unwrap();
        assert!(!app.schedule(&room, live(), t).unwrap());
        assert_eq!(db::handoff(&app.db(), "r").unwrap(), None);
        assert_eq!(app.prober("r", "b", "b", t).unwrap(), None);
        // a 的受限标记快到期、该派试拉了，但还在让出：不派。
        let due = db::Limit {
            until: until + MIN,
            ..limit
        };
        db::set_limit(&app.db(), "a", &due).unwrap();
        assert_eq!(app.probe_for("a", until - 1).unwrap(), None);
        // 到点自动恢复：a 回到档 1，可以试拉、当交接目标。
        assert!(app.probe_for("a", until).unwrap().is_some());
        seen(&app, "a", until, false);
        seen(&app, "b", until, false);
        assert_eq!(
            app.prober("r", "b", "b", until).unwrap().as_deref(),
            Some("a")
        );
        db::clear_limit(&app.db(), "a").unwrap();
        assert_eq!(
            app.pick(&room, &["b"], until, Pick::Handoff)
                .unwrap()
                .as_deref(),
            Some("a")
        );
    }

    /// agent 回显的暂停状态和应下发的不一致（两次长轮询之间改了开关、让出期间重启）：立刻回复，不挂满长轮询。
    #[tokio::test]
    async fn watch_returns_at_once_when_the_agents_hold_state_is_stale() {
        let (app, _) = test_app();
        let t = now_ms();
        assign(&app, "r", "a", t);
        db::set_yield(&app.db(), "a", Some(t + HOUR)).unwrap();
        let mut h = HeaderMap::new();
        h.insert("authorization", "Bearer ta".parse().unwrap());
        let req: Watch =
            serde_json::from_str(r#"{"rooms":["r"],"hold_uploads":false,"status":{}}"#).unwrap();
        let reply = tokio::time::timeout(
            Duration::from_secs(5),
            super::watch(State(app.clone()), h, Json(req)),
        )
        .await
        .expect("立刻回复")
        .unwrap();
        assert!(reply.0.hold_uploads);
    }

    /// 下发给让出机的 `Assignments.hold_uploads` 为真，其他为假。
    #[tokio::test]
    async fn yielding_agent_is_told_to_hold_uploads() {
        let (app, _) = test_app();
        let t = now_ms();
        assign(&app, "r", "a", t);
        assign(&app, "r2", "c", t);
        db::set_yield(&app.db(), "a", Some(t + HOUR)).unwrap();
        for (agent, hold) in [("a", true), ("c", false)] {
            let mut h = HeaderMap::new();
            h.insert("authorization", format!("Bearer t{agent}").parse().unwrap());
            let req: Watch = serde_json::from_str(r#"{"rooms":[],"status":{}}"#).unwrap();
            let reply = super::watch(State(app.clone()), h, Json(req))
                .await
                .unwrap();
            assert_eq!(reply.0.hold_uploads, hold, "{agent}");
        }
    }
}
