//! 总控的 SQLite：配置（账号、模板、房间）与运行状态（agent、分配、稿件、分P）。时间一律 unix 毫秒。
//! 表结构与迁移见 `docs/design/config-db.md`。
//!
//! 事件按内容幂等落库（按 `part_id`、`(room, session, at)`），不记 agent 的 `seq`。

use lubic_account::Studio;
use lubic_events::{Event, Owner};
use lubic_proto::{AgentEvent, RoomTask, Status};
use rusqlite::{Connection, OptionalExtension, Row, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::Path;
use tracing::{error, info, warn};

pub type Result<T> = rusqlite::Result<T>;

pub(crate) const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS agents (
  name TEXT PRIMARY KEY,
  last_seen INTEGER NOT NULL,
  status TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS assignments (
  room TEXT PRIMARY KEY,
  agent TEXT NOT NULL,
  session TEXT,
  assigned_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS archives (
  id INTEGER PRIMARY KEY,
  room TEXT NOT NULL,
  session TEXT NOT NULL,
  title TEXT NOT NULL,
  started_at INTEGER NOT NULL,
  ended_at INTEGER,
  phase TEXT NOT NULL,
  note TEXT,
  bvid TEXT
);
CREATE TABLE IF NOT EXISTS parts (
  part_id TEXT PRIMARY KEY,
  archive_id INTEGER NOT NULL REFERENCES archives(id),
  agent TEXT NOT NULL,
  path TEXT NOT NULL,
  opened_at INTEGER NOT NULL,
  size INTEGER,
  state TEXT NOT NULL,
  filename TEXT,
  line TEXT
);
CREATE INDEX IF NOT EXISTS parts_archive ON parts(archive_id);
CREATE TABLE IF NOT EXISTS limits (
  agent TEXT PRIMARY KEY,
  since INTEGER NOT NULL,
  until INTEGER NOT NULL,
  reason TEXT NOT NULL,
  need_bps REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS handoffs (
  room TEXT PRIMARY KEY,
  from_agent TEXT NOT NULL,
  to_agent TEXT NOT NULL,
  requested_at INTEGER NOT NULL,
  started_at INTEGER,
  start_lag INTEGER
);
";

/// 迁移 1（`user_version` 0 → 1）：配置入库。
const MIGRATION_1: &str = "
CREATE TABLE accounts (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  mid INTEGER NOT NULL UNIQUE,
  login TEXT NOT NULL,
  created_at INTEGER NOT NULL
);
CREATE TABLE templates (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  account_id INTEGER NOT NULL REFERENCES accounts(id),
  title TEXT NOT NULL,
  \"desc\" TEXT NOT NULL,
  tid INTEGER NOT NULL,
  tag TEXT NOT NULL,
  copyright INTEGER NOT NULL,
  source TEXT NOT NULL,
  is_only_self INTEGER NOT NULL
);
CREATE TABLE rooms (
  name TEXT PRIMARY KEY,
  url TEXT NOT NULL,
  template_id INTEGER NOT NULL REFERENCES templates(id),
  agents TEXT NOT NULL,
  split_time INTEGER,
  split_size_mb INTEGER,
  grace INTEGER,
  bili_qn INTEGER,
  tail_upload INTEGER NOT NULL,
  position INTEGER NOT NULL,
  deleted INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE sessions (token TEXT PRIMARY KEY, expires_at INTEGER NOT NULL);
ALTER TABLE archives ADD COLUMN account_id INTEGER;
ALTER TABLE assignments ADD COLUMN task TEXT;
";

/// 迁移 2（1 → 2）：合集。稿件记下 aid 与还没加进去的合集分区，加完清空。
const MIGRATION_2: &str = "
ALTER TABLE templates ADD COLUMN season_section_id INTEGER;
ALTER TABLE archives ADD COLUMN aid INTEGER;
ALTER TABLE archives ADD COLUMN season_section_id INTEGER;
";

/// 迁移 3（2 → 3）：房间的封面名。
const MIGRATION_3: &str = "
ALTER TABLE rooms ADD COLUMN cover_name TEXT;
";

/// 迁移 4（3 → 4）：封面名改叫显示名（封面与文件名的 `{streamer}` 都用它），加文件名模板。
const MIGRATION_4: &str = "
ALTER TABLE rooms RENAME COLUMN cover_name TO display_name;
ALTER TABLE rooms ADD COLUMN filename TEXT NOT NULL
  DEFAULT '{streamer} %Y-%m-%d %H{colon}%M{colon}%S';
";

/// 迁移 5（4 → 5）：交接记下新机起点是直播的哪一刻（码流 SEI 里的推流时刻），按它判断旧机何时收尾。
const MIGRATION_5: &str = "
ALTER TABLE handoffs ADD COLUMN start_live INTEGER;
";

/// 迁移 6（5 → 6）：滚动自动更新。排空标记一台最多一条；更新失败的提交（见 `docs/design/auto-update.md`）。
const MIGRATION_6: &str = "
CREATE TABLE drains (
  agent TEXT PRIMARY KEY,
  commit_id TEXT NOT NULL,
  since INTEGER NOT NULL,
  released_at INTEGER
);
CREATE TABLE failed_commits (
  commit_id TEXT NOT NULL,
  agent TEXT NOT NULL,
  at INTEGER NOT NULL
);
";

/// 迁移 7（6 → 7）：合集从模板挪到房间，模板只管投稿设置、可以多个房间复用。沿用各房间原模板的合集。
const MIGRATION_7: &str = "
ALTER TABLE rooms ADD COLUMN season_section_id INTEGER;
UPDATE rooms SET season_section_id =
  (SELECT season_section_id FROM templates t WHERE t.id = rooms.template_id);
ALTER TABLE templates DROP COLUMN season_section_id;
";

/// 迁移 8（7 → 8）：房间的抖音画质。
const MIGRATION_8: &str = "
ALTER TABLE rooms ADD COLUMN douyin_quality TEXT;
";

/// 迁移 9（8 → 9）：机器名单入库，见 `docs/design/machines.md`。
const MIGRATION_9: &str = "
CREATE TABLE machines (
  name TEXT PRIMARY KEY,
  token TEXT NOT NULL UNIQUE,
  enabled INTEGER NOT NULL DEFAULT 1,
  note TEXT NOT NULL DEFAULT '',
  created_at INTEGER NOT NULL
);
";

/// 迁移 10（9 → 10）：分P的收尾时刻，控制台悬停显示起止时间。
const MIGRATION_10: &str = "
ALTER TABLE parts ADD COLUMN closed_at INTEGER;
";

/// 迁移 11（10 → 11）：稿件投出的时刻，控制台「已投」列表用。
const MIGRATION_11: &str = "
ALTER TABLE archives ADD COLUMN submitted_at INTEGER;
";

/// 迁移 12（11 → 12）：控制台结束本场时记下场次键，这一场不再分配，下一场照常。
const MIGRATION_12: &str = "
CREATE TABLE skips (room TEXT PRIMARY KEY, session TEXT NOT NULL);
";

/// 迁移 13（12 → 13）：分P最后一张上传凭证，原片删了还能凭它从 UPOS 取回（见 `docs/deploy.md`「取回原片」）。
/// `auth` 是凭证，只在 `upos` 子命令里输出。
const MIGRATION_13: &str = "
CREATE TABLE upos_tickets (
  part_id TEXT PRIMARY KEY,
  endpoint TEXT NOT NULL,
  upos_uri TEXT NOT NULL,
  auth TEXT NOT NULL,
  issued_at INTEGER NOT NULL
);
";

/// 迁移 14（13 → 14）：房间的追加标签，投稿时接在模板标签后面。
const MIGRATION_14: &str = "
ALTER TABLE rooms ADD COLUMN tag TEXT NOT NULL DEFAULT '';
";

/// 迁移 15（14 → 15）：房间的封面背景，控制台裁好的 1146×717 JPEG。
/// 单独一张表，列房间时不用把图读出来；房间真删时一起删，见 [`purge_rooms`]。
const MIGRATION_15: &str = "
CREATE TABLE cover_bgs (room TEXT PRIMARY KEY, jpeg BLOB NOT NULL);
";

/// 迁移 16（15 → 16）：模板开关「不发动态」，投稿带 `no_disturbance = 1`。
const MIGRATION_16: &str = "
ALTER TABLE templates ADD COLUMN no_disturbance INTEGER NOT NULL DEFAULT 0;
";

/// 迁移 17（16 → 17）：稿件自动结束后的合并期限。期限内同房间再开录（哪怕平台换了场次键）
/// 就并回这份稿件；期限过了才投稿。手动场末不设，立刻可投。
const MIGRATION_17: &str = "
ALTER TABLE archives ADD COLUMN hold_until INTEGER;
";

/// 迁移 18（17 → 18）：房间级限制（`handoff-health.md` §7）。一条是「这个房间别在这台机器上录」，
/// 备用机故障试拉健康、房间迁走时记下；`limits` 只留整机限制。恢复只靠试拉验证，所以没有 `need_bps`。
const MIGRATION_18: &str = "
CREATE TABLE room_limits (
  room TEXT NOT NULL,
  agent TEXT NOT NULL,
  since INTEGER NOT NULL,
  until INTEGER NOT NULL,
  reason TEXT NOT NULL,
  PRIMARY KEY (room, agent)
);
";

/// 迁移 19（18 → 19）：新版人选分区，旧模板不自动转换。
const MIGRATION_19: &str = "
ALTER TABLE templates ADD COLUMN human_type2 INTEGER CHECK (human_type2 > 0);
";

/// 迁移 20（19 → 20）：机器「让出网络」到哪一刻（unix 毫秒），`NULL` 或已过去即未让出，见 `docs/design/load-balance.md`。
const MIGRATION_20: &str = "
ALTER TABLE machines ADD COLUMN yield_until INTEGER;
";

/// 迁移 21（20 → 21）：分P记下建文件时的时区偏移与当时直播标题，用于投稿时按总控时区兜底重渲染分P标题。
const MIGRATION_21: &str = "
ALTER TABLE parts ADD COLUMN tz_offset INTEGER;
ALTER TABLE parts ADD COLUMN title TEXT NOT NULL DEFAULT '';
";

/// 迁移 22（21 → 22）：一个逻辑房间可配置多个同平台直播间地址（按优先级存 JSON 数组，第一项同 `url` 为主房）；
/// 稿件记下首个分P实际录制的直播间地址（`{url}` 固定用它，历史稿件为 NULL 时回退主房地址）。
const MIGRATION_22: &str = "
ALTER TABLE rooms ADD COLUMN urls TEXT NOT NULL DEFAULT '[]';
ALTER TABLE archives ADD COLUMN source_url TEXT;
CREATE TABLE source_retirements (
  room TEXT NOT NULL,
  agent TEXT NOT NULL,
  PRIMARY KEY (room, agent)
);
";

/// 迁移 23（22 → 23）：接稿后仍能认出各分P原来所属的场次。
const MIGRATION_23: &str = "
ALTER TABLE parts ADD COLUMN session TEXT;
";

/// 房间文件名模板的默认值，同 biliup 生产上的 `filename_prefix`。与迁移 4 的列默认值一致。
pub const DEFAULT_FILENAME: &str = "{streamer} %Y-%m-%d %H{colon}%M{colon}%S";

fn default_filename() -> String {
    DEFAULT_FILENAME.into()
}

/// 迁移 1 之后的各步，第 i 个把 `user_version` 从 i + 1 升到 i + 2。
const LATER: [&str; 22] = [
    MIGRATION_2,
    MIGRATION_3,
    MIGRATION_4,
    MIGRATION_5,
    MIGRATION_6,
    MIGRATION_7,
    MIGRATION_8,
    MIGRATION_9,
    MIGRATION_10,
    MIGRATION_11,
    MIGRATION_12,
    MIGRATION_13,
    MIGRATION_14,
    MIGRATION_15,
    MIGRATION_16,
    MIGRATION_17,
    MIGRATION_18,
    MIGRATION_19,
    MIGRATION_20,
    MIGRATION_21,
    MIGRATION_22,
    MIGRATION_23,
];
const VERSION: i64 = LATER.len() as i64 + 1;
const MACHINES_VERSION: i64 = 9;

/// `archives` 行的合并期限：最后一个分P收尾（没有则开稿时刻）再加房间的下播宽限
/// （没设时 300 秒，同 agent 缺省的 `offline_grace`）。
const HOLD_UNTIL: &str =
    "COALESCE((SELECT MAX(closed_at) FROM parts WHERE archive_id = archives.id), started_at)
  + COALESCE((SELECT grace FROM rooms WHERE name = archives.room), 300) * 1000";

/// 稿件阶段。
pub const RECORDING: &str = "recording";
pub const ENDED: &str = "ended";
/// 提交请求可能已发出。重启后见到它说明提交结果未知。
pub const SUBMITTING: &str = "submitting";
pub const SUBMITTED: &str = "submitted";
/// 需要人看。处理完把 phase 改回 `ended`（重投）或 `submitted`。
pub const ATTENTION: &str = "attention";
/// 控制台丢弃：不投稿，没传的分P不再发上传票据。
pub const DISCARDED: &str = "discarded";

/// 打开并迁移到最新。`import` 只在首次迁移时调用（读 TOML 里的旧配置）。
pub fn open(
    path: &Path,
    machines: &[(String, String)],
    import: impl FnOnce() -> std::result::Result<Import, String>,
) -> std::result::Result<Connection, String> {
    let mut conn = Connection::open(path).map_err(|e| e.to_string())?;
    // 库里有投稿账号的 cookie：只给本用户读写（`-wal`/`-shm` 由 SQLite 按库文件的权限建）。
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
        .map_err(|e| e.to_string())?;
    conn.busy_timeout(std::time::Duration::from_secs(5))
        .and_then(|_| conn.pragma_update(None, "journal_mode", "WAL"))
        .map_err(|e| e.to_string())?;
    init(&mut conn, machines, import)?;
    Ok(conn)
}

fn init(
    conn: &mut Connection,
    machines: &[(String, String)],
    import: impl FnOnce() -> std::result::Result<Import, String>,
) -> std::result::Result<(), String> {
    migrate(conn, machines, import).map_err(|e| match e {
        Migrate::Db(e) => e.to_string(),
        Migrate::Import(e) => format!("importing config from TOML: {e}"),
    })
}

enum Migrate {
    Db(rusqlite::Error),
    Import(String),
}

impl From<rusqlite::Error> for Migrate {
    fn from(e: rusqlite::Error) -> Self {
        Migrate::Db(e)
    }
}

fn migrate(
    conn: &mut Connection,
    machines: &[(String, String)],
    import: impl FnOnce() -> std::result::Result<Import, String>,
) -> std::result::Result<(), Migrate> {
    conn.pragma_update(None, "foreign_keys", "ON")?;
    conn.execute_batch(SCHEMA)?;
    let version: i64 = conn.pragma_query_value(None, "user_version", |r| r.get(0))?;
    if version < 1 {
        return migrate_1(conn, machines, import);
    }
    for (i, sql) in LATER.iter().enumerate().skip(version as usize - 1) {
        let tx = conn.transaction()?;
        tx.execute_batch(sql)?;
        if i as i64 + 2 == MACHINES_VERSION {
            import_machines(&tx, machines)?;
        }
        tx.pragma_update(None, "user_version", i as i64 + 2)?;
        tx.commit()?;
    }
    Ok(())
}

fn migrate_1(
    conn: &mut Connection,
    machines: &[(String, String)],
    import: impl FnOnce() -> std::result::Result<Import, String>,
) -> std::result::Result<(), Migrate> {
    let data = import().map_err(Migrate::Import)?;
    // 建表、导入、写版本号在同一个事务里：要么全做完，要么什么也没发生。
    let tx = conn.transaction()?;
    // 导入按最新的表结构写，所以后面的迁移也在这里一起建。
    tx.execute_batch(MIGRATION_1)?;
    for sql in LATER {
        tx.execute_batch(sql)?;
    }
    import_machines(&tx, machines)?;
    if let Some(login) = &data.login {
        let mid = lubic_account::mid(login)
            .ok_or_else(|| Migrate::Import("cookies: no token_info.mid".into()))?;
        let account = save_account(&tx, "导入的账号", mid, login, now())?;
        for (room, studio) in &data.rooms {
            let mut tpl = Template::from_studio(&room.name, account, studio);
            tpl.id = template_id(&tx, &room.name)?.unwrap_or(0);
            let template_id = match tpl.id {
                0 => insert_template(&tx, &tpl)?,
                id => {
                    update_template(&tx, &tpl)?;
                    id
                }
            };
            let room = Room {
                template_id,
                ..room.clone()
            };
            save_room(&tx, &room)?;
            // 升级时正在录的房间：快照取导入的参数，免得 `/v1/watch` 里看不到它。
            tx.execute(
                "UPDATE assignments SET task = ?2 WHERE room = ?1",
                params![room.name, serde_json::to_string(&room.task()).unwrap()],
            )?;
        }
        tx.execute(
            "UPDATE archives SET account_id = ?1 WHERE account_id IS NULL",
            [account],
        )?;
    } else if !data.rooms.is_empty() {
        return Err(Migrate::Import(
            "rooms need `cookies` for their upload account".into(),
        ));
    }
    tx.pragma_update(None, "user_version", VERSION)?;
    tx.commit()?;
    info!(
        rooms = data.rooms.len(),
        "config imported into the database"
    );
    Ok(())
}

fn import_machines(conn: &Connection, machines: &[(String, String)]) -> Result<()> {
    let now = now();
    for (name, token) in machines {
        conn.execute(
            "INSERT INTO machines(name, token, enabled, note, created_at) VALUES (?1, ?2, 1, '', ?3)",
            params![name, token, now],
        )?;
    }
    info!(
        machines = machines.len(),
        "machines imported into the database"
    );
    Ok(())
}

fn now() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// 首次迁移时从 TOML 导入的旧配置。
#[derive(Default)]
pub struct Import {
    /// `cookies` 文件的内容。
    pub login: Option<Value>,
    /// 房间与它的 `[rooms.upload]`；`template_id` 由导入填。
    pub rooms: Vec<(Room, Studio)>,
}

/// 房间。`name` 是备注名：文件名、投稿模板的 `{name}`；建好后不改。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Room {
    pub name: String,
    /// 主房地址（即 [`Self::urls`] 的第一项）；兼容旧接口与单地址配置。
    #[serde(default)]
    pub url: String,
    /// 按优先级排列的同平台直播间地址，第一项是主房。旧接口只传 `url` 时自动补齐。
    #[serde(default)]
    pub urls: Vec<String>,
    pub template_id: i64,
    /// 优先机器列表；空表示本场结束后暂停，重新选择后恢复。
    pub agents: Vec<String>,
    /// 切段时长（秒）。
    #[serde(default)]
    pub split_time: Option<u64>,
    #[serde(default)]
    pub split_size_mb: Option<u64>,
    /// 下播宽限（秒），缺省 5 分钟。
    #[serde(default)]
    pub grace: Option<u64>,
    /// B 站画质 qn（10000 原画）。
    #[serde(default)]
    pub bili_qn: Option<u32>,
    /// 抖音画质：origin/uhd/hd/sd/ld/md，空则 origin。
    #[serde(default)]
    pub douyin_quality: Option<String>,
    /// 边录边传：必须配 `split_size_mb`。
    #[serde(default)]
    pub tail_upload: bool,
    /// 显示名：封面第一行与文件名模板里的 `{streamer}`，空则用 `name`。
    /// 房间名、标题里的 `{name}`、录制目录都不受它影响。
    #[serde(default)]
    pub display_name: Option<String>,
    /// 文件名模板（不含扩展名），同 biliup 的 `filename_prefix`：`{streamer}`、`{title}`、`{colon}` 与 strftime。
    /// 随录制任务下发给 agent，换机接力时两台的文件名也一致。
    #[serde(default = "default_filename")]
    pub filename: String,
    /// 投稿后加入的合集分区（section_id），空则不加。须是模板账号下的合集。
    #[serde(default)]
    pub season_section_id: Option<i64>,
    /// 追加标签，逗号分隔：投稿时接在模板标签后面，见 [`tags`]。
    #[serde(default)]
    pub tag: String,
    /// 已删除：不再检测下一场，在录的这一场照常收尾。
    #[serde(skip)]
    pub deleted: bool,
}

impl Room {
    /// 按优先级排列的直播间地址（第一项为主房）；兼容只填 `url` 的旧数据。
    pub fn sources(&self) -> Vec<&str> {
        if !self.urls.is_empty() {
            self.urls.iter().map(String::as_str).collect()
        } else if !self.url.is_empty() {
            vec![self.url.as_str()]
        } else {
            Vec::new()
        }
    }

    /// 主房地址（即列表第一项）。
    pub fn primary_url(&self) -> &str {
        self.urls
            .first()
            .map(String::as_str)
            .unwrap_or(self.url.as_str())
    }
}

fn room_row(r: &Row) -> Result<Room> {
    let url: String = r.get("url")?;
    let urls_raw: String = r.get("urls")?;
    let mut urls: Vec<String> = serde_json::from_str(&urls_raw).unwrap_or_default();
    if urls.is_empty() && !url.is_empty() {
        urls.push(url.clone());
    }
    let url = urls.first().cloned().unwrap_or(url);
    let agents: String = r.get("agents")?;
    Ok(Room {
        name: r.get("name")?,
        url,
        urls,
        template_id: r.get("template_id")?,
        agents: serde_json::from_str(&agents).unwrap_or_default(),
        split_time: r.get("split_time")?,
        split_size_mb: r.get("split_size_mb")?,
        grace: r.get("grace")?,
        bili_qn: r.get("bili_qn")?,
        douyin_quality: r.get("douyin_quality")?,
        tail_upload: r.get("tail_upload")?,
        display_name: r.get("display_name")?,
        filename: r.get("filename")?,
        season_section_id: r.get("season_section_id")?,
        tag: r.get("tag")?,
        deleted: r.get("deleted")?,
    })
}

/// 全部房间（含已删除），按列表顺序。
pub fn rooms(conn: &Connection) -> Result<Vec<Room>> {
    let mut stmt = conn.prepare("SELECT * FROM rooms ORDER BY position, name")?;
    stmt.query_map([], room_row)?.collect()
}

/// 一个房间（含已删除）。
pub fn room(conn: &Connection, name: &str) -> Result<Option<Room>> {
    conn.query_row("SELECT * FROM rooms WHERE name = ?1", [name], room_row)
        .optional()
}

/// 按名字新建或整行替换；已删除的同名房间就此复活。新建的排到最后。
pub fn save_room(conn: &Connection, room: &Room) -> Result<()> {
    let urls: Vec<&str> = room.sources();
    let primary = urls.first().copied().unwrap_or(room.url.as_str());
    conn.execute(
        "INSERT INTO rooms(name, url, urls, template_id, agents, split_time, split_size_mb, grace,
           bili_qn, tail_upload, display_name, filename, season_section_id, douyin_quality, tag, position)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15,
           (SELECT COALESCE(MAX(position), 0) + 1 FROM rooms))
         ON CONFLICT(name) DO UPDATE SET url = ?2, urls = ?3, template_id = ?4, agents = ?5,
           split_time = ?6, split_size_mb = ?7, grace = ?8, bili_qn = ?9, tail_upload = ?10,
           display_name = ?11, filename = ?12, season_section_id = ?13,
           douyin_quality = ?14, tag = ?15, deleted = 0",
        params![
            room.name,
            primary,
            serde_json::to_string(&urls).unwrap(),
            room.template_id,
            serde_json::to_string(&room.agents).unwrap(),
            room.split_time,
            room.split_size_mb,
            room.grace,
            room.bili_qn,
            room.tail_upload,
            room.display_name,
            room.filename,
            room.season_section_id,
            room.douyin_quality,
            room.tag,
        ],
    )?;
    Ok(())
}

/// 软删，返回是否删了。
pub fn delete_room(conn: &Connection, name: &str) -> Result<bool> {
    let n = conn.execute(
        "UPDATE rooms SET deleted = 1 WHERE name = ?1 AND deleted = 0",
        [name],
    )?;
    Ok(n > 0)
}

/// 已删除、没有分配、也没有还没投完的稿件的房间：真删掉（之后它的模板才能删），连同封面背景。
pub fn purge_rooms(conn: &Connection) -> Result<usize> {
    let n = conn.execute(
        "DELETE FROM rooms WHERE deleted = 1
           AND name NOT IN (SELECT room FROM assignments)
           AND name NOT IN (SELECT room FROM source_retirements)
           AND name NOT IN (SELECT room FROM archives WHERE phase NOT IN (?1, ?2))",
        [SUBMITTED, DISCARDED],
    )?;
    conn.execute(
        "DELETE FROM cover_bgs WHERE room NOT IN (SELECT name FROM rooms)",
        [],
    )?;
    conn.execute(
        "DELETE FROM room_limits WHERE room NOT IN (SELECT name FROM rooms)",
        [],
    )?;
    Ok(n)
}

/// 房间的封面背景 JPEG，没传过为空。
pub fn cover_bg(conn: &Connection, room: &str) -> Result<Option<Vec<u8>>> {
    conn.query_row("SELECT jpeg FROM cover_bgs WHERE room = ?1", [room], |r| {
        r.get(0)
    })
    .optional()
}

/// 有封面背景的房间名。
pub fn cover_bg_rooms(conn: &Connection) -> Result<std::collections::HashSet<String>> {
    let mut stmt = conn.prepare("SELECT room FROM cover_bgs")?;
    stmt.query_map([], |r| r.get(0))?.collect()
}

/// 设置或去掉（`None`）房间的封面背景。
pub fn set_cover_bg(conn: &Connection, room: &str, jpeg: Option<&[u8]>) -> Result<()> {
    match jpeg {
        Some(j) => conn.execute(
            "INSERT INTO cover_bgs(room, jpeg) VALUES (?1, ?2)
             ON CONFLICT(room) DO UPDATE SET jpeg = ?2",
            params![room, j],
        ),
        None => conn.execute("DELETE FROM cover_bgs WHERE room = ?1", [room]),
    }?;
    Ok(())
}

/// 按给的顺序重排；没列到的房间排在后面、保持原有先后。
pub fn order_rooms(conn: &mut Connection, names: &[String]) -> Result<()> {
    let tx = conn.transaction()?;
    tx.execute("UPDATE rooms SET position = position + ?1", [names.len()])?;
    for (i, name) in names.iter().enumerate() {
        tx.execute(
            "UPDATE rooms SET position = ?2 WHERE name = ?1",
            params![name, i],
        )?;
    }
    tx.commit()
}

/// B 站一份稿件最多的标签数。
pub const TAG_MAX: usize = 10;

/// 投稿用的标签：模板的在前、房间追加的在后，去空去重。不截断，超没超 [`TAG_MAX`] 由调用方处理。
pub fn tags(template: &str, room: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for t in template.split(',').chain(room.split(',')).map(str::trim) {
        if !t.is_empty() && !out.iter().any(|o| o == t) {
            out.push(t.into());
        }
    }
    out
}

/// 投稿模板。`title`/`desc`/`source` 支持 `{name}`、`{title}`、`{url}` 与开播时间的 strftime。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Template {
    pub id: i64,
    pub name: String,
    pub account_id: i64,
    pub title: String,
    pub desc: String,
    /// 分区，171 = 电子竞技。
    pub tid: u16,
    pub human_type2: Option<u16>,
    /// 标签，逗号分隔。
    pub tag: String,
    /// 1 自制，2 转载（转载必须填 `source`）。
    pub copyright: u8,
    pub source: String,
    pub is_only_self: bool,
    /// 投稿不推送粉丝动态。
    pub no_disturbance: bool,
}

impl Default for Template {
    fn default() -> Self {
        Self::from_studio("", 0, &Studio::default())
    }
}

impl Template {
    pub fn from_studio(name: &str, account_id: i64, s: &Studio) -> Self {
        Self {
            id: 0,
            name: name.into(),
            account_id,
            title: s.title.clone(),
            desc: s.desc.clone(),
            tid: s.tid,
            human_type2: s.human_type2,
            tag: s.tag.clone(),
            copyright: s.copyright,
            source: s.source.clone(),
            is_only_self: s.is_only_self == 1,
            no_disturbance: s.no_disturbance == 1,
        }
    }

    pub fn studio(&self) -> Studio {
        Studio {
            title: self.title.clone(),
            tid: self.tid,
            human_type2: self.human_type2,
            tag: self.tag.clone(),
            copyright: self.copyright,
            source: self.source.clone(),
            desc: self.desc.clone(),
            is_only_self: self.is_only_self.into(),
            no_disturbance: self.no_disturbance.into(),
            ..Default::default()
        }
    }
}

fn template_row(r: &Row) -> Result<Template> {
    Ok(Template {
        id: r.get("id")?,
        name: r.get("name")?,
        account_id: r.get("account_id")?,
        title: r.get("title")?,
        desc: r.get("desc")?,
        tid: r.get("tid")?,
        human_type2: r.get("human_type2")?,
        tag: r.get("tag")?,
        copyright: r.get("copyright")?,
        source: r.get("source")?,
        is_only_self: r.get("is_only_self")?,
        no_disturbance: r.get("no_disturbance")?,
    })
}

pub fn templates(conn: &Connection) -> Result<Vec<Template>> {
    let mut stmt = conn.prepare("SELECT * FROM templates ORDER BY name")?;
    stmt.query_map([], template_row)?.collect()
}

pub fn template(conn: &Connection, id: i64) -> Result<Option<Template>> {
    conn.query_row("SELECT * FROM templates WHERE id = ?1", [id], template_row)
        .optional()
}

fn template_id(conn: &Connection, name: &str) -> Result<Option<i64>> {
    conn.query_row("SELECT id FROM templates WHERE name = ?1", [name], |r| {
        r.get(0)
    })
    .optional()
}

/// 新建，返回 id（忽略 `t.id`）。
pub fn insert_template(conn: &Connection, t: &Template) -> Result<i64> {
    conn.execute(
        "INSERT INTO templates(name, account_id, title, \"desc\", tid, tag, copyright, source,
           is_only_self, no_disturbance, human_type2) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
        params![
            t.name,
            t.account_id,
            t.title,
            t.desc,
            t.tid,
            t.tag,
            t.copyright,
            t.source,
            t.is_only_self,
            t.no_disturbance,
            t.human_type2
        ],
    )?;
    Ok(conn.last_insert_rowid())
}

/// 按 `t.id` 整行替换，返回是否有这一行。
pub fn update_template(conn: &Connection, t: &Template) -> Result<bool> {
    let n = conn.execute(
        "UPDATE templates SET name = ?2, account_id = ?3, title = ?4, \"desc\" = ?5, tid = ?6,
           tag = ?7, copyright = ?8, source = ?9, is_only_self = ?10,
           no_disturbance = ?11, human_type2 = ?12
         WHERE id = ?1",
        params![
            t.id,
            t.name,
            t.account_id,
            t.title,
            t.desc,
            t.tid,
            t.tag,
            t.copyright,
            t.source,
            t.is_only_self,
            t.no_disturbance,
            t.human_type2
        ],
    )?;
    Ok(n > 0)
}

/// 还有房间在用时违反外键，返回错误。
pub fn delete_template(conn: &Connection, id: i64) -> Result<bool> {
    Ok(conn.execute("DELETE FROM templates WHERE id = ?1", [id])? > 0)
}

/// 投稿账号（不含登录信息）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AccountInfo {
    pub id: i64,
    pub name: String,
    pub mid: u64,
}

pub fn accounts(conn: &Connection) -> Result<Vec<AccountInfo>> {
    let mut stmt = conn.prepare("SELECT id, name, mid FROM accounts ORDER BY id")?;
    stmt.query_map([], |r| {
        Ok(AccountInfo {
            id: r.get(0)?,
            name: r.get(1)?,
            mid: r.get(2)?,
        })
    })?
    .collect()
}

pub fn account_login(conn: &Connection, id: i64) -> Result<Option<Value>> {
    let login: Option<String> = conn
        .query_row("SELECT login FROM accounts WHERE id = ?1", [id], |r| {
            r.get(0)
        })
        .optional()?;
    Ok(login.and_then(|l| serde_json::from_str(&l).ok()))
}

/// 按 `mid` 新建或更新登录信息，返回 id；重新登录保留已有名称。
pub fn save_account(
    conn: &Connection,
    name: &str,
    mid: u64,
    login: &Value,
    now: i64,
) -> Result<i64> {
    conn.query_row(
        "INSERT INTO accounts(name, mid, login, created_at) VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(mid) DO UPDATE SET login = ?3 RETURNING id",
        params![name, mid, login.to_string(), now],
        |r| r.get(0),
    )
}

pub fn rename_account(conn: &Connection, id: i64, name: &str) -> Result<bool> {
    Ok(conn.execute(
        "UPDATE accounts SET name = ?2 WHERE id = ?1",
        params![id, name],
    )? > 0)
}

/// 令牌刷新后写回。
pub fn set_login(conn: &Connection, id: i64, login: &Value) -> Result<()> {
    conn.execute(
        "UPDATE accounts SET login = ?2 WHERE id = ?1",
        params![id, login.to_string()],
    )?;
    Ok(())
}

/// 还有模板在用时违反外键，返回错误。
pub fn delete_account(conn: &Connection, id: i64) -> Result<bool> {
    Ok(conn.execute("DELETE FROM accounts WHERE id = ?1", [id])? > 0)
}

/// 给分P发凭证用哪个账号：已登记的分P按它的稿件钉住的账号，还没登记的按房间当前模板的账号。
pub fn ticket_account(conn: &Connection, part_id: &str, room: &str) -> Result<Option<i64>> {
    conn.query_row(
        "SELECT COALESCE(
           (SELECT a.account_id FROM parts p JOIN archives a ON a.id = p.archive_id
            WHERE p.part_id = ?1),
           (SELECT t.account_id FROM rooms r JOIN templates t ON t.id = r.template_id
            WHERE r.name = ?2))",
        params![part_id, room],
        |r| r.get(0),
    )
}

pub fn create_session(conn: &Connection, token: &str, expires_at: i64) -> Result<()> {
    conn.execute(
        "INSERT INTO sessions(token, expires_at) VALUES (?1, ?2)",
        params![token, expires_at],
    )?;
    Ok(())
}

pub fn session_valid(conn: &Connection, token: &str, now: i64) -> Result<bool> {
    conn.execute("DELETE FROM sessions WHERE expires_at <= ?1", [now])?;
    conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sessions WHERE token = ?1)",
        [token],
        |r| r.get(0),
    )
}

pub fn delete_session(conn: &Connection, token: &str) -> Result<()> {
    conn.execute("DELETE FROM sessions WHERE token = ?1", [token])?;
    Ok(())
}

/// `machines` 表里的一行。
#[derive(Debug, Clone, PartialEq)]
pub struct MachineRow {
    pub name: String,
    pub token: String,
    pub enabled: bool,
    pub note: String,
    pub created_at: i64,
    /// 让出网络到这一刻（unix 毫秒），可能已过去；判断用 [`yielding`]。
    pub yield_until: Option<i64>,
}

fn machine_row(r: &Row) -> Result<MachineRow> {
    Ok(MachineRow {
        name: r.get("name")?,
        token: r.get("token")?,
        enabled: r.get("enabled")?,
        note: r.get("note")?,
        created_at: r.get("created_at")?,
        yield_until: r.get("yield_until")?,
    })
}

/// 全部机器，按名字排序。
pub fn machines(conn: &Connection) -> Result<Vec<MachineRow>> {
    let mut stmt = conn.prepare("SELECT * FROM machines ORDER BY name")?;
    stmt.query_map([], machine_row)?.collect()
}

/// 按名字查一台机器。
pub fn machine(conn: &Connection, name: &str) -> Result<Option<MachineRow>> {
    conn.query_row(
        "SELECT * FROM machines WHERE name = ?1",
        [name],
        machine_row,
    )
    .optional()
}

/// 按 token 查机器名。
pub fn machine_by_token(conn: &Connection, token: &str) -> Result<Option<String>> {
    conn.query_row("SELECT name FROM machines WHERE token = ?1", [token], |r| {
        r.get(0)
    })
    .optional()
}

/// 机器是否存在且启用；不存在返回 false。
pub fn machine_enabled(conn: &Connection, name: &str) -> Result<bool> {
    Ok(conn
        .query_row(
            "SELECT enabled FROM machines WHERE name = ?1",
            [name],
            |r| r.get::<_, bool>(0),
        )
        .optional()?
        .unwrap_or(false))
}

/// 新增一台机器（默认启用）。
pub fn insert_machine(
    conn: &Connection,
    name: &str,
    token: &str,
    note: &str,
    now: i64,
) -> Result<()> {
    conn.execute(
        "INSERT INTO machines(name, token, enabled, note, created_at) VALUES (?1, ?2, 1, ?3, ?4)",
        params![name, token, note, now],
    )?;
    Ok(())
}

/// 更新机器的启用状态与备注（`None` 的字段不变），返回是否找到这台。
pub fn update_machine(
    conn: &Connection,
    name: &str,
    enabled: Option<bool>,
    note: Option<&str>,
) -> Result<bool> {
    let n = conn.execute(
        "UPDATE machines SET enabled = COALESCE(?2, enabled), note = COALESCE(?3, note)
         WHERE name = ?1",
        params![name, enabled, note],
    )?;
    Ok(n > 0)
}

/// 这台此刻是否在让出网络（`yield_until` 在 `now` 之后）。
pub fn yielding(conn: &Connection, name: &str, now: i64) -> Result<bool> {
    Ok(conn
        .query_row(
            "SELECT yield_until > ?2 FROM machines WHERE name = ?1",
            params![name, now],
            |r| r.get::<_, Option<bool>>(0),
        )
        .optional()?
        .flatten()
        .unwrap_or(false))
}

/// 设置或结束（`None`）让出网络，返回是否找到这台。
pub fn set_yield(conn: &Connection, name: &str, until: Option<i64>) -> Result<bool> {
    let n = conn.execute(
        "UPDATE machines SET yield_until = ?2 WHERE name = ?1",
        params![name, until],
    )?;
    Ok(n > 0)
}

/// 重置机器的 token，返回是否找到这台。
pub fn set_machine_token(conn: &Connection, name: &str, token: &str) -> Result<bool> {
    let n = conn.execute(
        "UPDATE machines SET token = ?2 WHERE name = ?1",
        params![name, token],
    )?;
    Ok(n > 0)
}

/// 这台 agent 名下还没传完（`open` 或 `closed`）的分P个数。
pub fn unfinished_parts(conn: &Connection, agent: &str) -> Result<i64> {
    conn.query_row(
        "SELECT COUNT(*) FROM parts WHERE agent = ?1 AND state IN ('open', 'closed')",
        [agent],
        |r| r.get(0),
    )
}

/// 这台 agent 参与交接（迁出或迁入）的房间名，按房间名排序。
pub fn handoffs_of(conn: &Connection, agent: &str) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(
        "SELECT room FROM handoffs WHERE from_agent = ?1 OR to_agent = ?1 ORDER BY room",
    )?;
    stmt.query_map([agent], |r| r.get(0))?.collect()
}

/// 把机器追加到所有未删除房间的优先列表末尾（已在列表里的不重复加）。
pub fn add_machine_to_rooms(conn: &Connection, name: &str) -> Result<()> {
    update_room_agents(conn, |r| {
        if !r.deleted && !r.agents.iter().any(|a| a == name) {
            r.agents.push(name.to_string());
            true
        } else {
            false
        }
    })
}

fn update_room_agents(conn: &Connection, mut f: impl FnMut(&mut Room) -> bool) -> Result<()> {
    for mut room in rooms(conn)? {
        if f(&mut room) {
            save_room(conn, &room)?;
        }
    }
    Ok(())
}

/// 删除一台机器：清理它的运行表行并从所有房间的 `agents` 列表里去掉它，`parts`/`archives` 保留。调用方负责事务。
pub fn delete_machine(conn: &Connection, name: &str) -> Result<()> {
    conn.execute("DELETE FROM machines WHERE name = ?1", [name])?;
    conn.execute("DELETE FROM agents WHERE name = ?1", [name])?;
    conn.execute("DELETE FROM limits WHERE agent = ?1", [name])?;
    conn.execute("DELETE FROM room_limits WHERE agent = ?1", [name])?;
    conn.execute("DELETE FROM drains WHERE agent = ?1", [name])?;
    conn.execute("DELETE FROM failed_commits WHERE agent = ?1", [name])?;
    update_room_agents(conn, |r| {
        let len = r.agents.len();
        r.agents.retain(|a| a != name);
        r.agents.len() != len
    })
}

/// 把这台 agent 名下 `open`/`closed` 的分P标成 `lost`，返回这些 `part_id`。
pub fn lose_parts(conn: &Connection, agent: &str) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(
        "SELECT part_id FROM parts
         WHERE agent = ?1 AND state IN ('open', 'closed')
         ORDER BY opened_at, part_id",
    )?;
    let ids: Vec<String> = stmt
        .query_map([agent], |r| r.get(0))?
        .collect::<Result<_>>()?;
    conn.execute(
        "UPDATE parts SET state = 'lost' WHERE agent = ?1 AND state IN ('open', 'closed')",
        [agent],
    )?;
    Ok(ids)
}

/// 撤掉这台 agent 的所有分配与交接。
pub fn drop_agent_work(conn: &Connection, agent: &str) -> Result<()> {
    conn.execute("DELETE FROM assignments WHERE agent = ?1", [agent])?;
    conn.execute(
        "DELETE FROM handoffs WHERE from_agent = ?1 OR to_agent = ?1",
        [agent],
    )?;
    Ok(())
}

/// 稿件阶段。
pub fn touch_agent(conn: &Connection, agent: &str, now: i64, status: &Status) -> Result<()> {
    conn.execute(
        "INSERT INTO agents(name, last_seen, status) VALUES (?1, ?2, ?3)
         ON CONFLICT(name) DO UPDATE SET last_seen = ?2, status = ?3",
        params![agent, now, serde_json::to_string(status).unwrap()],
    )?;
    Ok(())
}

/// 最近一次来访时刻与它报的状态。
pub fn agent(conn: &Connection, agent: &str) -> Result<Option<(i64, Status)>> {
    conn.query_row(
        "SELECT last_seen, status FROM agents WHERE name = ?1",
        [agent],
        |r| {
            let status: String = r.get(1)?;
            Ok((r.get(0)?, serde_json::from_str(&status).unwrap_or_default()))
        },
    )
    .optional()
}

/// 房间分给了谁、何时分的。
pub fn assignment(conn: &Connection, room: &str) -> Result<Option<(String, i64)>> {
    conn.query_row(
        "SELECT agent, assigned_at FROM assignments WHERE room = ?1",
        [room],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )
    .optional()
}

/// 改分配，同时结束这个房间进行中的交接（完成、改派都走这里）。
/// 录制参数 `task` 只在房间从没分配到分配时存下：失联改派、交接都沿用这一场开头的那份。
pub fn assign(
    conn: &Connection,
    room: &str,
    agent: &str,
    session: &str,
    now: i64,
    task: &RoomTask,
) -> Result<()> {
    conn.execute(
        "INSERT INTO assignments(room, agent, session, assigned_at, task) VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT(room) DO UPDATE SET agent = ?2, session = ?3, assigned_at = ?4,
           task = COALESCE(task, ?5)",
        params![room, agent, session, now, serde_json::to_string(task).unwrap()],
    )?;
    delete_handoff(conn, room)
}

pub fn unassign(conn: &Connection, room: &str) -> Result<()> {
    conn.execute("DELETE FROM assignments WHERE room = ?1", [room])?;
    delete_handoff(conn, room)
}

pub fn retiring(conn: &Connection, room: &str) -> Result<bool> {
    conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM source_retirements WHERE room = ?1)",
        [room],
        |r| r.get(0),
    )
}

pub fn retirements(conn: &Connection) -> Result<Vec<(String, String)>> {
    let mut stmt = conn.prepare("SELECT room, agent FROM source_retirements")?;
    stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
        .collect()
}

pub fn begin_retirement(
    conn: &Connection,
    room: &str,
    agents: &std::collections::HashSet<String>,
    now: i64,
) -> Result<()> {
    unassign(conn, room)?;
    for agent in agents {
        conn.execute(
            "INSERT INTO source_retirements(room, agent) VALUES (?1, ?2)",
            params![room, agent],
        )?;
    }
    finish_retirement(conn, room, now)?;
    Ok(())
}

pub fn acknowledge_retirement(
    conn: &Connection,
    room: &str,
    agent: &str,
    now: i64,
) -> Result<bool> {
    let n = conn.execute(
        "DELETE FROM source_retirements WHERE room = ?1 AND agent = ?2",
        params![room, agent],
    )?;
    Ok(n > 0 && finish_retirement(conn, room, now)?)
}

fn finish_retirement(conn: &Connection, room: &str, now: i64) -> Result<bool> {
    if retiring(conn, room)? {
        return Ok(false);
    }
    for (id, _) in recording_archives(conn, room)? {
        end_archive(conn, id, now)?;
    }
    Ok(true)
}

/// 这台 agent 该录的房间（分给它的，加上正交接给它的），按房间列表顺序，参数取分配时的快照。
pub fn assigned_tasks(conn: &Connection, agent: &str) -> Result<Vec<RoomTask>> {
    let mut stmt = conn.prepare(
        "SELECT a.task FROM assignments a LEFT JOIN rooms r ON r.name = a.room
         WHERE a.task IS NOT NULL
           AND (a.agent = ?1 OR a.room IN (SELECT room FROM handoffs WHERE to_agent = ?1))
         ORDER BY r.position, a.room",
    )?;
    let tasks = stmt.query_map([agent], |r| r.get::<_, String>(0))?;
    tasks
        .map(|t| Ok(serde_json::from_str(&t?).expect("task snapshot written by assign")))
        .collect()
}

/// 房间这一场的场次键与录制参数快照（同 [`assigned_tasks`] 发给 agent 的那份）。
pub fn assigned_task(conn: &Connection, room: &str) -> Result<Option<(Option<String>, RoomTask)>> {
    let row = conn
        .query_row(
            "SELECT session, task FROM assignments WHERE room = ?1 AND task IS NOT NULL",
            [room],
            |r| Ok((r.get(0)?, r.get::<_, String>(1)?)),
        )
        .optional()?;
    Ok(row.map(|(session, task)| {
        let task = serde_json::from_str(&task).expect("task snapshot written by assign");
        (session, task)
    }))
}

/// 分P所属的稿件 id 与房间。
pub fn part_owner(conn: &Connection, part_id: &str) -> Result<Option<(i64, String)>> {
    conn.query_row(
        "SELECT a.id, a.room FROM parts p JOIN archives a ON a.id = p.archive_id WHERE p.part_id = ?1",
        [part_id],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )
    .optional()
}

/// 某台 agent 分到的房间名（不含交接中的）。
pub fn rooms_of(conn: &Connection, agent: &str) -> Result<Vec<String>> {
    let mut stmt = conn.prepare("SELECT room FROM assignments WHERE agent = ?1 ORDER BY room")?;
    stmt.query_map([agent], |r| r.get(0))?.collect()
}

/// 受限标记。
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Limit {
    pub since: i64,
    pub until: i64,
    pub reason: String,
    /// 判恢复的上传速度门槛（字节/秒）。
    pub need_bps: f64,
}

pub fn limit(conn: &Connection, agent: &str) -> Result<Option<Limit>> {
    conn.query_row(
        "SELECT since, until, reason, need_bps FROM limits WHERE agent = ?1",
        [agent],
        |r| {
            Ok(Limit {
                since: r.get(0)?,
                until: r.get(1)?,
                reason: r.get(2)?,
                need_bps: r.get(3)?,
            })
        },
    )
    .optional()
}

pub fn set_limit(conn: &Connection, agent: &str, l: &Limit) -> Result<()> {
    conn.execute(
        "INSERT OR REPLACE INTO limits(agent, since, until, reason, need_bps) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![agent, l.since, l.until, l.reason, l.need_bps],
    )?;
    Ok(())
}

pub fn clear_limit(conn: &Connection, agent: &str) -> Result<()> {
    conn.execute("DELETE FROM limits WHERE agent = ?1", [agent])?;
    Ok(())
}

/// 房间级限制：`room` 别在 `agent` 上录。过了 `until` 还在表里的是待验证：试拉达标才删。
#[derive(Debug, Clone, PartialEq)]
pub struct RoomLimit {
    pub room: String,
    pub agent: String,
    pub since: i64,
    pub until: i64,
    pub reason: String,
}

fn room_limit_row(r: &rusqlite::Row) -> rusqlite::Result<RoomLimit> {
    Ok(RoomLimit {
        room: r.get("room")?,
        agent: r.get("agent")?,
        since: r.get("since")?,
        until: r.get("until")?,
        reason: r.get("reason")?,
    })
}

pub fn room_limit(conn: &Connection, room: &str, agent: &str) -> Result<Option<RoomLimit>> {
    conn.query_row(
        "SELECT * FROM room_limits WHERE room = ?1 AND agent = ?2",
        [room, agent],
        room_limit_row,
    )
    .optional()
}

/// 这台机器上的房间级限制，按到期先后。
pub fn room_limits_of(conn: &Connection, agent: &str) -> Result<Vec<RoomLimit>> {
    let mut stmt =
        conn.prepare("SELECT * FROM room_limits WHERE agent = ?1 ORDER BY until, room")?;
    stmt.query_map([agent], room_limit_row)?.collect()
}

pub fn set_room_limit(conn: &Connection, l: &RoomLimit) -> Result<()> {
    conn.execute(
        "INSERT OR REPLACE INTO room_limits(room, agent, since, until, reason) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![l.room, l.agent, l.since, l.until, l.reason],
    )?;
    Ok(())
}

pub fn clear_room_limit(conn: &Connection, room: &str, agent: &str) -> Result<()> {
    conn.execute(
        "DELETE FROM room_limits WHERE room = ?1 AND agent = ?2",
        [room, agent],
    )?;
    Ok(())
}

/// 排空标记：这台要换到 `commit`，`since` 起不再给它分新房间；`released_at` 是放行（回 go）的时刻。
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DrainMark {
    pub agent: String,
    pub commit: String,
    pub since: i64,
    pub released_at: Option<i64>,
}

fn drain_row(r: &rusqlite::Row) -> rusqlite::Result<DrainMark> {
    Ok(DrainMark {
        agent: r.get("agent")?,
        commit: r.get("commit_id")?,
        since: r.get("since")?,
        released_at: r.get("released_at")?,
    })
}

pub fn drain(conn: &Connection, agent: &str) -> Result<Option<DrainMark>> {
    conn.query_row("SELECT * FROM drains WHERE agent = ?1", [agent], drain_row)
        .optional()
}

pub fn drains(conn: &Connection) -> Result<Vec<DrainMark>> {
    let mut stmt = conn.prepare("SELECT * FROM drains ORDER BY since")?;
    stmt.query_map([], drain_row)?.collect()
}

/// 记排空。已有同一提交的沿用（保留 `since` 与放行状态），目标换了就重新开始。
pub fn start_drain(conn: &Connection, agent: &str, commit: &str, now: i64) -> Result<()> {
    conn.execute(
        "INSERT INTO drains(agent, commit_id, since) VALUES (?1, ?2, ?3)
         ON CONFLICT(agent) DO UPDATE SET commit_id = ?2, since = ?3, released_at = NULL
         WHERE commit_id <> ?2",
        params![agent, commit, now],
    )?;
    Ok(())
}

pub fn release_drain(conn: &Connection, agent: &str, now: i64) -> Result<()> {
    conn.execute(
        "UPDATE drains SET released_at = ?2 WHERE agent = ?1 AND released_at IS NULL",
        params![agent, now],
    )?;
    Ok(())
}

pub fn delete_drain(conn: &Connection, agent: &str) -> Result<()> {
    conn.execute("DELETE FROM drains WHERE agent = ?1", [agent])?;
    Ok(())
}

pub fn fail_commit(conn: &Connection, commit: &str, agent: &str, now: i64) -> Result<()> {
    conn.execute(
        "INSERT INTO failed_commits(commit_id, agent, at) VALUES (?1, ?2, ?3)",
        params![commit, agent, now],
    )?;
    Ok(())
}

pub fn commit_failed(conn: &Connection, commit: &str) -> Result<bool> {
    conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM failed_commits WHERE commit_id = ?1)",
        [commit],
        |r| r.get(0),
    )
}

/// 某台更新失败过的提交，最近的在前。
pub fn failed_commits(conn: &Connection, agent: &str) -> Result<Vec<(String, i64)>> {
    let mut stmt = conn.prepare(
        "SELECT commit_id, at FROM failed_commits WHERE agent = ?1 ORDER BY at DESC LIMIT 10",
    )?;
    stmt.query_map([agent], |r| Ok((r.get(0)?, r.get(1)?)))?
        .collect()
}

/// 进行中的换机交接。`started_at`：新机报了写出第一个关键帧（总控收到的时刻）；
/// `start_lag`：新机当时落后直播多少毫秒；`start_live`：新机写出的第一帧是直播的哪一刻（unix 毫秒，码流没有 SEI 时为空）。
#[derive(Debug, Clone, PartialEq)]
pub struct Handoff {
    pub from: String,
    pub to: String,
    pub requested_at: i64,
    pub started_at: Option<i64>,
    pub start_lag: Option<i64>,
    pub start_live: Option<i64>,
}

fn handoff_row(r: &rusqlite::Row) -> rusqlite::Result<Handoff> {
    Ok(Handoff {
        from: r.get("from_agent")?,
        to: r.get("to_agent")?,
        requested_at: r.get("requested_at")?,
        started_at: r.get("started_at")?,
        start_lag: r.get("start_lag")?,
        start_live: r.get("start_live")?,
    })
}

pub fn handoff(conn: &Connection, room: &str) -> Result<Option<Handoff>> {
    conn.query_row(
        "SELECT * FROM handoffs WHERE room = ?1",
        [room],
        handoff_row,
    )
    .optional()
}

/// 从这台 agent 交接出去、新机已开录的房间，带上当前分配的场次。
pub fn started_handoffs_from(
    conn: &Connection,
    agent: &str,
) -> Result<Vec<(String, String, Handoff)>> {
    let mut stmt = conn.prepare(
        "SELECT h.*, a.session FROM handoffs h JOIN assignments a ON a.room = h.room
         WHERE h.from_agent = ?1 AND h.started_at IS NOT NULL",
    )?;
    stmt.query_map([agent], |r| {
        Ok((r.get("room")?, r.get("session")?, handoff_row(r)?))
    })?
    .collect()
}

/// 从这台 agent 交接出去、已有切点（新机已报 `start_live`）的房间，按房间名排序。
pub fn cuts_for(conn: &Connection, agent: &str) -> Result<Vec<(String, i64)>> {
    let mut stmt = conn.prepare(
        "SELECT room, start_live FROM handoffs
         WHERE from_agent = ?1 AND started_at IS NOT NULL AND start_live IS NOT NULL
         ORDER BY room",
    )?;
    stmt.query_map([agent], |r| Ok((r.get(0)?, r.get(1)?)))?
        .collect()
}

pub fn start_handoff(conn: &Connection, room: &str, from: &str, to: &str, now: i64) -> Result<()> {
    conn.execute(
        "INSERT OR REPLACE INTO handoffs(room, from_agent, to_agent, requested_at) VALUES (?1, ?2, ?3, ?4)",
        params![room, from, to, now],
    )?;
    Ok(())
}

/// 开播均衡的负载：分配给 `agent` 的房间数加以它为目标、未完成的交接数，不含 `room` 本身（`load-balance.md` §1）。
pub fn load(conn: &Connection, agent: &str, room: &str) -> Result<i64> {
    conn.query_row(
        "SELECT (SELECT COUNT(*) FROM assignments WHERE agent = ?1 AND room <> ?2)
              + (SELECT COUNT(*) FROM handoffs WHERE to_agent = ?1 AND room <> ?2)",
        params![agent, room],
        |r| r.get(0),
    )
}

pub fn delete_handoff(conn: &Connection, room: &str) -> Result<()> {
    conn.execute("DELETE FROM handoffs WHERE room = ?1", [room])?;
    Ok(())
}

/// 房间在 `agent` 上正在写的分P从什么时候开录（迁回对准切段用）。
pub fn open_part_since(conn: &Connection, room: &str, agent: &str) -> Result<Option<i64>> {
    conn.query_row(
        "SELECT MAX(p.opened_at) FROM parts p JOIN archives a ON a.id = p.archive_id
         WHERE a.room = ?1 AND p.agent = ?2 AND p.state = 'open'",
        params![room, agent],
        |r| r.get(0),
    )
}

/// 房间还有没下播的稿件（agent 还在录或在下播宽限里）。
pub fn recording(conn: &Connection, room: &str) -> Result<bool> {
    conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM archives WHERE room = ?1 AND phase = ?2)",
        params![room, RECORDING],
        |r| r.get(0),
    )
}

/// 开播时刻、直播标题、稿件来源、各分P（按开录先后）。
pub type Session = (i64, String, Option<String>, Vec<SessionPart>);

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SessionPart {
    #[serde(skip)]
    pub part_id: String,
    /// `open`/`closed`/`uploaded`/`lost`/`empty`（录制侧删掉的空分段）。
    pub state: String,
    pub agent: String,
    pub opened_at: i64,
    pub closed_at: Option<i64>,
    pub size: Option<i64>,
}

/// 房间这一场（最新一份没下播的稿件）。
pub fn current_session(conn: &Connection, room: &str) -> Result<Option<Session>> {
    let Some((id, started, title, source_url)) = conn
        .query_row(
            "SELECT id, started_at, title, source_url FROM archives WHERE room = ?1 AND phase = ?2
             ORDER BY id DESC LIMIT 1",
            params![room, RECORDING],
            |r| Ok((r.get::<_, i64>(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .optional()?
    else {
        return Ok(None);
    };
    let mut stmt = conn.prepare(
        "SELECT part_id, state, agent, opened_at, closed_at, size FROM parts
         WHERE archive_id = ?1 ORDER BY opened_at, part_id",
    )?;
    let parts = stmt
        .query_map([id], |r| {
            Ok(SessionPart {
                part_id: r.get(0)?,
                state: r.get(1)?,
                agent: r.get(2)?,
                opened_at: r.get(3)?,
                closed_at: r.get(4)?,
                size: r.get(5)?,
            })
        })?
        .collect::<Result<_>>()?;
    Ok(Some((started, title, source_url, parts)))
}

/// 房间最近一场已结束直播的开录时刻（unix 毫秒）。
pub fn last_live_started_at(conn: &Connection, room: &str) -> Result<Option<i64>> {
    conn.query_row(
        "SELECT MAX(started_at) FROM archives WHERE room = ?1 AND ended_at IS NOT NULL",
        [room],
        |r| r.get(0),
    )
}

/// 一批事件在一个事务里落库。
pub fn apply_all(conn: &mut Connection, agent: &str, events: &[AgentEvent]) -> Result<()> {
    let tx = conn.transaction()?;
    for ev in events {
        apply(&tx, agent, ev)?;
    }
    tx.commit()
}

pub fn apply(conn: &Connection, agent: &str, event: &AgentEvent) -> Result<()> {
    match event {
        AgentEvent::Recorder { event, at } => match event {
            Event::PartOpened(p) => {
                part_seen(
                    conn,
                    agent,
                    &p.owner,
                    &p.part_id,
                    &p.path,
                    &p.title,
                    *at,
                    p.tz_offset_secs,
                    p.source_url.as_deref(),
                )?;
            }
            Event::PartClosed(p) => {
                let opened = at - p.duration.as_millis() as i64;
                part_seen(
                    conn,
                    agent,
                    &p.owner,
                    &p.part_id,
                    &p.path,
                    &p.title,
                    opened,
                    None,
                    p.source_url.as_deref(),
                )?;
                let updated = conn.execute(
                    "UPDATE parts SET state = 'closed', path = ?2, size = ?3, closed_at = ?4
                     WHERE part_id = ?1 AND state = 'open'",
                    params![p.part_id, p.path.to_string_lossy(), p.size as i64, at],
                )?;
                if updated > 0 {
                    // 换源退场收尾时若 `end_archive` 先于 `PartClosed` 到达，补齐最新的 `hold_until`。
                    conn.execute(
                        &format!(
                            "UPDATE archives SET hold_until = MAX(hold_until, {HOLD_UNTIL})
                             WHERE id = (SELECT archive_id FROM parts WHERE part_id = ?1)
                               AND phase = ?2 AND hold_until IS NOT NULL"
                        ),
                        params![p.part_id, ENDED],
                    )?;
                    join_late_source(conn, &p.part_id)?;
                }
            }
            Event::PartDiscarded(p) => {
                let opened = at - p.duration.as_millis() as i64;
                part_seen(
                    conn,
                    agent,
                    &p.owner,
                    &p.part_id,
                    &p.path,
                    &p.title,
                    opened,
                    None,
                    p.source_url.as_deref(),
                )?;
                conn.execute(
                    "UPDATE parts SET state = 'empty', size = ?2, closed_at = ?3
                     WHERE part_id = ?1 AND state = 'open'",
                    params![p.part_id, p.size as i64, at],
                )?;
            }
            Event::SessionEnded(owner) => {
                let n = conn.execute(
                    &format!(
                        "UPDATE archives SET phase = ?4, ended_at = ?3, hold_until = {HOLD_UNTIL}
                         WHERE room = ?1 AND session = ?2 AND phase = ?5 AND started_at <= ?3"
                    ),
                    params![owner.room, owner.session, at, ENDED, RECORDING],
                )?;
                if n > 0 {
                    info!(
                        room = owner.room,
                        session = owner.session,
                        agent,
                        "session ended"
                    );
                }
            }
        },
        AgentEvent::Uploaded {
            part_id,
            filename,
            line,
            size,
        } => {
            let n = conn.execute(
                "UPDATE parts SET state = 'uploaded', filename = ?2, line = ?3, size = ?4
                 WHERE part_id = ?1",
                params![part_id, filename, line, *size as i64],
            )?;
            if n == 0 {
                // 不该发生（agent 总是先报 PartOpened/PartClosed）。原片在 UPOS 上，记下文件名供人工找回。
                error!(
                    part_id,
                    filename, line, agent, "uploaded part is unknown, recorded only in this log"
                );
            }
        }
        AgentEvent::Recording {
            room,
            lag_ms,
            start_live_ms,
        } => {
            // 用总控自己的时钟：交接的重叠等待按它算，不受各机时钟偏差影响。
            let now = chrono::Utc::now().timestamp_millis();
            let n = conn.execute(
                "UPDATE handoffs SET started_at = ?3, start_lag = ?4, start_live = ?5
                 WHERE room = ?1 AND to_agent = ?2 AND started_at IS NULL",
                params![room, agent, now, lag_ms, start_live_ms],
            )?;
            if n > 0 {
                info!(
                    room,
                    agent,
                    ?lag_ms,
                    ?start_live_ms,
                    "handoff target is recording"
                );
            }
        }
        AgentEvent::Lost { part_id } => {
            conn.execute(
                "UPDATE parts SET state = 'lost' WHERE part_id = ?1 AND state != 'uploaded'",
                [part_id],
            )?;
        }
    }
    Ok(())
}

/// 第一次见到这个分P就登记，归入同一场次键下 `opened_at` 时还没结束的稿件，没有就开一份新的。
#[allow(clippy::too_many_arguments)]
fn part_seen(
    conn: &Connection,
    agent: &str,
    owner: &Owner,
    part_id: &str,
    path: &Path,
    title: &str,
    opened_at: i64,
    tz_offset: Option<i32>,
    source_url: Option<&str>,
) -> Result<()> {
    let known: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM parts WHERE part_id = ?1)",
        [part_id],
        |r| r.get(0),
    )?;
    if known {
        return Ok(());
    }
    let archive: Option<i64> = conn
        .query_row(
            "SELECT a.id FROM archives a
             WHERE a.room = ?1 AND a.phase IN (?3, ?4)
               AND (a.ended_at IS NULL OR a.ended_at > ?5)
               AND (a.session = ?2 OR (?2 != '' AND a.started_at <= ?5 AND EXISTS (
                 SELECT 1 FROM parts p WHERE p.archive_id = a.id AND p.session = ?2
               )))
             ORDER BY (a.session = ?2) DESC, a.id DESC LIMIT 1",
            params![owner.room, owner.session, RECORDING, ENDED, opened_at],
            |r| r.get(0),
        )
        .optional()?;
    let archive = match archive.map_or_else(|| rejoin(conn, owner, opened_at), |id| Ok(Some(id)))? {
        Some(id) => id,
        None => {
            let late: bool = conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM archives WHERE room = ?1 AND session = ?2
                   AND ended_at > ?3)",
                params![owner.room, owner.session, opened_at],
                |r| r.get(0),
            )?;
            if late {
                warn!(
                    part_id,
                    room = owner.room,
                    "part belongs to an archive already submitted, starting a new one"
                );
            }
            // 账号与实际录制的直播间地址在开稿件时（首个分P）钉住：
            // 新 agent 直接在事件里带上 `source_url`；旧 agent 回退到当前分配快照里的地址。
            let source_url = source_url
                .filter(|u| !u.is_empty())
                .map(str::to_string)
                .or_else(|| {
                    assigned_task(conn, &owner.room)
                        .ok()
                        .flatten()
                        .map(|(_, task)| task.url)
                });
            conn.execute(
                "INSERT INTO archives(room, session, title, started_at, phase, account_id, source_url)
                 VALUES (?1, ?2, ?3, ?4, ?5,
                   (SELECT t.account_id FROM rooms r JOIN templates t ON t.id = r.template_id
                    WHERE r.name = ?1),
                   ?6)",
                params![owner.room, owner.session, title, opened_at, RECORDING, source_url],
            )?;
            conn.last_insert_rowid()
        }
    };
    conn.execute(
        "INSERT INTO parts(part_id, archive_id, agent, path, opened_at, state, tz_offset, title, session)
         VALUES (?1, ?2, ?3, ?4, ?5, 'open', ?6, ?7, ?8)",
        params![
            part_id,
            archive,
            agent,
            path.to_string_lossy(),
            opened_at,
            tz_offset,
            title,
            owner.session,
        ],
    )?;
    Ok(())
}

/// 旧来源的事件晚到时，用真实的分P收尾时刻判断能否接到下一份稿件。
fn join_late_source(conn: &Connection, part_id: &str) -> Result<()> {
    let pair: Option<(i64, i64)> = conn
        .query_row(
            "SELECT old.id, next.id
             FROM parts part JOIN archives old ON old.id = part.archive_id
             JOIN rooms room ON room.name = old.room
             JOIN archives next ON next.id = (
                 SELECT id FROM archives
                 WHERE room = old.room AND started_at > old.started_at
                 ORDER BY started_at, id LIMIT 1
             )
             JOIN parts last ON last.archive_id = old.id AND last.closed_at = (
                 SELECT MAX(closed_at) FROM parts WHERE archive_id = old.id
             )
             WHERE part.part_id = ?1 AND json_array_length(room.urls) > 1
               AND old.session != next.session AND old.source_url != next.source_url
               AND old.account_id IS next.account_id
               AND (old.phase = ?2 OR (old.phase = ?3 AND old.hold_until IS NOT NULL))
               AND (next.phase = ?2 OR (next.phase = ?3 AND next.hold_until IS NOT NULL))
               AND next.started_at BETWEEN last.closed_at AND last.closed_at + COALESCE(room.grace, 300) * 1000",
            params![part_id, RECORDING, ENDED],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    if let Some((old, next)) = pair {
        conn.execute(
            "UPDATE archives
             SET (started_at, title, source_url) = (
                 SELECT started_at, title, source_url FROM archives WHERE id = ?1
             ) WHERE id = ?2",
            params![old, next],
        )?;
        conn.execute(
            "UPDATE parts SET archive_id = ?2 WHERE archive_id = ?1",
            params![old, next],
        )?;
        conn.execute("DELETE FROM archives WHERE id = ?1", [old])?;
        info!(old, next, "late parts joined the next source archive");
    }
    Ok(())
}

/// 下播宽限内重新开录、但场次键变了（抖音断开重开会换 room_id，交接或换房时两段也不同）：
/// 找房间最近一份还没投、最后收尾距 `opened_at` 不超过宽限的稿件，改成新场次键并回录制中；
/// 原稿件开稿时钉住的 `source_url` 保留不变。
fn rejoin(conn: &Connection, owner: &Owner, opened_at: i64) -> Result<Option<i64>> {
    let id: Option<i64> = conn
        .query_row(
            &format!(
                "SELECT id FROM archives
                 WHERE room = ?1 AND started_at <= ?3
                   AND ((phase = ?4 AND hold_until >= ?3) OR (phase = ?5 AND session != ?2 AND {HOLD_UNTIL} >= ?3))
                 ORDER BY id DESC LIMIT 1"
            ),
            params![owner.room, owner.session, opened_at, ENDED, RECORDING],
            |r| r.get(0),
        )
        .optional()?;
    if let Some(id) = id {
        conn.execute(
            "UPDATE archives SET session = ?2, phase = ?3, ended_at = NULL, hold_until = NULL
             WHERE id = ?1",
            params![id, owner.session, RECORDING],
        )?;
        info!(
            room = owner.room,
            id,
            session = owner.session,
            "reconnected within grace, continuing archive"
        );
    }
    Ok(id)
}

/// 房间还在录制中的稿件：(稿件 id, 场次键)。
pub fn recording_archives(conn: &Connection, room: &str) -> Result<Vec<(i64, String)>> {
    let mut stmt = conn
        .prepare("SELECT id, session FROM archives WHERE room = ?1 AND phase = ?2 ORDER BY id")?;
    stmt.query_map(params![room, RECORDING], |r| Ok((r.get(0)?, r.get(1)?)))?
        .collect()
}

/// 稿件有分P还在写（agent 还在录它）。
pub fn has_open_parts(conn: &Connection, archive: i64) -> Result<bool> {
    conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM parts WHERE archive_id = ?1 AND state = 'open')",
        [archive],
        |r| r.get(0),
    )
}

/// 总控替 agent 结束一份稿件（agent 没报场次结束时的兜底），之后照常投稿。
pub fn end_archive(conn: &Connection, archive: i64, now: i64) -> Result<bool> {
    let n = conn.execute(
        &format!(
            "UPDATE archives SET phase = ?3, ended_at = ?2, hold_until = {HOLD_UNTIL}
             WHERE id = ?1 AND phase = ?4"
        ),
        params![archive, now, ENDED, RECORDING],
    )?;
    Ok(n > 0)
}

/// 交接、改派完成时要用的场次：分配记下检测到的最新场次键。
pub fn set_assignment_session(conn: &Connection, room: &str, session: &str) -> Result<()> {
    conn.execute(
        "UPDATE assignments SET session = ?2 WHERE room = ?1 AND session IS NOT ?2",
        params![room, session],
    )?;
    Ok(())
}

/// 手动场末：把房间没结束的稿件标为结束，撤掉分配让 agent 停录，并记下场次键不再分配这一场。
/// 返回结束了几份稿件。
pub fn end_room(conn: &Connection, room: &str, now: i64) -> Result<usize> {
    let session: Option<String> = conn
        .query_row(
            "SELECT session FROM assignments WHERE room = ?1",
            [room],
            |r| r.get(0),
        )
        .optional()?
        .flatten();
    if let Some(session) = session {
        conn.execute(
            "INSERT OR REPLACE INTO skips(room, session) VALUES (?1, ?2)",
            params![room, session],
        )?;
    }
    unassign(conn, room)?;
    conn.execute(
        "UPDATE archives SET phase = ?3, ended_at = ?2 WHERE room = ?1 AND phase = ?4",
        params![room, now, ENDED, RECORDING],
    )
}

/// 这一场在控制台结束过，不再分配。
pub fn skipped(conn: &Connection, room: &str, session: &str) -> Result<bool> {
    conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM skips WHERE room = ?1 AND session = ?2)",
        params![room, session],
        |r| r.get(0),
    )
}

/// 丢弃一份下播后还没投的稿件（`ended`/`attention`），返回是否改了。
pub fn discard_archive(conn: &Connection, id: i64) -> Result<bool> {
    let n = conn.execute(
        "UPDATE archives SET phase = ?2 WHERE id = ?1 AND phase IN (?3, ?4)",
        params![id, DISCARDED, ENDED, ATTENTION],
    )?;
    Ok(n > 0)
}

/// 分P所属稿件已丢弃。
pub fn part_discarded(conn: &Connection, part_id: &str) -> Result<bool> {
    conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM parts p JOIN archives a ON a.id = p.archive_id
           WHERE p.part_id = ?1 AND a.phase = ?2)",
        params![part_id, DISCARDED],
        |r| r.get(0),
    )
}

/// 开始提交：只有还是 `ended` 的才标「提交中」（准备期间可能被丢弃），返回是否标上。
pub fn begin_submit(conn: &Connection, id: i64) -> Result<bool> {
    let n = conn.execute(
        "UPDATE archives SET phase = ?2, note = NULL WHERE id = ?1 AND phase = ?3",
        params![id, SUBMITTING, ENDED],
    )?;
    Ok(n > 0)
}

/// 已结束、过了合并期限、且分P全部传完或确认丢失的稿件。
pub fn ready(conn: &Connection, now: i64) -> Result<Vec<i64>> {
    let mut stmt = conn.prepare(
        "SELECT id FROM archives a WHERE phase = ?1 AND (hold_until IS NULL OR hold_until <= ?2)
           AND NOT EXISTS (
           SELECT 1 FROM parts p WHERE p.archive_id = a.id AND p.state IN ('open', 'closed'))
         ORDER BY id",
    )?;
    stmt.query_map(params![ENDED, now], |r| r.get(0))?.collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchivePart {
    pub path: String,
    pub filename: String,
    pub opened_at: i64,
    pub tz_offset: Option<i32>,
    pub title: String,
}

pub struct Archive {
    pub room: String,
    /// 开稿件时钉住的账号；房间当时不在库里则为空。
    pub account_id: Option<i64>,
    /// 稿件首个分P实际录制的直播间地址；历史稿件为空（投稿时回退房间主房地址）。
    pub source_url: Option<String>,
    pub title: String,
    pub started_at: i64,
    /// 已上传的分P，按开录时刻排序。
    pub parts: Vec<ArchivePart>,
}

pub fn archive(conn: &Connection, id: i64) -> Result<Archive> {
    let (room, title, started_at, account_id, source_url) = conn.query_row(
        "SELECT room, title, started_at, account_id, source_url FROM archives WHERE id = ?1",
        [id],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
    )?;
    let mut stmt = conn.prepare(
        "SELECT path, filename, opened_at, tz_offset, title FROM parts
         WHERE archive_id = ?1 AND state = 'uploaded'
         ORDER BY opened_at, part_id",
    )?;
    let parts = stmt
        .query_map([id], |r| {
            Ok(ArchivePart {
                path: r.get(0)?,
                filename: r.get(1)?,
                opened_at: r.get(2)?,
                tz_offset: r.get(3)?,
                title: r.get(4)?,
            })
        })?
        .collect::<Result<_>>()?;
    Ok(Archive {
        room,
        account_id,
        source_url,
        title,
        started_at,
        parts,
    })
}

pub fn set_phase(
    conn: &Connection,
    id: i64,
    phase: &str,
    note: Option<&str>,
    bvid: Option<&str>,
) -> Result<()> {
    conn.execute(
        "UPDATE archives SET phase = ?2, note = ?3, bvid = ?4 WHERE id = ?1",
        params![id, phase, note, bvid],
    )?;
    Ok(())
}

/// 投稿成功：记下 bvid、aid 与要加入的合集分区（加入成功后由 [`season_done`] 清空）。
pub fn submitted(
    conn: &Connection,
    id: i64,
    bvid: &str,
    aid: u64,
    season_section_id: Option<i64>,
    now: i64,
) -> Result<()> {
    set_phase(conn, id, SUBMITTED, None, Some(bvid))?;
    conn.execute(
        "UPDATE archives SET aid = ?2, season_section_id = ?3, submitted_at = ?4 WHERE id = ?1",
        params![id, aid, season_section_id, now],
    )?;
    Ok(())
}

/// 已投稿、还没加进合集的稿件：(id, 账号 id, aid, section_id)。
pub type SeasonPending = (i64, Option<i64>, u64, i64);

pub fn season_pending(conn: &Connection) -> Result<Vec<SeasonPending>> {
    let mut stmt = conn.prepare(
        "SELECT id, account_id, aid, season_section_id FROM archives
         WHERE phase = ?1 AND season_section_id IS NOT NULL ORDER BY id",
    )?;
    stmt.query_map([SUBMITTED], |r| {
        Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))
    })?
    .collect()
}

/// 加入合集成功或放弃：不再尝试。
pub fn season_done(conn: &Connection, id: i64) -> Result<()> {
    conn.execute(
        "UPDATE archives SET season_section_id = NULL WHERE id = ?1",
        [id],
    )?;
    Ok(())
}

/// 上传凭证约 5 天后失效（biliup 实测），过期的不留。
const UPOS_TICKET_TTL: i64 = 5 * 24 * 3600 * 1000;

/// 记下分P刚签发的上传凭证（同一分P重签覆盖：agent 只用最新一张），顺手清掉过期的。
pub fn save_upos_ticket(conn: &Connection, part_id: &str, ticket: &Value, now: i64) -> Result<()> {
    conn.execute(
        "DELETE FROM upos_tickets WHERE issued_at <= ?1",
        [now - UPOS_TICKET_TTL],
    )?;
    let field = |k: &str| ticket[k].as_str().unwrap_or_default().to_string();
    conn.execute(
        "INSERT OR REPLACE INTO upos_tickets(part_id, endpoint, upos_uri, auth, issued_at)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            part_id,
            field("endpoint"),
            field("upos_uri"),
            field("auth"),
            now
        ],
    )?;
    Ok(())
}

/// 可取回的分P。
pub struct UposTicket {
    pub part_id: String,
    pub path: String,
    pub size: Option<i64>,
    pub url: String,
    pub auth: String,
    pub issued_at: i64,
}

/// 已上传、凭证未过期且就是传成功的那张（文件名对得上）、`part_id` 等于或路径包含 `q` 的分P，按开录时间排。
pub fn upos_tickets(conn: &Connection, q: &str, now: i64) -> Result<Vec<UposTicket>> {
    let mut stmt = conn.prepare(
        "SELECT p.part_id, p.path, p.size, t.endpoint, t.upos_uri, t.auth, t.issued_at
         FROM upos_tickets t JOIN parts p ON p.part_id = t.part_id
         WHERE p.state = 'uploaded' AND t.issued_at > ?2 AND instr(t.upos_uri, p.filename) > 0
           AND (p.part_id = ?1 OR instr(p.path, ?1) > 0)
         ORDER BY p.opened_at",
    )?;
    stmt.query_map(params![q, now - UPOS_TICKET_TTL], |r| {
        let endpoint: String = r.get(3)?;
        let uri: String = r.get(4)?;
        let scheme = if endpoint.starts_with("//") {
            "https:"
        } else {
            ""
        };
        Ok(UposTicket {
            part_id: r.get(0)?,
            path: r.get(1)?,
            size: r.get(2)?,
            url: format!("{scheme}{endpoint}/{}", uri.trim_start_matches("upos://")),
            auth: r.get(5)?,
            issued_at: r.get(6)?,
        })
    })?
    .collect()
}

/// 没有任何已上传分P的稿件：连同它的分P一起删掉。
pub fn delete_archive(conn: &Connection, id: i64) -> Result<()> {
    conn.execute("DELETE FROM parts WHERE archive_id = ?1", [id])?;
    conn.execute("DELETE FROM archives WHERE id = ?1", [id])?;
    Ok(())
}

/// 启动时：停在「提交中」的稿件结果未知，交给人，返回它们的 (id, room)。
pub fn interrupted_submits(conn: &Connection) -> Result<Vec<(i64, String)>> {
    let mut stmt = conn.prepare("SELECT id, room FROM archives WHERE phase = ?1")?;
    let rows: Vec<(i64, String)> = stmt
        .query_map([SUBMITTING], |r| Ok((r.get(0)?, r.get(1)?)))?
        .collect::<Result<_>>()?;
    for (id, _) in &rows {
        set_phase(conn, *id, ATTENTION, Some("submit outcome unknown"), None)?;
    }
    Ok(rows)
}

/// 稿件是否存在。
pub fn archive_exists(conn: &Connection, id: i64) -> Result<bool> {
    conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM archives WHERE id = ?1)",
        [id],
        |r| r.get(0),
    )
}

/// 控制台「待投」的一份稿件：下播后还没投出去的（`ended`/`submitting`/`attention`）。
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PendingArchive {
    pub id: i64,
    pub room: String,
    /// 投稿用的账号：稿件钉住的，没有就用房间当前模板的；都没有为空。
    pub account_id: Option<i64>,
    pub title: String,
    pub started_at: i64,
    pub ended_at: Option<i64>,
    pub phase: String,
    pub note: Option<String>,
    /// 已传的分P数。
    pub parts_uploaded: i64,
    /// 分P总数，不含 `lost`/`empty`。
    pub parts_total: i64,
    /// 还没传完的分P数（`open`/`closed`）；不序列化给前端。
    #[serde(skip)]
    pub parts_unfinished: i64,
    /// 合并期限，见 [`MIGRATION_17`]；不序列化给前端。
    #[serde(skip)]
    pub hold_until: Option<i64>,
}

/// 下播后还没投出去的稿件，按 id 升序。
pub fn pending_archives(conn: &Connection) -> Result<Vec<PendingArchive>> {
    let mut stmt = conn.prepare(
        "SELECT a.id, a.room, COALESCE(a.account_id, t.account_id), a.title, a.started_at,
                a.ended_at, a.phase, a.note,
                (SELECT COUNT(*) FROM parts p WHERE p.archive_id = a.id AND p.state = 'uploaded'),
                (SELECT COUNT(*) FROM parts p WHERE p.archive_id = a.id AND p.state NOT IN ('lost', 'empty')),
                (SELECT COUNT(*) FROM parts p WHERE p.archive_id = a.id AND p.state IN ('open', 'closed')),
                a.hold_until
         FROM archives a
         LEFT JOIN rooms r ON r.name = a.room
         LEFT JOIN templates t ON t.id = r.template_id
         WHERE a.phase IN (?1, ?2, ?3)
         ORDER BY a.id",
    )?;
    stmt.query_map(params![ENDED, SUBMITTING, ATTENTION], |r| {
        Ok(PendingArchive {
            id: r.get(0)?,
            room: r.get(1)?,
            account_id: r.get(2)?,
            title: r.get(3)?,
            started_at: r.get(4)?,
            ended_at: r.get(5)?,
            phase: r.get(6)?,
            note: r.get(7)?,
            parts_uploaded: r.get(8)?,
            parts_total: r.get(9)?,
            parts_unfinished: r.get(10)?,
            hold_until: r.get(11)?,
        })
    })?
    .collect()
}

/// 控制台「已投」的一份稿件。
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SubmittedArchive {
    pub id: i64,
    pub room: String,
    pub title: String,
    pub started_at: i64,
    /// 投出时刻；迁移 11 之前投的没有记录，用下播时刻代替。
    pub submitted_at: i64,
    pub bvid: String,
}

/// `since`（unix 毫秒）之后投出的稿件，最近的在前。
pub fn submitted_since(conn: &Connection, since: i64) -> Result<Vec<SubmittedArchive>> {
    let mut stmt = conn.prepare(
        "SELECT id, room, title, started_at, COALESCE(submitted_at, ended_at, started_at) AS at, bvid
         FROM archives
         WHERE phase = ?1 AND bvid IS NOT NULL AND COALESCE(submitted_at, ended_at, started_at) >= ?2
         ORDER BY at DESC, id DESC",
    )?;
    stmt.query_map(params![SUBMITTED, since], |r| {
        Ok(SubmittedArchive {
            id: r.get(0)?,
            room: r.get(1)?,
            title: r.get(2)?,
            started_at: r.get(3)?,
            submitted_at: r.get(4)?,
            bvid: r.get(5)?,
        })
    })?
    .collect()
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use lubic_events::{CloseReason, PartClosed, PartOpened};
    use std::time::Duration;

    #[test]
    fn tags_put_template_first_then_room_without_blanks_or_repeats() {
        assert_eq!(tags(" a,b,,", "b, c ,a,d"), ["a", "b", "c", "d"]);
        assert!(tags("", "").is_empty());
    }

    /// 空库：一个账号（id 1）、一个模板（id 1），机器 a/b/c，房间 `r`（agents a、b）与 `r2`（agents c、a）。
    pub fn mem() -> Connection {
        let mut conn = Connection::open_in_memory().unwrap();
        init(&mut conn, &[], || Ok(Import::default())).unwrap();
        let login = serde_json::json!({"token_info": {"mid": 1}});
        save_account(&conn, "acc", 1, &login, 0).unwrap();
        let tpl = Template {
            name: "t".into(),
            account_id: 1,
            title: "{name}-{title}-%Y".into(),
            source: "{url}".into(),
            copyright: 2,
            ..Default::default()
        };
        insert_template(&conn, &tpl).unwrap();
        for (name, token) in [("a", "ta"), ("b", "tb"), ("c", "tc")] {
            insert_machine(&conn, name, token, "", 0).unwrap();
        }
        let room = |name: &str, url: &str, agents: &[&str]| Room {
            name: name.into(),
            url: url.into(),
            urls: vec![url.into()],
            template_id: 1,
            agents: agents.iter().map(|a| a.to_string()).collect(),
            split_time: None,
            split_size_mb: None,
            grace: None,
            bili_qn: None,
            douyin_quality: None,
            tail_upload: false,
            display_name: None,
            filename: DEFAULT_FILENAME.into(),
            season_section_id: None,
            tag: String::new(),
            deleted: false,
        };
        save_room(
            &conn,
            &Room {
                split_time: Some(120),
                ..room("r", "https://live.bilibili.com/6", &["a", "b"])
            },
        )
        .unwrap();
        save_room(
            &conn,
            &room("r2", "https://live.bilibili.com/7", &["c", "a"]),
        )
        .unwrap();
        conn
    }

    fn owner(session: &str) -> Owner {
        Owner {
            room: "r".into(),
            session: session.into(),
        }
    }

    pub fn opened(part: &str, session: &str, at: i64) -> AgentEvent {
        AgentEvent::Recorder {
            event: Event::PartOpened(PartOpened {
                owner: owner(session),
                part_id: part.into(),
                path: format!("/rec/{part}.flv.part").into(),
                title: "直播".into(),
                tz_offset_secs: Some(8 * 3600),
                source_url: None,
            }),
            at,
        }
    }

    pub fn closed(part: &str, session: &str, at: i64) -> AgentEvent {
        AgentEvent::Recorder {
            event: Event::PartClosed(PartClosed {
                owner: owner(session),
                part_id: part.into(),
                path: format!("/rec/{part}.flv").into(),
                duration: Duration::from_millis(10),
                size: 5,
                reason: CloseReason::TimedSplit,
                title: "直播".into(),
                closed_at_ms: None,
                source_url: None,
            }),
            at,
        }
    }

    pub fn uploaded(part: &str) -> AgentEvent {
        AgentEvent::Uploaded {
            part_id: part.into(),
            filename: format!("n-{part}"),
            line: "tx".into(),
            size: 5,
        }
    }

    pub fn ended(session: &str, at: i64) -> AgentEvent {
        AgentEvent::Recorder {
            event: Event::SessionEnded(owner(session)),
            at,
        }
    }

    fn states(conn: &Connection) -> Vec<(String, i64, String)> {
        let mut stmt = conn
            .prepare("SELECT part_id, archive_id, state FROM parts ORDER BY part_id")
            .unwrap();
        stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
            .unwrap()
            .collect::<Result<_>>()
            .unwrap()
    }

    /// 停在任一旧版本的库都补齐后面的迁移，再开一次什么也不做。第 3 版填过的封面名留作显示名；
    /// 第 6 版模板上的合集挪到用它的房间。
    /// 取回凭证：只给传成功的那张、5 天内的；重签覆盖旧的，过期的签发时顺手删掉。
    #[test]
    fn upos_tickets_keep_the_used_one_for_five_days() {
        let mut conn = mem();
        let day = 24 * 3600 * 1000;
        let t = |uri: &str| serde_json::json!({"endpoint": "//e.bilivideo.com", "upos_uri": uri, "auth": "A", "chunk_size": 1});
        apply_all(
            &mut conn,
            "a",
            &[
                opened("p1", "s", 1),
                opened("p2", "s", 2),
                closed("p2", "s", 3),
            ],
        )
        .unwrap();
        save_upos_ticket(&conn, "p1", &t("upos://b/old.flv"), 0).unwrap();
        save_upos_ticket(&conn, "p1", &t("upos://b/n-p1.flv"), day).unwrap();
        save_upos_ticket(&conn, "p2", &t("upos://b/n-other.flv"), day).unwrap();
        assert!(
            upos_tickets(&conn, "p1", day).unwrap().is_empty(),
            "还没传完"
        );
        apply_all(
            &mut conn,
            "a",
            &[closed("p1", "s", 2), uploaded("p1"), uploaded("p2")],
        )
        .unwrap();
        let found = upos_tickets(&conn, "/rec/p", day).unwrap();
        assert_eq!(found.len(), 1, "p2 的凭证不是传成功的那张");
        assert_eq!(found[0].part_id, "p1");
        assert_eq!(found[0].url, "https://e.bilivideo.com/b/n-p1.flv");
        assert_eq!(found[0].auth, "A");
        assert_eq!(found[0].size, Some(5));
        assert!(
            upos_tickets(&conn, "p1", 6 * day + 1).unwrap().is_empty(),
            "过期"
        );
        save_upos_ticket(&conn, "p3", &t("upos://b/x.flv"), 6 * day + 1).unwrap();
        let left: i64 = conn
            .query_row("SELECT COUNT(*) FROM upos_tickets", [], |r| r.get(0))
            .unwrap();
        assert_eq!(left, 1);
    }

    /// 房间级限制按「房间加机器」存；删机器、真删房间时一并清掉。
    #[test]
    fn room_limits_are_kept_per_room_and_machine() {
        let conn = mem();
        let l = |room: &str, agent: &str, until: i64| RoomLimit {
            room: room.into(),
            agent: agent.into(),
            since: 1,
            until,
            reason: "no_video".into(),
        };
        set_room_limit(&conn, &l("r", "a", 30)).unwrap();
        set_room_limit(&conn, &l("r2", "a", 20)).unwrap();
        set_room_limit(&conn, &l("r", "b", 10)).unwrap();
        assert_eq!(room_limit(&conn, "r", "a").unwrap(), Some(l("r", "a", 30)));
        assert_eq!(room_limit(&conn, "r2", "b").unwrap(), None);
        set_room_limit(&conn, &l("r", "a", 40)).unwrap();
        assert_eq!(
            room_limits_of(&conn, "a").unwrap(),
            [l("r2", "a", 20), l("r", "a", 40)],
            "按到期先后"
        );
        clear_room_limit(&conn, "r", "a").unwrap();
        assert_eq!(room_limits_of(&conn, "a").unwrap(), [l("r2", "a", 20)]);
        delete_machine(&conn, "a").unwrap();
        assert!(room_limits_of(&conn, "a").unwrap().is_empty());
        delete_room(&conn, "r").unwrap();
        purge_rooms(&conn).unwrap();
        assert_eq!(room_limit(&conn, "r", "b").unwrap(), None);
    }

    #[test]
    fn template_keeps_new_partition_through_storage_and_rendering() {
        let conn = mem();
        let mut value = serde_json::to_value(template(&conn, 1).unwrap().unwrap()).unwrap();
        value["human_type2"] = 1008.into();
        let mut t: Template = serde_json::from_value(value).unwrap();
        t.name = "new partition".into();
        t.id = insert_template(&conn, &t).unwrap();
        let stored = template(&conn, t.id).unwrap().unwrap();
        assert_eq!(serde_json::to_value(&stored).unwrap()["human_type2"], 1008);
        assert_eq!(
            serde_json::to_value(stored.studio()).unwrap()["human_type2"],
            1008
        );
        value = serde_json::to_value(stored).unwrap();
        value["human_type2"] = 1011.into();
        t = serde_json::from_value(value).unwrap();
        update_template(&conn, &t).unwrap();
        assert_eq!(template(&conn, t.id).unwrap(), Some(t));
    }

    #[test]
    fn migrates_older_databases_to_the_latest_version() {
        for from in 1..VERSION {
            let mut conn = Connection::open_in_memory().unwrap();
            conn.execute_batch(SCHEMA).unwrap();
            conn.execute_batch(MIGRATION_1).unwrap();
            for sql in &LATER[..from as usize - 1] {
                conn.execute_batch(sql).unwrap();
            }
            conn.pragma_update(None, "user_version", from).unwrap();
            if from == 3 {
                conn.pragma_update(None, "foreign_keys", "OFF").unwrap();
                conn.execute(
                    "INSERT INTO rooms(name, url, template_id, agents, tail_upload, position, cover_name)
                     VALUES ('Secret', 'u', 1, '[]', 0, 1, 'Secret（王者组排）')",
                    [],
                )
                .unwrap();
            }
            if from == 6 {
                conn.execute_batch(
                    "INSERT INTO accounts VALUES (1, 'a', 1, '{}', 0);
                     INSERT INTO templates VALUES (1, 't', 1, '', '', 171, '', 1, '', 0, 11);
                     INSERT INTO rooms(name, url, template_id, agents, tail_upload, position)
                       VALUES ('r', 'u', 1, '[]', 0, 1);",
                )
                .unwrap();
            }
            let m = [("m".into(), "tm".into())];
            // 停在机器入库之后的库，当时已导入过机器名单。
            if from >= MACHINES_VERSION {
                import_machines(&conn, &m).unwrap();
            }
            init(&mut conn, &m, || {
                panic!("import runs only on a fresh database")
            })
            .unwrap();
            if from == 6 {
                assert_eq!(
                    room(&conn, "r").unwrap().unwrap().season_section_id,
                    Some(11)
                );
                assert!(template(&conn, 1).is_ok(), "模板不再有合集列");
                let old = template(&conn, 1).unwrap().unwrap();
                assert_eq!(old.tid, 171);
                assert_eq!(old.human_type2, None, "旧模板不自动转换成新版分区");
            }
            if from == 3 {
                let room = room(&conn, "Secret").unwrap().unwrap();
                assert_eq!(room.display_name.as_deref(), Some("Secret（王者组排）"));
                assert_eq!(room.filename, DEFAULT_FILENAME);
            }
            let v: i64 = conn
                .pragma_query_value(None, "user_version", |r| r.get(0))
                .unwrap();
            assert_eq!(v, VERSION, "from {from}");
            let ms = machines(&conn).unwrap();
            assert_eq!(ms.len(), 1);
            assert_eq!(
                (&*ms[0].name, &*ms[0].token, ms[0].enabled),
                ("m", "tm", true)
            );
            conn.execute("UPDATE rooms SET display_name = 'x'", [])
                .unwrap();
            conn.execute("UPDATE rooms SET season_section_id = 1", [])
                .unwrap();
            assert_eq!(ms[0].yield_until, None);
            assert!(set_yield(&conn, "m", Some(200)).unwrap());
            assert!(yielding(&conn, "m", 199).unwrap());
            assert!(!yielding(&conn, "m", 200).unwrap(), "到点即失效");
            init(&mut conn, &m, || panic!()).unwrap();
            assert_eq!(machines(&conn).unwrap().len(), 1);
        }
    }

    #[test]
    fn delete_machine_cleans_runtime_rows_and_room_lists_while_keeping_parts() {
        let conn = mem();
        touch_agent(&conn, "a", 100, &Status::default()).unwrap();
        set_limit(
            &conn,
            "a",
            &Limit {
                since: 1,
                until: 2,
                reason: "r".into(),
                need_bps: 1.0,
            },
        )
        .unwrap();
        start_drain(&conn, "a", "c1", 10).unwrap();
        fail_commit(&conn, "c1", "a", 20).unwrap();
        apply(&conn, "a", &closed("p1", "s", 100)).unwrap();

        delete_machine(&conn, "a").unwrap();

        assert_eq!(machine(&conn, "a").unwrap(), None);
        assert_eq!(room(&conn, "r").unwrap().unwrap().agents, ["b"]);
        assert_eq!(room(&conn, "r2").unwrap().unwrap().agents, ["c"]);
        assert_eq!(agent(&conn, "a").unwrap(), None);
        assert_eq!(limit(&conn, "a").unwrap(), None);
        assert_eq!(drain(&conn, "a").unwrap(), None);
        assert!(failed_commits(&conn, "a").unwrap().is_empty());
        assert_eq!(states(&conn), [("p1".into(), 1, "closed".into())]);
    }

    #[test]
    fn season_pending_until_done() {
        let conn = mem();
        apply(&conn, "a", &closed("p1", "s", 200)).unwrap();
        submitted(&conn, 1, "BV1", 7, Some(11), 300).unwrap();
        assert_eq!(season_pending(&conn).unwrap(), [(1, Some(1), 7, 11)]);
        season_done(&conn, 1).unwrap();
        assert!(season_pending(&conn).unwrap().is_empty());
    }

    #[test]
    fn discarded_part_becomes_empty_and_settles() {
        let mut conn = mem();
        let AgentEvent::Recorder {
            event: Event::PartClosed(p),
            ..
        } = closed("e1", "s", 0)
        else {
            unreachable!()
        };
        let discarded = AgentEvent::Recorder {
            event: Event::PartDiscarded(PartClosed { size: 782, ..p }),
            at: 20_010,
        };
        apply_all(
            &mut conn,
            "a",
            &[opened("e1", "s", 20_000), discarded, ended("s", 30_000)],
        )
        .unwrap();
        let (_, _, _, parts) = current_session(&conn, "r").unwrap().unwrap_or_default();
        assert!(parts.is_empty(), "session ended");
        let row: (String, Option<i64>, Option<i64>) = conn
            .query_row("SELECT state, size, closed_at FROM parts", [], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?))
            })
            .unwrap();
        assert_eq!(row, ("empty".into(), Some(782), Some(20_010)));
        assert_eq!(ready(&conn, i64::MAX).unwrap(), [1]);
    }

    #[test]
    fn events_are_idempotent_and_archive_is_ready_when_every_part_settles() {
        let mut conn = mem();
        let evs = [
            opened("p1", "s", 100),
            closed("p1", "s", 200),
            opened("p2", "s", 200),
            uploaded("p1"),
        ];
        apply_all(&mut conn, "a", &evs).unwrap();
        apply_all(&mut conn, "a", &evs).unwrap(); // 重放无副作用
        assert_eq!(
            states(&conn),
            [
                ("p1".into(), 1, "uploaded".into()),
                ("p2".into(), 1, "open".into())
            ]
        );
        assert!(recording(&conn, "r").unwrap());
        apply(&conn, "a", &ended("s", 300)).unwrap();
        assert!(ready(&conn, i64::MAX).unwrap().is_empty(), "p2 still open");
        apply(
            &conn,
            "a",
            &AgentEvent::Lost {
                part_id: "p2".into(),
            },
        )
        .unwrap();
        assert_eq!(ready(&conn, i64::MAX).unwrap(), [1]);
        let a = archive(&conn, 1).unwrap();
        assert_eq!(
            a.parts,
            [ArchivePart {
                path: "/rec/p1.flv".into(),
                filename: "n-p1".into(),
                opened_at: 100,
                tz_offset: Some(8 * 3600),
                title: "直播".into(),
            }]
        );
        // 已上传的不会被晚到的 Lost 覆盖。
        apply(
            &conn,
            "a",
            &AgentEvent::Lost {
                part_id: "p1".into(),
            },
        )
        .unwrap();
        assert_eq!(archive(&conn, 1).unwrap().parts.len(), 1);
    }

    #[test]
    fn same_session_from_two_agents_merges_and_late_parts_follow_ended_at() {
        let mut conn = mem();
        apply_all(&mut conn, "a", &[opened("a1", "s", 100)]).unwrap();
        apply_all(&mut conn, "b", &[opened("b1", "s", 150)]).unwrap();
        assert_eq!(end_room(&conn, "r", 300).unwrap(), 1);
        // 结束前开录、晚到的 → 原稿件；结束后开录的 → 新稿件。
        apply_all(
            &mut conn,
            "b",
            &[opened("b2", "s", 250), opened("b3", "s", 400)],
        )
        .unwrap();
        let s = states(&conn);
        let archives: Vec<i64> = s.iter().map(|(_, a, _)| *a).collect();
        assert_eq!(archives, [1, 1, 1, 2]);
        // 旧的 SessionEnded 重放不会结束新稿件。
        apply(&conn, "a", &ended("s", 350)).unwrap();
        assert!(recording(&conn, "r").unwrap());
        // 下播宽限（缺省 5 分钟）外开录的另一场次键是另一份稿件。
        apply(&conn, "a", &opened("c1", "s2", 400 + 300_001)).unwrap();
        assert_eq!(states(&conn)[4].1, 3);
    }

    #[test]
    fn closed_without_opened_is_registered_and_submitting_becomes_attention_on_restart() {
        let conn = mem();
        apply(&conn, "a", &closed("p1", "s", 200)).unwrap();
        assert_eq!(states(&conn), [("p1".into(), 1, "closed".into())]);
        set_phase(&conn, 1, SUBMITTING, None, None).unwrap();
        assert_eq!(interrupted_submits(&conn).unwrap(), [(1, "r".to_string())]);
        let phase: String = conn
            .query_row("SELECT phase FROM archives WHERE id = 1", [], |r| r.get(0))
            .unwrap();
        assert_eq!(phase, ATTENTION);
    }

    #[test]
    fn migration_21_adds_tz_offset_and_title_and_archive_reads_them() {
        // 从 v20 迁移到最新：旧分P行 tz_offset 为 NULL、title 为空串。
        let mut conn = Connection::open_in_memory().unwrap();
        conn.pragma_update(None, "foreign_keys", "ON").unwrap();
        conn.execute_batch(SCHEMA).unwrap();
        conn.execute_batch(MIGRATION_1).unwrap();
        for sql in &LATER[..19] {
            conn.execute_batch(sql).unwrap();
        }
        conn.pragma_update(None, "user_version", 20i64).unwrap();
        conn.execute(
            "INSERT INTO archives(id, room, session, title, started_at, phase)
             VALUES (1, 'r', 's', '旧场', 1000, 'ended')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO parts(part_id, archive_id, agent, path, opened_at, state, filename)
             VALUES ('old1', 1, 'a', '/rec/old1.flv', 1000, 'uploaded', 'f-old1')",
            [],
        )
        .unwrap();

        init(&mut conn, &[], || Ok(Import::default())).unwrap();
        let ver: i64 = conn
            .pragma_query_value(None, "user_version", |r| r.get(0))
            .unwrap();
        assert_eq!(ver, VERSION);
        let a = archive(&conn, 1).unwrap();
        assert_eq!(
            a.parts,
            [ArchivePart {
                path: "/rec/old1.flv".into(),
                filename: "f-old1".into(),
                opened_at: 1000,
                tz_offset: None,
                title: String::new(),
            }]
        );

        // 新分P通过 PartOpened 记入 tz_offset_secs 与 title。
        let conn2 = mem();
        let ev_open = AgentEvent::Recorder {
            event: Event::PartOpened(PartOpened {
                owner: owner("s2"),
                part_id: "p-tz".into(),
                path: "/rec/p-tz.flv.part".into(),
                title: "下半场标题".into(),
                tz_offset_secs: Some(9 * 3600),
                source_url: None,
            }),
            at: 2000,
        };
        apply(&conn2, "a", &ev_open).unwrap();
        apply(&conn2, "a", &closed("p-tz", "s2", 2100)).unwrap();
        apply(&conn2, "a", &uploaded("p-tz")).unwrap();
        let a2 = archive(&conn2, 1).unwrap();
        assert_eq!(
            a2.parts,
            [ArchivePart {
                path: "/rec/p-tz.flv".into(),
                filename: "n-p-tz".into(),
                opened_at: 2000,
                tz_offset: Some(9 * 3600),
                title: "下半场标题".into(),
            }]
        );
    }

    #[test]
    fn late_parts_from_previous_source_join_within_grace() {
        let old_url = "https://live.bilibili.com/6";
        let new_url = "https://live.bilibili.com/60";
        for gap in [299_999, 300_001] {
            let conn = mem();
            let mut r = room(&conn, "r").unwrap().unwrap();
            r.urls = vec![old_url.into(), new_url.into()];
            save_room(&conn, &r).unwrap();
            let new_at = 20_010 + gap;
            let mut first_new = opened("new", "new-session", new_at);
            if let AgentEvent::Recorder {
                event: Event::PartOpened(p),
                ..
            } = &mut first_new
            {
                p.source_url = Some(new_url.into());
            }
            apply(&conn, "b", &first_new).unwrap();
            let mut first_old = opened("old", "old-session", 20_000);
            if let AgentEvent::Recorder {
                event: Event::PartOpened(p),
                ..
            } = &mut first_old
            {
                p.title = "旧来源标题".into();
                p.source_url = Some(old_url.into());
            }
            apply(&conn, "a", &first_old).unwrap();
            apply(&conn, "a", &closed("old", "old-session", 20_010)).unwrap();

            if gap < 300_000 {
                // 同一旧场次的下一条迟到事件不会把稿件的在录场次改回旧来源。
                apply(&conn, "a", &opened("old-2", "old-session", 20_010)).unwrap();
                apply(&conn, "a", &ended("old-session", 20_020)).unwrap();
                assert_eq!(
                    recording_archives(&conn, "r").unwrap(),
                    [(1, "new-session".into())]
                );
                assert!(states(&conn).iter().all(|(_, id, _)| *id == 1));
                let merged = archive(&conn, 1).unwrap();
                assert_eq!(merged.source_url.as_deref(), Some(old_url));
                assert_eq!(merged.started_at, 20_000);
                assert_eq!(merged.title, "旧来源标题");
            } else {
                assert_eq!(
                    states(&conn),
                    [
                        ("new".into(), 1, "open".into()),
                        ("old".into(), 2, "closed".into())
                    ]
                );
                assert_eq!(
                    archive(&conn, 1).unwrap().source_url.as_deref(),
                    Some(new_url)
                );
            }
        }
    }

    #[test]
    fn migration_22_preserves_single_url_and_pins_archive_source_url() {
        // 从 v21 迁移到最新：旧单地址房间自动读成单项 urls，旧稿件 source_url 为 None。
        let mut conn = Connection::open_in_memory().unwrap();
        conn.pragma_update(None, "foreign_keys", "ON").unwrap();
        conn.execute_batch(SCHEMA).unwrap();
        conn.execute_batch(MIGRATION_1).unwrap();
        for sql in &LATER[..20] {
            conn.execute_batch(sql).unwrap();
        }
        conn.pragma_update(None, "user_version", 21i64).unwrap();
        conn.execute_batch(
            "INSERT INTO accounts VALUES (1, 'a', 1, '{}', 0);
             INSERT INTO templates(id, name, account_id, title, \"desc\", tid, tag, copyright, source, is_only_self, no_disturbance)
               VALUES (1, 't', 1, '{name}', '', 171, '', 1, '', 0, 0);
             INSERT INTO rooms(name, url, template_id, agents, tail_upload, position)
               VALUES ('r', 'https://live.bilibili.com/101', 1, '[]', 0, 1);
             INSERT INTO archives(id, room, session, title, started_at, phase)
               VALUES (1, 'r', 's0', '旧场', 1000, 'ended');",
        )
        .unwrap();

        init(&mut conn, &[], || Ok(Import::default())).unwrap();
        let r = room(&conn, "r").unwrap().unwrap();
        assert_eq!(r.url, "https://live.bilibili.com/101");
        assert_eq!(r.urls, ["https://live.bilibili.com/101"]);
        assert_eq!(archive(&conn, 1).unwrap().source_url, None);

        // 多来源与归档 source_url 固定：首分P在副房，宽限内换主房仍保留副房地址；宽限外开新稿件用新地址。
        let conn2 = mem();
        let u1 = "https://live.bilibili.com/6";
        let u2 = "https://live.bilibili.com/60";
        let open_src =
            |part: &str, session: &str, at: i64, src: Option<&str>| AgentEvent::Recorder {
                event: Event::PartOpened(PartOpened {
                    owner: owner(session),
                    part_id: part.into(),
                    path: format!("/rec/{part}.flv.part").into(),
                    title: "直播".into(),
                    tz_offset_secs: Some(8 * 3600),
                    source_url: src.map(str::to_string),
                }),
                at,
            };
        let close_src =
            |part: &str, session: &str, at: i64, src: Option<&str>| AgentEvent::Recorder {
                event: Event::PartClosed(PartClosed {
                    owner: owner(session),
                    part_id: part.into(),
                    path: format!("/rec/{part}.flv").into(),
                    duration: Duration::from_secs(60),
                    size: 5,
                    reason: CloseReason::TimedSplit,
                    title: "直播".into(),
                    closed_at_ms: None,
                    source_url: src.map(str::to_string),
                }),
                at,
            };

        // 1) 首分P来自副房 u2；先 end_archive 再到 PartClosed 时也能按最新 closed_at 补齐 hold_until。
        apply(&conn2, "a", &open_src("p1", "s-u2", 1_000, Some(u2))).unwrap();
        assert!(end_archive(&conn2, 1, 100_000).unwrap());
        apply(&conn2, "a", &close_src("p1", "s-u2", 400_000, Some(u2))).unwrap();
        apply(&conn2, "a", &uploaded("p1")).unwrap();
        // 宽限（默认 300s）内切到主房 u1（400_000 + 60_000 <= 400_000 + 300_000）：归并入稿件 1，source_url 保持 u2。
        apply(&conn2, "a", &open_src("p2", "s-u1", 460_000, Some(u1))).unwrap();
        apply(&conn2, "a", &close_src("p2", "s-u1", 520_000, Some(u1))).unwrap();
        apply(&conn2, "a", &uploaded("p2")).unwrap();
        apply(&conn2, "a", &ended("s-u1", 520_000)).unwrap();
        let a1 = archive(&conn2, 1).unwrap();
        assert_eq!(a1.parts.len(), 2);
        assert_eq!(a1.source_url.as_deref(), Some(u2));

        // 2) 宽限外开新来源（520_000 + 301_000）：新建稿件 2，旧 agent 不带 source_url 时回退当前分配快照地址。
        let mut r2 = room(&conn2, "r").unwrap().unwrap();
        r2.urls = vec![u1.into(), u2.into()];
        let task_u1 = RoomTask {
            name: "r".into(),
            url: u1.into(),
            split_time: None,
            split_size_mb: None,
            grace: None,
            bili_qn: None,
            douyin_quality: None,
            filename: DEFAULT_FILENAME.into(),
            streamer: "r".into(),
            tail_upload: false,
        };
        assign(&conn2, "r", "a", "s-u1-next", 821_000, &task_u1).unwrap();
        apply(&conn2, "a", &open_src("p3", "s-u1-next", 821_000, None)).unwrap();
        apply(&conn2, "a", &close_src("p3", "s-u1-next", 881_000, None)).unwrap();
        apply(&conn2, "a", &uploaded("p3")).unwrap();
        let a2 = archive(&conn2, 2).unwrap();
        assert_eq!(a2.parts.len(), 1);
        assert_eq!(a2.source_url.as_deref(), Some(u1));
    }
}
