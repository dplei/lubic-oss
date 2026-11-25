//! agent 与总控之间的消息。全部是 HTTP `POST` + JSON，agent 主动发起，
//! 请求头 `Authorization: Bearer <token>` 标明是哪台 agent。
//!
//! | 路径 | 请求 | 响应 |
//! |---|---|---|
//! | [`WATCH`] | [`Watch`] | [`Assignments`]（长轮询，最多挂 [`WATCH_HOLD`]） |
//! | [`REPORT`] | [`Report`] | [`Ack`] |
//! | [`TICKET`] | [`TicketRequest`] | preupload 的原始 JSON，agent 反序列化成 `lubic_upos::Ticket` |
//! | [`DRAIN`] | [`DrainRequest`] | [`Drain`]（更新脚本调，见 `docs/design/auto-update.md`） |
//! | [`VERSION`]（`GET`，不要 token） | — | [`Version`] |
//!
//! 兼容约定：总控先于 agent 更新，混跑只会是新总控 + 旧 agent。新字段一律 `#[serde(default)]`，
//! 不删字段、不改名、不改语义；未知字段照常忽略（不开 `deny_unknown_fields`）。
//! 测试里存着旧版报文，新代码必须能反序列化。

use lubic_events::Event;
use serde::{Deserialize, Serialize};
use std::time::Duration;

pub const WATCH: &str = "/v1/watch";
pub const REPORT: &str = "/v1/report";
pub const TICKET: &str = "/v1/ticket";
pub const DRAIN: &str = "/v1/drain";
pub const VERSION: &str = "/v1/version";

/// 构建时的 git 提交号（Dockerfile 的 `GIT_COMMIT`），本地构建没传时为空串。
pub const COMMIT: &str = match option_env!("GIT_COMMIT") {
    Some(c) => c,
    None => "",
};

/// 期望状态没变时，`/v1/watch` 最多挂这么久再返回。也是心跳间隔。
pub const WATCH_HOLD: Duration = Duration::from_secs(25);

/// [`Status::record_skips`] 的统计窗口。
pub const SKIP_WINDOW: Duration = Duration::from_secs(60);

/// [`Status::record_connections`]、[`Status::record_gaps`] 报最近这么久内的记录；健康检测的门槛与各自的窗口在总控。
pub const HEALTH_WINDOW: Duration = Duration::from_secs(120);

/// 上传票据被拒（410）的响应正文：分P所属稿件已在控制台丢弃，agent 见到就删掉本地文件。
pub const DISCARDED: &str = "archive discarded";

/// 试拉在 agent 监测里使用的房间键前缀：`record_rates`/`record_skips` 里 `probe:<房间名>` 是试拉的结果
/// （见 [`Assignments::probe`]）。
pub const PROBE_PREFIX: &str = "probe:";

/// 故障试拉在 agent 监测里使用的房间键前缀：`trial:<房间名>` 是故障试拉的原始数（见 [`Assignments::trial`]）。
/// 转正后改用房间名本身。
pub const TRIAL_PREFIX: &str = "trial:";

/// [`Status::record_trial`] 报最近这么久的逐秒进度，盖住故障试拉的 90 秒时限（`handoff-health.md` §4.2）。
pub const TRIAL_WINDOW: Duration = Duration::from_secs(90);

/// agent 当前在跑的房间与本机状态。也是心跳。
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Watch {
    pub rooms: Vec<String>,
    /// 已收到切点的房间名（回显 [`Assignments::cuts`]），总控据此判断长轮询要不要立刻返回。
    #[serde(default)]
    pub cuts: Vec<String>,
    /// 正在试拉的房间名（回显 [`Assignments::probe`]，不带 [`PROBE_PREFIX`]），总控据此判断长轮询要不要立刻返回。
    #[serde(default)]
    pub probe: Option<String>,
    /// 正在故障试拉的房间名（回显 [`Assignments::trial`]，不带 [`TRIAL_PREFIX`]；转正后不再报）。
    #[serde(default)]
    pub trial: Option<String>,
    /// agent 此刻是否在暂停上传（回显 [`Assignments::hold_uploads`]），总控和应下发的值不同就立刻回复。
    /// 旧 agent 不发为 `None`，总控只在这次长轮询期间开关变了时立刻回复。
    #[serde(default)]
    pub hold_uploads: Option<bool>,
    pub status: Status,
}

/// 本机状态，给分配与受限判定用。监测的四个数只报原始窗口值，阈值与结论在总控。
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct Status {
    /// 录制目录所在盘的使用率，同 `df` 的 Use%。
    pub disk_usage: f64,
    pub disk_free: u64,
    /// 硬盘到水位停录了。
    pub paused: bool,
    /// 会报 [`Status::record_video`] 等健康字段的 agent 为真。总控据此区分「旧 agent 不报」与
    /// 「新 agent 没报这个房间」，只对后者做 no_video 兜底。旧 agent 不报（为假）。
    pub health_reports: bool,
    /// 支持故障试拉（[`Assignments::trial`]）的 agent 为真；旧 agent 不报（为假），总控不该给它发故障试拉。
    pub trial_support: bool,
    /// 待传（含正在传）的分段数与字节数。
    pub queued_parts: u32,
    pub queued_bytes: u64,
    /// 正在传的分段：(part_id, 已传字节, 总字节)。传失败在退避的不算。旧 agent 不报。
    pub uploading: Vec<(String, u64, u64)>,
    /// 还没被总控确认的事件数。
    pub outbox: u32,
    /// 各在录房间的录制实时倍率（收到的媒体时长 / 连接墙钟）：最近 2 分钟里各个 30 秒子窗口取最高，
    /// 所以它低于阈值就说明低了整整 2 分钟。历史不满 2 分钟、或一直断流的房间不报。
    pub record_rates: Vec<(String, f64)>,
    /// 各在录房间当前连接落后直播多少毫秒（见 `lubic::recorder::PaceCount::lag_ms`）。换机交接时总控据此等旧机录到新机的起点。
    pub record_lags: Vec<(String, i64)>,
    /// 各在录房间写到的内容落后直播多少毫秒，按码流 SEI 里推流端的时刻算（见 `PaceCount::live_lag_ms`）。
    /// 各机都对 NTP，可以跨机器比较；码流没有 SEI 的房间不报，交接退回 `record_lags`。
    pub record_live_lags: Vec<(String, i64)>,
    /// 各在录房间平台实际选中的画质代码（B 站 qn，抖音 origin/uhd/…）。旧 agent 不报。
    pub record_qualities: Vec<(String, String)>,
    /// 各房间最近 [`SKIP_WINDOW`] 内时间戳前跳跳过的内容：(房间名, 跳过的毫秒数, 跳变次数)。
    /// 断流不清空；只报跳变次数 > 0 的房间。旧 agent 不报。
    pub record_skips: Vec<(String, u64, u32)>,
    /// 各房间连续异常拉流尝试：(房间名, 次数, 是否发生视频停滞)。有效媒体恢复后清零；旧 agent 不报。
    pub record_failures: Vec<(String, u32, bool)>,
    /// 各房间最近 [`HEALTH_WINDOW`] 内结束的拉流连接：(房间名, 结束于多久前（毫秒）, 连接时长毫秒, 是否写出过有效视频)。
    /// 「多久前」都是 agent 发心跳时现算的，总控直接用，不跨机器比较绝对时刻。
    /// 只算建立起来的连接（FLV 读到文件头、HLS 取到播放列表），且结束后平台确认仍是同一场在播；
    /// 主动停止、交接收尾、换场、下播宽限内的不算，切段不结束连接。旧 agent 不报。
    pub record_connections: Vec<(String, u64, u64, bool)>,
    /// 平台确认在播的各房间：(房间名, 最近一次写出有效视频在多久前, 录制器这次确认在播在多久前)，都是毫秒；
    /// 没写出过视频为 `None`。下播宽限中、停录、交接收尾后不报。旧 agent 不报。
    pub record_video: Vec<(String, Option<u64>, u64)>,
    /// 各房间最近 [`HEALTH_WINDOW`] 内按可靠推流时刻测得的重连接缝：(房间名, 测得于多久前（毫秒）, 缺口毫秒)，重叠记 0。
    /// 无法测量（缺 SEI、HLS）、跨过下播宽限或换场的接缝不报。旧 agent 不报。
    pub record_gaps: Vec<(String, u64, u64)>,
    /// 故障试拉的逐秒进度：(键 `trial:<房间名>`, [(采样于多久前（毫秒）, 累计媒体毫秒, 累计连接墙钟毫秒)])，
    /// 从试拉开始累计，只留最近 [`TRIAL_WINDOW`]，按时间先后排。总控据此按任意观察窗口算媒体推进与实时时长、
    /// 扣除连上时的首包突发；连接结束、视频写出、停滞、跳失、接缝照常在其余字段的同一个键下报。旧 agent 不报。
    #[allow(clippy::type_complexity)]
    pub record_trial: Vec<(String, Vec<(u64, u64, u64)>)>,
    /// 最近 10 分钟本机写盘字节 / 墙钟。
    pub produce_bps: f64,
    /// 最近 10 分钟上传字节 / 上传忙碌时长；忙碌不足 60 秒报 `None`。
    pub upload_bps: Option<f64>,
    /// 最近 10 分钟里在上传的时间比例。
    pub upload_busy: f64,
    /// 本机程序的 git 提交号（[`COMMIT`]）。旧 agent 不报，为空串。
    pub commit: String,
}

/// 这台 agent 该录的全部房间：期望状态，不是增量命令。
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Assignments {
    pub rooms: Vec<RoomTask>,
    /// 换机交接的切点：(房间名, 切点推流时刻 unix 毫秒)。旧机在推流时刻 ≥ 切点的第一个关键帧之前收尾、
    /// 停录该房间（不重连），房间仍留在它的清单里直到总控改分配。
    #[serde(default)]
    pub cuts: Vec<(String, i64)>,
    /// 受限标记快到期时总控让这台机器试拉的房间（该房间此刻在别的机器上录）。agent 只拉流、不上传、不进稿件，
    /// 结果经 `record_rates`/`record_skips` 里 [`PROBE_PREFIX`] + 房间名的键报回。
    #[serde(default)]
    pub probe: Option<RoomTask>,
    /// 故障试拉（`handoff-health.md` §6.1）：agent 照常拉流，写进暂存目录，出结论前不发事件、不上传、不进稿件，
    /// 原始数经 [`TRIAL_PREFIX`] + 房间名的键报回。同一房间出现在 [`Self::rooms`] 里（`RoomTask` 须与此相同）
    /// 就在同一条连接上转正；从这里消失而没有转正就停掉并删除暂存内容。与 [`Self::probe`] 同时下发时只跑故障试拉。
    #[serde(default)]
    pub trial: Option<RoomTask>,
    /// 故障试拉转正时丢弃暂存内容的房间（`handoff-health.md` §6.2）：原机在试拉期间写出过视频，
    /// 切点前的那段由原机保留，候选机的暂存分段删掉、不发事件。不在这里的房间转正时保留全部暂存内容；
    /// 旧总控不发、旧 agent 不认识，都退回全部保留。
    #[serde(default)]
    pub trial_drop: Vec<String>,
    /// 总控要求暂停上传（「让出网络」，`load-balance.md` §3）。录制照常；旧总控不发为 false，旧 agent 不认识照常上传。
    #[serde(default)]
    pub hold_uploads: bool,
    /// 总控配置的统一时区偏移（UTC 东偏秒数，如 `+08:00` 为 `28800`）。
    /// 旧总控不发时为 `None`（agent 退回本机 `Local`）；旧 agent 忽略该字段。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tz_offset_secs: Option<i32>,
}

/// 录一个房间要的全部参数。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoomTask {
    pub name: String,
    pub url: String,
    pub split_time: Option<Duration>,
    pub split_size_mb: Option<u64>,
    pub grace: Option<Duration>,
    pub bili_qn: Option<u32>,
    /// 抖音画质：origin/uhd/hd/sd/ld/md，缺档时就近选。旧总控不发，agent 用 origin。
    #[serde(default)]
    pub douyin_quality: Option<String>,
    /// 文件名模板（同 biliup 的 `filename_prefix`），如 `{streamer} %Y-%m-%d %H{colon}%M{colon}%S`。
    /// 旧总控不发，agent 退回 `{streamer}_%Y-%m-%dT%H_%M_%S`。
    #[serde(default)]
    pub filename: String,
    /// 模板里 `{streamer}` 的值：房间显示名。旧总控不发，agent 用 `name`。
    #[serde(default)]
    pub streamer: String,
    /// 边录边传：录制中跟着文件增长逐块上传（见 `docs/design/tail-upload.md`）。开启时必有 `split_size_mb`。
    #[serde(default)]
    pub tail_upload: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Report {
    pub events: Vec<Envelope>,
}

/// 发件箱里的一条事件。`seq` 只在本机递增，总控按内容幂等处理，不记 `seq`。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Envelope {
    pub seq: u64,
    pub event: AgentEvent,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AgentEvent {
    /// 录制侧事件原样转发。`at` 是 agent 收到它的时刻（unix 毫秒）。
    Recorder { event: Event, at: i64 },
    /// 分段已传上 UPOS。总控落库并确认后，agent 才删本地文件。
    Uploaded {
        part_id: String,
        filename: String,
        line: String,
        size: u64,
    },
    /// 分段没有可传的内容（文件不见了、或录制侧丢弃的空分段），提交时跳过它。
    Lost { part_id: String },
    /// 本机这个房间的录制写出了第一个关键帧（每次起录制报一次）。换机交接据此通知旧机收尾。
    Recording {
        room: String,
        /// 写出第一个关键帧时，这条连接落后直播多少毫秒。
        #[serde(default)]
        lag_ms: Option<i64>,
        /// 写出的第一帧是直播的哪一刻（unix 毫秒，推流端时钟，取自码流 SEI）。码流没有 SEI 时为 `None`。
        #[serde(default)]
        start_live_ms: Option<i64>,
    },
}

/// 总控已落库的最大 `seq`，agent 删掉发件箱里 `<=` 它的事件。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ack {
    pub acked: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TicketRequest {
    /// 分P所属房间，总控据此选投稿账号（分P还没登记时用）。旧 agent 不带。
    #[serde(default)]
    pub room: String,
    pub part_id: String,
    pub file_name: String,
    pub size: u64,
    /// UPOS 线路（upcdn），由 agent 按本机网络指定。
    pub line: String,
}

/// 更新脚本问总控：本机能不能换到 `commit` 这一版。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DrainRequest {
    pub commit: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "lowercase")]
pub enum Drain {
    /// 还不能换，过一会再问。
    Wait { reason: String },
    /// 已排空，可以重建容器。
    Go,
    /// 心跳里已经是目标提交。
    Done,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Version {
    pub commit: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 加 `commit` / `cuts` 之前的旧报文，新总控与新 agent 必须照常读。
    const OLD_WATCH: &str = r#"{"rooms":["r"],"status":{"disk_usage":0.5,"disk_free":10,"paused":false,"queued_parts":1,"queued_bytes":2,"outbox":0,"record_rates":[["r",1.0]],"record_lags":[["r",3000]],"record_live_lags":[["r",4000]],"produce_bps":100.0,"upload_bps":null,"upload_busy":0.1}}"#;
    const OLD_ASSIGNMENTS: &str = r#"{"rooms":[{"name":"r","url":"u","split_time":null,"split_size_mb":null,"grace":null,"bili_qn":null}]}"#;
    const OLD_REPORT: &str = r#"{"events":[{"seq":1,"event":{"Uploaded":{"part_id":"p","filename":"f","line":"tx","size":3}}},{"seq":2,"event":{"Lost":{"part_id":"q"}}},{"seq":3,"event":{"Recording":{"room":"r","lag_ms":1200,"start_live_ms":null}}}]}"#;

    #[test]
    fn reads_messages_from_older_agents() {
        let w: Watch = serde_json::from_str(OLD_WATCH).unwrap();
        assert!(w.cuts.is_empty());
        assert_eq!(w.status.commit, "");
        assert_eq!(w.status.record_live_lags, vec![("r".to_string(), 4000)]);
        let a: Assignments = serde_json::from_str(OLD_ASSIGNMENTS).unwrap();
        assert_eq!(a.rooms.len(), 1);
        assert!(a.cuts.is_empty());
        let r: Report = serde_json::from_str(OLD_REPORT).unwrap();
        assert_eq!(r.events.len(), 3);
    }

    /// 加试拉与 `record_skips` 之前的报文：新字段缺省为空。
    #[test]
    fn reads_messages_without_probe_or_skips() {
        let w: Watch = serde_json::from_str(OLD_WATCH).unwrap();
        assert_eq!(w.probe, None);
        assert!(w.status.record_skips.is_empty());
        let a: Assignments = serde_json::from_str(OLD_ASSIGNMENTS).unwrap();
        assert_eq!(a.probe, None);
        let s: Status =
            serde_json::from_str(r#"{"disk_usage":0.5,"record_rates":[["r",1.0]]}"#).unwrap();
        assert!(s.record_skips.is_empty());
    }

    /// 健康检测的新字段：旧 agent 不报时为空；旧总控不认识它们也照常读（不开 `deny_unknown_fields`）。
    #[test]
    fn health_fields_mix_with_older_peers() {
        let w: Watch = serde_json::from_str(OLD_WATCH).unwrap();
        assert!(w.status.record_connections.is_empty());
        assert!(w.status.record_video.is_empty());
        assert!(w.status.record_gaps.is_empty());
        assert!(!w.status.health_reports, "旧 agent 不报健康字段");
        let s: Status = serde_json::from_str(
            r#"{"disk_usage":0.5,"record_connections":[["r",1,2,true]],"record_video":[["r",null,3]],"record_gaps":[["r",4,5]],"record_from_the_future":[1]}"#,
        )
        .unwrap();
        assert_eq!(s.record_connections, [("r".into(), 1, 2, true)]);
        assert_eq!(s.record_video, [("r".into(), None, 3)]);
        assert_eq!(s.record_gaps, [("r".into(), 4, 5)]);
    }

    /// 故障试拉：旧总控不发、旧 agent 不报时缺省为空；新报文旧代码照常读（未知字段忽略）。
    #[test]
    fn trial_fields_mix_with_older_peers() {
        let w: Watch = serde_json::from_str(OLD_WATCH).unwrap();
        assert_eq!(w.trial, None);
        assert!(!w.status.trial_support, "旧 agent 不支持故障试拉");
        assert!(w.status.record_trial.is_empty());
        let a: Assignments = serde_json::from_str(OLD_ASSIGNMENTS).unwrap();
        assert_eq!(a.trial, None);

        /// 加故障试拉之前的 `Assignments` 与 `Watch`。
        #[derive(Deserialize)]
        struct OldAssignments {
            rooms: Vec<RoomTask>,
            probe: Option<RoomTask>,
        }
        #[derive(Deserialize)]
        struct OldWatch {
            probe: Option<String>,
        }
        let room: RoomTask = serde_json::from_str(r#"{"name":"r","url":"u","split_time":null,"split_size_mb":null,"grace":null,"bili_qn":null}"#).unwrap();
        let new = Assignments {
            rooms: vec![room.clone()],
            trial: Some(room.clone()),
            ..Default::default()
        };
        let old: OldAssignments =
            serde_json::from_str(&serde_json::to_string(&new).unwrap()).unwrap();
        assert_eq!((old.rooms, old.probe), (vec![room], None));
        let new = Watch {
            trial: Some("r".into()),
            status: Status {
                trial_support: true,
                record_trial: vec![("trial:r".into(), vec![(1, 2, 3)])],
                ..Default::default()
            },
            ..Default::default()
        };
        let text = serde_json::to_string(&new).unwrap();
        assert_eq!(serde_json::from_str::<OldWatch>(&text).unwrap().probe, None);
        assert_eq!(serde_json::from_str::<Watch>(&text).unwrap(), new);
    }

    /// `Assignments::trial_drop`：旧总控不发时为空，agent 照旧保留全部暂存内容；旧 agent 不认识它也照常读、
    /// 照旧全部保留（不丢内容）。
    #[test]
    fn trial_drop_mixes_with_older_peers() {
        let a: Assignments = serde_json::from_str(OLD_ASSIGNMENTS).unwrap();
        assert!(a.trial_drop.is_empty());
        /// 加 `trial_drop` 之前的 `Assignments`。
        #[derive(Deserialize)]
        struct OldAssignments {
            rooms: Vec<RoomTask>,
            trial: Option<RoomTask>,
        }
        let room: RoomTask = serde_json::from_str(r#"{"name":"r","url":"u","split_time":null,"split_size_mb":null,"grace":null,"bili_qn":null}"#).unwrap();
        let new = Assignments {
            rooms: vec![room.clone()],
            trial_drop: vec!["r".into()],
            ..Default::default()
        };
        let text = serde_json::to_string(&new).unwrap();
        let old: OldAssignments = serde_json::from_str(&text).unwrap();
        assert_eq!((old.rooms, old.trial), (vec![room], None));
        assert_eq!(serde_json::from_str::<Assignments>(&text).unwrap(), new);
    }

    /// `Assignments::hold_uploads`：旧总控不发，读出为 false；带上时旧 agent 照常读（未知字段忽略）。
    #[test]
    fn hold_uploads_mixes_with_older_peers() {
        let a: Assignments = serde_json::from_str(OLD_ASSIGNMENTS).unwrap();
        assert!(!a.hold_uploads);
        /// 加 `hold_uploads` 之前的 `Assignments`。
        #[derive(Deserialize)]
        struct OldAssignments {
            rooms: Vec<RoomTask>,
            trial_drop: Vec<String>,
        }
        let new = Assignments {
            trial_drop: vec!["r".into()],
            hold_uploads: true,
            ..Default::default()
        };
        let text = serde_json::to_string(&new).unwrap();
        let old: OldAssignments = serde_json::from_str(&text).unwrap();
        assert_eq!((old.rooms, old.trial_drop), (vec![], vec!["r".to_string()]));
        assert_eq!(serde_json::from_str::<Assignments>(&text).unwrap(), new);
    }

    /// `Watch::hold_uploads`：旧 agent 不发，读出为 `None`；带上时旧总控照常读（未知字段忽略）。
    #[test]
    fn watch_hold_uploads_mixes_with_older_peers() {
        let w: Watch = serde_json::from_str(OLD_WATCH).unwrap();
        assert_eq!(w.hold_uploads, None);
        /// 加 `hold_uploads` 之前的 `Watch`。
        #[derive(Deserialize)]
        struct OldWatch {
            rooms: Vec<String>,
            status: Status,
        }
        let new = Watch {
            rooms: vec!["r".into()],
            hold_uploads: Some(true),
            ..Default::default()
        };
        let text = serde_json::to_string(&new).unwrap();
        let old: OldWatch = serde_json::from_str(&text).unwrap();
        assert_eq!(
            (old.rooms, old.status),
            (vec!["r".to_string()], Status::default())
        );
        assert_eq!(serde_json::from_str::<Watch>(&text).unwrap(), new);
    }

    /// `PartClosed::closed_at_ms`：旧 agent 发件箱里的报文没有它，读出为 `None`；平常不带，线上报文与旧版一致；
    /// 带上时旧总控照常读（未知字段忽略）。
    #[test]
    fn part_closed_at_mixes_with_older_peers() {
        let old = r#"{"seq":1,"event":{"Recorder":{"event":{"PartClosed":{"owner":{"room":"r","session":"s"},"part_id":"p","path":"/a.flv","duration":{"secs":5,"nanos":0},"size":1,"reason":"TimedSplit","title":"t"}},"at":9}}}"#;
        let e: Envelope = serde_json::from_str(old).unwrap();
        let AgentEvent::Recorder {
            event: Event::PartClosed(p),
            ..
        } = &e.event
        else {
            panic!()
        };
        assert_eq!(p.closed_at_ms, None);
        assert_eq!(serde_json::to_string(&e).unwrap(), old, "不带时报文不变");
        let mut p = p.clone();
        p.closed_at_ms = Some(7);
        let new = serde_json::to_value(&p).unwrap();
        assert_eq!(new["closed_at_ms"], 7);
        /// 加 `closed_at_ms` 之前的 `PartClosed`（只取几个字段）。
        #[derive(Deserialize)]
        struct OldPartClosed {
            part_id: String,
        }
        assert_eq!(
            serde_json::from_value::<OldPartClosed>(new)
                .unwrap()
                .part_id,
            "p"
        );
    }

    /// `Assignments::tz_offset_secs` 与 `PartOpened::tz_offset_secs`：旧报文缺省为 `None`，
    /// 新报文旧端照常读（未知字段忽略）。
    #[test]
    fn tz_offset_secs_mixes_with_older_peers() {
        let a: Assignments = serde_json::from_str(OLD_ASSIGNMENTS).unwrap();
        assert_eq!(a.tz_offset_secs, None);
        #[derive(Deserialize)]
        struct OldAssignments {
            rooms: Vec<RoomTask>,
        }
        let new = Assignments {
            tz_offset_secs: Some(28800),
            ..Default::default()
        };
        let text = serde_json::to_string(&new).unwrap();
        assert!(text.contains(r#""tz_offset_secs":28800"#), "{text}");
        let old: OldAssignments = serde_json::from_str(&text).unwrap();
        assert!(old.rooms.is_empty());
        assert_eq!(serde_json::from_str::<Assignments>(&text).unwrap(), new);

        let old_open = r#"{"seq":1,"event":{"Recorder":{"event":{"PartOpened":{"owner":{"room":"r","session":"s"},"part_id":"p","path":"/a.flv.part","title":"t"}},"at":9}}}"#;
        let e: Envelope = serde_json::from_str(old_open).unwrap();
        let AgentEvent::Recorder {
            event: Event::PartOpened(p),
            ..
        } = &e.event
        else {
            panic!()
        };
        assert_eq!(p.tz_offset_secs, None);
        assert_eq!(serde_json::to_string(&e).unwrap(), old_open);
        let mut p = p.clone();
        p.tz_offset_secs = Some(32400);
        let v = serde_json::to_value(&p).unwrap();
        assert_eq!(v["tz_offset_secs"], 32400);
        #[derive(Deserialize)]
        struct OldPartOpened {
            part_id: String,
        }
        assert_eq!(
            serde_json::from_value::<OldPartOpened>(v).unwrap().part_id,
            "p"
        );
    }

    #[test]
    fn drain_is_tagged_by_action() {
        let wait = Drain::Wait { reason: "x".into() };
        assert_eq!(
            serde_json::to_string(&wait).unwrap(),
            r#"{"action":"wait","reason":"x"}"#
        );
        assert_eq!(
            serde_json::to_string(&Drain::Go).unwrap(),
            r#"{"action":"go"}"#
        );
        assert_eq!(
            serde_json::to_string(&Drain::Done).unwrap(),
            r#"{"action":"done"}"#
        );
    }
}
