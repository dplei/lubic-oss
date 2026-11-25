//! 录制与上传之间唯一共享的东西：分段事件。
//!
//! 两边都只依赖本 crate，互不 import。agent 内经 channel 传递；以后拆进程只换传输层，
//! 所以类型全部可序列化，且只含值，不含句柄。

use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;

/// 录制归属：哪个房间、哪一场。多机接力的同一场按 `session` 归入同一稿件。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Owner {
    pub room: String,
    /// 平台场次键（`live_session_key`）。
    pub session: String,
}

/// 开始写一个新分段。`path` 是正在写的 `.part` 临时文件。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartOpened {
    pub owner: Owner,
    pub part_id: String,
    pub path: PathBuf,
    /// 直播标题，同 [`PartClosed::title`]。
    pub title: String,
    /// 建文件渲染 `path` 时实际使用的 UTC 偏移秒数（东偏为正，如 `+08:00` 为 `28800`）。
    /// 旧 agent 不报时为 `None`，总控投稿时按配置时区兜底重渲染分P标题。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tz_offset_secs: Option<i32>,
}

/// 分段已收尾，`path` 是改名后的最终文件，可以上传了。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartClosed {
    pub owner: Owner,
    pub part_id: String,
    pub path: PathBuf,
    /// 墙钟时长（从建文件到收尾），不是媒体时间轴；实时倍率接近 1 时两者相当。
    pub duration: Duration,
    pub size: u64,
    pub reason: CloseReason,
    /// 录这段时的直播标题，投稿标题模板要用。
    pub title: String,
    /// 实际收尾的时刻（unix 毫秒）。只有晚于收尾才补发的事件带（故障试拉转正时补发的暂存分段），
    /// agent 拿它当事件的 `at`，总控按 `at − duration` 推出的开录时刻与排序才对；平常为 `None`，按收到的时刻。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub closed_at_ms: Option<i64>,
}

/// 录制侧发给 agent 的事件。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Event {
    /// agent 据此在建文件时就记下分段归属，崩溃后能收回遗留的 `.part`。
    PartOpened(PartOpened),
    PartClosed(PartClosed),
    /// 分段没有音视频帧（断连瞬间常见），收尾时已删掉，不用上传。`path`、`size` 是删前的最终文件。
    PartDiscarded(PartClosed),
    /// 下播宽限期满，这一场不会再有新分段，可以提交了。
    SessionEnded(Owner),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CloseReason {
    TimedSplit,
    SizeSplit,
    StreamEnded,
    TransportError,
    Cancelled,
    Unknown,
}
