//! lubic：从 biliup 提炼出的最小稳定录制核心。
//!
//! - [`downloader::live`]：平台插件，回答「开播了吗、流地址是什么」。
//! - [`downloader::httpflv`] / [`downloader::hls`]：拉流、修时间戳、按规则切段写盘。
//! - [`recorder`]：轮询 → 录制 → 断流重连 → 下播宽限，串起上面两层。
//! - [`danmaku`]：跟着分段事件按分P录 B 站 / 抖音弹幕（XML）。
//! - [`edge_probe`]：拉流出问题时对照几个公共 DNS 会落到的边缘节点，只打日志。
//! - [`logging`]：总控与 agent 共用的日志初始化（stdout + 可选的按天文件）。

pub mod client;
pub mod danmaku;
pub mod downloader;
pub mod edge_probe;
pub mod logging;
pub mod recorder;

use rand::Rng;
use std::future::Future;
use std::time::Duration;
use tracing::info;

/// 等到 Ctrl-C 或 SIGTERM（launchd、`docker stop` 默认），总控与 agent 都按同样方式收尾。
pub async fn stop_signal() {
    use tokio::signal::unix::{SignalKind, signal};
    let mut term = signal(SignalKind::terminate()).expect("SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
}

/// 最多重试 3 次，等待 2/4/8 秒并叠加 0~1 秒抖动，避免多个房间同时重连撞在一起。
pub async fn retry<F, Fut, O, E>(mut f: F) -> Result<O, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<O, E>>,
    E: std::fmt::Display,
{
    const MAX_RETRIES: u32 = 3;
    let mut attempt = 0;
    loop {
        match f().await {
            Err(e) if attempt < MAX_RETRIES => {
                attempt += 1;
                let wait = 2f64.powi(attempt as i32) + rand::thread_rng().gen_range(0.0..1.0);
                info!("Retry attempt #{attempt}. Sleeping {wait:.1}s before the next attempt. {e}");
                tokio::time::sleep(Duration::from_secs_f64(wait)).await;
            }
            res => break res,
        }
    }
}
