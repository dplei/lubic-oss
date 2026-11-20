use clap::Parser;
use lubic::downloader::live::builtin_plugins;
use lubic::recorder::{RecordConfig, watch_room};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

/// 监视直播间，开播即录，按时长/大小切段写到本地。
#[derive(Parser)]
#[command(version)]
struct Cli {
    /// 直播间地址，可写成 `名字=地址` 指定文件名里的 {name}；否则取地址最后一段。
    #[arg(required = true)]
    rooms: Vec<String>,

    /// 输出模板（不含扩展名），支持 {name}、{title} 与 strftime 占位符。
    #[arg(short, long, default_value = "{name}/{name}_%Y-%m-%dT%H_%M_%S")]
    output: String,

    /// 按时长切段，如 30m、1h。
    #[arg(long)]
    split_time: Option<humantime::Duration>,

    /// 按大小切段，单位 MiB。
    #[arg(long)]
    split_size_mb: Option<u64>,

    /// 未开播时的检测间隔。
    #[arg(long, default_value = "30s")]
    poll: humantime::Duration,

    /// 下播宽限期：断流后持续复查这么久仍不在播才算下播。
    #[arg(long, default_value = "5m")]
    grace: humantime::Duration,

    /// 码流停顿看门狗：多久收不到数据就判断连。
    #[arg(long, default_value = "30s")]
    stall: humantime::Duration,

    /// B 站画质 qn（10000 原画）。
    #[arg(long)]
    bili_qn: Option<u32>,

    /// B 站 cookie 文件（biliup 登录产生的 cookies.json）。
    #[arg(long)]
    bili_cookie_file: Option<PathBuf>,

    /// 抖音画质：origin/uhd/hd/sd/ld/md。
    #[arg(long)]
    douyin_quality: Option<String>,

    /// 抖音 cookie 字符串。
    #[arg(long, env = "LUBIC_DOUYIN_COOKIE", hide_env_values = true)]
    douyin_cookie: Option<String>,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let cli = Cli::parse();

    let mut config = RecordConfig {
        output: cli.output,
        split_time: cli.split_time.map(Into::into),
        split_size: cli.split_size_mb.map(|mb| mb * 1024 * 1024),
        poll_interval: cli.poll.into(),
        offline_grace: cli.grace.into(),
        stall_timeout: Duration::from(cli.stall).max(Duration::from_secs(1)),
        ..Default::default()
    };
    if let Some(qn) = cli.bili_qn {
        config.options.bilibili.qn = qn;
    }
    if let Some(quality) = cli.douyin_quality {
        config.options.douyin.quality = quality;
    }
    config.credentials.bilibili_cookie_file = cli.bili_cookie_file;
    config.credentials.douyin_cookie = cli.douyin_cookie;
    let config = Arc::new(config);

    let plugins = builtin_plugins();
    let cancel = CancellationToken::new();
    let mut tasks = Vec::new();
    for room in cli.rooms {
        let (name, url) = match room.split_once('=') {
            Some((name, url)) if url.starts_with("http") => (name.to_string(), url.to_string()),
            _ => (default_name(&room), room),
        };
        let Some(plugin) = plugins.iter().find(|p| p.matches(&url)).cloned() else {
            error!(url, "no plugin matches this url, skipped");
            continue;
        };
        tasks.push(tokio::spawn(watch_room(
            plugin,
            name,
            url,
            config.clone(),
            cancel.clone(),
        )));
    }
    if tasks.is_empty() {
        error!("nothing to record");
        std::process::exit(2);
    }

    // Ctrl-C：通知所有房间收尾当前分段后退出。
    let _ = tokio::signal::ctrl_c().await;
    info!("shutting down, finalizing segments...");
    cancel.cancel();
    for task in tasks {
        let _ = task.await;
    }
}

/// 地址最后一个非空路径段，去掉 query，如 `https://live.bilibili.com/123?x` → `123`。
fn default_name(url: &str) -> String {
    url.split('?')
        .next()
        .unwrap_or(url)
        .trim_end_matches('/')
        .rsplit('/')
        .next()
        .filter(|s| !s.is_empty())
        .unwrap_or("room")
        .to_string()
}
