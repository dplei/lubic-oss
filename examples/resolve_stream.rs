//! 协议实测辅助：复用录制器的平台解析，将带签名的流地址写入私有文件。
use lubic::downloader::live::{LiveRequest, LiveStatus, builtin_plugins};
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().collect();
    if !(3..=4).contains(&args.len()) {
        return Err("usage: resolve_stream ROOM_URL OUTPUT_JSON [BILIBILI_COOKIE_FILE]".into());
    }
    let plugin = builtin_plugins()
        .into_iter()
        .find(|p| p.matches(&args[1]))
        .ok_or("unsupported platform")?;
    let mut options = lubic::downloader::live::LiveOptions::default();
    options.bilibili.qn = 10000;
    options.bilibili.cdn_fallback = true;
    let status = plugin
        .check_stream(LiveRequest {
            client: reqwest::Client::new(),
            url: args[1].clone(),
            name: "UPOS 有限缓冲验证".into(),
            options,
            // 不带登录 cookie 时 B 站只给低画质，原画要账号 cookie。
            credentials: lubic::downloader::live::LiveCredentials {
                bilibili_cookie_file: args.get(3).map(Into::into),
                ..Default::default()
            },
        })
        .await?;
    let LiveStatus::Live { stream } = status else {
        return Err("room is offline".into());
    };
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&args[2])?;
    file.write_all(&serde_json::to_vec(&stream)?)?;
    file.sync_all()?;
    println!(
        "live stream resolved; platform={}; format={}; quality={:?}",
        stream.platform, stream.suffix, stream.recording_quality
    );
    Ok(())
}
