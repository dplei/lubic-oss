//! 总控与 agent 共用的日志初始化：照常输出到 stdout，配了目录再按天写一份文件。

use std::path::Path;
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

/// 日志文件保留天数。
pub const KEEP_DAYS: usize = 14;

/// `dir` 下按天切分的文件：`<name>.YYYY-MM-DD.log`（UTC 日期），只留最近 [`KEEP_DAYS`] 个。
/// 旧文件由 tracing-appender 在启动和每天换文件时清理。
pub fn appender(dir: &Path, name: &str) -> Result<RollingFileAppender, String> {
    // 先建好目录：tracing-appender 建目录前就去清理，目录不在会打一行无害的报错。
    std::fs::create_dir_all(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    RollingFileAppender::builder()
        .rotation(Rotation::DAILY)
        .filename_prefix(name)
        .filename_suffix("log")
        .max_log_files(KEEP_DAYS)
        .build(dir)
        .map_err(|e| format!("{}: {e}", dir.display()))
}

/// 级别用 `RUST_LOG`，缺省 info。`dir` 为 `None` 时只输出到 stdout。
pub fn init(dir: Option<&Path>, name: &str) -> Result<(), String> {
    let file = dir
        .map(|d| appender(d, name))
        .transpose()?
        .map(|w| fmt::layer().with_ansi(false).with_writer(w));
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .with(fmt::layer())
        .with(file)
        .init();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn writes_daily_file_and_prunes_beyond_keep_days() {
        let dir = tempfile::tempdir().unwrap();
        for day in 1..=20 {
            std::fs::write(
                dir.path().join(format!("agent.2026-01-{day:02}.log")),
                "old",
            )
            .unwrap();
        }
        // 别的前缀不归它管。
        std::fs::write(dir.path().join("other.2026-01-01.log"), "keep").unwrap();

        let mut w = appender(dir.path(), "agent").unwrap();
        writeln!(w, "hello").unwrap();
        w.flush().unwrap();

        let today = chrono::Utc::now().format("agent.%Y-%m-%d.log").to_string();
        assert_eq!(
            std::fs::read_to_string(dir.path().join(&today)).unwrap(),
            "hello\n"
        );
        let ours = std::fs::read_dir(dir.path())
            .unwrap()
            .filter(|e| {
                e.as_ref()
                    .unwrap()
                    .file_name()
                    .to_str()
                    .unwrap()
                    .starts_with("agent.")
            })
            .count();
        assert_eq!(ours, KEEP_DAYS);
        assert!(dir.path().join("other.2026-01-01.log").exists());
    }
}
