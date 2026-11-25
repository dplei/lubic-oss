//! 故障试拉的暂存录制（`docs/design/handoff-health.md` §6.1）：录制器把分段写进暂存目录，
//! 事件先经这里的闸门。转正前一条都不放行（不上传、不进稿件）；转正时把已收尾的暂存分段移进正式目录、
//! 补发 `PartClosed`，之后照常放行。失败或取消由调用方删掉整个暂存目录。

use lubic::recorder::{RecordConfig, file_template, unix_now_ms};
use lubic_events::{Event, PartClosed};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tracing::{error, info};

pub struct Gate {
    inner: Mutex<Inner>,
}

struct Inner {
    /// 暂存目录（agent 的 `.trial/`）：路径在它下面的分段是暂存分段。
    staging: PathBuf,
    /// 正式录制的参数，算暂存分段该移到哪个目录。
    formal: RecordConfig,
    room: String,
    tx: UnboundedSender<Event>,
    promoted: bool,
    /// 转正时总控让丢弃暂存内容（§6.2）：暂存分段收尾即删，不补发。
    drop: bool,
    /// 转正前收尾的暂存分段，转正时补发。
    held: Vec<PartClosed>,
}

impl Gate {
    pub fn new(
        staging: PathBuf,
        formal: RecordConfig,
        room: String,
        tx: UnboundedSender<Event>,
    ) -> Self {
        Self {
            inner: Mutex::new(Inner {
                staging,
                formal,
                room,
                tx,
                promoted: false,
                drop: false,
                held: Vec::new(),
            }),
        }
    }

    /// 把录制器的事件过一遍闸门，直到录制停下（发送端都 drop）。
    pub async fn forward(&self, mut rx: UnboundedReceiver<Event>) {
        while let Some(event) = rx.recv().await {
            self.inner.lock().unwrap().pass(event);
        }
    }

    /// 转正：已收尾的暂存分段移进正式目录并补发，之后的事件照常放行。
    /// `drop` 时暂存分段（包括转正时正在写、到切点才收尾的那段）都删掉、不补发。
    pub fn promote(&self, drop: bool) {
        let mut inner = self.inner.lock().unwrap();
        inner.promoted = true;
        inner.drop = drop;
        for part in std::mem::take(&mut inner.held) {
            inner.release(part);
        }
    }

    /// 运行中总控时区更新时同步更新正式录制的时区（转正移文件时目录模板用到）。
    pub fn set_tz_offset(&self, tz: Option<chrono::FixedOffset>) {
        self.inner.lock().unwrap().formal.tz_offset = tz;
    }
}

impl Inner {
    fn pass(&mut self, event: Event) {
        let staged = |path: &Path| path.starts_with(&self.staging);
        match event {
            // 暂存分段只补 `PartClosed`，带上收尾的时刻：总控按 `at − duration` 推开录时刻并排序，
            // 一次补发多个分段时都按补发时刻算会把先短后长的两段排反。
            Event::PartClosed(mut part) if staged(&part.path) => match self.promoted {
                true => self.release(part),
                false => {
                    part.closed_at_ms = Some(unix_now_ms());
                    self.held.push(part);
                }
            },
            // 暂存分段的开录与空段、转正前的场次结束都不放行，稿件里不留痕迹。
            Event::PartOpened(p) if staged(&p.path) => {}
            Event::PartDiscarded(p) if staged(&p.path) => {}
            _ if !self.promoted => {}
            event => {
                let _ = self.tx.send(event);
            }
        }
    }

    fn release(&mut self, mut part: PartClosed) {
        if self.drop {
            match std::fs::remove_file(&part.path) {
                Ok(()) => {
                    info!(room = self.room, path = %part.path.display(), "trial segment dropped before the cut")
                }
                Err(e) => {
                    error!(room = self.room, path = %part.path.display(), error = %e, "failed to drop trial segment")
                }
            }
            return;
        }
        match self.move_out(&part.path, &part.title) {
            Ok(path) => {
                info!(room = self.room, path = %path.display(), "trial segment promoted");
                part.path = path;
                let _ = self.tx.send(Event::PartClosed(part));
            }
            // 移不出去就不报：暂存目录下次启动会被清掉，宁可丢这一段也不让总控等一个不存在的文件。
            Err(e) => {
                error!(room = self.room, path = %part.path.display(), error = %e, "failed to promote trial segment")
            }
        }
    }

    /// 移到正式录制此刻会写的目录，重名时加序号。
    fn move_out(&self, from: &Path, title: &str) -> std::io::Result<PathBuf> {
        let template = file_template(&self.formal, &self.room, title);
        let rendered = PathBuf::from(lubic::downloader::util::format_filename_in(
            &template,
            self.formal.tz_offset,
        ));
        let dir = rendered.parent().unwrap_or(Path::new("."));
        std::fs::create_dir_all(dir)?;
        let name = from.file_name().unwrap_or_default();
        let mut to = dir.join(name);
        let mut n = 0;
        while to.exists() {
            n += 1;
            let stem = from.file_stem().unwrap_or_default().to_string_lossy();
            let ext = from.extension().unwrap_or_default().to_string_lossy();
            to = dir.join(format!("{stem}-{n}.{ext}"));
        }
        if std::fs::rename(from, &to).is_err() {
            // 暂存目录与正式目录不在同一个文件系统。
            std::fs::copy(from, &to)?;
            std::fs::remove_file(from)?;
        }
        Ok(to)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lubic_events::{CloseReason, Owner, PartOpened};
    use std::time::Duration;
    use tokio::sync::mpsc::unbounded_channel;

    fn closed(path: PathBuf) -> PartClosed {
        PartClosed {
            owner: Owner::default(),
            part_id: "p".into(),
            path,
            duration: Duration::from_secs(30),
            size: 1,
            reason: CloseReason::Unknown,
            title: "t".into(),
            closed_at_ms: None,
        }
    }

    /// 转正前什么都不放行；转正时已收尾的暂存分段移进正式目录、只补 `PartClosed`，之后照常放行。
    #[test]
    fn holds_events_until_promoted_then_moves_staged_parts_out() {
        let tmp = tempfile::tempdir().unwrap();
        let staging = tmp.path().join(".trial");
        let formal = RecordConfig {
            output: tmp.path().join("rec/{name}{title}").display().to_string(),
            filename: Some("x".into()),
            ..Default::default()
        };
        let (tx, mut rx) = unbounded_channel();
        let gate = Gate::new(staging.clone(), formal, "r".into(), tx);
        let staged = |name: &str| {
            let path = staging.join("r").join(name);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(&path, name).unwrap();
            path
        };
        let opened = PartOpened {
            owner: Owner::default(),
            part_id: "p".into(),
            path: staged("a.flv.part"),
            title: String::new(),
            tz_offset_secs: None,
        };
        let pass = |e: Event| gate.inner.lock().unwrap().pass(e);
        pass(Event::PartOpened(opened.clone()));
        pass(Event::PartClosed(closed(staged("a.flv"))));
        pass(Event::PartDiscarded(closed(staged("b.flv"))));
        pass(Event::SessionEnded(Owner::default()));
        assert!(rx.try_recv().is_err(), "转正前不放行");
        // 正式目录里已有同名文件：加序号，不覆盖。
        std::fs::create_dir_all(tmp.path().join("rec/rt")).unwrap();
        std::fs::write(tmp.path().join("rec/rt/a.flv"), "old").unwrap();
        gate.promote(false);
        let Ok(Event::PartClosed(p)) = rx.try_recv() else {
            panic!()
        };
        assert_eq!(p.path, tmp.path().join("rec/rt/a-1.flv"));
        assert_eq!(std::fs::read_to_string(&p.path).unwrap(), "a.flv");
        assert!(rx.try_recv().is_err(), "暂存的开录与空段不补发");
        // 转正后：暂存分段收尾照样移出来；正式目录的分段原样放行。
        pass(Event::PartClosed(closed(staged("c.flv"))));
        let formal = tmp.path().join("rec/rt/d.flv.part");
        pass(Event::PartOpened(PartOpened {
            path: formal.clone(),
            ..opened
        }));
        let got: Vec<_> = std::iter::from_fn(|| rx.try_recv().ok()).collect();
        assert!(
            matches!(&got[..], [Event::PartClosed(c), Event::PartOpened(o)]
            if c.path == tmp.path().join("rec/rt/c.flv") && o.path == formal),
            "{got:?}"
        );
    }

    /// 转正时丢弃暂存内容（原机在试拉期间写出过视频，§6.2）：转正前与转正时正在写的暂存分段都删掉、不补发，
    /// 切点之后的正式分段照常放行。
    #[test]
    fn dropping_on_promotion_deletes_staged_parts_without_events() {
        let tmp = tempfile::tempdir().unwrap();
        let staging = tmp.path().join(".trial");
        let formal = RecordConfig {
            output: tmp.path().join("rec/{name}").display().to_string(),
            ..Default::default()
        };
        let (tx, mut rx) = unbounded_channel();
        let gate = Gate::new(staging.clone(), formal, "r".into(), tx);
        let staged = |name: &str| {
            let path = staging.join("r").join(name);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(&path, name).unwrap();
            path
        };
        let pass = |e: Event| gate.inner.lock().unwrap().pass(e);
        let (a, b) = (staged("a.flv"), staged("b.flv"));
        pass(Event::PartClosed(closed(a.clone())));
        gate.promote(true);
        // 转正时正在写的暂存分段在切点收尾。
        pass(Event::PartClosed(closed(b.clone())));
        assert!(!a.exists() && !b.exists(), "暂存分段删掉");
        assert!(rx.try_recv().is_err(), "不补发");
        let after = tmp.path().join("rec/r/c.flv");
        pass(Event::PartClosed(closed(after.clone())));
        assert!(matches!(rx.try_recv(), Ok(Event::PartClosed(p)) if p.path == after));
    }

    #[test]
    fn move_out_uses_formal_tz_offset_for_dated_output_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let staging = tmp.path().join(".trial");
        let tz = chrono::FixedOffset::east_opt(9 * 3600).unwrap();
        let formal = RecordConfig {
            output: tmp.path().join("rec_%:z/{name}").display().to_string(),
            filename: Some("x".into()),
            tz_offset: Some(tz),
            ..Default::default()
        };
        let (tx, mut rx) = unbounded_channel();
        let gate = Gate::new(staging.clone(), formal, "r".into(), tx);
        let staged = staging.join("r/a.flv");
        std::fs::create_dir_all(staged.parent().unwrap()).unwrap();
        std::fs::write(&staged, "data").unwrap();
        gate.inner
            .lock()
            .unwrap()
            .pass(Event::PartClosed(closed(staged)));
        gate.promote(false);
        let Ok(Event::PartClosed(p)) = rx.try_recv() else {
            panic!()
        };
        assert_eq!(p.path, tmp.path().join("rec_+09:00/r/a.flv"));
    }
}
