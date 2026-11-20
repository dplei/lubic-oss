//! 受限信号的采样：每 [`SAMPLE_EVERY`] 读一次各房间录制与上传的 [`Pace`]，算出报给总控的窗口值。
//! 只报数，不下结论；阈值在总控（见 `docs/design/handoff.md`）。

use lubic::recorder::{Pace, PaceCount, unix_now_ms};
use lubic_proto::{HEALTH_WINDOW, PROBE_PREFIX, SKIP_WINDOW, Status, TRIAL_PREFIX, TRIAL_WINDOW};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::Notify;

pub const SAMPLE_EVERY: Duration = Duration::from_secs(10);
pub const CHECK_EVERY: Duration = Duration::from_secs(1);
/// 写出第一个关键帧时还没见到 SEI，最多再等这么久再报。
const SEI_WAIT: Duration = Duration::from_secs(3);
const RECORD_WINDOW: Duration = Duration::from_secs(120);
/// 录制倍率按这么长的子窗口算，报最近 2 分钟里最高的那个：全部子窗口都低才算「持续」低。
const RATE_SLICE: Duration = Duration::from_secs(30);
/// 子窗口里持续收数据的时长不足这么久就不算它。
const MIN_SLICE_ACTIVE: Duration = Duration::from_secs(15);
/// 快速判慢：最近这么久里数据一直在流（[`PaceCount::flowing`]）却只收到不到 [`FAST_RATE_MAX`] 倍的内容，
/// 就不等 2 分钟窗口，立刻报这个倍率并叫醒上报。CDN 限速到 0.1~0.3 倍、每二三十秒断一次时，
/// 等满 2 分钟就丢了 1~2 分钟内容（2026-11-03 某房间 云服务器 A）；主播推流卡顿时数据不流，不算进来。
const FAST_SLICE: Duration = Duration::from_secs(20);
const FAST_MIN_FLOWING: Duration = Duration::from_secs(15);
const FAST_RATE_MAX: f64 = 0.4;
const UPLOAD_WINDOW: Duration = Duration::from_secs(600);
/// 窗口里计时不足这么久就不报倍率 / 上传速度，样本太少。
const MIN_WALL: Duration = Duration::from_secs(60);

#[derive(Default)]
pub struct Monitor {
    inner: Mutex<Inner>,
    /// 有房间新出现时间戳前跳时唤醒，agent 据此中断正在挂着的长轮询、立刻上报。
    pub alarm: Notify,
}

/// 试拉的房间（键以 [`PROBE_PREFIX`] 或故障试拉的 [`TRIAL_PREFIX`] 开头）：只报原始数，不算产出、不报开录、不叫醒上报。
fn is_probe(name: &str) -> bool {
    name.starts_with(PROBE_PREFIX) || is_trial(name)
}

fn is_trial(name: &str) -> bool {
    name.starts_with(TRIAL_PREFIX)
}

/// 某房间这次录制写出了第一个关键帧（见 `AgentEvent::Recording`）。
#[derive(Debug, PartialEq)]
pub struct Started {
    pub room: String,
    pub lag_ms: Option<i64>,
    pub start_live_ms: Option<i64>,
}

#[derive(Default)]
struct Inner {
    rooms: HashMap<String, Room>,
    /// 已停的录制写过的字节，产出速度要算上它们。
    gone_bytes: u64,
    produced: Window,
    upload: Window,
    /// 最近一次采样算出的值，填进 `Status`。
    rates: Vec<(String, f64)>,
    lags: Vec<(String, i64)>,
    skips: Vec<(String, u64, u32)>,
    produce_bps: f64,
    upload_bps: Option<f64>,
    upload_busy: f64,
}

struct Room {
    pace: Arc<Pace>,
    window: Window,
    /// 算 [`Status::record_skips`] 用的样本，保留 [`SKIP_WINDOW`]。断流时不清空：
    /// 一阵一阵来的数据正是丢内容的形态，清了就永远攒不出来。
    skips: Window,
    /// 上次采样时的累计 [`PaceCount::jumps`]：涨了就叫醒 [`Monitor::alarm`]。
    last_jumps: u32,
    /// 这次录制已报过「写出第一个关键帧」。
    reported: bool,
    /// 第一次见到写出关键帧（`media > 0`）的时刻：还没见到 SEI 时最多再等 [`SEI_WAIT`]。
    first_media_at: Option<Instant>,
    /// 上次采样时的 [`PaceCount::gaps`]：变了说明中间断过流，窗口从头积累。
    gaps: u32,
    /// 上次采样时最近一条结束的连接：变了说明开始了新连接，快速判慢只看 `fast_since` 之后的样本。
    last_connection: Option<(i64, u64, bool)>,
    fast_since: Option<Instant>,
    last_failures: (u32, bool),
    /// 故障试拉的逐秒样本 (时刻, 累计媒体, 累计连接墙钟)，只留最近 [`TRIAL_WINDOW`]（见 [`Status::record_trial`]）。
    trace: VecDeque<(Instant, Duration, Duration)>,
}

impl Monitor {
    /// 起一次录制：换一份新的计数器（同名的旧录制按已停处理）。
    pub fn start_room(&self, name: &str) -> Arc<Pace> {
        let mut inner = self.inner.lock().unwrap();
        inner.retire(name);
        let pace = Arc::new(Pace::default());
        inner.rooms.insert(
            name.to_string(),
            Room {
                pace: pace.clone(),
                window: Window::default(),
                skips: Window::default(),
                last_jumps: 0,
                reported: false,
                first_media_at: None,
                gaps: 0,
                last_connection: None,
                fast_since: None,
                last_failures: (0, false),
                trace: VecDeque::new(),
            },
        );
        pace
    }

    /// 故障试拉转正：同一份计数改记在房间名下，之后按正常录制报（算产出、补报开录、叫醒上报）。
    pub fn promote(&self, from: &str, to: &str) {
        let mut inner = self.inner.lock().unwrap();
        inner.retire(to);
        if let Some(mut room) = inner.rooms.remove(from) {
            room.reported = false;
            room.first_media_at = None;
            room.trace.clear();
            inner.rooms.insert(to.to_string(), room);
        }
    }

    pub fn stop_room(&self, name: &str) {
        self.inner.lock().unwrap().retire(name);
    }

    /// 检测哪些房间刚写出第一个关键帧（每 [`CHECK_EVERY`] 调一次）。
    pub fn check_started(&self, now: Instant) -> Vec<Started> {
        let mut inner = self.inner.lock().unwrap();
        let started = inner.check_started(now);
        let alarm = inner.check_failures();
        drop(inner);
        if alarm {
            self.alarm.notify_waiters();
        }
        started
    }

    /// 采样一次，返回本次才写出第一个关键帧的房间。
    pub fn sample(&self, now: Instant, upload: &Pace) -> Vec<Started> {
        let mut inner = self.inner.lock().unwrap();
        let started = inner.check_started(now);
        let mut rates = Vec::new();
        let mut lags = Vec::new();
        let mut skips = Vec::new();
        // 故障试拉要按 20 秒窗口判断，等不起 25 秒的长轮询：试拉期间每次采样都上报。
        let mut alarm = inner.check_failures() | inner.rooms.keys().any(|k| is_trial(k));
        let mut bytes = inner.gone_bytes;
        for (name, room) in &mut inner.rooms {
            let count = room.pace.read_at(now);
            let probe = is_probe(name);
            if !probe {
                bytes += count.bytes;
            }
            // 新出现前跳：叫醒主循环立刻上报，不等长轮询挂满。试拉的结论不急，不叫。
            if count.jumps > room.last_jumps && !probe {
                alarm = true;
            }
            room.last_jumps = count.jumps;
            room.skips.push(now, count, SKIP_WINDOW);
            if let Some(d) = room.skips.delta(now, SKIP_WINDOW)
                && d.jumps > 0
            {
                skips.push((name.clone(), d.skipped.as_millis() as u64, d.jumps));
            }
            // 断过流（超过 DATA_GAP 没收到数据）：之前的样本作废，重新攒满 2 分钟才出倍率，
            // 「持续 2 分钟低」说的是连续收数据的 2 分钟。限速连接被 CDN 断开、几秒内重连不算断流。
            if count.gaps != room.gaps {
                room.gaps = count.gaps;
                room.window.0.clear();
            }
            // 开始了新连接：快窗从头计时，新连接自己攒满流动时间才判慢。旧节点结束前内容常只给零点几倍，
            // 重连已经解决，不该再拿那段报（2026-11-17 云服务器 A、云服务器 B）。2 分钟窗口不清。
            let last_connection = room.pace.recent_connections(i64::MIN).last().copied();
            // 变回 `None` 只是上报时裁掉了旧记录（只裁最旧的，裁空才 `None`），不算新连接。
            if last_connection.is_some() && last_connection != room.last_connection {
                room.last_connection = last_connection;
                room.fast_since = Some(now);
            }
            room.window.push(now, count, RECORD_WINDOW);
            if let Some(lag) = count.lag_ms {
                lags.push((name.clone(), lag));
            }
            let fast = room
                .window
                .fast_rate(now, room.fast_since)
                .filter(|r| *r < FAST_RATE_MAX);
            // 快速判慢同样叫醒上报：总控拿到就标记受限、立刻交接。试拉的结论照样不急。
            alarm |= fast.is_some() && !probe;
            if let Some(rate) = fast.or_else(|| room.window.sustained_rate(now)) {
                rates.push((name.clone(), rate));
            }
        }
        rates.sort_by(|a, b| a.0.cmp(&b.0));
        inner.rates = rates;
        lags.sort();
        inner.lags = lags;
        skips.sort();
        inner.skips = skips;
        let produced = PaceCount {
            bytes,
            ..Default::default()
        };
        inner.produced.push(now, produced, UPLOAD_WINDOW);
        inner.produce_bps = inner.produced.per_sec(now, UPLOAD_WINDOW);
        inner.upload.push(now, upload.read(), UPLOAD_WINDOW);
        let d = inner.upload.delta(now, UPLOAD_WINDOW).unwrap_or_default();
        inner.upload_busy = d.wall.as_secs_f64() / UPLOAD_WINDOW.as_secs_f64();
        inner.upload_bps = (d.wall >= MIN_WALL).then(|| d.bytes as f64 / d.wall.as_secs_f64());
        drop(inner);
        // notify_waiters 只叫醒此刻在等的，不留 permit：下一次长轮询不会刚发出就被打断。
        if alarm {
            self.alarm.notify_waiters();
        }
        started
    }

    pub fn fill(&self, status: &mut Status) {
        let inner = self.inner.lock().unwrap();
        status.record_rates = inner.rates.clone();
        status.record_lags = inner.lags.clone();
        status.record_skips = inner.skips.clone();
        status.record_failures = inner
            .rooms
            .iter()
            .filter_map(|(name, room)| {
                let (attempts, stalled) = room.pace.record_failures();
                (attempts > 0 || stalled).then(|| (name.clone(), attempts, stalled))
            })
            .collect();
        status.record_failures.sort();
        status.health_reports = true;
        status.trial_support = true;
        let now_at = Instant::now();
        let ms = |d: Duration| d.as_millis() as u64;
        status.record_trial = inner
            .rooms
            .iter()
            .filter(|(name, _)| is_trial(name))
            .map(|(name, room)| {
                let samples = room.trace.iter().map(|(at, media, wall)| {
                    (
                        ms(now_at.saturating_duration_since(*at)),
                        ms(*media),
                        ms(*wall),
                    )
                });
                (name.clone(), samples.collect())
            })
            .collect();
        status.record_trial.sort();
        // 发心跳时现算距今多久，总控直接用，不跨机器比较绝对时刻。
        let now = unix_now_ms();
        let ago = |t: i64| (now - t).max(0) as u64;
        let since = now - HEALTH_WINDOW.as_millis() as i64;
        status.record_connections = inner
            .rooms
            .iter()
            .flat_map(|(name, room)| {
                let ends = room.pace.recent_connections(since);
                ends.into_iter()
                    .map(|(end, ms, video)| (name.clone(), ago(end), ms, video))
            })
            .collect();
        status.record_connections.sort();
        status.record_gaps = inner
            .rooms
            .iter()
            .flat_map(|(name, room)| {
                let gaps = room.pace.recent_gaps(since);
                gaps.into_iter().map(|(at, ms)| (name.clone(), ago(at), ms))
            })
            .collect();
        status.record_gaps.sort_by(|a, b| a.0.cmp(&b.0));
        status.record_video = inner
            .rooms
            .iter()
            .filter_map(|(name, room)| {
                let (video, since) = room.pace.video_status()?;
                Some((name.clone(), video.map(ago), ago(since)))
            })
            .collect();
        status.record_video.sort();
        // 现读而不用采样值：交接时总控拿它和收到心跳的时刻相减，算旧机此刻录到了直播的哪一刻，
        // 隔了一个采样周期的旧值在旧机录制变慢时会高估它的进度。
        let mut live_lags: Vec<_> = inner
            .rooms
            .iter()
            .filter_map(|(name, room)| Some((name.clone(), room.pace.read().live_lag_ms?)))
            .collect();
        live_lags.sort();
        status.record_live_lags = live_lags;
        status.record_qualities = inner
            .rooms
            .iter()
            .filter_map(|(name, room)| Some((name.clone(), room.pace.quality()?)))
            .collect();
        status.record_qualities.sort();
        status.produce_bps = inner.produce_bps;
        status.upload_bps = inner.upload_bps;
        status.upload_busy = inner.upload_busy;
    }
}

impl Inner {
    fn check_failures(&mut self) -> bool {
        let mut alarm = false;
        for (name, room) in &mut self.rooms {
            let failures = room.pace.record_failures();
            alarm |= failures != room.last_failures && !is_probe(name);
            room.last_failures = failures;
        }
        alarm
    }

    fn check_started(&mut self, now: Instant) -> Vec<Started> {
        let mut started = Vec::new();
        for (name, room) in &mut self.rooms {
            if is_trial(name) {
                let count = room.pace.read_at(now);
                room.trace.push_back((now, count.media, count.wall));
                while room
                    .trace
                    .front()
                    .is_some_and(|(at, ..)| *at + TRIAL_WINDOW < now)
                {
                    room.trace.pop_front();
                }
            }
            if room.reported || is_probe(name) {
                continue;
            }
            let count = room.pace.read_at(now);
            // 转正的试拉等切到正式分段再报：开录时刻报切点，不报暂存录制的第一帧（§6.2）。
            if room.pace.recording_ready() && (!room.pace.promoted() || room.pace.formal()) {
                let since = *room.first_media_at.get_or_insert(now);
                // SEI 约 2 秒一个，写出第一个 GOP 时多半已经见到；没见到最多再等 3 秒，交接才能按推流时刻比。
                if count.first_live_ms.is_some() || now.saturating_duration_since(since) >= SEI_WAIT
                {
                    room.reported = true;
                    started.push(Started {
                        room: name.clone(),
                        lag_ms: count.lag_ms,
                        start_live_ms: count.first_live_ms,
                    });
                }
            }
        }
        started.sort_by(|a, b| a.room.cmp(&b.room));
        started
    }

    fn retire(&mut self, name: &str) {
        if let Some(room) = self.rooms.remove(name)
            && !is_probe(name)
        {
            self.gone_bytes += room.pace.read().bytes;
        }
    }
}

/// 一串 (时刻, 累计值) 样本，只留最近一个窗口多一点。
#[derive(Default)]
struct Window(VecDeque<(Instant, PaceCount)>);

impl Window {
    fn push(&mut self, now: Instant, count: PaceCount, keep: Duration) {
        self.0.push_back((now, count));
        // 留一个刚好早于窗口起点的样本，窗口才能盖满。
        while self.0.len() > 2 && self.0[1].0 + keep <= now {
            self.0.pop_front();
        }
    }

    /// 窗口内最早的样本到最新样本的增量。
    fn delta(&self, now: Instant, over: Duration) -> Option<PaceCount> {
        let (_, last) = self.0.back()?;
        let (_, first) = self
            .0
            .iter()
            .find(|(t, _)| *t + over >= now)
            .unwrap_or(self.0.back()?);
        Some(PaceCount {
            wall: last.wall.saturating_sub(first.wall),
            media: last.media.saturating_sub(first.media),
            active: last.active.saturating_sub(first.active),
            flowing: last.flowing.saturating_sub(first.flowing),
            bytes: last.bytes.saturating_sub(first.bytes),
            skipped: last.skipped.saturating_sub(first.skipped),
            jumps: last.jumps.saturating_sub(first.jumps),
            ..Default::default()
        })
    }

    /// 最近 [`RECORD_WINDOW`] 里各个 [`RATE_SLICE`] 子窗口的倍率（Δmedia / Δactive）取最高。
    /// 分母是持续收数据的时长，不是墙钟：空挂、断流重连的那段不进分母（见 `PaceCount::active`）。
    /// 窗口里持续收数据还不满 2 分钟、或没有一个子窗口收数据够久，返回 `None`。
    /// 按收数据的时长而不是墙钟判断「满 2 分钟」：断流期间照常采样，墙钟早满了，但那段不算。
    fn sustained_rate(&self, now: Instant) -> Option<f64> {
        let ((_, oldest), (_, newest)) = (self.0.front()?, self.0.back()?);
        if newest.active.saturating_sub(oldest.active) < RECORD_WINDOW {
            return None;
        }
        let mut best: Option<f64> = None;
        for (j, (end_t, end)) in self.0.iter().enumerate() {
            if *end_t + RECORD_WINDOW < now + RATE_SLICE {
                continue; // 子窗口起点会落在 2 分钟之外
            }
            let Some((_, start)) = self
                .0
                .iter()
                .take(j)
                .rev()
                .find(|(t, _)| *t + RATE_SLICE <= *end_t)
            else {
                continue;
            };
            let active = end.active.saturating_sub(start.active);
            if active >= MIN_SLICE_ACTIVE {
                let rate =
                    end.media.saturating_sub(start.media).as_secs_f64() / active.as_secs_f64();
                best = Some(best.map_or(rate, |b| b.max(rate)));
            }
        }
        best
    }

    /// 最近 [`FAST_SLICE`] 的「收到的媒体 / 数据在流的时长」；数据在流不足 [`FAST_MIN_FLOWING`] 不报。
    /// 只用 `since` 之后的样本（见 `Room::fast_since`）。
    fn fast_rate(&self, now: Instant, since: Option<Instant>) -> Option<f64> {
        let (_, newest) = self.0.back()?;
        let (_, start) = self
            .0
            .iter()
            .rev()
            .take_while(|(t, _)| since.is_none_or(|since| *t >= since))
            .find(|(t, _)| *t + FAST_SLICE <= now)?;
        let flowing = newest.flowing.saturating_sub(start.flowing);
        (flowing >= FAST_MIN_FLOWING)
            .then(|| newest.media.saturating_sub(start.media).as_secs_f64() / flowing.as_secs_f64())
    }

    /// 窗口内字节增量 / 样本跨度。
    fn per_sec(&self, now: Instant, over: Duration) -> f64 {
        let Some((first_t, _)) = self.0.iter().find(|(t, _)| *t + over >= now) else {
            return 0.0;
        };
        let span = now.saturating_duration_since(*first_t).as_secs_f64();
        match self.delta(now, over) {
            Some(d) if span > 0.0 => d.bytes as f64 / span,
            _ => 0.0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_media_clock_without_written_video_does_not_confirm_handoff_ready() {
        let monitor = Monitor::default();
        let pace = monitor.start_room("r");
        let now = Instant::now();
        pace.add(Duration::from_secs(1), 808);
        assert!(monitor.check_started(now).is_empty());
        assert!(
            monitor
                .check_started(now + SEI_WAIT + CHECK_EVERY)
                .is_empty()
        );
        pace.record_ready();
        assert!(
            monitor
                .check_started(now + SEI_WAIT + CHECK_EVERY)
                .is_empty()
        );
        assert_eq!(
            monitor
                .check_started(now + SEI_WAIT * 2 + CHECK_EVERY)
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn failures_wake_the_one_second_check_and_recovery_clears_the_status() {
        let monitor = Monitor::default();
        let pace = monitor.start_room("r");
        let probe = monitor.start_room("probe:r");
        let check = || {
            let notified = monitor.alarm.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            monitor.check_started(Instant::now());
            let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
            std::future::Future::poll(notified, &mut cx).is_ready()
        };
        assert!(!check());
        pace.record_failure(false);
        pace.record_failure(true);
        assert!(check());
        let mut status = Status::default();
        monitor.fill(&mut status);
        assert_eq!(status.record_failures, [("r".into(), 2, true)]);
        assert!(!check());
        probe.record_failure(true);
        assert!(!check(), "试拉结果不唤醒正常录制告警");
        pace.record_ready();
        assert!(check(), "恢复也及时更新总控");
        monitor.fill(&mut status);
        assert_eq!(status.record_failures, [("probe:r".into(), 1, true)]);
    }

    /// 连接结束原样报给总控，只留最近 [`HEALTH_WINDOW`] 的；短不短由总控判。
    #[test]
    fn reports_recent_connection_ends_raw() {
        let monitor = Monitor::default();
        let pace = monitor.start_room("r");
        let now = unix_now_ms();
        pace.connection_ended(now - HEALTH_WINDOW.as_millis() as i64 - 10_000, 5_000, true);
        pace.connection_ended(now - 1_000, 40_000, true);
        pace.connection_ended(now, 3_000, false);
        let mut status = Status::default();
        monitor.fill(&mut status);
        // 报的是距今多久：心跳现算，跟总控的时钟无关。
        let ago: Vec<_> = status
            .record_connections
            .iter()
            .map(|(r, ago, ms, video)| (r.as_str(), *ago / 1_000, *ms, *video))
            .collect();
        assert_eq!(ago, [("r", 0, 3_000, false), ("r", 1, 40_000, true)]);
    }

    /// 故障试拉报逐秒进度的原始数（距今多久、累计媒体、累计连接墙钟），只留最近 [`TRIAL_WINDOW`]，
    /// 观察窗口与首包突发都由总控算；正常录制不报。故障试拉期间每次采样都叫醒上报，总控才能按 20 秒窗口及时判断。
    #[tokio::test]
    async fn trial_reports_per_second_progress_raw() {
        let monitor = Monitor::default();
        let trial = monitor.start_room("trial:r");
        monitor.start_room("r").add(Duration::from_secs(5), 1);
        let now = Instant::now();
        let at = |s: u64| now.checked_sub(Duration::from_secs(s)).unwrap();
        monitor.check_started(at(TRIAL_WINDOW.as_secs() + 5));
        trial.add(Duration::from_secs(6), 1); // 连上时的 GOP 缓存
        monitor.check_started(at(2));
        trial.add(Duration::from_secs(1), 1);
        monitor.check_started(at(1));
        let mut status = Status::default();
        monitor.fill(&mut status);
        let [(key, samples)] = &status.record_trial[..] else {
            panic!("{:?}", status.record_trial);
        };
        assert_eq!(key, "trial:r");
        let got: Vec<_> = samples
            .iter()
            .map(|(ago, media, _)| (ago / 1_000, *media))
            .collect();
        assert_eq!(got, [(2, 6_000), (1, 7_000)], "窗口外的样本丢掉，先后排");
        let notified = monitor.alarm.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        monitor.sample(now, &Pace::default());
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(std::future::Future::poll(notified, &mut cx).is_ready());
    }

    /// 测得的重连接缝原样报给总控，只留最近 [`HEALTH_WINDOW`] 的。
    #[test]
    fn reports_recent_reconnect_gaps_raw() {
        let monitor = Monitor::default();
        let pace = monitor.start_room("r");
        pace.reconnect_gap(800);
        pace.reconnect_gap(0);
        let mut status = Status::default();
        monitor.fill(&mut status);
        let gaps: Vec<_> = status
            .record_gaps
            .iter()
            .map(|(r, _, ms)| (r.as_str(), *ms))
            .collect();
        assert_eq!(gaps, [("r", 800), ("r", 0)]);
        assert!(status.record_gaps.iter().all(|g| g.1 < 1_000));
    }

    /// 平台确认在播时报最近一次有效视频与确认在播的起点；不确认在播的房间不报。
    #[test]
    fn reports_video_progress_only_while_confirmed_live() {
        let monitor = Monitor::default();
        let pace = monitor.start_room("r");
        monitor.start_room("q").video_written();
        let mut status = Status::default();
        pace.confirm_live();
        monitor.fill(&mut status);
        assert!(status.health_reports, "总控据此区分新旧 agent");
        assert!(matches!(&status.record_video[..], [(r, None, s)] if r == "r" && *s < 1_000));
        std::thread::sleep(Duration::from_millis(20));
        pace.video_written();
        monitor.fill(&mut status);
        assert!(
            matches!(&status.record_video[..], [(r, Some(v), s)] if r == "r" && *v < *s && *s >= 20),
            "距今多久，心跳时现算"
        );
        pace.unconfirm_live();
        monitor.fill(&mut status);
        assert!(status.record_video.is_empty());
    }

    fn count(wall: u64, media: u64, bytes: u64) -> PaceCount {
        PaceCount {
            wall: Duration::from_secs(wall),
            active: Duration::from_secs(wall),
            media: Duration::from_secs(media),
            bytes,
            ..Default::default()
        }
    }

    /// 逐秒模拟一个在录房间，每 10 秒采样一次，返回 (第几秒, 报出的倍率)。
    /// `recv(s)`：这一秒收到多少秒媒体；`None` 表示这一秒一个字节都没收到（断流空挂、断开重连、404 重试）。
    fn simulate(until: u64, recv: impl Fn(u64) -> Option<f64>) -> Vec<(u64, Option<f64>)> {
        let m = Monitor::default();
        let upload = Pace::default();
        let pace = m.start_room("r");
        let _connected = pace.running();
        let t0 = Instant::now();
        let mut out = Vec::new();
        for s in 0..=until {
            if let Some(media) = recv(s) {
                pace.data_at(t0 + Duration::from_secs(s));
                pace.add(Duration::from_secs_f64(media), 1);
            }
            if s % 10 == 0 {
                m.sample(t0 + Duration::from_secs(s), &upload);
                let mut status = Status::default();
                m.fill(&mut status);
                out.push((s, status.record_rates.first().map(|r| r.1)));
            }
        }
        out
    }

    /// 2026-10-23 线上误判：主播断流后连接空挂、反复重连，墙钟照走、媒体为 0，被判成倍率 0.81。
    /// 断流期间不计时、断流后窗口从头积累：整段都不该报出低倍率，恢复后攒满 2 分钟才再报。
    #[test]
    fn a_stream_outage_with_reconnect_waits_never_reports_a_low_rate() {
        let outage = 180..400; // 3 分 40 秒没有任何数据
        let rates = simulate(700, |s| (!outage.contains(&s)).then_some(1.0));
        let low: Vec<_> = rates
            .iter()
            .filter(|(_, r)| r.is_some_and(|r| r < 0.9))
            .collect();
        assert!(low.is_empty(), "断流不该报低倍率：{low:?}");
        assert!(
            rates.iter().any(|(s, r)| *s < 180 && r.is_some()),
            "断流前正常出数"
        );
        let resumed: Vec<_> = rates
            .iter()
            .filter(|(s, r)| *s >= 400 && r.is_some())
            .collect();
        assert!(
            resumed.first().is_some_and(|(s, _)| *s >= 520),
            "恢复后要重新攒满 2 分钟才出倍率：{resumed:?}"
        );
        assert!(
            resumed
                .iter()
                .all(|(_, r)| r.is_some_and(|r| (r - 1.0).abs() < 0.05))
        );
    }

    /// 真正的慢速下行：连着、数据一直在来，但只收到 0.5 倍速，仍要判低。
    /// 限速连接常被 CDN 每分钟左右断开一次、几秒后重连，这种短停顿不能把窗口清掉。
    #[test]
    fn a_connected_but_slow_download_is_still_reported_low() {
        let rates = simulate(300, |_| Some(0.5));
        let (s, r) = rates.iter().find(|(_, r)| r.is_some()).unwrap();
        assert!(*s <= 130 && (r.unwrap() - 0.5).abs() < 0.05, "{s} {r:?}");

        let blips = simulate(300, |s| (s % 60 >= 3).then_some(0.5));
        let last = blips.last().unwrap().1.expect("短停顿不清窗口，照常出数");
        assert!(last < 0.9, "{last}");
    }

    /// 2026-11-03 某房间 云服务器 A：CDN 限速到约 0.1 倍、每二三十秒断开重连一次。要在 30 秒内报出低倍率，
    /// 不等 2 分钟窗口；主播推流卡十几秒（数据不流）、正常录制都不能触发快速判慢。
    #[test]
    fn a_severe_throttle_is_reported_within_half_a_minute() {
        let throttled = simulate(400, |s| match s {
            ..200 => Some(1.0),
            _ if s % 25 == 0 => None, // 断开重连
            _ => Some(0.1),
        });
        let (s, r) = throttled
            .iter()
            .find(|(s, r)| *s > 200 && r.is_some_and(|r| r < 0.9))
            .unwrap();
        assert!(*s <= 230 && r.unwrap() < 0.4, "{s} {r:?}");

        let stutter = simulate(400, |s| (s % 60 >= 12).then_some(1.0));
        assert!(
            stutter.iter().all(|(_, r)| r.is_none_or(|r| r > 0.9)),
            "{stutter:?}"
        );
        let normal = simulate(400, |_| Some(1.0));
        assert!(normal.iter().all(|(_, r)| r.is_none_or(|r| r > 0.9)));
    }

    /// 2026-11-17 云服务器 A、云服务器 B：旧 CDN 节点最后 20 秒数据在流、内容却只给 0.3 倍，随后 `stream_end`，
    /// 重连到新节点就恢复了。重连后 1～2 秒的采样不能再拿旧连接那段报快速判慢；新连接自己慢照样报。
    #[test]
    fn a_slow_tail_of_an_ended_connection_is_not_reported_after_reconnect() {
        let run = |new_rate: f64| {
            let m = Monitor::default();
            let upload = Pace::default();
            let pace = m.start_room("r");
            let _connected = pace.running();
            let t0 = Instant::now();
            let mut low = Vec::new();
            for s in 0..=260u64 {
                let at = t0 + Duration::from_secs(s);
                if s == 200 {
                    pace.connection_ended(unix_now_ms(), 200_000, true); // stream_end，随即重连
                }
                pace.data_at(at);
                let media = match s {
                    180..200 => 0.3,
                    200.. => new_rate,
                    _ => 1.0,
                };
                pace.add(Duration::from_secs_f64(media), 1);
                m.sample(at, &upload);
                let mut status = Status::default();
                m.fill(&mut status);
                if status
                    .record_rates
                    .first()
                    .is_some_and(|r| r.1 < FAST_RATE_MAX)
                {
                    low.push(s);
                }
            }
            low
        };
        let healthy = run(1.0);
        assert!(healthy.iter().all(|s| *s < 200), "重连后误报：{healthy:?}");
        let slow = run(0.3);
        assert!(
            slow.iter().any(|s| (200..=225).contains(s)),
            "新连接照样慢要在约 20 秒内报：{slow:?}"
        );
    }

    /// 上报时裁掉超过 [`HEALTH_WINDOW`] 的连接记录，不算开始了新连接：慢连接要一直报，不能凭空多出 20 秒盲区。
    #[test]
    fn pruning_old_connection_ends_does_not_restart_the_fast_window() {
        let m = Monitor::default();
        let upload = Pace::default();
        let pace = m.start_room("r");
        let _connected = pace.running();
        let t0 = Instant::now();
        let mut missed = Vec::new();
        for s in 0..=100u64 {
            let at = t0 + Duration::from_secs(s);
            if s == 10 {
                // 结束时刻已在上报窗口之外，下一次 fill 就裁掉。
                pace.connection_ended(
                    unix_now_ms() - HEALTH_WINDOW.as_millis() as i64 - 1_000,
                    1,
                    true,
                );
            }
            pace.data_at(at);
            pace.add(Duration::from_secs_f64(0.3), 1);
            m.sample(at, &upload);
            if s == 50 {
                m.fill(&mut Status::default());
            }
            let fast = m
                .inner
                .lock()
                .unwrap()
                .rates
                .first()
                .is_some_and(|r| r.1 < FAST_RATE_MAX);
            if s >= 30 && !fast {
                missed.push(s);
            }
        }
        assert!(missed.is_empty(), "慢连接漏报：{missed:?}");
    }

    #[test]
    fn window_takes_the_delta_over_the_last_window_only() {
        let t0 = Instant::now();
        let mut w = Window::default();
        // 前 5 分钟倍率 1.0，后 2 分钟只收到一半。
        for i in 0..=42u64 {
            let wall = i * 10;
            let media = if wall <= 300 {
                wall
            } else {
                300 + (wall - 300) / 2
            };
            w.push(
                t0 + Duration::from_secs(wall),
                count(wall, media, wall * 100),
                RECORD_WINDOW,
            );
        }
        let now = t0 + Duration::from_secs(420);
        let d = w.delta(now, RECORD_WINDOW).unwrap();
        assert_eq!(d.wall, Duration::from_secs(120));
        assert_eq!(d.media, Duration::from_secs(60));
        assert!((w.per_sec(now, RECORD_WINDOW) - 100.0).abs() < 1e-9);
        assert!(w.0.len() <= 14, "旧样本要丢掉：{}", w.0.len());
    }

    /// 倍率要「持续」低才报低：一次 15 秒卡顿不算，限速前的正常数据也不该被平均掉。
    #[test]
    fn sustained_rate_is_the_best_slice_of_the_last_two_minutes() {
        let t0 = Instant::now();
        let feed = |media_at: &dyn Fn(u64) -> u64, until: u64| {
            let mut w = Window::default();
            for wall in (0..=until).step_by(10) {
                w.push(
                    t0 + Duration::from_secs(wall),
                    count(wall, media_at(wall), 0),
                    RECORD_WINDOW,
                );
            }
            w.sustained_rate(t0 + Duration::from_secs(until))
        };
        // 历史不满 2 分钟：不报。
        assert_eq!(feed(&|w| w, 110), None);
        // 中间卡了 20 秒（媒体不涨），其余正常：最好的子窗口仍是 1.0。
        let stall = |w: u64| {
            if w < 200 {
                w
            } else if w < 220 {
                200
            } else {
                w - 20
            }
        };
        assert_eq!(feed(&stall, 300), Some(1.0));
        // 300 秒起限到一半：刚限 30 秒时还有正常子窗口；满 2 分钟后全部子窗口都低。
        let throttled = |w: u64| if w <= 300 { w } else { 300 + (w - 300) / 2 };
        assert_eq!(feed(&throttled, 330), Some(1.0));
        assert_eq!(feed(&throttled, 420), Some(0.5));
        // 断流（墙钟不涨）的子窗口不算；全断就不报。
        assert_eq!(feed(&|_| 0, 300).map(|_| ()), Some(()));
        let mut w = Window::default();
        for i in 0..=30u64 {
            w.push(
                t0 + Duration::from_secs(i * 10),
                count(50, 50, 0),
                RECORD_WINDOW,
            );
        }
        assert_eq!(w.sustained_rate(t0 + Duration::from_secs(300)), None);
    }

    /// 故障试拉转正后，等切到正式分段才补报开录，带的是切点（正式分段第一帧）的推流时刻，
    /// 不是暂存录制的第一帧（§6.2）：旧机按它收尾，接缝处不再重复整段试拉。
    #[test]
    fn a_promoted_trial_reports_recording_at_the_cut() {
        let m = Monitor::default();
        let t0 = Instant::now();
        let p = m.start_room("trial:r");
        let _connected = p.running();
        p.add(Duration::from_secs(30), 1_000);
        p.record_ready();
        p.written_live(1_000_000, 1_030_000);
        assert!(m.check_started(t0).is_empty(), "试拉不报开录");
        p.promote();
        m.promote("trial:r", "r");
        assert!(
            m.check_started(t0 + CHECK_EVERY).is_empty(),
            "还没切到正式分段"
        );
        p.formal_started(Some(1_032_000));
        let started = m.check_started(t0 + CHECK_EVERY * 2);
        assert!(
            matches!(&started[..], [Started { room, start_live_ms: Some(1_032_000), .. }] if room == "r"),
            "{started:?}"
        );
    }

    #[test]
    fn sample_reports_first_media_once_and_keeps_bytes_of_stopped_rooms() {
        let m = Monitor::default();
        let upload = Pace::default();
        let t0 = Instant::now();
        let a = m.start_room("a");
        assert!(m.sample(t0, &upload).is_empty(), "还没有媒体");
        a.add(Duration::from_secs(2), 1_000);
        a.record_ready();
        // 码流没有 SEI：每秒检测一次，等满 3 秒（SEI_WAIT）看推流时刻会不会跟上，还没有就不带它报。
        for s in 1..=3 {
            assert!(
                m.check_started(t0 + Duration::from_secs(s)).is_empty(),
                "at {s}s"
            );
        }
        let none = Started {
            room: "a".into(),
            lag_ms: None,
            start_live_ms: None,
        };
        assert_eq!(m.check_started(t0 + Duration::from_secs(4)), [none]);
        a.add(Duration::from_secs(2), 1_000);
        assert!(m.sample(t0 + SAMPLE_EVERY, &upload).is_empty(), "只报一次");

        // 停掉的录制写过的字节仍算产出；重起同名录制要重新报第一个关键帧。
        m.stop_room("a");
        let a2 = m.start_room("a");
        let connected = a2.running();
        a2.add(Duration::from_secs(1), 1_000);
        a2.record_ready();
        a2.written_live(1_000_000, 1_001_000);
        a2.set_quality(Some("10000".into()));
        // 有推流时刻：1 秒级检测当即报出，不等 3 秒也不等 10 秒采样。
        let started = m.check_started(t0 + SAMPLE_EVERY + CHECK_EVERY);
        assert!(
            matches!(&started[..], [Started { room, lag_ms: Some(lag), start_live_ms: Some(1_000_000) }]
                if room == "a" && (-1000..=-900).contains(lag)),
            "{started:?}"
        );
        assert!(m.sample(t0 + SAMPLE_EVERY * 2, &upload).is_empty());
        let mut status = Status::default();
        m.fill(&mut status);
        assert_eq!(status.record_lags.len(), 1);
        assert_eq!(status.record_live_lags.len(), 1);
        assert_eq!(
            status.record_qualities,
            [("a".to_string(), "10000".to_string())]
        );
        drop(connected);
        assert!(
            (status.produce_bps - 3_000.0 / 20.0).abs() < 1e-9,
            "{}",
            status.produce_bps
        );
        // 墙钟没跑（没有连接），样本不足，不报倍率与上传速度。
        assert!(status.record_rates.is_empty());
        assert_eq!((status.upload_bps, status.upload_busy), (None, 0.0));
    }

    fn skips_at(m: &Monitor, at: Instant, upload: &Pace) -> Vec<(String, u64, u32)> {
        m.sample(at, upload);
        let mut status = Status::default();
        m.fill(&mut status);
        status.record_skips
    }

    /// 2026-10-30 云服务器 B：CDN 一阵一阵给数据，每次时间戳前跳约 20 秒，中间还断过流。
    /// 报的是最近 60 秒的差值，断流不清空；跳变在 60 秒之前就不再报；没有跳变的房间不报。
    #[test]
    fn record_skips_reports_the_last_minute_and_survives_gaps() {
        let m = Monitor::default();
        let upload = Pace::default();
        let t0 = Instant::now();
        let at = |s: u64| t0 + Duration::from_secs(s);
        let r = m.start_room("r");
        let _quiet = m.start_room("b");
        r.data_at(at(0));
        assert!(skips_at(&m, at(0), &upload).is_empty());
        r.skip(Duration::from_secs(20));
        assert_eq!(skips_at(&m, at(10), &upload), [("r".into(), 20_000, 1)]);
        r.skip(Duration::from_secs(20));
        r.skip(Duration::from_secs(20));
        // 40 秒没收到数据：断流（gaps 变了），窗口照样不清。
        r.data_at(at(40));
        assert_eq!(r.read_at(at(40)).gaps, 1);
        assert_eq!(skips_at(&m, at(40), &upload), [("r".into(), 60_000, 3)]);
        assert_eq!(skips_at(&m, at(60), &upload), [("r".into(), 60_000, 3)]);
        // 窗口从 60 秒内最早的样本（10 秒那次）算起：它之前的第一次跳变不再算。
        assert_eq!(skips_at(&m, at(70), &upload), [("r".into(), 40_000, 2)]);
        assert!(skips_at(&m, at(110), &upload).is_empty());
    }

    /// 试拉的房间：字节不算产出（停掉后也不算），不报开录；倍率与跳过量照常报，总控据此判断。
    #[test]
    fn probe_rooms_report_rates_and_skips_but_not_output_or_start() {
        let m = Monitor::default();
        let upload = Pace::default();
        let t0 = Instant::now();
        let at = |s: u64| t0 + Duration::from_secs(s);
        let a = m.start_room("a");
        let p = m.start_room("probe:r");
        let _connected = p.running();
        for s in 0..=130u64 {
            p.data_at(at(s));
            if s > 0 {
                p.add(Duration::from_secs(1), 100);
                a.add(Duration::from_secs(1), 10);
            }
            if s == 125 {
                p.skip(Duration::from_secs(30));
            }
            let started = if s % 10 == 0 {
                m.sample(at(s), &upload)
            } else {
                m.check_started(at(s))
            };
            assert!(started.iter().all(|x| x.room == "a"), "{s}: {started:?}");
        }
        let mut status = Status::default();
        m.fill(&mut status);
        assert!(
            (status.produce_bps - 1_300.0 / 130.0).abs() < 1e-9,
            "{}",
            status.produce_bps
        );
        assert!(
            matches!(&status.record_rates[..], [(k, r)] if k == "probe:r" && (r - 1.0).abs() < 0.05),
            "{:?}",
            status.record_rates
        );
        assert_eq!(status.record_skips, [("probe:r".into(), 30_000, 1)]);
        m.stop_room("probe:r");
        m.sample(at(140), &upload);
        m.fill(&mut status);
        assert!(
            (status.produce_bps - 1_300.0 / 140.0).abs() < 1e-9,
            "{}",
            status.produce_bps
        );
    }

    /// 采样一次，返回事先挂好的 `alarm.notified()` 有没有被叫醒。
    async fn wakes(m: &Monitor, upload: &Pace, at: Instant) -> bool {
        let notified = m.alarm.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        m.sample(at, upload);
        tokio::time::timeout(Duration::from_millis(50), notified)
            .await
            .is_ok()
    }

    /// 非试拉房间新出现前跳的那次采样叫醒 alarm；没有新跳变、或只有试拉房间跳变，不叫。
    #[tokio::test]
    async fn alarm_fires_on_new_jumps_of_recording_rooms_only() {
        let m = Monitor::default();
        let upload = Pace::default();
        let t0 = Instant::now();
        let r = m.start_room("r");
        let p = m.start_room("probe:r");
        assert!(!wakes(&m, &upload, t0).await, "没有跳变");
        r.skip(Duration::from_secs(20));
        assert!(wakes(&m, &upload, t0 + SAMPLE_EVERY).await, "新跳变");
        assert!(!wakes(&m, &upload, t0 + SAMPLE_EVERY * 2).await, "没有新增");
        p.skip(Duration::from_secs(20));
        assert!(!wakes(&m, &upload, t0 + SAMPLE_EVERY * 3).await, "试拉不叫");
    }
}
