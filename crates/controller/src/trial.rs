//! 先试拉再迁（`docs/design/handoff-health.md` §4、§6.1.2）：房间在它的录制机上达到门槛时，
//! 先让一台候选机故障试拉，健康了才迁。总开关 `health_trial` 关着时只打日志。

use super::*;

/// 一个房间在录它的机器上达到门槛的信号。
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Trouble {
    pub signal: &'static str,
    /// 基本录不到内容（§4.1）：备用机只观察 [`TRIAL_FAST_WINDOW_MS`]，其余 [`TRIAL_SLOW_WINDOW_MS`]。
    pub fast: bool,
}

impl Trouble {
    fn window_ms(&self) -> i64 {
        if self.fast {
            TRIAL_FAST_WINDOW_MS
        } else {
            TRIAL_SLOW_WINDOW_MS
        }
    }
}

/// 进行中的故障试拉，只在内存里：总控重启即取消，候选机拿到不含试拉的清单后自己清理。
#[derive(Clone, Debug)]
pub(crate) struct Trial {
    pub from: String,
    pub to: String,
    /// 下发给候选机的任务：原机这一场在录的那份快照。转正时候选机 `rooms` 里的必须与它完全相同。
    pub task: RoomTask,
    session: Option<String>,
    since: i64,
    trouble: Trouble,
}

/// 一个房间的故障试拉进度，只在内存里；原机恢复就丢掉。
#[derive(Debug, Default)]
pub(crate) struct Recheck {
    /// 开关关着时已经为它打过日志的 (录制机, 信号)：只在变化时再打。
    noted: Option<(String, &'static str)>,
    /// 这一刻前不再发起试拉。
    at: i64,
    /// 连续判为源站问题（含源站无流）的次数，复查间隔按它递增。
    source_fails: u32,
    /// 本轮试过的候选机；都试过一遍就清空重来。
    tried: Vec<String>,
    /// 最近一次失败的原因：同一原因只告警一次。
    last: &'static str,
}

/// 候选机一次故障试拉的观察结论。
#[derive(Debug, PartialEq)]
pub(crate) enum Verdict {
    Waiting,
    Healthy,
    /// 有视频但不达标。
    Unhealthy,
    /// 到时限一帧视频都没有。
    NoVideo,
}

/// 按候选机报的逐秒进度 `samples`（[`Status::record_trial`]：距今毫秒、累计媒体毫秒、累计连接墙钟毫秒，
/// 按时间先后）判一次故障试拉（§4.1）。`broken`：试拉期间出过停滞、异常断开或确认的内容跳失；
/// `elapsed_ms`：下发至今多久。
///
/// 达标：扣除首包突发后，最近至少 `window_ms` 里媒体推进不低于实时时长的 [`TRIAL_RATE_MIN`]。实时时长按采样的
/// 「距今」算，断开重连的空档也计入。首包突发：连上时 CDN 一次给出几秒 GOP 缓存，从第一次有媒体起
/// [`TRIAL_BURST_SETTLE_MS`] 之后才开始算。到 [`TRIAL_TIMEOUT_MS`] 还不达标就是失败。
pub(crate) fn observe(
    samples: &[(u64, u64, u64)],
    window_ms: i64,
    broken: bool,
    elapsed_ms: i64,
) -> Verdict {
    let timed_out = elapsed_ms >= TRIAL_TIMEOUT_MS;
    let Some(first) = samples.iter().find(|s| s.1 > 0) else {
        return if timed_out {
            Verdict::NoVideo
        } else {
            Verdict::Waiting
        };
    };
    if broken {
        return Verdict::Unhealthy;
    }
    let last = samples[samples.len() - 1];
    // 起点：首包突发之后、距最后一次采样至少一个观察窗口的采样里最晚的那个。
    let base = samples
        .iter()
        .rfind(|s| s.0 + TRIAL_BURST_SETTLE_MS <= first.0 && s.0 >= last.0 + window_ms as u64);
    let healthy = base.is_some_and(|b| {
        last.1.saturating_sub(b.1) as f64 >= TRIAL_RATE_MIN * (b.0 - last.0) as f64
    });
    match (healthy, timed_out) {
        (true, _) => Verdict::Healthy,
        (false, true) => Verdict::Unhealthy,
        (false, false) => Verdict::Waiting,
    }
}

impl App {
    /// 心跳带来新状态后，记下这台机器上各房间达到门槛的信号：第一期的三个健康信号（先调 [`App::health`]），
    /// 加上倍率、丢内容、空录与视频停滞。一个房间只留一条，基本录不到内容的优先。
    pub(crate) fn note_troubles(&self, agent: &str, s: &Status) {
        let mut found: Vec<(String, Trouble)> = Vec::new();
        let mut add = |room: &str, signal, fast| {
            let t = Trouble { signal, fast };
            found.push((room.to_string(), t));
        };
        for (a, room, signal) in self.health.lock().unwrap().iter() {
            match *signal {
                _ if a != agent => {}
                "no_video" => add(room, "no_video", true),
                "short_connections" | "reconnect_gaps" => add(room, signal, false),
                // 停录的机器（no_video_paused）第三期按整机故障迁，不试拉。
                _ => {}
            }
        }
        for (room, n, stalled) in &s.record_failures {
            if failures_bad(*n, *stalled) && !is_side(room) {
                add(room, "record_failures", true);
            }
        }
        for (room, v) in &s.record_rates {
            if *v < RECORD_RATE_MIN && !is_side(room) {
                add(room, "record_rate", *v < TRIAL_FAST_RATE);
            }
        }
        for (room, ms, jumps) in &s.record_skips {
            if skips_bad(*ms, *jumps) && !is_side(room) {
                add(room, "record_skips", false);
            }
        }
        // 稳定排序：基本录不到内容的在前，同类按上面的先后。
        found.sort_by_key(|(_, t)| !t.fast);
        let mut troubles = self.troubles.lock().unwrap();
        troubles.retain(|(a, _), _| a != agent);
        for (room, t) in found {
            troubles.entry((agent.to_string(), room)).or_insert(t);
        }
    }

    /// 给 `room` 挑一台故障试拉的候选机（§4.3）：按优先列表，要求在线、启用、没停录、没排空、没被整机隔离、没在让出网络、
    /// 支持故障试拉、不是原机、本轮没试过。对这个房间有房间级限制的只作末位候选，其中最近受限的
    /// （房间上一台待过的）优先。
    pub(crate) fn trial_candidate(
        &self,
        room: &db::Room,
        from: &str,
        tried: &[String],
        now: i64,
    ) -> db::Result<Option<String>> {
        // 没有 trial_support 的旧 agent 不认识 `Assignments.trial`，不能当候选（§6.1.1 约定 1）。
        let mut limited: Option<(i64, &String)> = None;
        for name in &room.agents {
            if name == from || tried.contains(name) {
                continue;
            }
            match self
                .availability(name, now, Some(&room.name))?
                .failure_trial_candidate
            {
                Some(TrialCandidate::Unrestricted) => return Ok(Some(name.clone())),
                Some(TrialCandidate::RoomLimited { since })
                    if limited.is_none_or(|(best, _)| since > best) =>
                {
                    limited = Some((since, name));
                }
                _ => {}
            }
        }
        Ok(limited.map(|(_, name)| name.clone()))
    }

    /// 推进所有房间的故障试拉。每次心跳、每个检测周期各跑一次。
    pub(crate) fn trial_step(&self, now: i64) -> db::Result<()> {
        let mut rooms: Vec<String> = self
            .troubles
            .lock()
            .unwrap()
            .keys()
            .map(|(_, room)| room.clone())
            .collect();
        rooms.extend(self.rechecks.lock().unwrap().keys().cloned());
        rooms.extend(self.trials.lock().unwrap().keys().cloned());
        rooms.sort();
        rooms.dedup();
        for room in rooms {
            self.trial_room(&room, now)?;
        }
        Ok(())
    }

    /// 这台机器此刻该做的故障试拉。
    pub(crate) fn trial_for(&self, agent: &str) -> Option<RoomTask> {
        let trials = self.trials.lock().unwrap();
        trials
            .values()
            .find(|t| t.to == agent)
            .map(|t| t.task.clone())
    }

    /// 下发给这台机器的 (到期前试拉, 故障试拉)。两者不同时下发（§6.1.1 约定 3）：故障试拉优先，
    /// 被挤掉的恢复验证等下一次机会（§4.2）。
    pub(crate) fn side_tasks(
        &self,
        agent: &str,
        now: i64,
    ) -> db::Result<(Option<RoomTask>, Option<RoomTask>)> {
        Ok(match self.trial_for(agent) {
            Some(trial) => (None, Some(trial)),
            None => (self.probe_for(agent, now)?, None),
        })
    }

    fn trial_room(&self, name: &str, now: i64) -> db::Result<()> {
        let assignment = db::assignment(&self.db(), name)?;
        let trouble = assignment.as_ref().and_then(|(agent, _)| {
            let key = (agent.clone(), name.to_string());
            self.troubles.lock().unwrap().get(&key).cloned()
        });
        let offline = self.detections.lock().unwrap().get(name).copied().flatten() == Some(false);
        let handing = db::handoff(&self.db(), name)?.is_some();
        let snapshot = db::assigned_task(&self.db(), name)?;
        let trial = self.trials.lock().unwrap().remove(name);
        if let Some(t) = trial {
            // 取消（§4.5）。原机失联不在这里：检测周期的失联改派会直接把房间交给候选机。
            let cancel = match (&assignment, &snapshot) {
                (Some((agent, _)), Some((session, task)))
                    if *agent == t.from && *task == t.task =>
                {
                    if *session != t.session {
                        Some("session_changed")
                    } else if offline {
                        Some("offline")
                    } else if handing {
                        Some("handoff")
                    } else {
                        None
                    }
                }
                _ => Some("assignment_changed"),
            };
            if let Some(reason_code) = cancel {
                info!(
                    event_name = "health.trial",
                    outcome = "cancelled",
                    reason_code,
                    room = name,
                    agent = t.from,
                    candidate = t.to,
                    "failure trial cancelled"
                );
                if trouble.is_none() {
                    self.rechecks.lock().unwrap().remove(name);
                }
                self.changed.notify_waiters();
                return Ok(());
            }
            let fit = self.availability(&t.to, now, None)?.continue_failure_trial;
            if !fit {
                self.trial_failed(name, &t, "candidate_lost", now);
                return Ok(());
            }
            // 下发之后的心跳才算数：上一次试拉留在旧心跳里的进度不能当成这一次的。
            let (seen, status) = db::agent(&self.db(), &t.to)?
                .filter(|(seen, _)| *seen > t.since)
                .unwrap_or_default();
            let key = format!("{TRIAL_PREFIX}{name}");
            let samples = status.record_trial.iter().find(|(k, _)| *k == key);
            let samples = samples.map(|(_, v)| v.as_slice()).unwrap_or_default();
            let broken = status.record_connections.iter().any(|(k, ..)| *k == key)
                || status
                    .record_failures
                    .iter()
                    .any(|(k, _, stalled)| *k == key && *stalled)
                || status
                    .record_skips
                    .iter()
                    .any(|(k, ms, jumps)| *k == key && skips_bad(*ms, *jumps))
                || status
                    .record_gaps
                    .iter()
                    .any(|(k, _, ms)| *k == key && *ms > 0);
            match observe(samples, t.trouble.window_ms(), broken, now - t.since) {
                Verdict::Waiting => {
                    self.trials.lock().unwrap().insert(name.to_string(), t);
                }
                Verdict::Healthy if trouble.is_none() => {
                    // 原机信号中途回落不取消，观察完才看：此刻已恢复就不迁（断续故障会让信号反复回落）。
                    info!(
                        event_name = "health.trial",
                        outcome = "cancelled",
                        reason_code = "recovered",
                        room = name,
                        agent = t.from,
                        candidate = t.to,
                        "failure trial cancelled"
                    );
                    self.rechecks.lock().unwrap().remove(name);
                    self.changed.notify_waiters();
                }
                Verdict::Healthy => {
                    let drop = self.origin_wrote_video(name, &t.from, seen, samples)?;
                    if drop {
                        // 先记下再发起交接：拿到这个房间的那次下发一定带上它。
                        let mut drops = self.trial_drops.lock().unwrap();
                        drops.insert(name.to_string(), t.to.clone());
                    }
                    self.promote(name, &t, drop, now)?
                }
                Verdict::Unhealthy => self.trial_failed(name, &t, "source_problem", now),
                Verdict::NoVideo => {
                    // 原机还写得出视频，说明源站有流，拉不到的是这台候选机；两边都没有才是源站无流（C8）。
                    let origin = db::agent(&self.db(), &t.from)?;
                    let has_video = origin.is_some_and(|(_, s)| {
                        s.record_video.iter().any(|(r, video, _)| {
                            r == name && video.is_some_and(|v| (v as i64) < NO_VIDEO_MS)
                        })
                    });
                    let kind = if has_video {
                        "candidate_no_video"
                    } else {
                        "source_no_stream"
                    };
                    self.trial_failed(name, &t, kind, now);
                }
            }
            return Ok(());
        }
        let room = db::room(&self.db(), name)?;
        let (Some((from, _)), Some(trouble), Some(room)) = (assignment, trouble, room) else {
            // 原机恢复（或房间撤了、换了机器）：进度与递增间隔归零。
            self.rechecks.lock().unwrap().remove(name);
            return Ok(());
        };
        // 平台不确认在播、或已经在交接：不试拉。
        if offline || handing {
            return Ok(());
        }
        if !self.config.health_trial {
            let candidate = self.trial_candidate(&room, &from, &[], now)?;
            let mut rechecks = self.rechecks.lock().unwrap();
            let recheck = rechecks.entry(name.to_string()).or_default();
            let noted = Some((from.clone(), trouble.signal));
            if recheck.noted != noted {
                recheck.noted = noted;
                info!(
                    event_name = "health.trial",
                    outcome = "skipped",
                    reason_code = "disabled",
                    room = name,
                    agent = from,
                    signal = trouble.signal,
                    window_s = trouble.window_ms() / 1000,
                    candidate = candidate.as_deref().unwrap_or("(none)"),
                    "would start a failure trial, health_trial is off"
                );
            }
            return Ok(());
        }
        // 整机受限的机器上的房间走现有交接迁走，不再逐个试拉（§5.2）。
        if !self.availability(&from, now, None)?.failure_trial_origin {
            return Ok(());
        }
        let Some((session, task)) = snapshot else {
            return Ok(());
        };
        let mut tried = {
            let rechecks = self.rechecks.lock().unwrap();
            match rechecks.get(name) {
                Some(r) if now < r.at => return Ok(()),
                Some(r) => r.tried.clone(),
                None => Vec::new(),
            }
        };
        // 每台机器同时最多试一个房间：名额被占着就等，不算失败（§4.2）。
        let mut busy = tried.clone();
        busy.extend(self.trials.lock().unwrap().values().map(|t| t.to.clone()));
        let mut to = self.trial_candidate(&room, &from, &busy, now)?;
        if to.is_none() && self.trial_candidate(&room, &from, &tried, now)?.is_some() {
            return Ok(());
        }
        if to.is_none() && !tried.is_empty() {
            // 都试过一遍了：下一轮从头轮换。
            tried.clear();
            to = self.trial_candidate(&room, &from, &[], now)?;
        }
        let mut rechecks = self.rechecks.lock().unwrap();
        let recheck = rechecks.entry(name.to_string()).or_default();
        recheck.tried = tried;
        let Some(to) = to else {
            recheck.at = now + TRIAL_RECHECK_MS;
            if recheck.last != "no_candidate" {
                recheck.last = "no_candidate";
                warn!(
                    event_name = "health.trial",
                    outcome = "failed",
                    reason_code = "no_candidate",
                    room = name,
                    agent = from,
                    signal = trouble.signal,
                    next_s = TRIAL_RECHECK_MS / 1000,
                    "no candidate for a failure trial, staying on this agent"
                );
            }
            return Ok(());
        };
        drop(rechecks);
        info!(
            event_name = "health.trial",
            outcome = "started",
            room = name,
            agent = from,
            candidate = to,
            signal = trouble.signal,
            window_s = trouble.window_ms() / 1000,
            "failure trial started"
        );
        let trial = Trial {
            from,
            to,
            task,
            session,
            since: now,
            trouble,
        };
        self.trials.lock().unwrap().insert(name.to_string(), trial);
        self.changed.notify_waiters();
        Ok(())
    }

    /// 试拉健康、原机问题还在：发起交接。候选机的 `rooms` 里多出这个房间（任务就是试拉的那份快照），
    /// 它在同一条连接上转正并补报开录，旧机按现有交接协议收尾。原机记一条房间级限制；
    /// 候选机在这个房间上原有的限制解除（§4.3）。
    fn promote(&self, name: &str, t: &Trial, drop: bool, now: i64) -> db::Result<()> {
        db::start_handoff(&self.db(), name, &t.from, &t.to, now)?;
        self.limit_room(name, &t.from, t.trouble.signal, now)?;
        db::clear_room_limit(&self.db(), name, &t.to)?;
        self.rechecks.lock().unwrap().remove(name);
        info!(
            event_name = "health.trial",
            outcome = "promoted",
            room = name,
            agent = t.from,
            candidate = t.to,
            signal = t.trouble.signal,
            staged = if drop { "dropped" } else { "kept" },
            "failure trial healthy, handing off to the candidate"
        );
        self.changed.notify_waiters();
        Ok(())
    }

    /// 候选机开始写出媒体之后，原机还写出过视频、且到现在仍在写（§6.2）：转正时切点前的那段由原机保留，
    /// 候选机丢弃暂存段。候选机的开录时刻取观察窗口里最早有媒体的那次进度（窗口外更早的算不到，只会偏晚、偏向保留）；
    /// 原机没报 `record_video`（旧 agent）或报的是没视频，都不丢。
    fn origin_wrote_video(
        &self,
        name: &str,
        origin: &str,
        seen: i64,
        samples: &[(u64, u64, u64)],
    ) -> db::Result<bool> {
        let Some(staged_from) = samples
            .iter()
            .filter(|(_, media, _)| *media > 0)
            .map(|(ago, ..)| seen - *ago as i64)
            .min()
        else {
            return Ok(false);
        };
        let origin = db::agent(&self.db(), origin)?;
        Ok(origin.is_some_and(|(origin_seen, s)| {
            s.record_video.iter().any(|(r, video, _)| {
                r == name
                    && video.is_some_and(|v| {
                        (v as i64) < NO_VIDEO_MS && origin_seen - v as i64 >= staged_from
                    })
            })
        }))
    }

    /// 转正时要丢弃暂存内容的房间（下发给 `agent` 的 [`Assignments::trial_drop`]），只发到交接结束。
    pub(crate) fn trial_drop_for(&self, agent: &str) -> db::Result<Vec<String>> {
        let mut drops = self.trial_drops.lock().unwrap();
        let mut live = HashMap::new();
        for (room, to) in drops.drain() {
            if db::handoff(&self.db(), &room)?.is_some_and(|h| h.to == to) {
                live.insert(room, to);
            }
        }
        *drops = live;
        let mut rooms: Vec<_> = drops
            .iter()
            .filter(|(_, to)| *to == agent)
            .map(|(room, _)| room.clone())
            .collect();
        rooms.sort();
        Ok(rooms)
    }

    /// 给「房间加机器」记一条房间级限制，时长同整机限制的 20→40→60 分钟递增：解除后 `limited_for` 内再受限、
    /// 或限制还在时又受限，接着上次的档位往上走。
    fn limit_room(&self, room: &str, agent: &str, reason: &str, now: i64) -> db::Result<()> {
        let max = self.config.limited_for.as_millis() as i64;
        let key = (room.to_string(), agent.to_string());
        let released = self.room_released.lock().unwrap().get(&key).copied();
        let existing = db::room_limit(&self.db(), room, agent)?;
        let (since, held) = match (existing, released) {
            (Some(l), _) => (l.since, l.until - l.since),
            (None, Some((at, held))) if now - at < max => (now, held),
            _ => (now, 0),
        };
        let limit = db::RoomLimit {
            room: key.0,
            agent: key.1,
            since,
            until: now + self.next_hold(held),
            reason: reason.to_string(),
        };
        db::set_room_limit(&self.db(), &limit)?;
        self.escalate(agent, room, now)
    }

    /// 整机升级（§5.2）：`agent` 上未到期的房间级限制达到 [`TRIAL_ESCALATE_ROOMS`] 条，就记一条整机限制，
    /// 之后分配、迁走、到期前试拉、解除都走现有整机受限的流程。到期待验证的不算，证据已经过时。
    /// 房间级限制原样保留，各自验证解除。已有整机限制（如上传受限）时不改它。
    fn escalate(&self, agent: &str, room: &str, now: i64) -> db::Result<()> {
        if db::limit(&self.db(), agent)?.is_some() {
            return Ok(());
        }
        let mut rooms: Vec<String> = db::room_limits_of(&self.db(), agent)?
            .into_iter()
            .filter(|l| now < l.until)
            .map(|l| l.room)
            .collect();
        if rooms.len() < TRIAL_ESCALATE_ROOMS {
            return Ok(());
        }
        rooms.sort();
        let rooms = rooms.join(",");
        // 档位同 `judge`：解除后 `limited_for` 内再受限，接着上次的往上走。
        let max = self.config.limited_for.as_millis() as i64;
        let held = match self.released.lock().unwrap().get(agent) {
            Some(&(at, held)) if now - at < max => held,
            _ => 0,
        };
        let hold = self.next_hold(held);
        warn!(
            event_name = "health.trial",
            outcome = "escalated",
            agent,
            room,
            rooms,
            hold_s = hold / 1000,
            "room limits reached the threshold, limiting the whole agent"
        );
        // need_bps 为 0：恢复不看上传速度，只看到期前试拉（同录制类整机限制）。
        let limit = db::Limit {
            since: now,
            until: now + hold,
            reason: format!("room limits {rooms}"),
            need_bps: 0.0,
        };
        db::set_limit(&self.db(), agent, &limit)?;
        // 其余房间尽快走现有交接迁走，同 `judge` 标录制受限时。
        self.tick_now.notify_one();
        Ok(())
    }

    /// 试拉没成：原机继续录，记下这台候选机本轮试过，按原因定多久再试（§4.4）。
    fn trial_failed(&self, name: &str, t: &Trial, kind: &'static str, now: i64) {
        let mut rechecks = self.rechecks.lock().unwrap();
        let recheck = rechecks.entry(name.to_string()).or_default();
        recheck.tried.push(t.to.clone());
        let source = matches!(kind, "source_problem" | "source_no_stream");
        let wait = if source {
            // 源站的问题换机也没用：2→4→8 分钟递增、10 分钟封顶，免得主播网络差时持续双拉（G4）。
            let wait =
                (TRIAL_RECHECK_MS << recheck.source_fails.min(8)).min(TRIAL_SOURCE_RECHECK_MAX_MS);
            recheck.source_fails += 1;
            wait
        } else {
            TRIAL_RECHECK_MS
        };
        recheck.at = now + wait;
        // 同一原因只告警一次；源站无流多半是下播后平台滞后，不告警（C8）。
        let first = std::mem::replace(&mut recheck.last, kind) != kind;
        if first && kind != "source_no_stream" {
            warn!(
                event_name = "health.trial",
                outcome = "failed",
                reason_code = kind,
                room = name,
                agent = t.from,
                candidate = t.to,
                signal = t.trouble.signal,
                next_s = wait / 1000,
                "failure trial failed, staying on this agent"
            );
        } else {
            info!(
                event_name = "health.trial",
                outcome = "failed",
                reason_code = kind,
                room = name,
                agent = t.from,
                candidate = t.to,
                signal = t.trouble.signal,
                next_s = wait / 1000,
                "failure trial failed, staying on this agent"
            );
        }
        drop(rechecks);
        self.changed.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::{assign, capture_logs, config, db_room, seen, test_apps};

    const MIN: i64 = 60_000;
    const T: i64 = 100 * MIN;

    /// 表驱动用例：(说明, 构造场景)。
    type Case = (&'static str, fn(&App));

    /// 支持故障试拉的 agent 的心跳。
    fn capable() -> Status {
        Status {
            health_reports: true,
            trial_support: true,
            ..Default::default()
        }
    }

    fn slow(room: &str, rate: f64) -> Status {
        Status {
            record_rates: vec![(room.into(), rate)],
            produce_bps: 400.0,
            ..capable()
        }
    }

    /// 一次心跳：同 `/v1/watch` 里的顺序。
    fn beat(app: &App, agent: &str, s: &Status, now: i64) {
        db::touch_agent(&app.db(), agent, now, s).unwrap();
        app.judge(agent, s, now).unwrap();
        app.health(agent, s, now);
        app.note_troubles(agent, s);
        app.trial_step(now).unwrap();
    }

    /// 跑 `f`，返回期间的 `health.trial` 日志行。
    fn trial_logs(f: impl FnOnce()) -> Vec<String> {
        capture_logs(f)
            .lines()
            .filter(|l| l.contains("health.trial"))
            .map(str::to_string)
            .collect()
    }

    /// 三台都在优先列表里的 `r`。
    fn room3(app: &App) -> db::Room {
        let mut room = db_room(app, "r");
        room.agents = vec!["a".into(), "b".into(), "c".into()];
        room
    }

    fn limit_room(app: &App, agent: &str, since: i64) {
        let l = db::RoomLimit {
            room: "r".into(),
            agent: agent.into(),
            since,
            until: since + 20 * MIN,
            reason: "no_video".into(),
        };
        db::set_room_limit(&app.db(), &l).unwrap();
    }

    /// 选机顺序（§4.3）：先挑没有限制的；对这个房间有房间级限制的只作末位候选，最近受限的优先。
    #[test]
    fn candidates_without_limits_come_first_and_room_limited_ones_last() {
        let (app, ..) = test_apps(config());
        let room = room3(&app);
        let pick = |tried: &[&str]| {
            let tried: Vec<String> = tried.iter().map(|a| a.to_string()).collect();
            app.trial_candidate(&room, "a", &tried, T).unwrap()
        };
        assert_eq!(pick(&[]), None, "都不在线");
        for agent in ["a", "b", "c"] {
            db::touch_agent(&app.db(), agent, T, &capable()).unwrap();
        }
        assert_eq!(pick(&[]).as_deref(), Some("b"), "按优先列表，跳过原机");
        assert_eq!(pick(&["b"]).as_deref(), Some("c"), "本轮试过的不再挑");
        assert_eq!(pick(&["b", "c"]), None);
        limit_room(&app, "b", T - 30 * MIN);
        assert_eq!(pick(&[]).as_deref(), Some("c"), "有房间级限制的排到末位");
        assert_eq!(pick(&["c"]).as_deref(), Some("b"), "没有别的才用它");
        limit_room(&app, "c", T - 10 * MIN);
        assert_eq!(pick(&[]).as_deref(), Some("c"), "都受限：最近受限的优先");
        // 到期待验证的也只作末位候选。
        limit_room(&app, "c", T - 60 * MIN);
        assert_eq!(pick(&[]).as_deref(), Some("b"));
    }

    /// 整机隔离、停用、排空、停录、失联与不支持故障试拉的旧 agent 都不当候选。
    #[test]
    fn unavailable_machines_and_old_agents_are_never_candidates() {
        let unfit: [Case; 6] = [
            ("整机隔离", |app| {
                let l = db::Limit {
                    since: T,
                    until: T + 20 * MIN,
                    reason: "upload".into(),
                    need_bps: 0.0,
                };
                db::set_limit(&app.db(), "b", &l).unwrap();
            }),
            ("停用", |app| {
                db::update_machine(&app.db(), "b", Some(false), None).unwrap();
            }),
            ("排空", |app| {
                db::start_drain(&app.db(), "b", "c1", T).unwrap();
            }),
            ("停录", |app| {
                let s = Status {
                    paused: true,
                    ..capable()
                };
                db::touch_agent(&app.db(), "b", T, &s).unwrap();
            }),
            ("失联", |app| {
                db::touch_agent(&app.db(), "b", T - 10 * MIN, &capable()).unwrap();
            }),
            ("旧 agent", |app| seen(app, "b", T, false)),
        ];
        for (why, make) in unfit {
            let (app, ..) = test_apps(config());
            for agent in ["a", "b", "c"] {
                db::touch_agent(&app.db(), agent, T, &capable()).unwrap();
            }
            make(&app);
            let got = app.trial_candidate(&room3(&app), "a", &[], T).unwrap();
            assert_eq!(got.as_deref(), Some("c"), "{why}");
        }
    }

    /// 开关关着：走到要试拉这一步只打一条日志（房间、信号、会选哪台），不下发试拉，现有判定照旧。
    #[test]
    fn switched_off_it_only_logs_what_it_would_trial() {
        let (app, ..) = test_apps(config());
        assert!(!app.config.health_trial, "默认关闭");
        db::touch_agent(&app.db(), "b", T, &capable()).unwrap();
        assign(&app, "r", "a", T);
        app.detected("r", Some(true));
        let lines = trial_logs(|| {
            beat(&app, "a", &slow("r", 0.2), T);
            beat(&app, "a", &slow("r", 0.2), T + 10_000);
            // 不是它在录的房间不算。
            beat(&app, "c", &slow("r", 0.2), T + 10_000);
        });
        assert_eq!(lines.len(), 1, "只在变化时打一条：{lines:?}");
        let line = &lines[0];
        for want in [
            "room=\"r\"",
            "agent=\"a\"",
            "signal=\"record_rate\"",
            "window_s=20",
            "candidate=\"b\"",
            "reason_code=\"disabled\"",
        ] {
            assert!(line.contains(want), "缺 {want}：{line}");
        }
        assert!(line.is_ascii(), "{line}");
        assert_eq!(app.trial_for("b"), None, "不下发故障试拉");
        // 现有行为不变：照旧标整机、照旧交接。
        assert!(db::limit(&app.db(), "a").unwrap().is_some());
        let live = Some(Some("s".to_string()));
        assert!(app.schedule(&db_room(&app, "r"), live, T + 10_000).unwrap());
        assert_eq!(db::handoff(&app.db(), "r").unwrap().unwrap().to, "b");

        // 轻度慢观察 60 秒；没有候选机也照样记一条。恢复后再出问题重新记。
        let (app, ..) = test_apps(config());
        assign(&app, "r", "a", T);
        let lines = trial_logs(|| {
            beat(&app, "a", &slow("r", 0.6), T);
            beat(&app, "a", &capable(), T + 10_000);
            let stalled = Status {
                record_failures: vec![("r".into(), 1, true)],
                ..capable()
            };
            beat(&app, "a", &stalled, T + 20_000);
        });
        assert_eq!(lines.len(), 2, "{lines:?}");
        assert!(
            lines[0].contains("window_s=60") && lines[0].contains("candidate=\"(none)\""),
            "{lines:?}"
        );
        assert!(
            lines[1].contains("signal=\"record_failures\"") && lines[1].contains("window_s=20"),
            "{lines:?}"
        );
    }

    /// 第一期的三个健康信号也走到要试拉这一步；同时有几个信号时，基本录不到内容的优先。
    #[test]
    fn health_signals_and_record_signals_become_one_trouble_per_room() {
        let (app, ..) = test_apps(config());
        let s = Status {
            record_connections: (1..=3).map(|i| ("r".into(), i * 1000, 1, true)).collect(),
            record_video: vec![("r2".into(), None, 2 * MIN as u64)],
            record_skips: vec![("r2".into(), 40_000, 3), ("trial:r".into(), 40_000, 3)],
            ..capable()
        };
        app.health("a", &s, T);
        app.note_troubles("a", &s);
        let got = |room: &str| {
            let key = ("a".to_string(), room.to_string());
            app.troubles.lock().unwrap().get(&key).cloned()
        };
        let trouble = |signal, fast| Some(Trouble { signal, fast });
        assert_eq!(got("r"), trouble("short_connections", false));
        assert_eq!(got("r2"), trouble("no_video", true));
        assert_eq!(got("trial:r"), None, "故障试拉的键不算");
        app.health("a", &capable(), T);
        app.note_troubles("a", &capable());
        assert!(app.troubles.lock().unwrap().is_empty(), "恢复后清掉");
    }

    fn on() -> Arc<App> {
        let mut config = config();
        config.health_trial = true;
        test_apps(config).0
    }

    /// 故障试拉的逐秒进度：连上 `secs` 秒，开头一次给出 `burst_ms` 的 GOP 缓存，之后按 `rate` 倍推进。
    fn run(secs: u64, burst_ms: u64, rate: f64) -> Vec<(u64, u64, u64)> {
        (1..=secs)
            .map(|i| {
                let media = burst_ms + (rate * 1000.0 * i as f64) as u64;
                ((secs - i) * 1000, media, i * 1000)
            })
            .collect()
    }

    /// 候选机的心跳：正在故障试拉 `room`。
    fn trialing(room: &str, samples: Vec<(u64, u64, u64)>) -> Status {
        Status {
            record_trial: vec![(format!("trial:{room}"), samples)],
            ..capable()
        }
    }

    /// 录制机的心跳：`room` 平台确认在播，但两分钟没写出视频。
    fn no_video(room: &str) -> Status {
        Status {
            record_video: vec![(room.into(), None, 2 * MIN as u64)],
            ..capable()
        }
    }

    /// 三台都在线、支持故障试拉；`r` 在 a 上录，a 报持续无视频。返回下发给候选机的任务。
    fn started(app: &App, candidate: &str) -> RoomTask {
        for agent in ["a", "b", "c"] {
            db::touch_agent(&app.db(), agent, T, &capable()).unwrap();
        }
        assign(app, "r", "a", T);
        app.detected("r", Some(true));
        beat(app, "a", &no_video("r"), T);
        app.trial_for(candidate).expect("候选机被下发故障试拉")
    }

    /// `r` 的优先列表改成三台。
    fn three_agents(app: &App) {
        let room = room3(app);
        db::save_room(&app.db(), &room).unwrap();
    }

    fn room_limit(app: &App, room: &str, agent: &str) -> Option<db::RoomLimit> {
        db::room_limit(&app.db(), room, agent).unwrap()
    }

    /// 20 秒与 60 秒两种观察窗口、首包突发扣除、90 秒时限（§4.1、§4.2）。
    #[test]
    fn observation_needs_a_full_window_of_realtime_media_after_the_first_burst() {
        use Verdict::*;
        let (fast, slow) = (TRIAL_FAST_WINDOW_MS, TRIAL_SLOW_WINDOW_MS);
        // 第一秒是首包突发，再过 2 秒才开始算：连上 23 秒才攒够 20 秒的窗口。
        assert_eq!(observe(&run(22, 6_000, 1.0), fast, false, 25_000), Waiting);
        assert_eq!(observe(&run(23, 6_000, 1.0), fast, false, 25_000), Healthy);
        assert_eq!(observe(&run(23, 6_000, 1.0), slow, false, 25_000), Waiting);
        assert_eq!(observe(&run(62, 6_000, 1.0), slow, false, 65_000), Waiting);
        assert_eq!(observe(&run(63, 6_000, 1.0), slow, false, 65_000), Healthy);
        assert_eq!(observe(&run(23, 0, 0.9), fast, false, 25_000), Healthy);
        // 只有 0.7 倍的连接：不扣首包突发的话 (6 + 0.7 × 20) / 20 = 1.0 就放过去了。
        let weak = run(23, 6_000, 0.7);
        assert!((weak[22].1 - weak[2].1 + 6_000) as f64 >= 0.9 * 20_000.0);
        assert_eq!(observe(&weak, fast, false, 25_000), Waiting);
        assert_eq!(
            observe(&run(85, 6_000, 0.7), fast, false, 90_000),
            Unhealthy
        );
        assert_eq!(
            observe(&run(85, 6_000, 0.89), slow, false, 90_000),
            Unhealthy
        );
        // 开头慢、之后连续 20 秒达标也算；60 秒的窗口还不够。
        let mut late = run(30, 6_000, 0.5);
        let (base, wall) = (late[29].1, late[29].2);
        for s in &mut late {
            s.0 += 22_000;
        }
        late.extend((1..=22).map(|i| ((22 - i) * 1000, base + i * 1000, wall + i * 1000)));
        assert_eq!(observe(&late, fast, false, 55_000), Healthy);
        assert_eq!(observe(&late, slow, false, 55_000), Waiting);
        // 一帧都没有：到时限才下结论。
        let empty: Vec<_> = (0..10).map(|i| ((9 - i) * 1000, 0, i * 1000)).collect();
        assert_eq!(observe(&empty, fast, false, 89_999), Waiting);
        assert_eq!(observe(&empty, fast, false, 90_000), NoVideo);
        assert_eq!(observe(&[], fast, true, 90_000), NoVideo);
        // 停滞、异常断开、内容跳失：有视频也立刻判不达标。
        assert_eq!(observe(&run(23, 6_000, 1.0), fast, true, 25_000), Unhealthy);
    }

    /// 试拉健康且原机问题还在：发起交接，候选机 `rooms` 里的任务与下发的试拉完全相同（§6.1.1 约定 2），
    /// 原机记房间级限制，候选机在这个房间上原有的限制解除。
    #[test]
    fn a_healthy_trial_hands_off_with_the_identical_task() {
        let app = on();
        // 候选机 b 之前在这个房间上受限：没有别的候选，仍用它。
        limit_room(&app, "b", T - 30 * MIN);
        let task = started(&app, "b");
        // 开录后房间改了配置：试拉与转正都沿用这一场开头的快照。
        let mut room = db_room(&app, "r");
        room.split_time = Some(7200);
        db::save_room(&app.db(), &room).unwrap();
        assert_ne!(db_room(&app, "r").task(), task);
        assert_eq!(app.desired("a").unwrap(), std::slice::from_ref(&task));
        assert!(app.desired("b").unwrap().is_empty(), "试拉不进 rooms");

        beat(&app, "b", &trialing("r", run(10, 6_000, 1.0)), T + 12_000);
        assert_eq!(app.trial_for("b"), Some(task.clone()), "还没攒够 20 秒");
        assert_eq!(db::handoff(&app.db(), "r").unwrap(), None);

        let at = T + 25_000;
        let lines = trial_logs(|| beat(&app, "b", &trialing("r", run(23, 6_000, 1.0)), at));
        assert!(lines[0].contains("outcome=\"promoted\""), "{lines:?}");
        assert_eq!(app.desired("b").unwrap(), [task], "同一份任务进了 rooms");
        assert_eq!(app.trial_for("b"), None);
        let h = db::handoff(&app.db(), "r").unwrap().unwrap();
        assert_eq!((&*h.from, &*h.to), ("a", "b"));
        let l = room_limit(&app, "r", "a").unwrap();
        assert_eq!(
            (l.since, l.until, &*l.reason),
            (at, at + 20 * MIN, "no_video")
        );
        assert_eq!(room_limit(&app, "r", "b"), None, "试拉健康即解除");
        assert!(db::limit(&app.db(), "a").unwrap().is_none(), "不标整机");
        // 之后是现有交接：候选机补报开录，旧机收尾，分配改到候选机；原机受限，不迁回。
        let ev = lubic_proto::AgentEvent::Recording {
            room: "r".into(),
            lag_ms: Some(500),
            start_live_ms: None,
        };
        db::apply(&app.db(), "b", &ev).unwrap();
        let done = now_ms() + HANDOFF_MAX_WAIT_MS;
        for agent in ["a", "b"] {
            db::touch_agent(&app.db(), agent, done, &capable()).unwrap();
        }
        let live = || Some(Some("s".to_string()));
        assert!(app.schedule(&db_room(&app, "r"), live(), done).unwrap());
        assert_eq!(db::assignment(&app.db(), "r").unwrap().unwrap().0, "b");
        assert!(!app.schedule(&db_room(&app, "r"), live(), done).unwrap());
    }

    /// 转正时按原机有无视频定暂存内容的去留（§6.2）：候选机开始写出媒体之后原机还写出过视频，就让候选机丢弃切点前的暂存段；
    /// 原机这段时间没视频、或判断不了，都保留。切点是候选机报的正式分段开头，经现有交接下发给原机。
    #[test]
    fn staged_content_is_dropped_only_when_the_origin_wrote_video_meanwhile() {
        // 原机碎片化地录着，候选机开录（T+4s）之后还写出过视频：丢弃。
        let choppy = |video_ago| Status {
            record_connections: (1..=3)
                .map(|i| ("r".into(), i * 1000, 5_000, true))
                .collect(),
            record_video: vec![("r".into(), Some(video_ago), 10 * MIN as u64)],
            ..capable()
        };
        let app = on();
        for agent in ["a", "b", "c"] {
            db::touch_agent(&app.db(), agent, T, &capable()).unwrap();
        }
        assign(&app, "r", "a", T);
        beat(&app, "a", &choppy(1_000), T);
        assert!(app.trial_for("b").is_some());
        assert!(app.trial_drop_for("b").unwrap().is_empty(), "试拉中不发");
        beat(&app, "a", &choppy(1_000), T + 60_000);
        // 连接碎片化看 60 秒窗口。
        let at = T + 68_000;
        let lines = trial_logs(|| beat(&app, "b", &trialing("r", run(65, 6_000, 1.0)), at));
        assert!(lines[0].contains("staged=\"dropped\""), "{lines:?}");
        assert_eq!(app.trial_drop_for("b").unwrap(), ["r"]);
        assert!(app.trial_drop_for("a").unwrap().is_empty());
        // 候选机报的开录时刻就是切点，原机按它收尾。
        let ev = lubic_proto::AgentEvent::Recording {
            room: "r".into(),
            lag_ms: Some(500),
            start_live_ms: Some(1_032_000),
        };
        db::apply(&app.db(), "b", &ev).unwrap();
        assert_eq!(
            db::cuts_for(&app.db(), "a").unwrap(),
            [("r".to_string(), 1_032_000)]
        );
        // 交接结束（这里直接撤掉）后不再发。
        db::delete_handoff(&app.db(), "r").unwrap();
        assert!(app.trial_drop_for("b").unwrap().is_empty());

        // 原机最后一次视频在候选机开录（T+4s）之前：试拉期间它没写出内容，保留。
        let app = on();
        for agent in ["a", "b", "c"] {
            db::touch_agent(&app.db(), agent, T, &capable()).unwrap();
        }
        assign(&app, "r", "a", T);
        beat(&app, "a", &choppy(1_000), T);
        let lines = trial_logs(|| beat(&app, "b", &trialing("r", run(65, 6_000, 1.0)), at));
        assert!(lines[0].contains("staged=\"kept\""), "{lines:?}");
        assert!(app.trial_drop_for("b").unwrap().is_empty());

        // 原机持续无视频：保留。
        let app = on();
        started(&app, "b");
        beat(&app, "b", &trialing("r", run(23, 6_000, 1.0)), T + 25_000);
        assert!(db::handoff(&app.db(), "r").unwrap().is_some());
        assert!(app.trial_drop_for("b").unwrap().is_empty());
    }

    /// 房间级限制的时长按「房间加机器」递增：解除后 1 小时内再受限接着往上走。
    #[test]
    fn room_limits_escalate_per_room_and_machine() {
        let app = on();
        let until = |room: &str, at: i64| {
            app.limit_room(room, "a", "no_video", at).unwrap();
            room_limit(&app, room, "a").unwrap().until - at
        };
        assert_eq!(until("r", T), 20 * MIN);
        assert_eq!(until("r", T + 5 * MIN), 40 * MIN, "限制还在时又受限");
        db::clear_room_limit(&app.db(), "r", "a").unwrap();
        let key = ("r".to_string(), "a".to_string());
        let released = (T + 30 * MIN, 40 * MIN);
        app.room_released.lock().unwrap().insert(key, released);
        assert_eq!(until("r", T + 60 * MIN), 60 * MIN, "解除不到 1 小时");
        assert_eq!(until("r2", T + 60 * MIN), 20 * MIN, "别的房间各记各的");
        db::clear_room_limit(&app.db(), "r", "a").unwrap();
        assert_eq!(until("r", T + 91 * MIN), 20 * MIN, "稳定满 1 小时从头起");
    }

    /// 原机信号中途回落不取消，观察完再看：原机仍有问题就转正，已恢复才取消（线上某房间：断续的重连缺口让信号
    /// 反复回落，试拉被连取消两次，晚了约 2 分钟才迁走）。
    #[test]
    fn a_trial_runs_to_the_end_even_if_the_origin_recovers_meanwhile() {
        for still_bad in [true, false] {
            let app = on();
            started(&app, "b");
            let lines = trial_logs(|| beat(&app, "a", &capable(), T + 10_000));
            assert!(lines.is_empty(), "{lines:?}");
            assert!(app.trial_for("b").is_some(), "信号回落后接着试");
            if still_bad {
                beat(&app, "a", &no_video("r"), T + 20_000);
            }
            let lines =
                trial_logs(|| beat(&app, "b", &trialing("r", run(23, 6_000, 1.0)), T + 25_000));
            let handoff = db::handoff(&app.db(), "r").unwrap();
            if still_bad {
                assert!(handoff.is_some(), "{lines:?}");
            } else {
                assert!(
                    lines[0].contains("outcome=\"cancelled\"")
                        && lines[0].contains("reason_code=\"recovered\""),
                    "{lines:?}"
                );
                assert_eq!(handoff, None);
                assert_eq!(app.trial_for("b"), None);
                assert_eq!(room_limit(&app, "r", "a"), None);
            }
        }
    }

    /// 取消（§4.5）：下播、撤销分配、换场、别的原因已在交接。候选机收到不含试拉的清单，不迁、不加限制。
    #[test]
    fn a_trial_is_cancelled_when_the_room_or_the_origin_changes() {
        let cancels: [Case; 4] = [
            ("assignment_changed", |app| {
                db::unassign(&app.db(), "r").unwrap()
            }),
            ("session_changed", |app| {
                db::set_assignment_session(&app.db(), "r", "s2").unwrap()
            }),
            ("offline", |app| app.detected("r", Some(false))),
            ("handoff", |app| {
                db::start_handoff(&app.db(), "r", "a", "c", T).unwrap()
            }),
        ];
        for (reason, change) in cancels {
            let app = on();
            started(&app, "b");
            let lines = trial_logs(|| {
                change(&app);
                app.trial_step(T + 10_000).unwrap();
            });
            assert_eq!(lines.len(), 1, "{reason}: {lines:?}");
            assert!(
                lines[0].contains("outcome=\"cancelled\"")
                    && lines[0].contains(&format!("reason_code=\"{reason}\"")),
                "{lines:?}"
            );
            assert_eq!(app.trial_for("b"), None, "{reason}");
            assert_eq!(room_limit(&app, "r", "a"), None, "{reason}");
            // 试拉健康的心跳晚到也不再迁。
            beat(&app, "b", &trialing("r", run(23, 6_000, 1.0)), T + 25_000);
            if reason != "handoff" {
                assert_eq!(db::handoff(&app.db(), "r").unwrap(), None, "{reason}");
            }
        }
    }

    /// 候选机失联、停录、被停用或进入排空：取消，本轮不再试这台，2 分钟后轮换下一台。
    #[test]
    fn a_lost_or_unfit_candidate_is_dropped_and_the_next_one_tried() {
        let lost: [Case; 4] = [
            ("失联", |_| {}),
            ("停录", |app| {
                let s = Status {
                    paused: true,
                    ..capable()
                };
                db::touch_agent(&app.db(), "b", T + 3 * MIN, &s).unwrap();
            }),
            ("停用", |app| {
                db::update_machine(&app.db(), "b", Some(false), None).unwrap();
            }),
            ("排空", |app| {
                db::start_drain(&app.db(), "b", "c1", T).unwrap();
            }),
        ];
        for (why, change) in lost {
            let app = on();
            three_agents(&app);
            started(&app, "b");
            change(&app);
            // b 的上一次心跳停在 T；a、c 照常。
            let at = T + 3 * MIN;
            db::touch_agent(&app.db(), "c", at, &capable()).unwrap();
            let lines = trial_logs(|| beat(&app, "a", &no_video("r"), at));
            assert!(
                lines[0].contains("reason_code=\"candidate_lost\"") && lines[0].contains("WARN"),
                "{why}: {lines:?}"
            );
            assert_eq!(app.trial_for("b"), None, "{why}");
            beat(&app, "a", &no_video("r"), at + 2 * MIN - 1);
            assert_eq!(app.trial_for("c"), None, "{why}: 2 分钟内不再试");
            db::touch_agent(&app.db(), "c", at + 2 * MIN, &capable()).unwrap();
            beat(&app, "a", &no_video("r"), at + 2 * MIN);
            assert!(app.trial_for("c").is_some(), "{why}: 轮换下一台");
            assert_eq!(room_limit(&app, "r", "a"), None);
        }
    }

    /// 原机失联：候选机在线就直接改派给它，暂存内容转正（任务相同）；候选机也不在线就照常改派。
    #[test]
    fn origin_lost_during_a_trial_reassigns_to_the_candidate() {
        let live = || Some(Some("s".to_string()));
        let at = T + 5 * MIN;
        let app = on();
        three_agents(&app);
        let task = started(&app, "b");
        for agent in ["b", "c"] {
            db::touch_agent(&app.db(), agent, at, &capable()).unwrap();
        }
        assert!(app.schedule(&db_room(&app, "r"), live(), at).unwrap());
        assert_eq!(db::assignment(&app.db(), "r").unwrap().unwrap().0, "b");
        assert_eq!(app.desired("b").unwrap(), [task]);
        assert_eq!(app.trial_for("b"), None);

        let app = on();
        three_agents(&app);
        started(&app, "b");
        db::touch_agent(&app.db(), "c", at, &capable()).unwrap();
        assert!(app.schedule(&db_room(&app, "r"), live(), at).unwrap());
        assert_eq!(db::assignment(&app.db(), "r").unwrap().unwrap().0, "c");
        assert_eq!(app.trial_for("b"), None, "试拉取消");
    }

    /// 备用机同样异常（有视频但不达标）：判源站问题，只告警一次，不迁、不加限制；
    /// 复查间隔 2→4→8 分钟递增、10 分钟封顶，候选机轮换（§4.4、G4）。原机恢复后归零。
    #[test]
    fn source_problems_back_off_and_rotate_candidates() {
        let app = on();
        three_agents(&app);
        started(&app, "b");
        let mut at = T;
        let lines = trial_logs(|| {
            for (wait, candidate) in [(2, "b"), (4, "c"), (8, "b"), (10, "c"), (10, "b")] {
                assert!(app.trial_for(candidate).is_some(), "{wait} {candidate}");
                let end = at + TRIAL_TIMEOUT_MS;
                beat(&app, candidate, &trialing("r", run(85, 6_000, 0.7)), end);
                assert_eq!(app.trial_for(candidate), None);
                at = end + wait * MIN;
                for agent in ["b", "c"] {
                    db::touch_agent(&app.db(), agent, at, &capable()).unwrap();
                }
                beat(&app, "a", &no_video("r"), at - 1);
                assert!(app.trials.lock().unwrap().is_empty(), "{wait} 分钟内不再试");
                beat(&app, "a", &no_video("r"), at);
            }
        });
        let failed: Vec<_> = lines
            .iter()
            .filter(|l| l.contains("reason_code=\"source_problem\""))
            .collect();
        assert_eq!(failed.len(), 5, "{lines:?}");
        assert_eq!(
            failed.iter().filter(|l| l.contains("WARN")).count(),
            1,
            "只告警一次：{lines:?}"
        );
        assert_eq!(room_limit(&app, "r", "a"), None);
        assert_eq!(db::handoff(&app.db(), "r").unwrap(), None);
        assert!(db::limit(&app.db(), "a").unwrap().is_none());
        // 原机恢复：进行中的试拉观察完就取消，递增间隔归零；再出问题立刻重新试。
        beat(&app, "a", &capable(), at + 10_000);
        beat(&app, "c", &trialing("r", run(23, 6_000, 1.0)), at + 25_000);
        assert!(app.trials.lock().unwrap().is_empty());
        assert!(app.rechecks.lock().unwrap().is_empty());
        beat(&app, "a", &no_video("r"), at + 20_000);
        assert!(!app.trials.lock().unwrap().is_empty());
    }

    /// 候选机也拉不到任何视频、原机也没有：判源站无流（多半是下播后平台滞后），只记 info，不告警、不加限制（C8）。
    /// 原机还写得出视频时，拉不到的是候选机：告警，2 分钟后换下一台。
    #[test]
    fn no_video_on_both_sides_is_a_source_without_stream() {
        let app = on();
        three_agents(&app);
        started(&app, "b");
        let end = T + TRIAL_TIMEOUT_MS;
        beat(&app, "b", &capable(), end - 1);
        assert!(app.trial_for("b").is_some(), "到 90 秒才下结论");
        let lines = trial_logs(|| beat(&app, "b", &capable(), end));
        assert_eq!(lines.len(), 1, "{lines:?}");
        assert!(
            lines[0].contains("reason_code=\"source_no_stream\"") && lines[0].contains("INFO"),
            "{lines:?}"
        );
        assert_eq!(app.trial_for("b"), None);
        assert_eq!(room_limit(&app, "r", "a"), None);
        assert_eq!(db::handoff(&app.db(), "r").unwrap(), None);
        beat(&app, "a", &no_video("r"), end + 2 * MIN - 1);
        assert!(app.trials.lock().unwrap().is_empty());

        // 原机在碎片化地录着（有视频）：候选机一帧没有，不能说源站无流。
        let app = on();
        three_agents(&app);
        for agent in ["a", "b", "c"] {
            db::touch_agent(&app.db(), agent, T, &capable()).unwrap();
        }
        assign(&app, "r", "a", T);
        let choppy = Status {
            record_connections: (1..=3)
                .map(|i| ("r".into(), i * 1000, 5_000, true))
                .collect(),
            record_video: vec![("r".into(), Some(1_000), 10 * MIN as u64)],
            ..capable()
        };
        beat(&app, "a", &choppy, T);
        assert!(app.trial_for("b").is_some());
        let lines = trial_logs(|| beat(&app, "b", &capable(), end));
        assert!(
            lines[0].contains("reason_code=\"candidate_no_video\"") && lines[0].contains("WARN"),
            "{lines:?}"
        );
        db::touch_agent(&app.db(), "c", end + 2 * MIN, &capable()).unwrap();
        beat(&app, "a", &choppy, end + 2 * MIN);
        assert!(app.trial_for("c").is_some(), "2 分钟后换下一台");
    }

    /// 没有候选机：留在原机，告警一次，2 分钟后再看。新旧 agent 混跑时，不报 `trial_support` 的旧 agent 不当候选。
    #[test]
    fn without_a_capable_candidate_the_room_stays_and_old_agents_get_no_trial() {
        let app = on();
        three_agents(&app);
        db::touch_agent(&app.db(), "a", T, &capable()).unwrap();
        seen(&app, "b", T, false);
        assign(&app, "r", "a", T);
        let lines = trial_logs(|| {
            for i in 0..3 {
                seen(&app, "b", T + i * 2 * MIN, false);
                beat(&app, "a", &slow("r", 0.2), T + i * 2 * MIN);
            }
        });
        assert_eq!(lines.len(), 1, "只告警一次：{lines:?}");
        assert!(
            lines[0].contains("reason_code=\"no_candidate\"") && lines[0].contains("WARN"),
            "{lines:?}"
        );
        assert_eq!(app.trial_for("b"), None, "旧 agent 不下发故障试拉");
        assert!(
            db::limit(&app.db(), "a").unwrap().is_none(),
            "开关开着不标整机"
        );
        let live = Some(Some("s".to_string()));
        assert!(
            !app.schedule(&db_room(&app, "r"), live, T + 4 * MIN)
                .unwrap()
        );
        // 支持的 c 上线：越过旧的 b 选它。
        db::touch_agent(&app.db(), "c", T + 6 * MIN, &capable()).unwrap();
        seen(&app, "b", T + 6 * MIN, false);
        beat(&app, "a", &slow("r", 0.2), T + 6 * MIN);
        assert!(app.trial_for("c").is_some());
        assert_eq!(app.trial_for("b"), None);
    }

    /// 每台机器同时最多试一个房间：名额被占着的房间等着，不算失败；故障试拉与到期前试拉不同时下发。
    #[test]
    fn one_trial_per_machine_and_never_together_with_a_probe() {
        let app = on();
        let mut r2 = db_room(&app, "r2");
        r2.agents = vec!["a".into(), "b".into()];
        db::save_room(&app.db(), &r2).unwrap();
        for agent in ["a", "b", "c"] {
            db::touch_agent(&app.db(), agent, T, &capable()).unwrap();
        }
        assign(&app, "r", "a", T);
        assign(&app, "r2", "a", T);
        let both = Status {
            record_video: vec![
                ("r".into(), None, 2 * MIN as u64),
                ("r2".into(), None, 2 * MIN as u64),
            ],
            ..capable()
        };
        let lines = trial_logs(|| beat(&app, "a", &both, T));
        assert_eq!(lines.len(), 1, "r2 等名额，不告警：{lines:?}");
        assert_eq!(app.trial_for("b").unwrap().name, "r");
        assert_eq!(app.trials.lock().unwrap().len(), 1);
        // r 的试拉出了结论，名额空出来，r2 接着试。
        beat(&app, "b", &trialing("r", run(23, 6_000, 1.0)), T + 25_000);
        assert_eq!(app.trial_for("b").unwrap().name, "r2");

        // b 另有一条房间级限制快到期（房间 q 在 c 上录）：平时让它试拉验证，故障试拉时让位。
        let app = on();
        let l = db::RoomLimit {
            room: "r2".into(),
            agent: "b".into(),
            since: T - 20 * MIN,
            until: T + MIN,
            reason: "no_video".into(),
        };
        db::set_room_limit(&app.db(), &l).unwrap();
        assign(&app, "r2", "c", T);
        let probe = db_room(&app, "r2").task();
        assert_eq!(app.side_tasks("b", T).unwrap(), (Some(probe.clone()), None));
        let task = started(&app, "b");
        assert_eq!(app.side_tasks("b", T).unwrap(), (None, Some(task)));
        beat(&app, "a", &capable(), T + 10_000);
        beat(&app, "b", &trialing("r", run(23, 6_000, 1.0)), T + 25_000);
        assert_eq!(app.side_tasks("b", T).unwrap(), (Some(probe), None));
    }

    /// 线上场景：房间刚分到机器，连上后几十秒没有关键帧，按 `record rate 0.00` 报上来。
    /// 开关开着时只对这个房间试拉，同机正常录制的另一个房间不动；出了画面，观察完就取消。
    #[test]
    fn a_room_without_keyframes_at_the_start_is_trialled_alone() {
        let app = on();
        for agent in ["a", "b"] {
            db::touch_agent(&app.db(), agent, T, &capable()).unwrap();
        }
        assign(&app, "r2", "a", T - 60 * MIN);
        assign(&app, "r", "a", T - 40_000);
        for room in ["r", "r2"] {
            app.detected(room, Some(true));
        }
        let rates = |r: f64| Status {
            record_rates: vec![("r".into(), r), ("r2".into(), 1.0)],
            produce_bps: 400.0,
            ..capable()
        };
        beat(&app, "a", &rates(0.0), T);
        assert!(db::limit(&app.db(), "a").unwrap().is_none(), "不判整机受限");
        assert_eq!(app.trial_for("b").unwrap().name, "r");
        assert_eq!(app.trial_for("c"), None, "r2 不试拉");
        let live = || Some(Some("s".to_string()));
        for room in ["r", "r2"] {
            assert!(!app.schedule(&db_room(&app, room), live(), T).unwrap());
            assert_eq!(db::handoff(&app.db(), room).unwrap(), None, "{room} 不迁");
        }
        // 第一个关键帧出来，倍率恢复：观察完取消试拉，谁也没动。
        beat(&app, "a", &rates(1.0), T + 10_000);
        beat(&app, "b", &trialing("r", run(23, 6_000, 1.0)), T + 25_000);
        assert_eq!(app.trial_for("b"), None);
        for room in ["r", "r2"] {
            assert_eq!(db::assignment(&app.db(), room).unwrap().unwrap().0, "a");
            assert_eq!(room_limit(&app, room, "a"), None);
        }
    }

    /// 开关开着时上传受限仍按整机故障标记、走现有交接。
    #[test]
    fn upload_limits_still_mark_the_whole_machine() {
        let app = on();
        let s = Status {
            produce_bps: 1000.0,
            upload_bps: Some(10.0),
            upload_busy: 1.0,
            ..capable()
        };
        beat(&app, "a", &s, T);
        let l = db::limit(&app.db(), "a").unwrap().unwrap();
        assert!(l.reason.starts_with("upload"), "{}", l.reason);
    }

    /// 下发故障试拉：没回显的长轮询立刻返回并带上 `trial`、不带 `probe`；回显了同一房间就挂住（§6.1.1 约定 3）。
    #[tokio::test]
    async fn http_watch_returns_the_trial_and_holds_once_echoed() {
        let app = on();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(axum::serve(listener, router(app.clone())).into_future());
        let now = now_ms();
        for agent in ["a", "b"] {
            db::touch_agent(&app.db(), agent, now, &capable()).unwrap();
        }
        assign(&app, "r", "a", now);
        beat(&app, "a", &no_video("r"), now);
        let client = reqwest::Client::new();
        let watch = |trial: Option<&str>| {
            client
                .post(format!("{base}{}", lubic_proto::WATCH))
                .bearer_auth("tb")
                .json(&Watch {
                    trial: trial.map(Into::into),
                    status: capable(),
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
        assert_eq!(got.trial.map(|t| t.name).as_deref(), Some("r"));
        assert_eq!(got.probe, None);
        assert!(got.rooms.is_empty());
        let held = tokio::spawn(watch(Some("r")));
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!held.is_finished(), "已在故障试拉，挂住");
        held.abort();
    }

    /// 和 `r` 一样、改了名字和优先列表的房间，存进库里。
    fn room_like_r(app: &App, name: &str, agents: &[&str]) -> db::Room {
        let mut room = db_room(app, "r");
        room.name = name.into();
        room.agents = agents.iter().map(|a| a.to_string()).collect();
        db::save_room(&app.db(), &room).unwrap();
        room
    }

    /// 整机升级（§5.2）：同机未到期的房间级限制达到 2 条才标整机。同一房间再受限还是 1 条，
    /// 到期待验证的不算；已有整机限制时不改它。
    #[test]
    fn two_live_room_limits_on_one_machine_limit_the_whole_machine() {
        let app = on();
        app.limit_room("r", "a", "no_video", T).unwrap();
        app.limit_room("r", "a", "no_video", T + MIN).unwrap();
        app.limit_room("r2", "b", "no_video", T + MIN).unwrap();
        assert_eq!(db::limit(&app.db(), "a").unwrap(), None, "1 条不升级");
        // r 的限制到 T + 41 分钟；之后它待验证，不算。
        app.limit_room("r2", "a", "no_video", T + 50 * MIN).unwrap();
        assert_eq!(db::limit(&app.db(), "a").unwrap(), None, "待验证的不算");
        let at = T + 55 * MIN;
        let lines = trial_logs(|| app.limit_room("r", "a", "record_rate", at).unwrap());
        let l = db::limit(&app.db(), "a").unwrap().expect("2 条升级");
        assert_eq!(
            (l.since, l.until, &*l.reason, l.need_bps),
            (at, at + 20 * MIN, "room limits r,r2", 0.0)
        );
        assert_eq!(lines.len(), 1, "{lines:?}");
        for want in [
            "outcome=\"escalated\"",
            "agent=\"a\"",
            "room=\"r\"",
            "rooms=\"r,r2\"",
        ] {
            assert!(lines[0].contains(want), "缺 {want}：{}", lines[0]);
        }
        assert!(lines[0].is_ascii(), "{}", lines[0]);
        assert_eq!(db::limit(&app.db(), "b").unwrap(), None);
        app.limit_room("r3", "a", "no_video", at + MIN).unwrap();
        assert_eq!(
            db::limit(&app.db(), "a").unwrap(),
            Some(l),
            "已有整机限制不改"
        );
    }

    /// 整机限制按现有流程到期解除，房间级限制原样保留、各自验证：受限的房间仍不分回这台，别的房间可以。
    /// 解除后 1 小时内再升级，整机档位接着往上走。
    #[test]
    fn room_limits_outlive_the_whole_machine_limit_they_escalated_to() {
        let app = on();
        app.limit_room("r", "a", "no_video", T).unwrap();
        app.limit_room("r2", "a", "no_video", T).unwrap();
        let until = db::limit(&app.db(), "a").unwrap().unwrap().until;
        for agent in ["a", "b"] {
            db::touch_agent(&app.db(), agent, until, &capable()).unwrap();
        }
        // 到期，没有到期前试拉的结果：按现有语义按时解除。
        app.judge("a", &capable(), until).unwrap();
        assert_eq!(db::limit(&app.db(), "a").unwrap(), None);
        assert!(room_limit(&app, "r", "a").is_some() && room_limit(&app, "r2", "a").is_some());
        let r3 = room_like_r(&app, "r3", &["a", "b"]);
        let pick = |room: &db::Room| app.pick(room, &[], until, Pick::Handoff).unwrap();
        assert_eq!(
            pick(&db_room(&app, "r")).as_deref(),
            Some("b"),
            "房间级限制还在"
        );
        assert_eq!(pick(&r3).as_deref(), Some("a"));
        // 两条都到期待验证：再记一条不升级；r 又受限凑满 2 条才再升级，档位从 40 分钟起。
        app.limit_room("r3", "a", "no_video", until + 5 * MIN)
            .unwrap();
        assert_eq!(db::limit(&app.db(), "a").unwrap(), None);
        let at = until + 6 * MIN;
        app.limit_room("r", "a", "no_video", at).unwrap();
        let l = db::limit(&app.db(), "a").unwrap().unwrap();
        assert_eq!((l.until, &*l.reason), (at + 40 * MIN, "room limits r,r3"));
    }

    /// 开关关着不升级：限制表里留着开关开过时的两条也不标整机，录制信号照旧按原因标整机。
    #[test]
    fn switched_off_room_limits_never_limit_the_whole_machine() {
        let (app, ..) = test_apps(config());
        for room in ["r", "r2"] {
            let l = db::RoomLimit {
                room: room.into(),
                agent: "a".into(),
                since: T,
                until: T + 20 * MIN,
                reason: "no_video".into(),
            };
            db::set_room_limit(&app.db(), &l).unwrap();
        }
        assign(&app, "r", "a", T);
        beat(&app, "a", &capable(), T);
        assert_eq!(db::limit(&app.db(), "a").unwrap(), None);
        beat(&app, "a", &slow("r", 0.2), T + 10_000);
        let l = db::limit(&app.db(), "a").unwrap().unwrap();
        assert!(l.reason.starts_with("record rate"), "{}", l.reason);
    }

    /// 一台机器整机限流，上面 3 个房间都录不到内容，候选机两台：前两个房间试拉健康、迁走后机器标整机受限，
    /// 第三个房间不再试拉，按整机受限走现有交接迁走；新开播的房间不分给它。总控重启后（限制都在库里）照旧。
    #[test]
    fn a_throttled_machine_is_limited_after_two_rooms_and_the_third_moves_without_a_trial() {
        let app = on();
        let rooms = ["r", "r2", "r3"];
        for name in rooms {
            room_like_r(&app, name, &["a", "b", "c"]);
            assign(&app, name, "a", T);
            app.detected(name, Some(true));
        }
        for agent in ["a", "b", "c"] {
            db::touch_agent(&app.db(), agent, T, &capable()).unwrap();
        }
        let s = Status {
            record_video: rooms
                .iter()
                .map(|r| (r.to_string(), None, 2 * MIN as u64))
                .collect(),
            ..capable()
        };
        beat(&app, "a", &s, T);
        let on_trial = |agent| app.trial_for(agent).map(|t| t.name);
        assert_eq!(on_trial("b").as_deref(), Some("r"));
        assert_eq!(on_trial("c").as_deref(), Some("r2"));
        // 两台候选机都试拉健康（c 的心跳先到库里，b 的心跳推进这一轮）。
        let at = T + 25_000;
        db::touch_agent(&app.db(), "c", at, &trialing("r2", run(23, 6_000, 1.0))).unwrap();
        let lines = trial_logs(|| beat(&app, "b", &trialing("r", run(23, 6_000, 1.0)), at));
        let outcomes: Vec<_> = lines
            .iter()
            .filter_map(|l| l.split("outcome=\"").nth(1)?.split('"').next())
            .collect();
        // 升级在第二个房间记限制时发生，先于它的 promoted 日志。
        assert_eq!(outcomes, ["promoted", "escalated", "promoted"], "{lines:?}");
        let l = db::limit(&app.db(), "a").unwrap().expect("标整机受限");
        assert_eq!(l.reason, "room limits r,r2");
        assert!(app.trials.lock().unwrap().is_empty(), "第三个房间不再试拉");
        beat(&app, "a", &s, at + 10_000);
        assert!(app.trials.lock().unwrap().is_empty(), "之后也不再试拉");

        let live = || Some(Some("s".to_string()));
        let now = at + 10_000;
        for agent in ["b", "c"] {
            db::touch_agent(&app.db(), agent, now, &capable()).unwrap();
        }
        assert!(app.schedule(&db_room(&app, "r3"), live(), now).unwrap());
        let h = db::handoff(&app.db(), "r3").unwrap().unwrap();
        assert_eq!((&*h.from, &*h.to), ("a", "b"), "按整机受限交接");
        assert_eq!(
            room_limit(&app, "r3", "a"),
            None,
            "没经过试拉，不记房间级限制"
        );
        let r4 = room_like_r(&app, "r4", &["a", "b"]);
        assert!(app.schedule(&r4, live(), now).unwrap());
        assert_eq!(db::assignment(&app.db(), "r4").unwrap().unwrap().0, "b");

        // 总控重启：进行中的试拉、档位记忆都没了，限制与交接在库里。
        let mut config = config();
        config.health_trial = true;
        let (mut fresh, ..) = test_apps(config);
        Arc::get_mut(&mut fresh).unwrap().db = app.db.clone();
        drop(app);
        let r5 = room_like_r(&fresh, "r5", &["a", "c"]);
        assert!(fresh.schedule(&r5, live(), now).unwrap());
        assert_eq!(db::assignment(&fresh.db(), "r5").unwrap().unwrap().0, "c");
        assert!(!fresh.schedule(&db_room(&fresh, "r3"), live(), now).unwrap());
        assert_eq!(db::handoff(&fresh.db(), "r3").unwrap().unwrap().to, "b");
        let room = room_like_r(&fresh, "r6", &["b", "a"]);
        assert_eq!(fresh.trial_candidate(&room, "b", &[], now).unwrap(), None);
        beat(&fresh, "a", &s, now);
        assert!(fresh.trials.lock().unwrap().is_empty(), "重启后也不再试拉");
    }
}
