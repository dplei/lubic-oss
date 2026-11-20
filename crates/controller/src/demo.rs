//! 控制台演示页：用假数据起一个真的总控，抓下页面与各个 GET 接口的返回，拼成一个不连总控的静态页。
//! 平时随测试跑一遍，接口或页面改了导致拼不出来就失败；设了 `LUBIC_DEMO_OUT` 时把演示页写进那个目录
//! （`tools/console-demo.sh`）。数据全是虚构的，按公开版规则不写机器名、房间名等个人信息。

use crate::db::{self, Limit};
use crate::tests::{FakeQr, test_apps};
use crate::{App, Config};
use argon2::password_hash::{PasswordHasher, SaltString, rand_core::OsRng};
use lubic_events::{CloseReason, Event, Owner, PartClosed, PartOpened};
use lubic_proto::{AgentEvent, Status};
use serde_json::{Map, Value, json};
use std::sync::Arc;
use std::time::Duration;

const MIN: i64 = 60_000;
const HOUR: i64 = 60 * MIN;

fn part_event(
    room: &str,
    session: &str,
    part: &str,
    at: i64,
    close: bool,
    title: &str,
) -> AgentEvent {
    let owner = Owner {
        room: room.into(),
        session: session.into(),
    };
    let event = if close {
        Event::PartClosed(PartClosed {
            owner,
            part_id: part.into(),
            path: format!("/rec/{part}.flv").into(),
            duration: Duration::from_secs(3600),
            size: 2_400_000_000,
            reason: CloseReason::TimedSplit,
            title: title.into(),
            closed_at_ms: None,
        })
    } else {
        Event::PartOpened(PartOpened {
            owner,
            part_id: part.into(),
            path: format!("/rec/{part}.flv.part").into(),
            title: title.into(),
        })
    };
    AgentEvent::Recorder { event, at }
}

fn uploaded(part: &str) -> AgentEvent {
    AgentEvent::Uploaded {
        part_id: part.into(),
        filename: format!("n-{part}"),
        line: "tx".into(),
        size: 2_400_000_000,
    }
}

fn ended(room: &str, session: &str, at: i64) -> AgentEvent {
    AgentEvent::Recorder {
        event: Event::SessionEnded(Owner {
            room: room.into(),
            session: session.into(),
        }),
        at,
    }
}

/// 一场直播：`parts` 个一小时的分P，从 `start` 开始；`live` 为真时最后一个还在录，否则这一场已下播。
/// `uploaded` 是前几个已传完。
fn session(
    app: &App,
    agent: &str,
    room: &str,
    key: &str,
    start: i64,
    parts: usize,
    uploaded_n: usize,
    live: bool,
    title: &str,
) {
    let mut events = Vec::new();
    for i in 0..parts {
        let id = format!("{key}-{i}");
        let at = start + i as i64 * HOUR;
        events.push(part_event(room, key, &id, at, false, title));
        if !(live && i + 1 == parts) {
            events.push(part_event(room, key, &id, at + HOUR, true, title));
        }
        if i < uploaded_n {
            events.push(uploaded(&id));
        }
    }
    if !live {
        events.push(ended(room, key, start + parts as i64 * HOUR + 5 * MIN));
    }
    db::apply_all(&mut app.db(), agent, &events).unwrap();
}

/// 造一个看得出各种状态的场景：在录、在传、受限、让出网络、离线，待投、已投。
fn seed(app: &App, now: i64) {
    // 测试库自带的房间、机器换成演示用的；账号 1 与模板 1 沿用，改个名字。
    app.db()
        .execute_batch(
            "DELETE FROM rooms; DELETE FROM machines;
             UPDATE accounts SET name = '示例账号';
             UPDATE templates SET name = '直播回放', title = '【直播回放】{name} %Y-%m-%d {title}',
                 tid = 171, tag = '直播录像';",
        )
        .unwrap();
    for (name, note) in [
        ("cloud-a", "云服务器 A"),
        ("cloud-b", "云服务器 B"),
        ("home", "家庭宽带机"),
        ("spare", "备用机"),
    ] {
        db::insert_machine(
            &app.db(),
            name,
            &format!("demo-{name}"),
            note,
            now - 20 * 24 * HOUR,
        )
        .unwrap();
    }
    for (name, url, agents, split) in [
        (
            "阿橘",
            "https://live.bilibili.com/10000001",
            &["cloud-a", "cloud-b", "home"][..],
            3600,
        ),
        (
            "小满",
            "https://live.douyin.com/10000002",
            &["home", "cloud-a"][..],
            3600,
        ),
        (
            "夜航",
            "https://live.bilibili.com/10000003",
            &["cloud-b", "cloud-a"][..],
            1800,
        ),
    ] {
        let room = db::Room {
            name: name.into(),
            url: url.into(),
            template_id: 1,
            agents: agents.iter().map(|a| a.to_string()).collect(),
            split_time: Some(split),
            split_size_mb: None,
            grace: None,
            bili_qn: None,
            douyin_quality: None,
            tail_upload: false,
            display_name: None,
            filename: db::DEFAULT_FILENAME.into(),
            season_section_id: None,
            tag: String::new(),
            deleted: false,
        };
        db::save_room(&app.db(), &room).unwrap();
    }

    // 先造已下播的场次（稿件 id 按建的先后：1、2、3）。
    // 夜航：前天一场已投稿；昨晚一场分P都传完，等投稿。
    session(
        app,
        "cloud-b",
        "夜航",
        "s-yh1",
        now - 2 * 24 * HOUR,
        2,
        2,
        false,
        "通宵马拉松",
    );
    db::submitted(
        &app.db(),
        1,
        "BV1demo00001",
        1,
        None,
        now - 2 * 24 * HOUR + 4 * HOUR,
    )
    .unwrap();
    // 阿橘上一场：最后一个分P还在传。
    session(
        app,
        "cloud-b",
        "阿橘",
        "s-aju0",
        now - 26 * HOUR,
        4,
        3,
        false,
        "冲分",
    );
    session(
        app,
        "cloud-b",
        "夜航",
        "s-yh2",
        now - 14 * HOUR,
        3,
        3,
        false,
        "周末联机",
    );
    // 阿橘：在 cloud-a 上录第 3 个分P，第 2 个在传。
    session(
        app,
        "cloud-a",
        "阿橘",
        "s-aju",
        now - 2 * HOUR - 20 * MIN,
        3,
        1,
        true,
        "今天也是排位日",
    );
    crate::tests::assign(app, "阿橘", "cloud-a", now - 2 * HOUR - 20 * MIN);
    // 小满：家庭宽带机在让出网络，兜底录在 cloud-a 上。
    session(
        app,
        "cloud-a",
        "小满",
        "s-xm",
        now - 40 * MIN,
        1,
        0,
        true,
        "深夜杂谈",
    );
    crate::tests::assign(app, "小满", "cloud-a", now - 40 * MIN);

    let a = Status {
        disk_usage: 0.42,
        disk_free: 520 << 30,
        trial_support: true,
        health_reports: true,
        queued_parts: 1,
        queued_bytes: 2_400_000_000,
        uploading: vec![("s-aju-1".into(), 1_560_000_000, 2_400_000_000)],
        record_rates: vec![("阿橘".into(), 1.0), ("小满".into(), 0.99)],
        produce_bps: 1_300_000.0,
        upload_bps: Some(4_200_000.0),
        ..Default::default()
    };
    db::touch_agent(&app.db(), "cloud-a", now - 3_000, &a).unwrap();
    let b = Status {
        disk_usage: 0.67,
        disk_free: 180 << 30,
        trial_support: true,
        health_reports: true,
        queued_parts: 1,
        queued_bytes: 2_400_000_000,
        uploading: vec![("s-aju0-3".into(), 600_000_000, 2_400_000_000)],
        ..Default::default()
    };
    db::touch_agent(&app.db(), "cloud-b", now - 8_000, &b).unwrap();
    db::set_limit(
        &app.db(),
        "cloud-b",
        &Limit {
            since: now - 25 * MIN,
            until: now + 35 * MIN,
            reason: "record rate 0.42".into(),
            need_bps: 0.0,
        },
    )
    .unwrap();
    let home = Status {
        disk_usage: 0.31,
        disk_free: 1300 << 30,
        trial_support: true,
        health_reports: true,
        ..Default::default()
    };
    db::touch_agent(&app.db(), "home", now - 5_000, &home).unwrap();
    db::set_yield(&app.db(), "home", Some(now + 2 * HOUR)).unwrap();
    // spare：三天没来过。
    db::touch_agent(&app.db(), "spare", now - 3 * 24 * HOUR, &Status::default()).unwrap();
}

/// 标准 base64，演示页把封面内联成 data URL。
fn base64(bytes: &[u8]) -> String {
    const T: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for c in bytes.chunks(3) {
        let n = (c[0] as u32) << 16
            | (*c.get(1).unwrap_or(&0) as u32) << 8
            | *c.get(2).unwrap_or(&0) as u32;
        for i in 0..4 {
            out.push(if i <= c.len() {
                T[(n >> (18 - 6 * i) & 63) as usize] as char
            } else {
                '='
            });
        }
    }
    out
}

/// 替换一处，找不到就失败：页面改了要同步改这里。
fn replace_once(page: &mut String, from: &str, to: &str) {
    assert!(
        page.contains(from),
        "演示页找不到 {from:?}，控制台页面改过了，同步改 demo.rs"
    );
    *page = page.replacen(from, to, 1);
}

const SHIM: &str = r#"<script>
(() => {
  const D = __DEMO__;
  const reply = (v, status = 200) => Promise.resolve(new Response(JSON.stringify(v), { status, headers: { 'content-type': 'application/json' } }));
  window.__demoCover = name => D.covers[name] || '';
  window.fetch = (path, opt = {}) => {
    const method = (opt.method || 'GET').toUpperCase();
    const p = decodeURIComponent(String(path).split('?')[0]);
    if (method === 'POST' && (p === '/api/login' || p === '/api/logout')) return reply({});
    if (method === 'GET' && p in D.get) return reply(D.get[p]);
    return reply({ error: '演示模式：只能浏览，改动不会保存' }, 400);
  };
})();
</script>
<style>.demo-badge{position:fixed;right:12px;bottom:12px;z-index:9999;padding:4px 10px;border-radius:999px;font:12px/1.6 system-ui,sans-serif;background:#171717;color:#fff;opacity:.85;pointer-events:none}</style>
"#;

#[tokio::test]
async fn console_demo() {
    let mut config: Config = crate::tests::config();
    let params = argon2::Params::new(8, 1, 1, None).unwrap();
    let hasher = argon2::Argon2::new(argon2::Algorithm::Argon2id, argon2::Version::V0x13, params);
    config.admin_password_hash = Some(
        hasher
            .hash_password(b"demo", &SaltString::generate(&mut OsRng))
            .unwrap()
            .to_string(),
    );
    let (app, _, _): (Arc<App>, _, FakeQr) = test_apps(config);
    let now = crate::now_ms();
    seed(&app, now);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(axum::serve(listener, crate::router(app.clone())).into_future());
    let http = reqwest::Client::new();
    let res = http
        .post(format!("{base}/api/login"))
        .json(&json!({"password": "demo"}))
        .send()
        .await
        .unwrap();
    let cookie = res.headers()["set-cookie"]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();
    let get = async |path: &str| {
        let res = http
            .get(format!("{base}{path}"))
            .header("cookie", &cookie)
            .send()
            .await
            .unwrap();
        assert!(res.status().is_success(), "GET {path}: {}", res.status());
        res.bytes().await.unwrap()
    };

    let mut fixtures = Map::new();
    for path in [
        "/api/rooms",
        "/api/machines",
        "/api/submits",
        "/api/templates",
        "/api/accounts",
    ] {
        fixtures.insert(
            path.into(),
            serde_json::from_slice(&get(path).await).unwrap(),
        );
    }
    for path in ["/api/accounts/1/types", "/api/accounts/1/seasons"] {
        fixtures.insert(
            path.into(),
            serde_json::from_slice(&get(path).await).unwrap(),
        );
    }
    for name in ["cloud-a", "cloud-b", "home", "spare"] {
        fixtures.insert(
            format!("/api/machines/{name}/token"),
            json!({"token": format!("demo-{name}-token")}),
        );
    }
    let mut covers = Map::new();
    for room in ["阿橘", "小满", "夜航"] {
        let jpeg = get(&format!("/api/rooms/{}/cover", urlencode(room))).await;
        covers.insert(
            room.into(),
            Value::String(format!("data:image/jpeg;base64,{}", base64(&jpeg))),
        );
    }
    let favicon = get("/favicon.svg").await;

    let mut page = String::from_utf8(get("/").await.to_vec()).unwrap();
    replace_once(&mut page, r#"href="/favicon.svg""#, r#"href="favicon.svg""#);
    replace_once(
        &mut page,
        r#"src="/api/rooms/${enc(d.name)}/cover""#,
        r#"src="${__demoCover(d.name)}""#,
    );
    let shim = SHIM.replace(
        "__DEMO__",
        &json!({"get": fixtures, "covers": covers}).to_string(),
    );
    replace_once(&mut page, "<script", &format!("{shim}<script"));
    replace_once(
        &mut page,
        "</body>",
        r#"<div class="demo-badge">演示数据 · 只读</div></body>"#,
    );

    if let Ok(out) = std::env::var("LUBIC_DEMO_OUT") {
        let out = std::path::Path::new(&out);
        std::fs::create_dir_all(out).unwrap();
        std::fs::write(out.join("index.html"), page).unwrap();
        std::fs::write(out.join("favicon.svg"), &favicon).unwrap();
    }
}

fn urlencode(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}
