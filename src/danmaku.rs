//! B 站 / 抖音直播弹幕：跟着分段事件按分P写 XML（B 站弹幕格式），与视频同名、同目录。
//!
//! [`tee`] 夹在录制与 agent 之间，事件原样转发：B 站场次（键为 `{room_id}-{开播时刻}`）连 TCP 弹幕
//! 服务器（协议同网页端）；抖音场次（键为纯数字 `room_id`）用匿名 `ttwid` HTTP 轮询 `webcast/im/fetch`
//! （不签名）。每条弹幕的时刻 = 收到时刻 − 分P开录时刻；分P收尾写结束标签，空分段连 XML 一起删。

mod douyin;

use crate::downloader::live::bilibili::{BILIBILI_USER_AGENT, load_cookie};
use crate::downloader::live::wbi::WbiSigner;
use flate2::read::ZlibDecoder;
use lubic_events::Event;
use reqwest::header::{COOKIE, HeaderMap, HeaderValue, REFERER, USER_AGENT};
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};
use tokio::task::JoinHandle;
use tracing::{info, warn};

const HEARTBEAT: Duration = Duration::from_secs(30);
/// 这么久收不到任何包（心跳回包 30 秒一个）就当连接死了，重连。
const READ_TIMEOUT: Duration = Duration::from_secs(70);
/// 没有分P在写超过这么久就断开（房间停录或换机后不会再有 `SessionEnded`）。
const IDLE_DISCONNECT: Duration = Duration::from_secs(600);
const FALLBACK_HOST: (&str, u16) = ("broadcastlv.chat.bilibili.com", 2243);

const OP_HEARTBEAT: u32 = 2;
const OP_MESSAGE: u32 = 5;
const OP_AUTH: u32 = 7;
const OP_AUTH_REPLY: u32 = 8;

/// 把录制事件原样转给返回的 channel，顺带录 B 站 / 抖音弹幕。`cookie_file` 同录制用的 cookie 文件，没有也能录。
pub fn tee(
    mut rx: UnboundedReceiver<Event>,
    cookie_file: Option<PathBuf>,
) -> UnboundedReceiver<Event> {
    let (tx, out) = unbounded_channel();
    tokio::spawn(async move {
        let mut rooms: HashMap<String, Room> = HashMap::new();
        while let Some(ev) = rx.recv().await {
            on_event(&mut rooms, &ev, cookie_file.as_deref());
            if tx.send(ev).is_err() {
                break;
            }
        }
        for (_, room) in rooms.drain() {
            room.stop();
        }
    });
    out
}

struct Room {
    session: String,
    sink: Arc<Mutex<Option<Sink>>>,
    conn: JoinHandle<()>,
}

impl Room {
    fn stop(self) {
        self.conn.abort();
        if let Some(s) = self.sink.lock().unwrap().take() {
            s.finish();
        }
    }
}

fn on_event(rooms: &mut HashMap<String, Room>, ev: &Event, cookie_file: Option<&Path>) {
    match ev {
        Event::PartOpened(p) => {
            let Some(plat) = platform(&p.owner.session) else {
                return;
            };
            let stale = rooms
                .get(&p.owner.room)
                .is_some_and(|r| r.session != p.owner.session || r.conn.is_finished());
            if stale {
                rooms.remove(&p.owner.room).unwrap().stop();
            }
            let room = rooms.entry(p.owner.room.clone()).or_insert_with(|| {
                let sink = Arc::new(Mutex::new(None));
                let conn = match plat {
                    Platform::Bilibili(room_id) => {
                        info!(
                            room = p.owner.room,
                            platform = "bilibili",
                            room_id,
                            "danmaku connecting"
                        );
                        tokio::spawn(connect_loop(
                            p.owner.room.clone(),
                            room_id,
                            cookie_file.map(Path::to_path_buf),
                            sink.clone(),
                        ))
                    }
                    Platform::Douyin(room_id) => {
                        info!(
                            room = p.owner.room,
                            platform = "douyin",
                            room_id,
                            "danmaku connecting"
                        );
                        tokio::spawn(douyin::poll_loop(
                            p.owner.room.clone(),
                            room_id,
                            sink.clone(),
                        ))
                    }
                };
                Room {
                    session: p.owner.session.clone(),
                    conn,
                    sink,
                }
            });
            let path = xml_path(&p.path);
            match Sink::create(&path, &p.part_id) {
                Ok(new) => {
                    if let Some(old) = room.sink.lock().unwrap().replace(new) {
                        old.finish();
                    }
                }
                Err(e) => warn!(path = %path.display(), error = %e, "danmaku file create failed"),
            }
        }
        Event::PartClosed(p) | Event::PartDiscarded(p) => {
            let Some(room) = rooms.get(&p.owner.room) else {
                return;
            };
            let mut sink = room.sink.lock().unwrap();
            if sink.as_ref().is_some_and(|s| s.part_id == p.part_id) {
                let s = sink.take().unwrap();
                let path = s.path.clone();
                s.finish();
                if matches!(ev, Event::PartDiscarded(_)) {
                    let _ = std::fs::remove_file(path);
                }
            }
        }
        Event::SessionEnded(owner) => {
            if rooms
                .get(&owner.room)
                .is_some_and(|r| r.session == owner.session)
            {
                rooms.remove(&owner.room).unwrap().stop();
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum Platform {
    Bilibili(u64),
    Douyin(String),
}

/// B 站场次键是 `{room_id}-{开播时刻}`；抖音是纯数字 `room_id`。
fn platform(session: &str) -> Option<Platform> {
    if let Some((room, start)) = session.split_once('-') {
        start.parse::<u64>().ok()?;
        return Some(Platform::Bilibili(room.parse().ok()?));
    }
    if !session.is_empty() && session.bytes().all(|b| b.is_ascii_digit()) {
        return Some(Platform::Douyin(session.to_string()));
    }
    None
}

fn idle_too_long(sink: &Mutex<Option<Sink>>, idle_since: &mut Option<Instant>) -> bool {
    if sink.lock().unwrap().is_some() {
        *idle_since = None;
        false
    } else {
        idle_since.get_or_insert_with(Instant::now).elapsed() >= IDLE_DISCONNECT
    }
}

/// `x.flv.part` / `x.flv` → `x.xml`。
fn xml_path(video: &Path) -> PathBuf {
    let s = video.to_string_lossy();
    PathBuf::from(s.strip_suffix(".part").unwrap_or(&s)).with_extension("xml")
}

/// 正在写的分P弹幕文件。逐条写、逐条 flush，崩溃也只缺结束标签。
struct Sink {
    part_id: String,
    path: PathBuf,
    start: Instant,
    out: BufWriter<File>,
    count: u64,
}

impl Sink {
    fn create(path: &Path, part_id: &str) -> std::io::Result<Self> {
        let mut out = BufWriter::new(File::create(path)?);
        out.write_all(
            b"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<i>\n<chatserver>chat.bilibili.com</chatserver>\n<chatid>0</chatid>\n",
        )?;
        out.flush()?;
        Ok(Self {
            part_id: part_id.to_string(),
            path: path.to_path_buf(),
            start: Instant::now(),
            out,
            count: 0,
        })
    }

    fn write(&mut self, d: &Danmaku) {
        let line = d.xml(self.start.elapsed());
        if let Err(e) = self
            .out
            .write_all(line.as_bytes())
            .and_then(|_| self.out.flush())
        {
            warn!(path = %self.path.display(), error = %e, "danmaku write failed");
        }
        self.count += 1;
    }

    fn finish(mut self) {
        let _ = self.out.write_all(b"</i>\n").and_then(|_| self.out.flush());
        info!(path = %self.path.display(), count = self.count, "danmaku file closed");
    }
}

#[derive(Debug, PartialEq)]
struct Danmaku {
    mode: u64,
    size: u64,
    color: u64,
    /// 发送时刻，unix 毫秒。
    ts: u64,
    uid: u64,
    user: String,
    text: String,
}

impl Danmaku {
    /// `DANMU_MSG` 的 `info`：`[[_, mode, size, color, ts, ...], text, [uid, uname, ...], ...]`。
    fn parse(msg: &Value) -> Option<Self> {
        if !msg["cmd"].as_str()?.starts_with("DANMU_MSG") {
            return None;
        }
        let info = &msg["info"];
        let meta = &info[0];
        Some(Self {
            mode: meta[1].as_u64().unwrap_or(1),
            size: meta[2].as_u64().unwrap_or(25),
            color: meta[3].as_u64().unwrap_or(0xffffff),
            ts: meta[4].as_u64().unwrap_or(0),
            uid: info[2][0].as_u64().unwrap_or(0),
            user: info[2][1].as_str().unwrap_or("").to_string(),
            text: info[1].as_str()?.to_string(),
        })
    }

    /// `p` = 出现秒数,模式,字号,颜色,发送时刻(毫秒),弹幕池,uid,0，同 B 站 / 录播姬。
    fn xml(&self, at: Duration) -> String {
        format!(
            "<d p=\"{:.3},{},{},{},{},0,{},0\" user=\"{}\">{}</d>\n",
            at.as_secs_f64(),
            self.mode,
            self.size,
            self.color,
            self.ts,
            self.uid,
            escape(&self.user),
            escape(&self.text)
        )
    }
}

fn escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            // XML 1.0 不允许的控制字符直接丢掉，免得整个文件解析失败。
            c if (c as u32) < 0x20 && !matches!(c, '\t' | '\n' | '\r') => {}
            c => out.push(c),
        }
    }
    out
}

/// 16 字节头：总长 u32、头长 u16、协议版本 u16、操作码 u32、序号 u32，大端。
fn packet(op: u32, body: &[u8]) -> Vec<u8> {
    let mut p = Vec::with_capacity(16 + body.len());
    p.extend_from_slice(&(16 + body.len() as u32).to_be_bytes());
    p.extend_from_slice(&16u16.to_be_bytes());
    p.extend_from_slice(&1u16.to_be_bytes());
    p.extend_from_slice(&op.to_be_bytes());
    p.extend_from_slice(&1u32.to_be_bytes());
    p.extend_from_slice(body);
    p
}

/// 拆一个包体里的消息：版本 2 是 zlib 压缩的多个包，版本 0 是一条 JSON。
fn messages(ver: u16, op: u32, body: &[u8], out: &mut Vec<Value>) {
    if op != OP_MESSAGE {
        return;
    }
    match ver {
        2 => {
            let mut raw = Vec::new();
            if ZlibDecoder::new(body).read_to_end(&mut raw).is_err() {
                return;
            }
            let mut rest = raw.as_slice();
            while rest.len() >= 16 {
                let len = u32::from_be_bytes(rest[0..4].try_into().unwrap()) as usize;
                let head = u16::from_be_bytes(rest[4..6].try_into().unwrap()) as usize;
                if len < head || len > rest.len() {
                    break;
                }
                let ver = u16::from_be_bytes(rest[6..8].try_into().unwrap());
                let op = u32::from_be_bytes(rest[8..12].try_into().unwrap());
                messages(ver, op, &rest[head..len], out);
                rest = &rest[len..];
            }
        }
        0 => {
            if let Ok(v) = serde_json::from_slice(body) {
                out.push(v);
            }
        }
        _ => {}
    }
}

/// 一直连着直到被 abort 或长时间没分P在写；断了就退避重连。
async fn connect_loop(
    room: String,
    room_id: u64,
    cookie_file: Option<PathBuf>,
    sink: Arc<Mutex<Option<Sink>>>,
) {
    let client = reqwest::Client::new();
    let wbi = WbiSigner::new(client.clone());
    let mut idle_since: Option<Instant> = None;
    let mut failures = 0u32;
    loop {
        let cookie = load_cookie(None, cookie_file.as_deref());
        let result = connect_once(&client, &wbi, room_id, cookie, &sink, &mut idle_since).await;
        if idle_since.is_some_and(|t| t.elapsed() >= IDLE_DISCONNECT) {
            info!(room, "danmaku idle, disconnected");
            return;
        }
        match result {
            Ok(()) => failures = 0,
            Err(e) => {
                failures += 1;
                warn!(room, failures, error = e, "danmaku connection lost");
            }
        }
        tokio::time::sleep(Duration::from_secs(2u64.pow(failures.min(5)))).await;
    }
}

async fn connect_once(
    client: &reqwest::Client,
    wbi: &WbiSigner,
    room_id: u64,
    cookie: Option<String>,
    sink: &Mutex<Option<Sink>>,
    idle_since: &mut Option<Instant>,
) -> Result<(), String> {
    let (cookie, uid, buvid) = identity(client, cookie).await;
    let (host, port, token) = match danmu_info(client, wbi, room_id, &cookie).await {
        Ok(v) => v,
        Err(e) => {
            warn!(
                room_id,
                error = e,
                "getDanmuInfo failed, connecting without token"
            );
            (FALLBACK_HOST.0.to_string(), FALLBACK_HOST.1, String::new())
        }
    };
    let mut stream = tokio::time::timeout(
        Duration::from_secs(10),
        TcpStream::connect((host.as_str(), port)),
    )
    .await
    .map_err(|_| "connect timeout".to_string())?
    .map_err(|e| e.to_string())?;
    let auth = json!({
        "uid": uid, "roomid": room_id, "protover": 2, "buvid": buvid,
        "platform": "web", "type": 2, "key": token,
    });
    stream
        .write_all(&packet(OP_AUTH, auth.to_string().as_bytes()))
        .await
        .map_err(|e| e.to_string())?;

    let mut heartbeat = tokio::time::interval(HEARTBEAT);
    let mut idle_check = tokio::time::interval(Duration::from_secs(30));
    let mut msgs = Vec::new();
    loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                stream.write_all(&packet(OP_HEARTBEAT, b"")).await.map_err(|e| e.to_string())?;
            }
            _ = idle_check.tick() => {
                if idle_too_long(sink, idle_since) {
                    return Ok(());
                }
            }
            head = tokio::time::timeout(READ_TIMEOUT, read_head(&mut stream)) => {
                let (len, head_len, ver, op) = head.map_err(|_| "read timeout".to_string())??;
                let mut body = vec![0; len.saturating_sub(head_len)];
                stream.read_exact(&mut body).await.map_err(|e| e.to_string())?;
                if op == OP_AUTH_REPLY {
                    let code = serde_json::from_slice::<Value>(&body).ok().and_then(|v| v["code"].as_i64());
                    if code != Some(0) {
                        return Err(format!("auth rejected: {}", String::from_utf8_lossy(&body)));
                    }
                    info!(room_id, uid, "danmaku connected");
                    continue;
                }
                msgs.clear();
                messages(ver, op, &body, &mut msgs);
                let mut sink = sink.lock().unwrap();
                if let Some(s) = sink.as_mut() {
                    for d in msgs.iter().filter_map(Danmaku::parse) {
                        s.write(&d);
                    }
                }
            }
        }
    }
}

/// 读包头，返回（总长、头长、版本、操作码）。
async fn read_head(stream: &mut TcpStream) -> Result<(usize, usize, u16, u32), String> {
    let mut h = [0u8; 16];
    stream.read_exact(&mut h).await.map_err(|e| e.to_string())?;
    let len = u32::from_be_bytes(h[0..4].try_into().unwrap()) as usize;
    let head = u16::from_be_bytes(h[4..6].try_into().unwrap()) as usize;
    if head < 16 || len < head || len > 16 << 20 {
        return Err(format!("bad packet header len={len} head={head}"));
    }
    // 头长超过 16 时多出的部分跳过。
    let mut extra = vec![0; head - 16];
    stream
        .read_exact(&mut extra)
        .await
        .map_err(|e| e.to_string())?;
    let ver = u16::from_be_bytes(h[6..8].try_into().unwrap());
    let op = u32::from_be_bytes(h[8..12].try_into().unwrap());
    Ok((len, head, ver, op))
}

/// 补齐 `buvid3`（没有它 getDanmuInfo 会被风控），返回（cookie、uid、buvid3）。没登录时 uid 为 0。
async fn identity(client: &reqwest::Client, cookie: Option<String>) -> (String, u64, String) {
    let mut cookie = cookie.unwrap_or_default();
    let field = |c: &str, name: &str| {
        c.split(';')
            .filter_map(|kv| kv.trim().split_once('='))
            .find(|(k, _)| *k == name)
            .map(|(_, v)| v.to_string())
    };
    let uid = field(&cookie, "DedeUserID")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let buvid = match field(&cookie, "buvid3") {
        Some(b) => b,
        None => {
            let b = fetch_buvid(client).await.unwrap_or_default();
            if !b.is_empty() {
                cookie = if cookie.is_empty() {
                    format!("buvid3={b}")
                } else {
                    format!("{cookie};buvid3={b}")
                };
            }
            b
        }
    };
    (cookie, uid, buvid)
}

async fn fetch_buvid(client: &reqwest::Client) -> Option<String> {
    let v: Value = client
        .get("https://api.bilibili.com/x/frontend/finger/spi")
        .header(USER_AGENT, BILIBILI_USER_AGENT)
        .timeout(Duration::from_secs(10))
        .send()
        .await
        .ok()?
        .json()
        .await
        .ok()?;
    v["data"]["b_3"].as_str().map(str::to_string)
}

/// 返回（弹幕服务器、TCP 端口、token）。
async fn danmu_info(
    client: &reqwest::Client,
    wbi: &WbiSigner,
    room_id: u64,
    cookie: &str,
) -> Result<(String, u16, String), String> {
    let mut headers = HeaderMap::new();
    headers.insert(USER_AGENT, HeaderValue::from_static(BILIBILI_USER_AGENT));
    headers.insert(
        REFERER,
        HeaderValue::from_static("https://live.bilibili.com/"),
    );
    if let Ok(c) = HeaderValue::from_str(cookie) {
        headers.insert(COOKIE, c);
    }
    let mut params = BTreeMap::from([
        ("id".to_string(), room_id.to_string()),
        ("type".to_string(), "0".to_string()),
        ("web_location".to_string(), "444.8".to_string()),
    ]);
    wbi.sign(&mut params, &headers)
        .await
        .map_err(|e| e.to_string())?;
    let v: Value = client
        .get("https://api.live.bilibili.com/xlive/web-room/v1/index/getDanmuInfo")
        .headers(headers)
        .query(&params)
        .timeout(Duration::from_secs(10))
        .send()
        .await
        .map_err(|e| e.to_string())?
        .json()
        .await
        .map_err(|e| e.to_string())?;
    if v["code"].as_i64() != Some(0) {
        return Err(format!("code {} {}", v["code"], v["message"]));
    }
    let token = v["data"]["token"].as_str().unwrap_or_default().to_string();
    let host = &v["data"]["host_list"][0];
    let name = host["host"].as_str().ok_or("empty host_list")?.to_string();
    let port = host["port"].as_u64().unwrap_or(2243) as u16;
    Ok((name, port, token))
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::Compression;
    use flate2::write::ZlibEncoder;
    use lubic_events::{CloseReason, Owner, PartClosed, PartOpened};

    fn danmu(text: &str) -> Value {
        json!({"cmd": "DANMU_MSG:4:0:2:2:2:0", "info": [
            [0, 1, 25, 16777215, 1730000000123u64, 0], text, [42, "观众<A>"], []
        ]})
    }

    fn raw(ver: u16, op: u32, body: &[u8]) -> Vec<u8> {
        let mut p = packet(op, body);
        p[6..8].copy_from_slice(&ver.to_be_bytes());
        p
    }

    #[test]
    fn zlib_body_holds_several_packets() {
        let mut inner = raw(0, OP_MESSAGE, danmu("a").to_string().as_bytes());
        inner.extend(raw(
            0,
            OP_MESSAGE,
            json!({"cmd": "SEND_GIFT"}).to_string().as_bytes(),
        ));
        inner.extend(raw(0, OP_MESSAGE, danmu("b").to_string().as_bytes()));
        let mut z = ZlibEncoder::new(Vec::new(), Compression::default());
        z.write_all(&inner).unwrap();
        let mut out = Vec::new();
        messages(2, OP_MESSAGE, &z.finish().unwrap(), &mut out);
        let texts: Vec<_> = out
            .iter()
            .filter_map(Danmaku::parse)
            .map(|d| d.text)
            .collect();
        assert_eq!(texts, ["a", "b"]);
        assert_eq!(out.len(), 3);
    }

    #[test]
    fn danmaku_line_is_escaped_bilibili_xml() {
        let d = Danmaku::parse(&danmu("1<2 & \"ok\"\u{1}")).unwrap();
        assert_eq!(
            d.xml(Duration::from_millis(61_500)),
            "<d p=\"61.500,1,25,16777215,1730000000123,0,42,0\" user=\"观众&lt;A&gt;\">1&lt;2 &amp; &quot;ok&quot;</d>\n"
        );
    }

    #[test]
    fn platform_detection() {
        assert_eq!(
            platform("22603245-1730000000"),
            Some(Platform::Bilibili(22603245))
        );
        assert_eq!(
            platform("7703020291286502153"),
            Some(Platform::Douyin("7703020291286502153".into()))
        );
        assert_eq!(platform(""), None);
        assert_eq!(platform("abc"), None);
        assert_eq!(platform("12-x"), None);
        assert_eq!(
            xml_path(Path::new("/r/a/x_01.flv.part")),
            Path::new("/r/a/x_01.xml")
        );
        assert_eq!(
            xml_path(Path::new("/r/a/x_01.ts")),
            Path::new("/r/a/x_01.xml")
        );
    }

    fn owner(session: &str) -> Owner {
        Owner {
            room: "r".into(),
            session: session.into(),
        }
    }

    fn opened(dir: &Path, session: &str, id: &str) -> Event {
        Event::PartOpened(PartOpened {
            owner: owner(session),
            part_id: id.into(),
            path: dir.join(format!("{id}.flv.part")),
            title: String::new(),
            tz_offset_secs: None,
        })
    }

    fn closed(dir: &Path, session: &str, id: &str) -> PartClosed {
        PartClosed {
            owner: owner(session),
            part_id: id.into(),
            path: dir.join(format!("{id}.flv")),
            duration: Duration::ZERO,
            size: 0,
            reason: CloseReason::TimedSplit,
            title: String::new(),
            closed_at_ms: None,
        }
    }

    #[tokio::test]
    async fn parts_get_their_own_xml_and_discarded_parts_lose_it() {
        let dir = tempfile::tempdir().unwrap();
        let mut rooms = HashMap::new();
        on_event(&mut rooms, &opened(dir.path(), "1-2", "p1"), None);
        let d = Danmaku::parse(&danmu("hi")).unwrap();
        rooms["r"].sink.lock().unwrap().as_mut().unwrap().write(&d);
        on_event(
            &mut rooms,
            &Event::PartClosed(closed(dir.path(), "1-2", "p1")),
            None,
        );
        on_event(&mut rooms, &opened(dir.path(), "1-2", "p2"), None);
        on_event(
            &mut rooms,
            &Event::PartDiscarded(closed(dir.path(), "1-2", "p2")),
            None,
        );
        on_event(&mut rooms, &Event::SessionEnded(owner("1-2")), None);
        assert!(rooms.is_empty());

        let xml = std::fs::read_to_string(dir.path().join("p1.xml")).unwrap();
        assert!(xml.contains(">hi</d>\n</i>\n"), "{xml}");
        assert!(!dir.path().join("p2.xml").exists());
    }

    #[tokio::test]
    async fn tee_forwards_every_event_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let s = "x";
        let events = [
            opened(dir.path(), s, "p1"),
            Event::PartClosed(closed(dir.path(), s, "p1")),
            Event::PartDiscarded(closed(dir.path(), s, "p2")),
            Event::SessionEnded(owner(s)),
        ];
        let (tx, rx) = unbounded_channel();
        let mut out = tee(rx, None);
        for ev in &events {
            tx.send(ev.clone()).unwrap();
        }
        for expected in &events {
            assert_eq!(&out.recv().await.unwrap(), expected);
        }
        let has_xml = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(Result::ok)
            .any(|e| e.path().extension().is_some_and(|ext| ext == "xml"));
        assert!(!has_xml);
    }

    #[tokio::test]
    async fn new_session_finishes_previous_xml() {
        let dir = tempfile::tempdir().unwrap();
        let mut rooms = HashMap::new();
        on_event(&mut rooms, &opened(dir.path(), "1-2", "p1"), None);
        on_event(&mut rooms, &opened(dir.path(), "1-3", "p2"), None);

        let xml = std::fs::read_to_string(dir.path().join("p1.xml")).unwrap();
        assert!(xml.ends_with("</i>\n"), "{xml}");
        assert_eq!(rooms["r"].session, "1-3");
        assert_eq!(
            rooms["r"].sink.lock().unwrap().as_ref().unwrap().part_id,
            "p2"
        );

        on_event(&mut rooms, &Event::SessionEnded(owner("1-2")), None);
        assert!(rooms.contains_key("r"));
        on_event(&mut rooms, &Event::SessionEnded(owner("1-3")), None);
        assert!(rooms.is_empty());
    }

    #[tokio::test]
    async fn close_of_another_part_keeps_current_sink() {
        let dir = tempfile::tempdir().unwrap();
        let mut rooms = HashMap::new();
        on_event(&mut rooms, &opened(dir.path(), "1-2", "p1"), None);
        on_event(
            &mut rooms,
            &Event::PartClosed(closed(dir.path(), "1-2", "p0")),
            None,
        );
        assert_eq!(
            rooms["r"].sink.lock().unwrap().as_ref().unwrap().part_id,
            "p1"
        );
        let p1_xml = dir.path().join("p1.xml");
        assert!(!std::fs::read_to_string(&p1_xml).unwrap().contains("</i>"));

        on_event(
            &mut rooms,
            &Event::PartDiscarded(closed(dir.path(), "1-2", "p0")),
            None,
        );
        assert!(p1_xml.exists());
        assert!(!std::fs::read_to_string(&p1_xml).unwrap().contains("</i>"));

        on_event(&mut rooms, &Event::SessionEnded(owner("1-2")), None);
    }

    #[tokio::test]
    async fn reopen_without_close_finishes_previous_xml() {
        let dir = tempfile::tempdir().unwrap();
        let mut rooms = HashMap::new();
        on_event(&mut rooms, &opened(dir.path(), "1-2", "p1"), None);
        on_event(&mut rooms, &opened(dir.path(), "1-2", "p2"), None);

        let xml = std::fs::read_to_string(dir.path().join("p1.xml")).unwrap();
        assert!(xml.ends_with("</i>\n"), "{xml}");
        assert_eq!(
            rooms["r"].sink.lock().unwrap().as_ref().unwrap().part_id,
            "p2"
        );
        assert_eq!(rooms.len(), 1);

        on_event(&mut rooms, &Event::SessionEnded(owner("1-2")), None);
    }

    #[tokio::test]
    #[ignore]
    async fn douyin_poll_live() {
        let Ok(room_id) = std::env::var("DOUYIN_ROOM_ID") else {
            return;
        };
        if room_id.is_empty() {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let mut rooms = HashMap::new();
        on_event(&mut rooms, &opened(dir.path(), &room_id, "p1"), None);
        tokio::time::sleep(Duration::from_secs(40)).await;
        on_event(
            &mut rooms,
            &Event::PartClosed(closed(dir.path(), &room_id, "p1")),
            None,
        );
        on_event(&mut rooms, &Event::SessionEnded(owner(&room_id)), None);

        let xml_file = dir.path().join("p1.xml");
        assert!(xml_file.exists());
        let xml = std::fs::read_to_string(&xml_file).unwrap();
        assert!(xml.ends_with("</i>\n"), "{xml}");
        assert!(xml.lines().any(|line| line.starts_with("<d ")), "{xml}");
    }
}
