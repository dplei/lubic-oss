use super::{Danmaku, Sink, idle_too_long};
use crate::downloader::live::douyin::{
    DOUYIN_USER_AGENT, common_params, fetch_ttwid, generate_ms_token,
};
use flate2::read::GzDecoder;
use rand::Rng;
use reqwest::Client;
use reqwest::header::{COOKIE, REFERER, USER_AGENT};
use std::collections::{HashSet, VecDeque};
use std::io::Read;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tracing::{debug, info, warn};

const IM_FETCH_URL: &str = "https://live.douyin.com/webcast/im/fetch/";
const DOUYIN_REFERER: &str = "https://live.douyin.com/";
const DEFAULT_INTERVAL: Duration = Duration::from_millis(1000);
const MIN_INTERVAL_MS: u64 = 500;
const MAX_INTERVAL_MS: u64 = 5000;
const MAX_SEEN: usize = 4096;

/// 一直轮询抖音弹幕直到被 abort 或长时间没有分P在写。
pub(super) async fn poll_loop(room: String, room_id: String, sink: Arc<Mutex<Option<Sink>>>) {
    let client = Client::new();
    let user_unique_id = random_19_digits();
    let mut ttwid = fetch_ttwid(&client).await;
    let mut poller = Poller::new();
    let mut idle_since: Option<Instant> = None;
    let mut failures = 0u32;
    let mut connected_logged = false;

    loop {
        if idle_too_long(&sink, &mut idle_since) {
            info!(room, "danmaku idle, disconnected");
            return;
        }

        match poll_once(&client, &room_id, &user_unique_id, &ttwid, &mut poller).await {
            Ok(chats) => {
                failures = 0;
                if !connected_logged {
                    connected_logged = true;
                    info!(room, room_id, "danmaku connected");
                }
                if !chats.is_empty()
                    && let Some(s) = sink.lock().unwrap().as_mut()
                {
                    for d in &chats {
                        s.write(d);
                    }
                }
                tokio::time::sleep(poller.interval).await;
            }
            Err(error) => {
                failures += 1;
                warn!(room, failures, error, "douyin danmaku poll failed");
                tokio::time::sleep(Duration::from_secs(2u64.pow(failures.min(5)))).await;
                if failures >= 3 {
                    poller.reset();
                    ttwid = fetch_ttwid(&client).await;
                }
            }
        }
    }
}

async fn poll_once(
    client: &Client,
    room_id: &str,
    user_unique_id: &str,
    ttwid: &str,
    poller: &mut Poller,
) -> Result<Vec<Danmaku>, String> {
    let params = fetch_params(
        room_id,
        user_unique_id,
        &poller.cursor,
        &poller.internal_ext,
    );
    let resp = client
        .get(IM_FETCH_URL)
        .header(USER_AGENT, DOUYIN_USER_AGENT)
        .header(REFERER, DOUYIN_REFERER)
        .header(COOKIE, format!("ttwid={ttwid}"))
        .query(&params)
        .timeout(Duration::from_secs(10))
        .send()
        .await
        .map_err(|e| e.without_url().to_string())?;
    let status = resp.status();
    if !status.is_success() {
        return Err(format!("http {status}"));
    }
    let body = resp
        .bytes()
        .await
        .map_err(|e| e.without_url().to_string())?;
    poller.absorb(&body)
}

fn fetch_params(
    room_id: &str,
    user_unique_id: &str,
    cursor: &str,
    internal_ext: &str,
) -> Vec<(String, String)> {
    let mut params = common_params();
    params.extend([
        ("resp_content_type".to_string(), "protobuf".to_string()),
        ("did_rule".to_string(), "3".to_string()),
        ("device_id".to_string(), String::new()),
        ("endpoint".to_string(), "live_pc".to_string()),
        ("support_wrds".to_string(), "1".to_string()),
        ("user_unique_id".to_string(), user_unique_id.to_string()),
        ("identity".to_string(), "audience".to_string()),
        ("need_persist_msg_count".to_string(), "15".to_string()),
        ("insert_task_id".to_string(), String::new()),
        ("live_reason".to_string(), String::new()),
        ("room_id".to_string(), room_id.to_string()),
        ("version_code".to_string(), "180800".to_string()),
        ("last_rtt".to_string(), "0".to_string()),
        ("fetch_rule".to_string(), "1".to_string()),
        ("cursor".to_string(), cursor.to_string()),
        ("internal_ext".to_string(), internal_ext.to_string()),
        ("cookie_enabled".to_string(), "true".to_string()),
        ("screen_width".to_string(), "1920".to_string()),
        ("screen_height".to_string(), "1080".to_string()),
        ("browser_online".to_string(), "true".to_string()),
        ("tz_name".to_string(), "Asia/Shanghai".to_string()),
        ("msToken".to_string(), generate_ms_token()),
    ]);
    params
}

fn random_19_digits() -> String {
    let mut rng = rand::thread_rng();
    let mut s = String::with_capacity(19);
    s.push(char::from(b'1' + rng.gen_range(0..9)));
    for _ in 0..18 {
        s.push(char::from(b'0' + rng.gen_range(0..10)));
    }
    s
}

/// 纯同步的轮询状态机：负责 protobuf 解码、首包历史消息跳过与 `msg_id` 去重。
struct Poller {
    cursor: String,
    internal_ext: String,
    interval: Duration,
    seen_set: HashSet<u64>,
    seen_order: VecDeque<u64>,
    started: bool,
}

impl Poller {
    fn new() -> Self {
        Self {
            cursor: String::new(),
            internal_ext: String::new(),
            interval: DEFAULT_INTERVAL,
            seen_set: HashSet::new(),
            seen_order: VecDeque::new(),
            started: false,
        }
    }

    fn reset(&mut self) {
        self.cursor.clear();
        self.internal_ext.clear();
        self.started = false;
    }

    fn absorb(&mut self, body: &[u8]) -> Result<Vec<Danmaku>, String> {
        if body.is_empty() {
            return Err("empty body".to_string());
        }
        let gunzipped;
        let data = if body.starts_with(&[0x1f, 0x8b]) {
            let mut out = Vec::new();
            GzDecoder::new(body)
                .read_to_end(&mut out)
                .map_err(|e| format!("gunzip: {e}"))?;
            if out.is_empty() {
                return Err("empty gunzipped body".to_string());
            }
            gunzipped = out;
            gunzipped.as_slice()
        } else {
            body
        };

        let resp = decode_response(data)?;
        if !resp.cursor.is_empty() {
            self.cursor = resp.cursor;
        }
        if !resp.internal_ext.is_empty() {
            self.internal_ext = resp.internal_ext;
        }
        self.interval = if resp.fetch_interval == 0 {
            DEFAULT_INTERVAL
        } else {
            Duration::from_millis(resp.fetch_interval.clamp(MIN_INTERVAL_MS, MAX_INTERVAL_MS))
        };

        let is_history = !self.started;
        self.started = true;
        let mut out = Vec::new();
        for (msg_id, chat) in resp.chats {
            let is_new = self.record_seen(msg_id);
            if !is_history && is_new {
                out.push(chat);
            }
        }
        Ok(out)
    }

    fn record_seen(&mut self, id: u64) -> bool {
        if !self.seen_set.insert(id) {
            return false;
        }
        if self.seen_order.len() >= MAX_SEEN
            && let Some(oldest) = self.seen_order.pop_front()
        {
            self.seen_set.remove(&oldest);
        }
        self.seen_order.push_back(id);
        true
    }
}

struct DecodedResponse {
    chats: Vec<(u64, Danmaku)>,
    cursor: String,
    fetch_interval: u64,
    internal_ext: String,
}

enum Wire<'a> {
    Varint(u64),
    Bytes(&'a [u8]),
    Other,
}

impl<'a> Wire<'a> {
    fn varint(self) -> Result<u64, String> {
        match self {
            Self::Varint(v) => Ok(v),
            _ => Err("expected varint".to_string()),
        }
    }

    fn bytes(self) -> Result<&'a [u8], String> {
        match self {
            Self::Bytes(b) => Ok(b),
            _ => Err("expected length-delimited".to_string()),
        }
    }

    fn str(self) -> Result<&'a str, String> {
        std::str::from_utf8(self.bytes()?).map_err(|e| e.to_string())
    }
}

fn read_varint(buf: &[u8], pos: &mut usize) -> Result<u64, String> {
    let mut result = 0u64;
    let mut shift = 0u32;
    while *pos < buf.len() {
        let byte = buf[*pos];
        *pos += 1;
        result |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            return Ok(result);
        }
        shift += 7;
        if shift >= 64 {
            return Err("varint overflow".to_string());
        }
    }
    Err("unexpected EOF in varint".to_string())
}

fn parse_wire_fields(buf: &[u8]) -> Result<Vec<(u32, Wire<'_>)>, String> {
    let mut pos = 0;
    let mut fields = Vec::new();
    while pos < buf.len() {
        let tag = read_varint(buf, &mut pos)?;
        let field_number = (tag >> 3) as u32;
        let wire_type = (tag & 0x07) as u8;
        if field_number == 0 {
            return Err("field number 0".to_string());
        }
        let val = match wire_type {
            0 => Wire::Varint(read_varint(buf, &mut pos)?),
            1 => {
                if pos + 8 > buf.len() {
                    return Err("fixed64 out of bounds".to_string());
                }
                pos += 8;
                Wire::Other
            }
            2 => {
                let len = read_varint(buf, &mut pos)? as usize;
                if pos + len > buf.len() {
                    return Err("length-delimited out of bounds".to_string());
                }
                let slice = &buf[pos..pos + len];
                pos += len;
                Wire::Bytes(slice)
            }
            5 => {
                if pos + 4 > buf.len() {
                    return Err("fixed32 out of bounds".to_string());
                }
                pos += 4;
                Wire::Other
            }
            other => return Err(format!("unsupported wire type {other}")),
        };
        fields.push((field_number, val));
    }
    Ok(fields)
}

fn decode_response(buf: &[u8]) -> Result<DecodedResponse, String> {
    let fields = parse_wire_fields(buf)?;
    let mut chats = Vec::new();
    let mut cursor = String::new();
    let mut fetch_interval = 0u64;
    let mut internal_ext = String::new();
    let mut known = false;

    for (num, val) in fields {
        match num {
            1 => {
                known = true;
                let msg_bytes = val.bytes()?;
                match decode_message(msg_bytes) {
                    Ok(Some(chat)) => chats.push(chat),
                    Ok(None) => {}
                    Err(error) => debug!(error, "skip undecodable douyin message"),
                }
            }
            2 => {
                known = true;
                cursor = val.str()?.to_string();
            }
            3 => {
                known = true;
                fetch_interval = val.varint()?;
            }
            5 => {
                known = true;
                internal_ext = val.str()?.to_string();
            }
            _ => {}
        }
    }
    if !known {
        return Err("no recognized response fields".to_string());
    }
    Ok(DecodedResponse {
        chats,
        cursor,
        fetch_interval,
        internal_ext,
    })
}

fn decode_message(buf: &[u8]) -> Result<Option<(u64, Danmaku)>, String> {
    let fields = parse_wire_fields(buf)?;
    let mut method = "";
    let mut payload: &[u8] = &[];
    let mut msg_id = 0u64;
    for (num, val) in fields {
        match num {
            1 => method = val.str()?,
            2 => payload = val.bytes()?,
            3 => msg_id = val.varint()?,
            _ => {}
        }
    }
    if method != "WebcastChatMessage" {
        return Ok(None);
    }
    Ok(Some((msg_id, decode_chat_message(payload)?)))
}

fn decode_chat_message(buf: &[u8]) -> Result<Danmaku, String> {
    let fields = parse_wire_fields(buf)?;
    let mut uid = 0u64;
    let mut user = String::new();
    let mut text = String::new();
    let mut event_time = 0u64;

    for (num, val) in fields {
        match num {
            2 => {
                for (unum, uval) in parse_wire_fields(val.bytes()?)? {
                    match unum {
                        1 => uid = uval.varint()?,
                        3 => user = uval.str()?.to_string(),
                        _ => {}
                    }
                }
            }
            3 => text = val.str()?.to_string(),
            15 => event_time = val.varint()?,
            _ => {}
        }
    }
    let ts = if event_time > 0 {
        event_time * 1000
    } else {
        now_ms()
    };
    Ok(Danmaku {
        mode: 1,
        size: 25,
        color: 0xffffff,
        ts,
        uid,
        user,
        text,
    })
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::Compression;
    use flate2::write::GzEncoder;
    use std::io::Write;

    fn put_varint(mut val: u64, out: &mut Vec<u8>) {
        while val >= 0x80 {
            out.push(((val & 0x7f) as u8) | 0x80);
            val >>= 7;
        }
        out.push(val as u8);
    }

    fn varint_field(field: u32, val: u64, out: &mut Vec<u8>) {
        put_varint((field as u64) << 3, out);
        put_varint(val, out);
    }

    fn bytes_field(field: u32, data: &[u8], out: &mut Vec<u8>) {
        put_varint(((field as u64) << 3) | 2, out);
        put_varint(data.len() as u64, out);
        out.extend_from_slice(data);
    }

    fn encode_msg(method: &str, msg_id: u64, payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        bytes_field(1, method.as_bytes(), &mut out);
        bytes_field(2, payload, &mut out);
        varint_field(3, msg_id, &mut out);
        out
    }

    fn encode_chat(
        msg_id: u64,
        uid: u64,
        nickname: &str,
        content: &str,
        event_time: u64,
    ) -> Vec<u8> {
        let mut user = Vec::new();
        varint_field(1, uid, &mut user);
        bytes_field(3, nickname.as_bytes(), &mut user);

        let mut chat = Vec::new();
        bytes_field(2, &user, &mut chat);
        bytes_field(3, content.as_bytes(), &mut chat);
        if event_time > 0 {
            varint_field(15, event_time, &mut chat);
        }
        encode_msg("WebcastChatMessage", msg_id, &chat)
    }

    fn encode_response(
        messages: &[Vec<u8>],
        cursor: &str,
        fetch_interval: Option<u64>,
        internal_ext: &str,
    ) -> Vec<u8> {
        let mut out = Vec::new();
        for m in messages {
            bytes_field(1, m, &mut out);
        }
        if !cursor.is_empty() {
            bytes_field(2, cursor.as_bytes(), &mut out);
        }
        if let Some(interval) = fetch_interval {
            varint_field(3, interval, &mut out);
        }
        if !internal_ext.is_empty() {
            bytes_field(5, internal_ext.as_bytes(), &mut out);
        }
        out
    }

    #[test]
    fn history_is_skipped_and_new_chats_are_returned() {
        let mut poller = Poller::new();
        let body1 = encode_response(
            &[
                encode_chat(10, 1, "u1", "hist1", 1700000001),
                encode_chat(11, 2, "u2", "hist2", 1700000002),
            ],
            "cur-1",
            Some(1200),
            "ext-1",
        );
        assert!(poller.absorb(&body1).unwrap().is_empty());
        assert_eq!(poller.cursor, "cur-1");
        assert_eq!(poller.internal_ext, "ext-1");
        assert_eq!(poller.interval, Duration::from_millis(1200));

        let body2 = encode_response(
            &[
                encode_chat(11, 2, "u2", "hist2", 1700000002),
                encode_chat(12, 100, "Alice", "hello", 1700000010),
                encode_msg("WebcastMemberMessage", 13, b"\x08\x01"),
                encode_chat(14, 200, "Bob", "world", 1700000020),
            ],
            "cur-2",
            Some(1500),
            "ext-2",
        );
        let chats = poller.absorb(&body2).unwrap();
        assert_eq!(
            chats,
            vec![
                Danmaku {
                    mode: 1,
                    size: 25,
                    color: 0xffffff,
                    ts: 1700000010_000,
                    uid: 100,
                    user: "Alice".to_string(),
                    text: "hello".to_string(),
                },
                Danmaku {
                    mode: 1,
                    size: 25,
                    color: 0xffffff,
                    ts: 1700000020_000,
                    uid: 200,
                    user: "Bob".to_string(),
                    text: "world".to_string(),
                },
            ]
        );
        assert_eq!(poller.cursor, "cur-2");
        assert_eq!(poller.internal_ext, "ext-2");
        assert_eq!(poller.interval, Duration::from_millis(1500));
    }

    #[test]
    fn duplicate_msg_ids_are_dropped() {
        let mut poller = Poller::new();
        let init = encode_response(&[], "c0", Some(1000), "");
        assert!(poller.absorb(&init).unwrap().is_empty());

        let b1 = encode_response(
            &[encode_chat(99, 7, "u", "once", 1700000000)],
            "c1",
            Some(1000),
            "",
        );
        let b2 = encode_response(
            &[encode_chat(99, 7, "u", "once", 1700000000)],
            "c2",
            Some(1000),
            "",
        );
        assert_eq!(poller.absorb(&b1).unwrap().len(), 1);
        assert!(poller.absorb(&b2).unwrap().is_empty());
    }

    #[test]
    fn seen_ids_are_bounded() {
        let mut poller = Poller::new();
        let msgs: Vec<_> = (1..=5000)
            .map(|id| encode_chat(id, 1, "u", "t", 1700000000))
            .collect();
        let body = encode_response(&msgs, "c", Some(1000), "");
        poller.absorb(&body).unwrap();
        assert!(poller.seen_set.len() <= MAX_SEEN);
        assert!(poller.seen_order.len() <= MAX_SEEN);
        assert_eq!(poller.seen_set.len(), MAX_SEEN);
    }

    #[test]
    fn interval_is_clamped() {
        let mut poller = Poller::new();
        for (input, expected_ms) in [
            (Some(0), 1000),
            (Some(100), 500),
            (Some(1000), 1000),
            (Some(60000), 5000),
            (None, 1000),
        ] {
            let body = encode_response(&[], "c", input, "");
            poller.absorb(&body).unwrap();
            assert_eq!(poller.interval, Duration::from_millis(expected_ms));
        }
    }

    #[test]
    fn gzip_body_is_accepted() {
        let init = encode_response(&[], "c0", Some(1000), "e0");
        let next = encode_response(
            &[encode_chat(42, 9, "nick", "gzipped", 1700000005)],
            "c1",
            Some(2000),
            "e1",
        );
        let mut gz = GzEncoder::new(Vec::new(), Compression::default());
        gz.write_all(&next).unwrap();
        let compressed = gz.finish().unwrap();

        let mut p_raw = Poller::new();
        p_raw.absorb(&init).unwrap();
        let out_raw = p_raw.absorb(&next).unwrap();

        let mut p_gz = Poller::new();
        p_gz.absorb(&init).unwrap();
        let out_gz = p_gz.absorb(&compressed).unwrap();

        assert_eq!(out_gz, out_raw);
        assert_eq!(p_gz.cursor, "c1");
        assert_eq!(p_gz.internal_ext, "e1");
        assert_eq!(p_gz.interval, Duration::from_millis(2000));
    }

    #[test]
    fn garbage_body_is_an_error() {
        let mut poller = Poller::new();
        let samples: &[&[u8]] = &[
            b"",
            b"\x00",
            b"\x80\x80\x80",
            b"\x0a\x10short",
            b"\x1f\x8b\x08\x00truncated_gzip",
            b"\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x01",
            b"\x0b",
        ];
        for sample in samples {
            assert!(poller.absorb(sample).is_err(), "sample={sample:?}");
        }
    }

    #[test]
    fn bad_message_is_skipped() {
        let mut poller = Poller::new();
        let init = encode_response(&[], "c0", Some(1000), "");
        assert!(poller.absorb(&init).unwrap().is_empty());

        let body = encode_response(
            &[
                encode_chat(1, 10, "u1", "first", 1700000001),
                encode_msg("WebcastChatMessage", 2, b"\x12\x10truncated"),
                encode_chat(3, 20, "u2", "second", 1700000002),
            ],
            "c1",
            Some(1000),
            "",
        );
        let chats = poller.absorb(&body).unwrap();
        assert_eq!(
            chats,
            vec![
                Danmaku {
                    mode: 1,
                    size: 25,
                    color: 0xffffff,
                    ts: 1700000001_000,
                    uid: 10,
                    user: "u1".to_string(),
                    text: "first".to_string(),
                },
                Danmaku {
                    mode: 1,
                    size: 25,
                    color: 0xffffff,
                    ts: 1700000002_000,
                    uid: 20,
                    user: "u2".to_string(),
                    text: "second".to_string(),
                },
            ]
        );
        assert_eq!(poller.cursor, "c1");
    }
}
