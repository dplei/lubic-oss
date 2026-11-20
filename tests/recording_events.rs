//! Task 12: the recording file layer assigns a stable identity and reports it natively.
//!
//! These tests assert on the `lubic::event` target only. The old lines keep their own target,
//! so a change here can never be mistaken for a change in the old output.

use lubic::downloader::util::{LifecycleFile, SegmentCloseReason, close_reason_code};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use tracing::field::{Field, Visit};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::{Context, SubscriberExt};

/// One captured event: the target it was routed to, and its fields as text.
type Record = (String, BTreeMap<String, String>);

#[derive(Clone, Default)]
struct Captured(Arc<Mutex<Vec<Record>>>);

struct Collector<'a>(&'a mut BTreeMap<String, String>);
impl Visit for Collector<'_> {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().to_string(), value.to_string());
    }
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.0.insert(field.name().to_string(), value.to_string());
    }
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0
            .insert(field.name().to_string(), format!("{value:?}"));
    }
}

impl<S: tracing::Subscriber> Layer<S> for Captured {
    fn on_event(&self, event: &tracing::Event<'_>, _: Context<'_, S>) {
        let mut fields = BTreeMap::new();
        event.record(&mut Collector(&mut fields));
        self.0
            .lock()
            .unwrap()
            .push((event.metadata().target().to_string(), fields));
    }
}

impl Captured {
    /// Native events only: anything on another target belongs to the old sinks.
    fn native(&self, name: &str) -> Vec<BTreeMap<String, String>> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|(target, fields)| {
                target == "lubic::event"
                    && fields.get("event_name").map(String::as_str) == Some(name)
            })
            .map(|(_, fields)| fields.clone())
            .collect()
    }

    fn targets(&self) -> Vec<String> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .map(|(target, _)| target.clone())
            .collect()
    }
}

#[test]
fn each_created_file_gets_its_own_identity_and_reports_it_on_close() {
    let directory = tempfile::tempdir().unwrap();
    let template = directory
        .path()
        .join("segment-%Y%m%d")
        .display()
        .to_string();
    let captured = Captured::default();
    let closed: Arc<Mutex<Vec<(String, String)>>> = Arc::default();
    let hook_closed = closed.clone();

    let subscriber = tracing_subscriber::registry().with(captured.clone());
    tracing::subscriber::with_default(subscriber, || {
        let mut file = LifecycleFile::with_hook(&template, "flv", move |event| {
            hook_closed
                .lock()
                .unwrap()
                .push((event.part_id.clone(), event.path.display().to_string()));
        });

        let path = file.create().unwrap().to_path_buf();
        let first = file.identity().unwrap().clone();
        std::fs::write(&path, b"first-segment").unwrap();
        file.finalize(SegmentCloseReason::TimedSplit).unwrap();

        let path = file.create().unwrap().to_path_buf();
        let second = file.identity().unwrap().clone();
        std::fs::write(&path, b"second").unwrap();
        file.finalize(SegmentCloseReason::StreamEnded).unwrap();

        assert_ne!(first.segment_id, second.segment_id);
        // Same template within the same period must not reuse the path: the second rename
        // would silently overwrite the first segment (dplei/biliup#88).
        assert_ne!(first.original_file, second.original_file);
        assert_eq!(
            std::fs::read(&first.original_file).unwrap(),
            b"first-segment"
        );
        assert_eq!(std::fs::read(&second.original_file).unwrap(), b"second");
    });

    let created = captured.native("recording.segment_created");
    assert_eq!(created.len(), 2);
    assert_ne!(created[0]["segment_id"], created[1]["segment_id"]);
    assert!(created[0]["segment_id"].starts_with("seg-"));

    let closed_events = captured.native("recording.segment_closed");
    assert_eq!(closed_events.len(), 2);
    assert_eq!(closed_events[0]["reason_code"], "split_limit");
    assert_eq!(closed_events[1]["reason_code"], "stream_end");
    assert_eq!(closed_events[0]["size_bytes"], "13");
    // The identity reported at close is the one assigned at creation, not a fresh one.
    assert_eq!(closed_events[0]["segment_id"], created[0]["segment_id"]);

    let hook_calls = closed.lock().unwrap().clone();
    assert_eq!(hook_calls.len(), 2);
    assert_eq!(hook_calls[0].0, created[0]["segment_id"]);
    assert!(hook_calls[0].1.ends_with(".flv"));

    // The old "Save to ..." line still goes to its own target and is untouched.
    assert!(
        captured
            .targets()
            .iter()
            .any(|target| target.starts_with("lubic::downloader")),
        "old output must keep its own target: {:?}",
        captured.targets()
    );
}

#[test]
fn close_reasons_map_onto_the_frozen_v1_vocabulary() {
    assert_eq!(
        close_reason_code(SegmentCloseReason::TimedSplit),
        "split_limit"
    );
    assert_eq!(
        close_reason_code(SegmentCloseReason::SizeSplit),
        "split_limit"
    );
    assert_eq!(
        close_reason_code(SegmentCloseReason::StreamEnded),
        "stream_end"
    );
    assert_eq!(
        close_reason_code(SegmentCloseReason::TransportError),
        "transport_error"
    );
    assert_eq!(
        close_reason_code(SegmentCloseReason::Cancelled),
        "user_cancel"
    );
    assert_eq!(close_reason_code(SegmentCloseReason::Unknown), "unknown");
}

/// Real loopback responses, with independently advancing playlist revisions per path.
struct HlsFixture {
    url: String,
    requests: Arc<Mutex<BTreeMap<String, usize>>>,
    server: tokio::task::JoinHandle<()>,
}

impl Drop for HlsFixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl HlsFixture {
    async fn new(routes: Vec<(&str, Vec<&str>)>) -> Self {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let routes: BTreeMap<String, Vec<String>> = routes
            .into_iter()
            .map(|(path, bodies)| (path.into(), bodies.into_iter().map(String::from).collect()))
            .collect();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/index.m3u8", listener.local_addr().unwrap());
        let requests: Arc<Mutex<BTreeMap<String, usize>>> = Arc::default();
        let seen = requests.clone();
        let server = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                while !request.ends_with(b"\r\n\r\n") {
                    let mut chunk = [0; 1024];
                    let n = socket.read(&mut chunk).await.unwrap();
                    if n == 0 {
                        break;
                    }
                    request.extend_from_slice(&chunk[..n]);
                }
                let text = String::from_utf8_lossy(&request);
                let path = text.split_whitespace().nth(1).unwrap();
                let body = {
                    let mut seen = seen.lock().unwrap();
                    let count = seen.entry(path.into()).or_default();
                    let body = routes
                        .get(path)
                        .map(|bodies| &bodies[(*count).min(bodies.len() - 1)]);
                    *count += 1;
                    body
                };
                let (status, body) = body.map_or((404, "absent"), |body| (200, body.as_str()));
                let response = format!(
                    "HTTP/1.1 {status} Fixture\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = socket.write_all(response.as_bytes()).await;
            }
        });
        Self {
            url,
            requests,
            server,
        }
    }
}

const FIRST: &str = "#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:0\n#EXTINF:0.6,\na.ts\n#EXTINF:0.6,\nb.ts\n";
const LAST: &str = "#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:3\n#EXT-X-DISCONTINUITY\n#EXTINF:0.6,\nc.ts\n#EXT-X-ENDLIST\n";

#[tokio::test]
async fn hls_zero_repeat_gap_and_discontinuity_preserve_file_identity() {
    use lubic::client::StatelessClient;
    use lubic::downloader::{
        hls,
        util::{RecordingOwner, Segmentable},
    };
    let fixture = HlsFixture::new(vec![
        ("/index.m3u8", vec![FIRST, FIRST, LAST]),
        ("/a.ts", vec!["A"]),
        ("/b.ts", vec!["B"]),
        ("/c.ts", vec!["C"]),
    ])
    .await;
    let directory = tempfile::tempdir().unwrap();
    let template = directory.path().join("segment-%s-%f").display().to_string();
    let captured = Captured::default();
    let _guard =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(captured.clone()));
    let closed = Arc::new(Mutex::new(Vec::new()));
    let hook = closed.clone();
    let file = LifecycleFile::with_hook(&template, "ts", move |event| {
        hook.lock()
            .unwrap()
            .push((event.part_id.clone(), std::fs::read(&event.path).unwrap()));
    })
    .with_owner(RecordingOwner {
        room: "controlled-room".into(),
        session: "controlled-session".into(),
    });
    let ready = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let callback_ready = ready.clone();
    tokio::time::timeout(
        std::time::Duration::from_secs(8),
        hls::download_with_ready(
            &fixture.url,
            &StatelessClient::new(Default::default()),
            file,
            Segmentable::new(None, None),
            move || {
                callback_ready.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            },
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(ready.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(
        *fixture.requests.lock().unwrap(),
        BTreeMap::from([
            ("/index.m3u8".into(), 3),
            ("/a.ts".into(), 1),
            ("/b.ts".into(), 1),
            ("/c.ts".into(), 1)
        ])
    );
    let closed = closed.lock().unwrap();
    assert_eq!(closed.len(), 2);
    assert_eq!(closed[0].1, b"AB");
    assert_eq!(closed[1].1, b"C");
    let created = captured.native("recording.segment_created");
    let finished = captured.native("recording.segment_closed");
    assert_eq!(created.len(), 2);
    assert_eq!(finished.len(), 2);
    for (index, event) in created.iter().enumerate() {
        assert_eq!(event["segment_id"], closed[index].0);
        assert_eq!(event["segment_id"], finished[index]["segment_id"]);
        assert_eq!(event["original_file"], finished[index]["original_file"]);
        assert_eq!(event["room"], "controlled-room");
        assert_eq!(event["session"], "controlled-session");
    }
    let gap = captured.native("recording.hls_gap");
    assert_eq!(gap.len(), 1);
    assert_eq!(gap[0]["segment_id"], created[0]["segment_id"]);
    assert_eq!(gap[0]["media_sequence"], "3");
    assert_eq!(gap[0]["previous_media_sequence"], "1");
    assert_eq!(gap[0]["missing_segments"], "1");
    assert!(
        !gap[0].contains_key("gap_ms"),
        "sequence loss is not measured time"
    );
    let discontinuity = captured.native("recording.hls_discontinuity");
    assert_eq!(discontinuity.len(), 1);
    assert_eq!(discontinuity[0]["segment_id"], created[1]["segment_id"]);
    assert!(captured.native("recording.disconnected").is_empty());
    assert_eq!(finished[1]["reason_code"], "stream_end");
}

/// 重连后首片就带 DISCONTINUITY 时，曾先切出一个空文件，同一秒的下一段撞名得到 -1 后缀。
#[tokio::test]
async fn hls_discontinuity_on_first_segment_keeps_the_open_file() {
    use lubic::client::StatelessClient;
    use lubic::downloader::{hls, util::Segmentable};
    let playlist = "#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:0\n#EXT-X-DISCONTINUITY\n#EXTINF:0.6,\na.ts\n#EXT-X-ENDLIST\n";
    let fixture =
        HlsFixture::new(vec![("/index.m3u8", vec![playlist]), ("/a.ts", vec!["A"])]).await;
    let directory = tempfile::tempdir().unwrap();
    let template = directory
        .path()
        .join("segment-%Y%m%d%H%M%S")
        .display()
        .to_string();
    let captured = Captured::default();
    let _guard =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(captured.clone()));
    tokio::time::timeout(
        std::time::Duration::from_secs(3),
        hls::download(
            &fixture.url,
            &StatelessClient::new(Default::default()),
            LifecycleFile::new(&template, "ts"),
            Segmentable::new(None, None),
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(captured.native("recording.segment_created").len(), 1);
    let closed = captured.native("recording.segment_closed");
    assert_eq!(closed.len(), 1);
    assert_eq!(closed[0]["size_bytes"], "1");
    assert_eq!(captured.native("recording.hls_discontinuity").len(), 1);
    let names: Vec<_> = std::fs::read_dir(directory.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect();
    assert_eq!(names.len(), 1, "{names:?}");
    assert!(!names[0].contains("-1"), "{names:?}");
}

#[tokio::test]
async fn hls_fractional_duration_reaches_split_limit_and_endlist_finishes() {
    use lubic::client::StatelessClient;
    use lubic::downloader::{hls, util::Segmentable};
    let playlist = format!("{FIRST}#EXT-X-ENDLIST\n");
    let fixture = HlsFixture::new(vec![
        ("/index.m3u8", vec![&playlist]),
        ("/a.ts", vec!["A"]),
        ("/b.ts", vec!["B"]),
    ])
    .await;
    let directory = tempfile::tempdir().unwrap();
    let template = directory.path().join("segment-%s-%f").display().to_string();
    let captured = Captured::default();
    let _guard =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(captured.clone()));
    tokio::time::timeout(
        std::time::Duration::from_secs(3),
        hls::download(
            &fixture.url,
            &StatelessClient::new(Default::default()),
            LifecycleFile::new(&template, "ts"),
            Segmentable::new(Some(std::time::Duration::from_secs(1)), None),
        ),
    )
    .await
    .unwrap()
    .unwrap();
    let closed = captured.native("recording.segment_closed");
    assert_eq!(closed[0]["reason_code"], "split_limit");
    assert_eq!(closed[0]["size_bytes"], "2");
    // The existing eager split creates an empty final file; do not count it as received media.
    assert_eq!(closed.last().unwrap()["reason_code"], "stream_end");
    assert_eq!(fixture.requests.lock().unwrap()["/index.m3u8"], 1);
}

/// 暂存录制转正（故障试拉）：HLS 在下一个分片边界切段，之后的文件写正式目录。
#[tokio::test]
async fn hls_promotion_splits_at_the_next_segment_into_the_new_template() {
    use lubic::client::StatelessClient;
    use lubic::downloader::{hls, util::Segmentable};
    use std::sync::atomic::{AtomicBool, Ordering};
    let playlist = format!("{FIRST}#EXT-X-ENDLIST\n");
    let fixture = HlsFixture::new(vec![
        ("/index.m3u8", vec![&playlist]),
        ("/a.ts", vec!["A"]),
        ("/b.ts", vec!["B"]),
    ])
    .await;
    let directory = tempfile::tempdir().unwrap();
    let staging = directory.path().join("staging");
    let formal = directory.path().join("formal");
    std::fs::create_dir_all(&staging).unwrap();
    std::fs::create_dir_all(&formal).unwrap();
    let template = |dir: &std::path::Path| dir.join("segment-%s-%f").display().to_string();
    let promoted = Arc::new(AtomicBool::new(false));
    let (flag, target) = (promoted.clone(), template(&formal));
    let closed = Arc::new(Mutex::new(Vec::new()));
    let hook = closed.clone();
    let file = LifecycleFile::with_hook(&template(&staging), "ts", move |event| {
        hook.lock().unwrap().push((
            event.path.parent().unwrap().to_path_buf(),
            std::fs::read(&event.path).unwrap(),
        ));
    })
    .with_relocation(Some(Box::new(move || {
        flag.load(Ordering::SeqCst).then(|| target.clone())
    })));
    tokio::time::timeout(
        std::time::Duration::from_secs(3),
        hls::download_with_ready(
            &fixture.url,
            &StatelessClient::new(Default::default()),
            file,
            Segmentable::new(None, None),
            // 第一片写完就转正。
            || promoted.store(true, Ordering::SeqCst),
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        *closed.lock().unwrap(),
        [(staging, b"A".to_vec()), (formal, b"B".to_vec())]
    );
}

#[tokio::test]
async fn hls_invalid_playlist_and_http_failure_never_signal_ready() {
    use lubic::client::StatelessClient;
    use lubic::downloader::{hls, util::Segmentable};
    for (body, reason, files) in [
        ("not a playlist", "invalid_playlist", 0),
        (
            "#EXTM3U\n#EXT-X-I-FRAME-STREAM-INF:BANDWIDTH=10,URI=\"iframe.m3u8\"\n",
            "invalid_playlist",
            0,
        ),
        (
            "#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXTINF:1,\nabsent.ts\n#EXT-X-ENDLIST\n",
            "http_error",
            1,
        ),
    ] {
        let fixture = HlsFixture::new(vec![("/index.m3u8", vec![body])]).await;
        let directory = tempfile::tempdir().unwrap();
        let template = directory.path().join("segment").display().to_string();
        let captured = Captured::default();
        let _guard =
            tracing::subscriber::set_default(tracing_subscriber::registry().with(captured.clone()));
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(3),
            hls::download_with_ready(
                &fixture.url,
                &StatelessClient::new(Default::default()),
                LifecycleFile::new(&template, "ts"),
                Segmentable::new(None, None),
                || panic!("failed media must not signal ready"),
            ),
        )
        .await
        .unwrap();
        assert!(result.is_err());
        let events = captured.native("recording.disconnected");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["reason_code"], reason);
        assert_eq!(captured.native("recording.segment_created").len(), files);
    }
}

#[tokio::test]
async fn hls_empty_live_playlist_waits_and_invalid_refresh_is_an_error() {
    use lubic::client::StatelessClient;
    use lubic::downloader::{error::Error, hls, util::Segmentable};
    let fixture = HlsFixture::new(vec![
        (
            "/index.m3u8",
            vec![
                "#EXTM3U\n#EXT-X-TARGETDURATION:1\n",
                FIRST,
                "not a playlist",
            ],
        ),
        ("/a.ts", vec!["A"]),
        ("/b.ts", vec!["B"]),
    ])
    .await;
    let directory = tempfile::tempdir().unwrap();
    let template = directory.path().join("segment").display().to_string();
    let captured = Captured::default();
    let _guard =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(captured.clone()));
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(8),
        hls::download(
            &fixture.url,
            &StatelessClient::new(Default::default()),
            LifecycleFile::new(&template, "ts"),
            Segmentable::new(None, None),
        ),
    )
    .await
    .unwrap();
    assert!(matches!(result, Err(Error::HlsInvalidPlaylist)));
    assert_eq!(std::fs::read(format!("{template}.ts")).unwrap(), b"AB");
    assert_eq!(
        captured.native("recording.segment_closed")[0]["reason_code"],
        "transport_error"
    );
    assert_eq!(
        captured.native("recording.disconnected")[0]["reason_code"],
        "invalid_playlist"
    );
}
