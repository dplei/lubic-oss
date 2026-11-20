//! 把一个本地文件传上 B 站 UPOS，返回 UPOS 文件名。**只传字节**：
//! 不登录、不读 cookie、不调 preupload。凭证（[`Ticket`]）由持有账号的一方申请后交给这里。
//!
//! 协议：`POST {url}?uploads` 拿 upload_id → 分块 `PUT {url}?partNumber=…` → `POST {url}?uploadId=…` 合并。

use futures::{StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::BTreeSet;
use std::fmt;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::watch;
use tracing::warn;

/// preupload 返回的上传凭证，原样反序列化即可（多余字段忽略）。
///
/// `auth` 是凭证：不要写进日志或事件。`Debug` 已把它打码。
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ticket {
    /// 形如 `//upos-cs-upcdntx.bilivideo.com`，协议相对。
    pub endpoint: String,
    pub auth: String,
    /// 形如 `upos://ugcfx2lf/n2610…flv`。
    pub upos_uri: String,
    pub chunk_size: u64,
    pub biz_id: u64,
}

impl fmt::Debug for Ticket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Ticket")
            .field("endpoint", &self.endpoint)
            .field("upos_uri", &self.upos_uri)
            .field("chunk_size", &self.chunk_size)
            .field("biz_id", &self.biz_id)
            .finish_non_exhaustive()
    }
}

impl Ticket {
    /// 对象地址。上传、HEAD、GET 都是它，只是方法不同。协议相对的 endpoint 补 https。
    pub fn object_url(&self) -> String {
        let scheme = if self.endpoint.starts_with("//") {
            "https:"
        } else {
            ""
        };
        let path = self.upos_uri.trim_start_matches("upos://");
        format!("{scheme}{}/{path}", self.endpoint)
    }

    /// 投稿时用的 UPOS 文件名：`upos_uri` 去掉目录和扩展名。
    pub fn filename(&self) -> String {
        let name = self.upos_uri.rsplit('/').next().unwrap_or_default();
        name.split_once('.')
            .map_or(name, |(stem, _)| stem)
            .to_string()
    }
}

/// 断点信息。调用方在 `on_progress` 里把它持久化，进程重启后连同同一张 [`Ticket`] 传回来，
/// 已完成的分块不会再传。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Checkpoint {
    pub upload_id: Option<String>,
    /// 已被服务端接受的分块（从 0 开始）。
    pub done: BTreeSet<u64>,
}

#[derive(Debug, Clone)]
pub struct Options {
    pub concurrency: usize,
    /// 单块失败后的重试次数（不含首次）。
    pub retries: u32,
    pub chunk_timeout: Duration,
    /// 向 UPOS 声明的总大小，缺省为文件实际大小。边录边传时声明值会大于实际字节数。
    pub declared_size: Option<u64>,
    /// 跟着文件增长上传时，多久看一次文件大小。
    pub poll: Duration,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            concurrency: 3,
            retries: 3,
            chunk_timeout: Duration::from_secs(240),
            declared_size: None,
            poll: Duration::from_secs(2),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("http: {0}")]
    Http(#[from] reqwest::Error),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("upos: {0}")]
    Upos(String),
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ChunkQuery<'a> {
    upload_id: &'a str,
    chunks: u64,
    total: u64,
    chunk: u64,
    size: u64,
    part_number: u64,
    start: u64,
    end: u64,
}

/// 上传 `path`，成功返回 UPOS 文件名。`checkpoint` 每推进一步就回调一次 `on_progress`。
pub async fn upload(
    client: &reqwest::Client,
    path: &Path,
    ticket: &Ticket,
    options: &Options,
    checkpoint: &mut Checkpoint,
    on_progress: impl FnMut(&Checkpoint),
) -> Result<String> {
    let (_tx, growth) = watch::channel(Growth {
        path: path.to_path_buf(),
        closed: true,
    });
    follow(
        client,
        growth,
        ticket,
        options,
        checkpoint,
        on_progress,
        |_| {},
    )
    .await
}

/// 一个还在写的文件：路径（收尾时会从 `.part` 改成最终文件名）与是否已写完。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Growth {
    pub path: PathBuf,
    pub closed: bool,
}

/// 跟着文件增长上传：没写完时只传已写满的块，`closed` 后补传最后不满的一块并合并，返回 UPOS 文件名。
///
/// 文件必须只追加、不回头改写。`options.declared_size` 是 preupload 时声明的大小，文件长过它就报错。
/// 没写完时每隔 `options.poll`（或 `growth` 一变）看一次文件大小；读到 `NotFound` 当作正在改名，等下一轮。
/// `growth` 的发送端没了（调用方放弃）就报错返回。`on_busy(true/false)` 包住每一批请求，
/// 等文件长的时间不算忙碌。
pub async fn follow(
    client: &reqwest::Client,
    mut growth: watch::Receiver<Growth>,
    ticket: &Ticket,
    options: &Options,
    checkpoint: &mut Checkpoint,
    mut on_progress: impl FnMut(&Checkpoint),
    mut on_busy: impl FnMut(bool),
) -> Result<String> {
    let chunk_size = ticket.chunk_size.max(1);
    let url = ticket.object_url();
    loop {
        let Growth { path, closed } = growth.borrow_and_update().clone();
        let size = match tokio::fs::metadata(&path).await {
            Ok(m) => m.len(),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound && !closed => 0,
            Err(e) => return Err(e.into()),
        };
        if closed && size == 0 {
            return Err(Error::Upos(format!("{} is empty", path.display())));
        }
        let total = options.declared_size.unwrap_or(size);
        if size > total {
            return Err(Error::Upos(format!(
                "{} outgrew its declared size: {size} > {total}",
                path.display()
            )));
        }
        let ready = if closed {
            size.div_ceil(chunk_size)
        } else {
            size / chunk_size
        };
        let pending: Vec<u64> = (0..ready)
            .filter(|i| !checkpoint.done.contains(i))
            .collect();
        if !pending.is_empty() || closed {
            on_busy(true);
            let batch = async {
                let upload_id = start(client, &url, ticket, checkpoint, &mut on_progress).await?;
                let (url, upload_id, path) = (&url, &upload_id, &path);
                let mut results = futures::stream::iter(pending)
                    .map(|i| {
                        let start = i * chunk_size;
                        let len = chunk_size.min(size - start);
                        let query = ChunkQuery {
                            upload_id,
                            chunks: total.div_ceil(chunk_size),
                            total,
                            chunk: i,
                            size: len,
                            part_number: i + 1,
                            start,
                            end: start + len,
                        };
                        async move {
                            let body = read_range(path, start, len).await?;
                            put_chunk(client, url, ticket, &query, body, options).await?;
                            Ok::<_, Error>(i)
                        }
                    })
                    .buffer_unordered(options.concurrency.max(1));
                while let Some(i) = results.try_next().await? {
                    checkpoint.done.insert(i);
                    on_progress(checkpoint);
                }
                drop(results);
                if closed {
                    let name = path
                        .file_name()
                        .and_then(|n| n.to_str())
                        .unwrap_or_default();
                    complete(client, url, ticket, name, upload_id, ready).await?;
                }
                Ok::<_, Error>(())
            }
            .await;
            on_busy(false);
            match batch {
                Ok(()) if closed => return Ok(ticket.filename()),
                Ok(()) => {}
                // 改名的瞬间读 `.part`：等 `closed` 带来新路径。
                Err(Error::Io(e)) if e.kind() == std::io::ErrorKind::NotFound && !closed => {}
                Err(e) => return Err(e),
            }
        }
        tokio::select! {
            changed = growth.changed() => if changed.is_err() {
                return Err(Error::Upos("growing file abandoned".into()));
            },
            _ = tokio::time::sleep(options.poll) => {}
        }
    }
}

/// 拿 upload_id：断点里有就沿用，否则 `POST ?uploads` 新开一个。
async fn start(
    client: &reqwest::Client,
    url: &str,
    ticket: &Ticket,
    checkpoint: &mut Checkpoint,
    on_progress: &mut impl FnMut(&Checkpoint),
) -> Result<String> {
    if let Some(id) = &checkpoint.upload_id {
        return Ok(id.clone());
    }
    let res: serde_json::Value = client
        .post(format!("{url}?uploads&output=json"))
        .header("X-Upos-Auth", &ticket.auth)
        .timeout(Duration::from_secs(60))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let id = res["upload_id"]
        .as_str()
        .ok_or_else(|| Error::Upos(format!("no upload_id: {res}")))?
        .to_string();
    checkpoint.upload_id = Some(id.clone());
    checkpoint.done.clear();
    on_progress(checkpoint);
    Ok(id)
}

/// 合并前 `parts` 块。声明大小大于实传时，块数按实际字节算，没传的块不列。
async fn complete(
    client: &reqwest::Client,
    url: &str,
    ticket: &Ticket,
    name: &str,
    upload_id: &str,
    parts: u64,
) -> Result<()> {
    let parts: Vec<_> = (1..=parts)
        .map(|n| json!({"partNumber": n, "eTag": "etag"}))
        .collect();
    let res: serde_json::Value = client
        .post(url)
        .query(&[
            ("name", name),
            ("uploadId", upload_id),
            ("biz_id", &ticket.biz_id.to_string()),
            ("output", "json"),
            ("profile", "ugcupos/bup"),
        ])
        .header("X-Upos-Auth", &ticket.auth)
        .json(&json!({ "parts": parts }))
        .timeout(Duration::from_secs(60))
        .send()
        .await?
        .json()
        .await?;
    if res["OK"] != 1 {
        return Err(Error::Upos(format!("complete rejected: {res}")));
    }
    Ok(())
}

async fn read_range(path: &Path, start: u64, len: u64) -> Result<Vec<u8>> {
    let mut file = tokio::fs::File::open(path).await?;
    file.seek(std::io::SeekFrom::Start(start)).await?;
    let mut buf = vec![0; len as usize];
    file.read_exact(&mut buf).await?;
    Ok(buf)
}

async fn put_chunk(
    client: &reqwest::Client,
    url: &str,
    ticket: &Ticket,
    query: &ChunkQuery<'_>,
    body: Vec<u8>,
    options: &Options,
) -> Result<()> {
    let mut attempt = 0;
    loop {
        let result = client
            .put(url)
            .header("X-Upos-Auth", &ticket.auth)
            .query(query)
            .timeout(options.chunk_timeout)
            .body(body.clone())
            .send()
            .await
            .and_then(|r| r.error_for_status());
        match result {
            Ok(_) => return Ok(()),
            Err(e) if attempt < options.retries => {
                attempt += 1;
                warn!(chunk = query.chunk, attempt, error = %e, "upos chunk failed, retrying");
                tokio::time::sleep(Duration::from_secs(1 << attempt)).await;
            }
            Err(e) => return Err(e.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::sync::{Arc, Mutex};
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpListener;

    #[test]
    fn ticket_paths() {
        let t = Ticket {
            endpoint: "//upos-cs-upcdntx.bilivideo.com".into(),
            auth: "secret".into(),
            upos_uri: "upos://ugcfx2lf/n123.flv".into(),
            chunk_size: 1,
            biz_id: 1,
        };
        assert_eq!(
            t.object_url(),
            "https://upos-cs-upcdntx.bilivideo.com/ugcfx2lf/n123.flv"
        );
        assert_eq!(t.filename(), "n123");
        assert!(!format!("{t:?}").contains("secret"));
    }

    /// 一个只认 UPOS 三种请求的假服务器。`fail_first` 中的分块第一次 PUT 返回 500。
    async fn fake_upos(fail_first: Vec<u64>) -> (String, Arc<Mutex<Vec<String>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let log = Arc::new(Mutex::new(Vec::new()));
        let failed = Arc::new(Mutex::new(fail_first));
        let log2 = log.clone();
        tokio::spawn(async move {
            loop {
                let (mut sock, _) = listener.accept().await.unwrap();
                let log = log2.clone();
                let failed = failed.clone();
                tokio::spawn(async move {
                    let mut buf = Vec::new();
                    let mut tmp = [0u8; 4096];
                    let (head, body_len) = loop {
                        let n = sock.read(&mut tmp).await.unwrap();
                        if n == 0 {
                            return;
                        }
                        buf.extend_from_slice(&tmp[..n]);
                        if let Some(p) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                            let head = String::from_utf8_lossy(&buf[..p]).to_string();
                            let len = head
                                .lines()
                                .find_map(|l| {
                                    l.to_ascii_lowercase()
                                        .strip_prefix("content-length: ")
                                        .map(|v| v.parse::<usize>().unwrap())
                                })
                                .unwrap_or(0);
                            buf.drain(..p + 4);
                            break (head, len);
                        }
                    };
                    while buf.len() < body_len {
                        let n = sock.read(&mut tmp).await.unwrap();
                        buf.extend_from_slice(&tmp[..n]);
                    }
                    let line = head.lines().next().unwrap().to_string();
                    let mut status = "200 OK";
                    let body = if line.starts_with("POST") && line.contains("uploads") {
                        r#"{"upload_id":"U1"}"#.to_string()
                    } else if line.starts_with("PUT") {
                        let chunk: u64 = line
                            .split("chunk=")
                            .nth(1)
                            .unwrap()
                            .split('&')
                            .next()
                            .unwrap()
                            .parse()
                            .unwrap();
                        let mut f = failed.lock().unwrap();
                        if let Some(p) = f.iter().position(|c| *c == chunk) {
                            f.remove(p);
                            status = "500 Internal Server Error";
                        } else {
                            log.lock().unwrap().push(format!("{line} len={body_len}"));
                        }
                        String::new()
                    } else {
                        log.lock()
                            .unwrap()
                            .push(format!("{line} {}", String::from_utf8_lossy(&buf)));
                        r#"{"OK":1}"#.to_string()
                    };
                    let resp = format!(
                        "HTTP/1.1 {status}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    sock.write_all(resp.as_bytes()).await.unwrap();
                });
            }
        });
        (format!("http://127.0.0.1:{}", addr.port()), log)
    }

    #[tokio::test]
    async fn uploads_chunks_retries_and_resumes() {
        let (endpoint, log) = fake_upos(vec![1]).await;
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), vec![7u8; 25]).unwrap();
        let ticket = Ticket {
            endpoint,
            auth: "a".into(),
            upos_uri: "upos://bucket/n42.flv".into(),
            chunk_size: 10,
            biz_id: 9,
        };
        let client = reqwest::Client::new();
        let options = Options {
            retries: 1,
            ..Default::default()
        };
        // 模拟上次已传完第 0 块。
        let mut cp = Checkpoint {
            upload_id: Some("U1".into()),
            done: [0].into(),
        };
        let mut seen = 0;
        let name = upload(&client, file.path(), &ticket, &options, &mut cp, |_| {
            seen += 1
        })
        .await
        .unwrap();
        assert_eq!(name, "n42");
        assert_eq!(cp.done, [0, 1, 2].into());
        assert_eq!(seen, 2);
        let log = log.lock().unwrap();
        let puts: Vec<_> = log.iter().filter(|l| l.starts_with("PUT")).collect();
        assert_eq!(puts.len(), 2, "第 0 块不应重传: {log:?}");
        assert!(
            puts.iter()
                .any(|l| l.contains("chunk=2") && l.contains("len=5"))
        );
        assert!(log.last().unwrap().contains(r#""partNumber":3"#));
    }

    fn puts(log: &Mutex<Vec<String>>) -> Vec<String> {
        log.lock()
            .unwrap()
            .iter()
            .filter(|l| l.starts_with("PUT"))
            .cloned()
            .collect()
    }

    /// 跟着文件增长传：没写完只传写满的块，改名后收尾补最后一块；分块按声明大小报 total/chunks，
    /// 合并只列实际块数。
    #[tokio::test]
    async fn follows_a_growing_file_and_declares_the_larger_size() {
        let (endpoint, log) = fake_upos(vec![]).await;
        let dir = tempfile::tempdir().unwrap();
        let part = dir.path().join("a.flv.part");
        std::fs::write(&part, vec![1u8; 15]).unwrap();
        let ticket = Ticket {
            endpoint,
            auth: "a".into(),
            upos_uri: "upos://bucket/n7.flv".into(),
            chunk_size: 10,
            biz_id: 9,
        };
        let options = Options {
            declared_size: Some(40),
            poll: Duration::from_millis(10),
            ..Default::default()
        };
        let (tx, rx) = watch::channel(Growth {
            path: part.clone(),
            closed: false,
        });
        let busy = Arc::new(Mutex::new(Vec::new()));
        let busy2 = busy.clone();
        let task = tokio::spawn(async move {
            let mut cp = Checkpoint::default();
            let client = reqwest::Client::new();
            let r = follow(
                &client,
                rx,
                &ticket,
                &options,
                &mut cp,
                |_| {},
                |b| busy2.lock().unwrap().push(b),
            )
            .await;
            (r, cp)
        });
        let wait_puts = |n: usize| {
            let log = log.clone();
            async move {
                for _ in 0..300 {
                    if puts(&log).len() >= n {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                puts(&log)
            }
        };
        let p = wait_puts(1).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(puts(&log).len(), 1, "不满的尾巴不传: {p:?}");
        assert!(p[0].contains("chunk=0") && p[0].contains("total=40") && p[0].contains("chunks=4"));

        // 写到 28 字节：第 1 块写满了。
        std::fs::OpenOptions::new()
            .append(true)
            .open(&part)
            .unwrap()
            .write_all(&[2u8; 13])
            .unwrap();
        let p = wait_puts(2).await;
        assert!(p[1].contains("chunk=1") && p[1].contains("len=10"), "{p:?}");

        // 收尾：改名后才告诉它，中间读 .part 会 NotFound。
        let done = dir.path().join("a.flv");
        std::fs::rename(&part, &done).unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
        tx.send(Growth {
            path: done,
            closed: true,
        })
        .unwrap();
        let (r, cp) = task.await.unwrap();
        assert_eq!(r.unwrap(), "n7");
        assert_eq!(cp.done, [0, 1, 2].into());
        let log = log.lock().unwrap();
        let p: Vec<_> = log.iter().filter(|l| l.starts_with("PUT")).collect();
        assert!(p[2].contains("chunk=2") && p[2].contains("len=8"), "{p:?}");
        let merge = log.last().unwrap();
        assert!(merge.contains(r#""partNumber":3"#) && !merge.contains(r#""partNumber":4"#));
        assert!(merge.contains("name=a.flv&"), "合并用最终文件名: {merge}");
        let busy = busy.lock().unwrap();
        assert_eq!(busy.first(), Some(&true));
        assert_eq!(
            busy.iter().filter(|b| **b).count(),
            busy.iter().filter(|b| !**b).count()
        );
    }

    #[tokio::test]
    async fn follow_fails_when_the_file_outgrows_its_declared_size_or_is_abandoned() {
        let (endpoint, _log) = fake_upos(vec![]).await;
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), vec![1u8; 25]).unwrap();
        let ticket = Ticket {
            endpoint,
            auth: "a".into(),
            upos_uri: "upos://bucket/n7.flv".into(),
            chunk_size: 10,
            biz_id: 9,
        };
        let client = reqwest::Client::new();
        let options = Options {
            declared_size: Some(20),
            poll: Duration::from_millis(10),
            ..Default::default()
        };
        let growth = |closed| {
            watch::channel(Growth {
                path: file.path().to_path_buf(),
                closed,
            })
        };
        let (_tx, rx) = growth(true);
        let e = follow(
            &client,
            rx,
            &ticket,
            &options,
            &mut Default::default(),
            |_| {},
            |_| {},
        )
        .await
        .unwrap_err();
        assert!(e.to_string().contains("outgrew"), "{e}");

        let options = Options {
            declared_size: Some(40),
            ..options
        };
        let (tx, rx) = growth(false);
        drop(tx);
        let e = follow(
            &client,
            rx,
            &ticket,
            &options,
            &mut Default::default(),
            |_| {},
            |_| {},
        )
        .await
        .unwrap_err();
        assert!(e.to_string().contains("abandoned"), "{e}");
    }
}
