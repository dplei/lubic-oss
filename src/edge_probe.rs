//! CDN 节点观测：短连或视频停滞后，用系统 DNS 和公共 DNS 解析同一个拉流域名，
//! 记下各自会落到的边缘节点，只打日志、不改变录制用的连接。攒够数据再决定要不要在重连时换节点。

use crate::downloader::util::EVENT_TARGET;
use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;
use tokio::net::UdpSocket;
use tracing::info;

/// 对照的公共 DNS：阿里、114、腾讯。系统默认解析器另算一组。
const RESOLVERS: [&str; 3] = ["223.5.5.5", "114.114.114.114", "119.29.29.29"];
const DNS_TIMEOUT: Duration = Duration::from_secs(2);
/// 跟 302 的请求只读响应头：调度节点回 302 就拿到真正的边缘节点；直接出流（200）就立刻断开。
const HTTP_TIMEOUT: Duration = Duration::from_secs(3);

pub(crate) const SWITCH_RESOLVER: &str = "114.114.114.114";

pub(crate) async fn alternate_address(url: &str) -> Option<(String, SocketAddr)> {
    let url = url::Url::parse(url).ok()?;
    let host = url.host_str()?;
    let port = url.port_or_known_default()?;
    let ip = resolve_a(SWITCH_RESOLVER, host).await?.into_iter().min()?;
    Some((host.to_string(), SocketAddr::new(ip.into(), port)))
}

/// 解析 `url` 的域名并逐组探一次，结果打一条 `recording.edge_probe` 日志。在后台跑，不拖慢重连。
pub async fn probe(
    room: String,
    session: String,
    trigger: &'static str,
    url: String,
    headers: HashMap<String, String>,
) {
    let Some((host, port)) = url::Url::parse(&url)
        .ok()
        .and_then(|u| Some((u.host_str()?.to_string(), u.port_or_known_default()?)))
    else {
        return;
    };
    let system = tokio::net::lookup_host((host.as_str(), port))
        .await
        .map(|addrs| {
            addrs
                .filter_map(|a| match a.ip() {
                    std::net::IpAddr::V4(ip) => Some(ip),
                    _ => None,
                })
                .collect()
        })
        .unwrap_or_default();
    let mut groups = vec![("system", system)];
    for resolver in RESOLVERS {
        groups.push((
            resolver,
            resolve_a(resolver, &host).await.unwrap_or_default(),
        ));
    }
    let mut results = Vec::new();
    for (resolver, mut ips) in groups {
        ips.sort();
        ips.dedup();
        let edge = match ips.first() {
            Some(ip) => follow(&url, &host, SocketAddr::new((*ip).into(), port), &headers).await,
            None => "-".to_string(),
        };
        let ips: Vec<_> = ips.iter().map(Ipv4Addr::to_string).collect();
        results.push(format!("{resolver}={}->{edge}", ips.join(",")));
    }
    info!(
        target: EVENT_TARGET,
        event_name = "recording.edge_probe",
        outcome = "measured",
        reason_code = trigger,
        host,
        results = results.join(" "),
        room,
        session,
        "拉流节点对照"
    );
}

/// 把 `host` 指到 `addr` 发一次请求、不跟跳转：302 返回跳转目标的主机，200 返回 `addr` 本身（它就是边缘节点）。
async fn follow(
    url: &str,
    host: &str,
    addr: SocketAddr,
    headers: &HashMap<String, String>,
) -> String {
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .resolve(host, addr)
        .timeout(HTTP_TIMEOUT)
        .build();
    let Ok(client) = client else {
        return "error".to_string();
    };
    let mut request = client.get(url);
    for (k, v) in headers {
        request = request.header(k, v);
    }
    match request.send().await {
        Ok(resp) if resp.status().is_redirection() => resp
            .headers()
            .get(reqwest::header::LOCATION)
            .and_then(|l| url::Url::parse(l.to_str().ok()?).ok())
            .and_then(|u| u.host_str().map(str::to_string))
            .unwrap_or_else(|| "redirect".to_string()),
        // 响应体不读，drop 即断开。
        Ok(resp) => format!("{}({})", addr.ip(), resp.status().as_u16()),
        Err(_) => "error".to_string(),
    }
}

/// 向 `resolver` 发一个 A 记录查询（UDP 53）。
async fn resolve_a(resolver: &str, host: &str) -> Option<Vec<Ipv4Addr>> {
    let socket = UdpSocket::bind("0.0.0.0:0").await.ok()?;
    let id = rand::random::<u16>();
    socket
        .send_to(&query(id, host), (resolver, 53))
        .await
        .ok()?;
    let mut buf = [0u8; 1500];
    let n = tokio::time::timeout(DNS_TIMEOUT, socket.recv(&mut buf))
        .await
        .ok()?
        .ok()?;
    parse_a(id, &buf[..n])
}

fn query(id: u16, host: &str) -> Vec<u8> {
    let mut q = id.to_be_bytes().to_vec();
    // RD=1，一个问题。
    q.extend_from_slice(&[0x01, 0x00, 0, 1, 0, 0, 0, 0, 0, 0]);
    for label in host.trim_end_matches('.').split('.') {
        q.push(label.len() as u8);
        q.extend_from_slice(label.as_bytes());
    }
    q.extend_from_slice(&[0, 0, 1, 0, 1]);
    q
}

/// 取应答里全部 A 记录（CNAME 链上的其他记录跳过）。
fn parse_a(id: u16, msg: &[u8]) -> Option<Vec<Ipv4Addr>> {
    if msg.len() < 12 || msg[..2] != id.to_be_bytes() {
        return None;
    }
    let qd = u16::from_be_bytes([msg[4], msg[5]]);
    let an = u16::from_be_bytes([msg[6], msg[7]]);
    let mut i = 12;
    for _ in 0..qd {
        i = skip_name(msg, i)? + 4;
    }
    let mut ips = Vec::new();
    for _ in 0..an {
        i = skip_name(msg, i)?;
        let rr = msg.get(i..i + 10)?;
        let len = u16::from_be_bytes([rr[8], rr[9]]) as usize;
        let data = msg.get(i + 10..i + 10 + len)?;
        if rr[..4] == [0, 1, 0, 1] && len == 4 {
            ips.push(Ipv4Addr::new(data[0], data[1], data[2], data[3]));
        }
        i += 10 + len;
    }
    Some(ips)
}

fn skip_name(msg: &[u8], mut i: usize) -> Option<usize> {
    loop {
        let len = *msg.get(i)?;
        match len {
            0 => return Some(i + 1),
            l if l & 0xc0 == 0xc0 => return Some(i + 2),
            l => i += 1 + l as usize,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 一条 CNAME 加两条 A 的应答（名字用压缩指针），只取出两条 A。
    #[test]
    fn parses_a_records_behind_a_cname() {
        let mut msg = query(7, "pull.example.com");
        msg[2..4].copy_from_slice(&[0x81, 0x80]);
        msg[6..8].copy_from_slice(&[0, 3]);
        // CNAME -> edge.cdn（长度 10）
        msg.extend_from_slice(&[0xc0, 12, 0, 5, 0, 1, 0, 0, 0, 60, 0, 10]);
        msg.extend_from_slice(&[4, b'e', b'd', b'g', b'e', 3, b'c', b'd', b'n', 0]);
        for last in [1u8, 2] {
            msg.extend_from_slice(&[0xc0, 12, 0, 1, 0, 1, 0, 0, 0, 60, 0, 4, 10, 0, 0, last]);
        }
        assert_eq!(
            parse_a(7, &msg).unwrap(),
            [Ipv4Addr::new(10, 0, 0, 1), Ipv4Addr::new(10, 0, 0, 2)]
        );
        assert_eq!(parse_a(8, &msg), None, "id 不符");
        assert_eq!(parse_a(7, &msg[..msg.len() - 3]), None, "截断");
    }
}
