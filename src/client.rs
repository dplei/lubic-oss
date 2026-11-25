use crate::retry;
use reqwest::Response;
use reqwest::header::HeaderMap;
use std::time::Duration;

/// 拉流用的 HTTP 客户端：`client` 带平台要求的默认头，`headers` 是每次请求额外附加的头。
#[derive(Debug, Clone)]
pub struct StatelessClient {
    pub client: reqwest::Client,
    pub headers: HeaderMap,
}

impl StatelessClient {
    pub fn new(headers: HeaderMap) -> Self {
        Self::with_resolution(headers, None).expect("static client config is valid")
    }

    pub(crate) fn with_resolution(
        headers: HeaderMap,
        resolution: Option<(&str, std::net::SocketAddr)>,
    ) -> reqwest::Result<Self> {
        let builder = reqwest::Client::builder()
            .user_agent("Mozilla/5.0 (X11; Linux x86_64; rv:60.1) Gecko/20100101 Firefox/60.1")
            .default_headers(headers)
            .connect_timeout(Duration::from_secs(60));
        let builder = match resolution {
            Some((host, addr)) => builder.resolve(host, addr),
            None => builder,
        };
        Ok(Self {
            client: builder.build()?,
            headers: HeaderMap::new(),
        })
    }

    /// GET 并重试瞬时失败；非 2xx 也算失败。
    pub async fn retryable(&self, url: &str) -> reqwest::Result<Response> {
        let resp = retry(|| self.client.get(url).headers(self.headers.clone()).send()).await?;
        resp.error_for_status_ref()?;
        Ok(resp)
    }
}

impl Default for StatelessClient {
    fn default() -> Self {
        Self::new(HeaderMap::new())
    }
}
