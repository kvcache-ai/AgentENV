//! Connect protocol client for talking to envd through the AgentENV reverse proxy.
//!
//! envd (forked from e2b-dev) speaks the ConnectRPC family of protocols. Native
//! gRPC requires HTTP/2 between the AgentENV reverse proxy and envd, which the
//! stock proxy does not do. Connect protocol is HTTP/1.1-friendly: unary is a
//! plain POST, server-streaming is a single request followed by chunked
//! envelopes — so it passes through the existing proxy unchanged.
//!
//! Dispatch is keyed on the `x-agentenv-sandbox-id` / `x-agentenv-target-port`
//! headers: the server's fallback route forwards any path carrying those
//! headers to the sandbox, so no `/proxy` prefix is prepended here.

use anyhow::{anyhow, bail, Context, Result};
use bytes::{Buf, Bytes, BytesMut};
use envd::process::{process_event, ListRequest, ProcessConfig, Pty, StartRequest, StartResponse};
use futures::{stream::unfold, Stream, StreamExt};
use prost::Message;
use reqwest::Client as HttpClient;
use std::fmt;
use std::io::Write;
use std::pin::Pin;
use std::time::Duration;

pub const ENVD_PORT_STR: &str = "49983";

const PROCESS_SERVICE: &str = "process.Process";
const FLAG_DATA: u8 = 0x00;
const FLAG_END: u8 = 0x02;

#[derive(Debug)]
pub struct RpcError {
    pub code: String,
    pub message: String,
}

impl RpcError {
    pub fn is_code(&self, code: &str) -> bool {
        self.code == code
    }
}

impl fmt::Display for RpcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.message.is_empty() {
            write!(f, "{}", self.code)
        } else {
            write!(f, "{}: {}", self.code, self.message)
        }
    }
}

impl std::error::Error for RpcError {}

pub struct Transport {
    http: HttpClient,
    base_url: String,
    api_key: String,
    sandbox_id: String,
    envd_access_token: Option<String>,
}

impl Transport {
    pub fn new(
        base_url: &str,
        api_key: &str,
        sandbox_id: &str,
        envd_access_token: Option<&str>,
    ) -> Result<Self> {
        let http = Self::http_client(base_url).context("building Connect-RPC HTTP client")?;
        Ok(Self {
            http,
            base_url: base_url.trim_end_matches('/').to_string(),
            api_key: api_key.to_string(),
            sandbox_id: sandbox_id.to_string(),
            envd_access_token: envd_access_token.map(str::to_owned),
        })
    }

    fn http_client(base_url: &str) -> Result<HttpClient> {
        let builder = HttpClient::builder()
            .http1_only()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(2))
            .tcp_keepalive(Duration::from_secs(3))
            .tcp_keepalive_interval(Duration::from_secs(1))
            .tcp_keepalive_retries(2)
            .pool_max_idle_per_host(0)
            .pool_idle_timeout(None);
        let builder = if bypass_proxy_for_base_url(base_url) {
            builder.no_proxy()
        } else {
            builder
        };
        #[cfg(any(target_os = "android", target_os = "fuchsia", target_os = "linux"))]
        let builder = builder.tcp_user_timeout(Duration::from_secs(3));
        builder.build().context("building HTTP client")
    }

    fn url(&self, service: &str, method: &str) -> String {
        format!("{}/{}/{}", self.base_url, service, method)
    }

    fn auth(&self, builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        let builder = builder
            .header("X-API-Key", &self.api_key)
            .header("x-agentenv-sandbox-id", &self.sandbox_id)
            .header("x-agentenv-target-port", ENVD_PORT_STR)
            .header("Connect-Protocol-Version", "1");
        match &self.envd_access_token {
            Some(token) => builder.header("X-Access-Token", token),
            None => builder,
        }
    }

    fn unary_request(
        &self,
        service: &str,
        method: &str,
        username: Option<&str>,
    ) -> reqwest::RequestBuilder {
        let request = self
            .auth(self.http.post(self.url(service, method)))
            .header("Content-Type", "application/proto");
        match username {
            Some(username) => request.basic_auth(username, Option::<&str>::None),
            None => request,
        }
    }

    pub async fn ready(&self) -> Result<()> {
        self.unary::<_, envd::process::ListResponse>("List", ListRequest {})
            .await
            .map(|_| ())
    }

    pub async fn unary<Req, Resp>(&self, method: &str, req: Req) -> Result<Resp>
    where
        Req: Message,
        Resp: Message + Default,
    {
        self.unary_service(PROCESS_SERVICE, method, req, None).await
    }

    pub async fn unary_service<Req, Resp>(
        &self,
        service: &str,
        method: &str,
        req: Req,
        username: Option<&str>,
    ) -> Result<Resp>
    where
        Req: Message,
        Resp: Message + Default,
    {
        let body = req.encode_to_vec();
        let resp = self
            .unary_request(service, method, username)
            .body(body)
            .send()
            .await
            .context("sending unary request")?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            if let Some(error) = parse_connect_http_error(&text) {
                return Err(error.into());
            }
            bail!("{}: {}", status, text.trim());
        }

        let bytes = resp.bytes().await?;
        Resp::decode(bytes).context("decoding unary response")
    }

    pub async fn server_stream<Req, Resp>(
        &self,
        method: &str,
        req: Req,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Resp>> + Send>>>
    where
        Req: Message,
        Resp: Message + Default + Send + 'static,
    {
        let initial = encode_envelope(FLAG_DATA, &req.encode_to_vec());
        let http = Self::http_client(&self.base_url)?;
        let resp = self
            .auth(http.post(self.url(PROCESS_SERVICE, method)))
            .header("Content-Type", "application/connect+proto")
            .body(initial)
            .send()
            .await
            .context("opening server stream")?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            bail!("{}: {}", status, text.trim());
        }

        Ok(envelope_stream(resp.bytes_stream()))
    }

    pub async fn client_stream<Req, Resp, S>(&self, method: &str, stream: S) -> Result<Resp>
    where
        Req: Message,
        Resp: Message + Default + Send + 'static,
        S: Stream<Item = Req> + Send + 'static,
    {
        let http = Self::http_client(&self.base_url)?;
        let body = stream.map(|req| {
            Ok::<_, std::convert::Infallible>(Bytes::from(encode_envelope(
                FLAG_DATA,
                &req.encode_to_vec(),
            )))
        });
        let resp = self
            .auth(http.post(self.url(PROCESS_SERVICE, method)))
            .header("Content-Type", "application/connect+proto")
            .body(reqwest::Body::wrap_stream(body))
            .send()
            .await
            .context("opening client stream")?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            bail!("{}: {}", status, text.trim());
        }

        let mut stream = envelope_stream::<_, _, Resp>(resp.bytes_stream());
        match stream.next().await {
            Some(Ok(msg)) => Ok(msg),
            Some(Err(err)) => Err(err),
            None => bail!("client stream ended without response"),
        }
    }
}

pub(crate) fn bypass_proxy_for_base_url(base_url: &str) -> bool {
    let Ok(url) = reqwest::Url::parse(base_url) else {
        return false;
    };
    match url.host_str() {
        Some("localhost" | "::1") => true,
        Some(host) => host.starts_with("127."),
        None => false,
    }
}

fn encode_envelope(flag: u8, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(5 + payload.len());
    out.push(flag);
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(payload);
    out
}

struct StreamState<S> {
    body: S,
    buf: BytesMut,
    done: bool,
}

fn envelope_stream<S, E, Resp>(body: S) -> Pin<Box<dyn Stream<Item = Result<Resp>> + Send>>
where
    S: Stream<Item = std::result::Result<Bytes, E>> + Send + Unpin + 'static,
    E: std::error::Error + Send + Sync + 'static,
    Resp: Message + Default + Send + 'static,
{
    let state = StreamState {
        body,
        buf: BytesMut::new(),
        done: false,
    };

    Box::pin(
        unfold(state, |mut state| async move {
            loop {
                if state.done {
                    return None;
                }
                match try_decode_envelope::<Resp>(&mut state.buf) {
                    Ok(Some(Envelope::Data(msg))) => return Some((Ok(msg), state)),
                    Ok(Some(Envelope::End(err))) => {
                        state.done = true;
                        return err.map(|e| (Err(e), state));
                    }
                    Ok(None) => {
                        // Need more bytes.
                    }
                    Err(err) => {
                        state.done = true;
                        return Some((Err(err), state));
                    }
                }

                match state.body.next().await {
                    Some(Ok(chunk)) => state.buf.extend_from_slice(&chunk),
                    Some(Err(err)) => {
                        state.done = true;
                        return Some((Err(anyhow!(err).context("reading stream chunk")), state));
                    }
                    None => return None,
                }
            }
        })
        .fuse(),
    )
}

enum Envelope<Resp> {
    Data(Resp),
    End(Option<anyhow::Error>),
}

fn try_decode_envelope<Resp>(buf: &mut BytesMut) -> Result<Option<Envelope<Resp>>>
where
    Resp: Message + Default,
{
    if buf.len() < 5 {
        return Ok(None);
    }
    let flag = buf[0];
    let len = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
    if buf.len() < 5 + len {
        return Ok(None);
    }
    buf.advance(5);
    let payload = buf.split_to(len).freeze();

    match flag {
        FLAG_DATA => {
            let msg = Resp::decode(payload).context("decoding stream message")?;
            Ok(Some(Envelope::Data(msg)))
        }
        FLAG_END => {
            if payload.is_empty() || payload.as_ref() == b"{}" {
                return Ok(Some(Envelope::End(None)));
            }
            let err = parse_connect_error(&payload);
            Ok(Some(Envelope::End(err)))
        }
        other => {
            bail!("unexpected Connect envelope flag: 0x{:02x}", other)
        }
    }
}

fn parse_connect_error(body: &[u8]) -> Option<anyhow::Error> {
    #[derive(serde::Deserialize)]
    struct Trailer {
        #[serde(default)]
        error: Option<ErrBody>,
    }
    #[derive(serde::Deserialize)]
    struct ErrBody {
        #[serde(default)]
        code: Option<String>,
        #[serde(default)]
        message: Option<String>,
    }

    let trailer: Trailer = serde_json::from_slice(body).ok()?;
    let err = trailer.error?;
    let code = err.code.unwrap_or_else(|| "unknown".into());
    let message = err.message.unwrap_or_default();
    Some(RpcError { code, message }.into())
}

fn parse_connect_http_error(body: &str) -> Option<RpcError> {
    #[derive(serde::Deserialize)]
    struct ErrBody {
        #[serde(default)]
        code: Option<String>,
        #[serde(default)]
        message: Option<String>,
    }

    let error: ErrBody = serde_json::from_str(body).ok()?;
    let code = error.code.filter(|code| !code.is_empty())?;
    Some(RpcError {
        code,
        message: error.message.unwrap_or_default(),
    })
}

pub struct StartOpts<'a> {
    pub cmd: &'a str,
    pub args: Vec<String>,
    pub envs: std::collections::HashMap<String, String>,
    pub pty: Option<(u32, u32)>,
    pub stdin: bool,
}

pub fn build_start_request(opts: StartOpts<'_>) -> StartRequest {
    StartRequest {
        process: Some(ProcessConfig {
            cmd: opts.cmd.to_string(),
            args: opts.args,
            envs: opts.envs,
            cwd: None,
        }),
        pty: opts.pty.map(|(cols, rows)| Pty {
            size: Some(envd::process::pty::Size { cols, rows }),
        }),
        tag: None,
        stdin: Some(opts.stdin),
    }
}

/// Collects the terminal-related environment variables that most interactive
/// programs need. `TERM` is remapped to a value the sandbox is likely to have
/// in its terminfo DB — host values like `xterm-ghostty` or `alacritty`
/// break ncurses programs (top, htop, w) when the sandbox has no matching
/// terminfo entry.
pub fn pty_envs() -> std::collections::HashMap<String, String> {
    const PASS_THROUGH: &[&str] = &["COLORTERM", "LANG", "LC_ALL", "LC_CTYPE"];
    let mut out = std::collections::HashMap::new();
    for key in PASS_THROUGH {
        if let Ok(v) = std::env::var(key) {
            out.insert((*key).to_string(), v);
        }
    }
    out.insert(
        "TERM".into(),
        remap_term(std::env::var("TERM").ok().as_deref()),
    );
    out
}

/// Drains a non-interactive Start stream, writing stdout/stderr events and
/// returning the exit code from the terminating End event.
pub async fn drain_output<S>(mut stream: S) -> Result<i32>
where
    S: Stream<Item = Result<StartResponse>> + Unpin,
{
    let stdout = std::io::stdout();
    let stderr = std::io::stderr();
    while let Some(msg) = stream.next().await {
        let msg = msg?;
        let Some(evt) = msg.event.and_then(|w| w.event) else {
            continue;
        };
        match evt {
            process_event::Event::Data(d) => match d.output {
                Some(process_event::data_event::Output::Stdout(b)) => {
                    stdout.lock().write_all(&b)?;
                }
                Some(process_event::data_event::Output::Stderr(b)) => {
                    stderr.lock().write_all(&b)?;
                }
                _ => {}
            },
            process_event::Event::End(end) => return Ok(end.exit_code),
            _ => {}
        }
    }
    bail!("stream ended without exit event")
}

fn remap_term(local: Option<&str>) -> String {
    const SAFE: &[&str] = &[
        "xterm",
        "xterm-256color",
        "screen",
        "screen-256color",
        "tmux",
        "tmux-256color",
        "linux",
        "vt100",
        "vt220",
        "dumb",
    ];
    match local {
        Some(t) if SAFE.contains(&t) => t.to_string(),
        _ => "xterm-256color".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::{parse_connect_http_error, RpcError, Transport};
    use reqwest::header::AUTHORIZATION;

    #[test]
    fn rpc_error_display_preserves_code() {
        let error = RpcError {
            code: "permission_denied".into(),
            message: "access denied".into(),
        };

        assert_eq!(error.to_string(), "permission_denied: access denied");
    }

    #[test]
    fn connect_http_error_requires_non_empty_code() {
        assert!(parse_connect_http_error(r#"{"message":"gateway failure"}"#).is_none());
        assert!(parse_connect_http_error(r#"{"code":"","message":"failure"}"#).is_none());

        let error = parse_connect_http_error(
            r#"{"code":"unauthenticated","message":"invalid credentials"}"#,
        )
        .unwrap();
        assert_eq!(error.code, "unauthenticated");
        assert_eq!(error.message, "invalid credentials");
    }

    #[test]
    fn unary_user_is_sent_as_basic_auth() {
        let transport = Transport::new("http://127.0.0.1", "api-key", "sandbox-id", None).unwrap();
        let request = transport
            .unary_request("filesystem.Filesystem", "Stat", Some("app"))
            .build()
            .unwrap();

        assert_eq!(
            request.headers().get(AUTHORIZATION).unwrap(),
            "Basic YXBwOg=="
        );

        let request = transport
            .unary_request("filesystem.Filesystem", "Stat", None)
            .build()
            .unwrap();
        assert!(!request.headers().contains_key(AUTHORIZATION));
    }

    #[test]
    fn envd_access_token_is_sent_on_connect_requests() {
        let transport = Transport::new(
            "http://127.0.0.1",
            "api-key",
            "sandbox-id",
            Some("envd-token"),
        )
        .unwrap();

        let request = transport
            .unary_request("process.Process", "List", None)
            .build()
            .unwrap();

        assert_eq!(
            request.headers().get("x-access-token").unwrap(),
            "envd-token"
        );
    }
}
