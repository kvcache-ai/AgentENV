//! Private transports for the executor and native Codex workspace helpers.

use super::runtime::write_private;
use anyhow::{bail, ensure, Context, Result};
use futures::{Sink, SinkExt, Stream, StreamExt};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, UnixListener, UnixStream};
use tokio::task::{JoinHandle, JoinSet};
use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Error as WsError, Message};
use tokio_tungstenite::{accept_hdr_async, client_async, connect_async, WebSocketStream};

pub(super) struct Channel {
    sink: Pin<Box<dyn Sink<Message, Error = WsError> + Send>>,
    stream: Pin<Box<dyn Stream<Item = std::result::Result<Message, WsError>> + Send>>,
}

impl Channel {
    fn new<S: AsyncRead + AsyncWrite + Unpin + Send + 'static>(socket: WebSocketStream<S>) -> Self {
        let (sink, stream) = socket.split();
        Self {
            sink: Box::pin(sink),
            stream: Box::pin(stream),
        }
    }

    pub async fn unix(path: &Path) -> Result<Self> {
        let (socket, _) = client_async("ws://localhost", UnixStream::connect(path).await?).await?;
        Ok(Self::new(socket))
    }

    async fn send(&mut self, message: &Value) -> Result<()> {
        self.sink
            .send(Message::Text(serde_json::to_string(message)?.into()))
            .await?;
        Ok(())
    }

    async fn receive(&mut self) -> Result<Value> {
        loop {
            match self
                .stream
                .next()
                .await
                .context("Codex connection closed")??
            {
                Message::Text(text) => return Ok(serde_json::from_str(&text)?),
                Message::Binary(bytes) => return Ok(serde_json::from_slice(&bytes)?),
                Message::Ping(bytes) => self.sink.send(Message::Pong(bytes)).await?,
                Message::Close(_) => bail!("Codex connection closed"),
                _ => (),
            }
        }
    }
}

#[derive(Clone)]
pub(super) struct Proxy {
    url: String,
    sandbox: String,
    traffic_token: String,
}

impl Proxy {
    pub fn new(api_url: &str, sandbox: &crate::client::sandboxes::Sandbox) -> Result<Self> {
        let mut url = reqwest::Url::parse(api_url)?;
        let scheme = match url.scheme() {
            "http" => "ws",
            "https" => "wss",
            _ => bail!("Invalid AgentENV URL"),
        };
        url.set_scheme(scheme)
            .map_err(|()| anyhow::anyhow!("Invalid proxy URL"))?;
        url.set_path(&format!("{}/proxy", url.path().trim_end_matches('/')));
        Ok(Self {
            url: url.to_string(),
            sandbox: sandbox.sandbox_id.clone(),
            traffic_token: sandbox
                .traffic_access_token
                .clone()
                .context("AgentENV did not return a private traffic token")?,
        })
    }

    async fn connect(&self, port: u16, bearer: Option<&str>) -> Result<Channel> {
        // Construct the upstream request ourselves: no model/platform key or
        // caller-supplied headers may be forwarded into the sandbox.
        let mut request = self.url.as_str().into_client_request()?;
        let headers = request.headers_mut();
        headers.insert("e2b-sandbox-id", self.sandbox.parse()?);
        headers.insert("e2b-sandbox-port", port.to_string().parse()?);
        headers.insert("e2b-traffic-access-token", self.traffic_token.parse()?);
        if let Some(token) = bearer {
            headers.insert("authorization", format!("Bearer {token}").parse()?);
        }
        let (socket, _) =
            tokio::time::timeout(Duration::from_secs(15), connect_async(request)).await??;
        Ok(Channel::new(socket))
    }

    pub(super) async fn check(&self, port: u16, bearer: Option<&str>) -> Result<()> {
        let mut channel = self.connect(port, bearer).await?;
        channel.sink.send(Message::Close(None)).await?;
        Ok(())
    }

    // tungstenite's handshake callback requires its unboxed HTTP error type.
    #[allow(clippy::result_large_err)]
    pub async fn relay(&self) -> Result<(String, Service)> {
        let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await?;
        let path = format!("/{}", uuid::Uuid::new_v4().simple());
        let url = format!("ws://{}{path}", listener.local_addr()?);
        let proxy = self.clone();
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let Ok((stream, _)) = accepted else { break; };
                        let path = path.clone();
                        let proxy = proxy.clone();
                        connections.spawn(async move {
                            let socket = tokio::time::timeout(Duration::from_secs(10), accept_hdr_async(stream,
                                move |request: &tokio_tungstenite::tungstenite::handshake::server::Request, response| {
                                    if request.uri().path() != path || request.headers().contains_key("origin") {
                                        return Err(tokio_tungstenite::tungstenite::http::Response::builder().status(403)
                                            .body(Some("Forbidden".into())).unwrap());
                                    }
                                    Ok(response)
                                })).await??;
                            let mut downstream = Channel::new(socket);
                            let mut upstream = proxy.connect(4501, None).await?;
                            loop {
                                tokio::select! {
                                    frame = downstream.stream.next() => match frame {
                                        Some(Ok(Message::Close(_))) | None => break,
                                        Some(frame) => upstream.sink.send(frame?).await?,
                                    },
                                    frame = upstream.stream.next() => match frame {
                                        Some(Ok(Message::Close(_))) | None => break,
                                        Some(frame) => downstream.sink.send(frame?).await?,
                                    },
                                }
                            }
                            Ok::<_, anyhow::Error>(())
                        });
                    },
                    _ = connections.join_next(), if !connections.is_empty() => (),
                }
            }
        });
        Ok((url, Service(task)))
    }
}

pub(super) struct Service(JoinHandle<()>);
impl Drop for Service {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[derive(Default)]
pub(super) struct Events {
    resumable: HashMap<String, bool>,
    pub thread_id: Option<String>,
    pub error: Option<String>,
    session: PathBuf,
}

pub(super) type SharedEvents = Arc<Mutex<Events>>;

impl Events {
    pub fn new(session: &Path) -> Result<SharedEvents> {
        let thread_id = match std::fs::read_to_string(session.join("thread-id")) {
            Ok(id) => Some(id.trim().to_owned()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error.into()),
        };
        Ok(Arc::new(Mutex::new(Self {
            session: session.to_owned(),
            thread_id,
            ..Self::default()
        })))
    }

    pub fn observe(&mut self, message: &Value) -> Result<()> {
        let method = message["method"].as_str().unwrap_or_default();
        let params = &message["params"];
        let thread = params
            .get("thread")
            .or_else(|| message["result"].get("thread"));
        if let Some(thread) = thread.filter(|t| t.get("ephemeral").is_some()) {
            if let Some(id) = thread["id"].as_str() {
                self.resumable.insert(
                    id.into(),
                    thread["ephemeral"] != true
                        && thread["parentThreadId"].is_null()
                        && thread["threadSource"] != "system",
                );
            }
        }
        if method == "turn/started" {
            if let Some(thread_id) = params["threadId"].as_str().filter(|id| {
                self.resumable.get(*id) == Some(&true) && self.thread_id.as_deref() != Some(*id)
            }) {
                write_private(&self.session.join("thread-id"), thread_id.as_bytes())?;
                self.thread_id = Some(thread_id.into());
            }
        }
        Ok(())
    }
}

pub(super) struct Rpc {
    channel: Channel,
    counter: u64,
}

impl Rpc {
    pub async fn connect(path: &Path) -> Result<Self> {
        let mut rpc = Self {
            channel: Channel::unix(path).await?,
            counter: 0,
        };
        rpc.call(
            "initialize",
            json!({"clientInfo":{"name":"agentenv","version":"1"},
            "capabilities":{"experimentalApi":true}}),
        )
        .await?;
        rpc.channel
            .send(&json!({"method":"initialized","params":{}}))
            .await?;
        Ok(rpc)
    }

    pub async fn call(&mut self, method: &str, params: Value) -> Result<Value> {
        self.counter += 1;
        let id = self.counter;
        self.channel
            .send(&json!({"id":id,"method":method,"params":params}))
            .await?;
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let message = tokio::time::timeout(
                deadline.saturating_duration_since(Instant::now()),
                self.channel.receive(),
            )
            .await
            .with_context(|| format!("Codex {method} timed out"))??;
            if message["id"] == id && message.get("method").is_none() {
                if let Some(error) = message.get("error") {
                    bail!("Codex {method}: {error}");
                }
                return Ok(message["result"].clone());
            }
        }
    }
}

fn sandbox_method(method: &str) -> bool {
    method == "command/exec"
        || method.starts_with("command/exec/")
        || method.starts_with("fs/")
        || method.starts_with("process/")
        || method == "fuzzyFileSearch"
        || method.starts_with("fuzzyFileSearch/")
}

pub(super) async fn gateway(
    path: &Path,
    harness: PathBuf,
    proxy: Proxy,
    token: String,
    events: SharedEvents,
) -> Result<Service> {
    let listener = UnixListener::bind(path)?;
    Ok(Service(tokio::spawn(async move {
        let mut connections = JoinSet::new();
        loop {
            tokio::select! {
                accepted = listener.accept() => {
                    let Ok((stream, _)) = accepted else { break; };
                    let (harness, proxy, token, events) = (harness.clone(), proxy.clone(), token.clone(), events.clone());
                    connections.spawn(async move {
                        let result = forward(stream, &harness, &proxy, &token, &events).await;
                        if let Err(error) = result { events.lock().unwrap().error = Some(format!("{error:#}")); }
                    });
                },
                _ = connections.join_next(), if !connections.is_empty() => (),
            }
        }
    })))
}

// tungstenite's handshake callback requires its unboxed HTTP error type.
#[allow(clippy::result_large_err)]
async fn forward(
    stream: UnixStream,
    harness: &Path,
    proxy: &Proxy,
    token: &str,
    events: &SharedEvents,
) -> Result<()> {
    let socket = accept_hdr_async(
        stream,
        |request: &tokio_tungstenite::tungstenite::handshake::server::Request, response| {
            if request.headers().contains_key("origin") {
                return Err(tokio_tungstenite::tungstenite::http::Response::builder()
                    .status(403)
                    .body(None)
                    .unwrap());
            }
            Ok(response)
        },
    )
    .await?;
    let mut ui = Channel::new(socket);
    let first = tokio::time::timeout(Duration::from_secs(15), ui.receive()).await??;
    ensure!(
        first["method"] == "initialize",
        "Expected Codex initialization"
    );
    let mut main = Channel::unix(harness).await?;
    let mut guest = proxy.connect(4502, Some(token)).await?;
    guest.send(&first).await?;
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let reply = guest.receive().await?;
            if reply["id"] == first["id"] {
                ensure!(
                    reply.get("error").is_none(),
                    "Sandbox UI initialization failed"
                );
                return Ok::<_, anyhow::Error>(());
            }
        }
    })
    .await??;
    main.send(&first).await?;
    let mut pending: HashMap<String, (bool, Value)> = HashMap::new();
    let mut sequence = 0_u64;
    loop {
        let (mut message, from_main) = tokio::select! {
            message = ui.receive() => {
                // A normal /quit closes this connection before the launcher
                // stops the harness; it is not a gateway failure.
                let Ok(mut message) = message else { return Ok(()); };
                let to_guest = if let Some(method) = message["method"].as_str() {
                    if method == "initialized" {
                        guest.send(&message).await?;
                        main.send(&message).await?;
                        continue;
                    }
                    sandbox_method(method)
                } else {
                    let (origin, id) = pending.remove(message["id"].as_str().unwrap_or_default()).context("Unknown Codex response")?;
                    message["id"] = id;
                    !origin
                };
                if to_guest { guest.send(&message).await?; } else { main.send(&message).await?; }
                continue;
            },
            message = main.receive() => (message?, true),
            message = guest.receive() => (message?, false),
        };
        if from_main {
            events.lock().unwrap().observe(&message)?;
        } else if message["method"]
            .as_str()
            .is_some_and(|m| !sandbox_method(m))
        {
            continue;
        }
        if message.get("method").is_some() && message.get("id").is_some() {
            sequence += 1;
            let key = format!("agentenv:{sequence}");
            pending.insert(key.clone(), (from_main, message["id"].clone()));
            message["id"] = json!(key);
        }
        ui.send(&message).await?;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[allow(clippy::result_large_err)] // Required handshake callback signature.
    async fn relay_authenticates_only_the_owned_sandbox_and_preserves_binary_frames() -> Result<()>
    {
        let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await?;
        let proxy = Proxy {
            url: format!("ws://{}/proxy", listener.local_addr()?),
            sandbox: "owned-sandbox".into(),
            traffic_token: "traffic-secret".into(),
        };
        let upstream = tokio::spawn(async move {
            let (stream, _) = listener.accept().await?;
            let mut socket = accept_hdr_async(
                stream,
                |request: &tokio_tungstenite::tungstenite::handshake::server::Request, response| {
                    assert_eq!(request.uri().path(), "/proxy");
                    assert_eq!(request.headers()["e2b-sandbox-id"], "owned-sandbox");
                    assert_eq!(request.headers()["e2b-sandbox-port"], "4501");
                    assert_eq!(
                        request.headers()["e2b-traffic-access-token"],
                        "traffic-secret"
                    );
                    assert!(!request.headers().contains_key("authorization"));
                    assert!(!request.headers().contains_key("x-api-key"));
                    Ok(response)
                },
            )
            .await?;
            let message = socket.next().await.context("No binary frame")??;
            socket.send(message).await?;
            Ok::<_, anyhow::Error>(())
        });
        let (url, _service) = proxy.relay().await?;
        let bad = url.rsplit_once('/').unwrap().0.to_owned() + "/wrong-secret";
        assert!(connect_async(bad).await.is_err());
        let mut browser = url.as_str().into_client_request()?;
        browser
            .headers_mut()
            .insert("origin", "https://untrusted.invalid".parse()?);
        assert!(connect_async(browser).await.is_err());
        let mut request = url.as_str().into_client_request()?;
        request
            .headers_mut()
            .insert("authorization", "Bearer model-secret".parse()?);
        request
            .headers_mut()
            .insert("x-api-key", "platform-secret".parse()?);
        request
            .headers_mut()
            .insert("e2b-sandbox-id", "wrong-sandbox".parse()?);
        let (mut socket, _) = connect_async(request).await?;
        let payload = Message::Binary(vec![0, 1, 255, 10].into());
        socket.send(payload.clone()).await?;
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), socket.next())
                .await?
                .context("No reply")??,
            payload
        );
        upstream.await??;
        Ok(())
    }

    #[test]
    fn helpers_stay_in_guest_and_model_apis_stay_on_host() {
        for method in [
            "command/exec",
            "command/exec/write",
            "fs/readFile",
            "process/list",
            "fuzzyFileSearch",
        ] {
            assert!(sandbox_method(method));
        }
        for method in [
            "thread/start",
            "turn/start",
            "account/read",
            "config/read",
            "command/executor",
        ] {
            assert!(!sandbox_method(method));
        }
    }

    #[test]
    fn helper_threads_cannot_replace_resumable_thread() -> Result<()> {
        let root = tempfile::tempdir()?;
        let events = Events::new(root.path())?;
        let mut events = events.lock().unwrap();
        for thread in [
            json!({"id":"conversation","ephemeral":false}),
            json!({"id":"title","ephemeral":true}),
            json!({"id":"child","ephemeral":false,"parentThreadId":"conversation"}),
            json!({"id":"system","ephemeral":false,"threadSource":"system"}),
        ] {
            let id = thread["id"].as_str().unwrap();
            events.observe(&json!({"method":"thread/started","params":{"thread":thread}}))?;
            events.observe(
                &json!({"method":"turn/started","params":{"threadId":id,"turn":{"id":"turn"}}}),
            )?;
        }
        assert_eq!(
            std::fs::read_to_string(root.path().join("thread-id"))?,
            "conversation"
        );
        Ok(())
    }

    #[test]
    fn conversation_id_survives_restart_without_rewriting_unchanged_ids() -> Result<()> {
        use std::os::unix::fs::MetadataExt;
        let root = tempfile::tempdir()?;
        let path = root.path().join("thread-id");
        let turn = |id| json!({"method":"turn/started","params":{"threadId":id}});
        for id in ["conversation", "conversation", "new-conversation"] {
            let events = Events::new(root.path())?;
            let mut events = events.lock().unwrap();
            let previous = std::fs::File::open(&path).ok();
            let unchanged = events.thread_id.as_deref() == Some(id);
            events.observe(&json!({"result":{"thread":{"id":id,"ephemeral":false}}}))?;
            events.observe(&turn(id))?;
            if unchanged {
                assert_eq!(
                    previous.unwrap().metadata()?.ino(),
                    std::fs::metadata(&path)?.ino()
                );
            }
            let saved = std::fs::File::open(&path)?;
            events.observe(&turn(id))?;
            assert_eq!(saved.metadata()?.ino(), std::fs::metadata(&path)?.ino());
            assert_eq!(std::fs::read_to_string(&path)?, id);
            assert_eq!(
                Events::new(root.path())?
                    .lock()
                    .unwrap()
                    .thread_id
                    .as_deref(),
                Some(id)
            );
        }
        Ok(())
    }

    #[test]
    fn conversation_id_io_errors_are_not_treated_as_success() -> Result<()> {
        let root = tempfile::tempdir()?;
        let events = Events::new(root.path())?;
        let mut events = events.lock().unwrap();
        let path = root.path().join("thread-id");
        std::fs::create_dir(&path)?;
        assert!(Events::new(root.path()).is_err());
        events.observe(&json!({"result":{"thread":{"id":"conversation","ephemeral":false}}}))?;
        let turn = json!({"method":"turn/started","params":{"threadId":"conversation"}});
        assert!(events.observe(&turn).is_err());
        assert!(events.thread_id.is_none());
        std::fs::remove_dir(&path)?;
        events.observe(&turn)?;
        assert_eq!(std::fs::read_to_string(path)?, "conversation");
        Ok(())
    }
}
