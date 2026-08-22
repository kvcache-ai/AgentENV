use std::collections::HashMap;
use std::fmt;
use std::fs::File;
use std::io::{Read, Write};
use std::net::{IpAddr, Ipv4Addr, Shutdown, SocketAddr, TcpListener, TcpStream};
use std::os::fd::{AsFd, AsRawFd};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex, RwLock};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use anyhow::{Context, Result};
use httparse::Status;
use nix::sched::{setns, CloneFlags};
use tls_parser::{parse_tls_handshake_msg_client_hello, parse_tls_raw_record, TlsRecordType};
use tracing::{debug, warn};

use super::resolver::HostNetResolver;
use super::SandboxNetworkPolicy;

const MAX_PREFACE_BYTES: usize = 64 * 1024;
const PREFACE_TIMEOUT: Duration = Duration::from_secs(10);
const UPSTREAM_TIMEOUT: Duration = Duration::from_secs(30);
const EGRESS_PROXY_PORT: u16 = 15000;
// Bound resource use without imposing an idle timeout on established streams;
// E2B permits long-lived application connections.
const MAX_CONNECTIONS_PER_PROXY: usize = 256;
const SO_ORIGINAL_DST: libc::c_int = 80;

struct ListenerHandle {
    stop: mpsc::Sender<()>,
    join: JoinHandle<()>,
}

enum UpstreamDecision {
    Forward(SocketAddr),
    Deny,
    Unavailable,
}

#[derive(Clone)]
struct ActiveEgressPolicy {
    policy: SandboxNetworkPolicy,
}

struct ConnectionPermit {
    active: Arc<AtomicUsize>,
}

struct ConnectionSockets {
    client: TcpStream,
    upstream: Option<TcpStream>,
    cancel: Arc<AtomicBool>,
    closed: bool,
}

struct ConnectionState {
    sockets: HashMap<usize, Arc<Mutex<ConnectionSockets>>>,
    joins: HashMap<usize, JoinHandle<()>>,
}

impl Drop for ConnectionPermit {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Namespace-local transparent proxy for egress capabilities that require
/// connection inspection or mediation.
pub(crate) struct EgressProxy {
    active: RwLock<HashMap<Ipv4Addr, ActiveEgressPolicy>>,
    pending: Mutex<HashMap<Ipv4Addr, ActiveEgressPolicy>>,
    listeners: Mutex<HashMap<Ipv4Addr, ListenerHandle>>,
    listener_start: Mutex<()>,
    active_connections: Arc<AtomicUsize>,
    next_connection_id: AtomicUsize,
    connections: Mutex<HashMap<Ipv4Addr, ConnectionState>>,
    resolver: HostNetResolver,
}

impl fmt::Debug for EgressProxy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let active_policy_count = self
            .active
            .read()
            .map(|policies| policies.len())
            .unwrap_or(0);
        let pending_policy_count = self
            .pending
            .lock()
            .map(|policies| policies.len())
            .unwrap_or(0);
        let listener_count = self
            .listeners
            .lock()
            .map(|listeners| listeners.len())
            .unwrap_or(0);

        formatter
            .debug_struct("EgressProxy")
            .field("port", &EGRESS_PROXY_PORT)
            .field("active_policy_count", &active_policy_count)
            .field("pending_policy_count", &pending_policy_count)
            .field("listener_count", &listener_count)
            .finish()
    }
}

impl EgressProxy {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            active: RwLock::new(HashMap::new()),
            pending: Mutex::new(HashMap::new()),
            listeners: Mutex::new(HashMap::new()),
            listener_start: Mutex::new(()),
            active_connections: Arc::new(AtomicUsize::new(0)),
            next_connection_id: AtomicUsize::new(0),
            connections: Mutex::new(HashMap::new()),
            resolver: HostNetResolver::new(),
        })
    }

    pub(crate) fn port(&self) -> u16 {
        EGRESS_PROXY_PORT
    }

    /// Ensure that a listener is running in the sandbox namespace for the given host interaction IP.
    pub(crate) fn ensure_listener(
        self: &Arc<Self>,
        host_interaction_ip: Ipv4Addr,
        netns_path: &Path,
    ) -> Result<()> {
        let _startup_guard = self
            .listener_start
            .lock()
            .expect("egress proxy listener startup lock poisoned");
        if self
            .listeners
            .lock()
            .expect("egress proxy listener lock poisoned")
            .contains_key(&host_interaction_ip)
        {
            return Ok(());
        }

        let netns = File::open(netns_path)
            .with_context(|| format!("open egress proxy namespace {}", netns_path.display()))?;
        let (stop_tx, stop_rx) = mpsc::channel();
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        let proxy = Arc::clone(self);
        let join = thread::Builder::new()
            .name(format!("agentenv-egress-proxy-{host_interaction_ip}"))
            .spawn(move || {
                if let Err(error) = setns(netns.as_fd(), CloneFlags::CLONE_NEWNET) {
                    let _ = ready_tx.send(Err(format!("enter egress proxy namespace: {error}")));
                    return;
                }
                let listener = match TcpListener::bind((Ipv4Addr::UNSPECIFIED, EGRESS_PROXY_PORT)) {
                    Ok(listener) => listener,
                    Err(error) => {
                        let _ = ready_tx.send(Err(format!("bind egress proxy port: {error}")));
                        return;
                    }
                };
                if let Err(error) = listener.set_nonblocking(true) {
                    let _ = ready_tx.send(Err(format!("set egress proxy nonblocking: {error}")));
                    return;
                }
                let _ = ready_tx.send(Ok(()));
                run_namespace_listener(listener, stop_rx, proxy, host_interaction_ip);
            })
            .context("spawn egress proxy namespace listener")?;

        match ready_rx.recv() {
            Ok(Ok(())) => {
                self.listeners
                    .lock()
                    .expect("egress proxy listener lock poisoned")
                    .insert(
                        host_interaction_ip,
                        ListenerHandle {
                            stop: stop_tx,
                            join,
                        },
                    );
                Ok(())
            }
            Ok(Err(error)) => {
                let _ = join.join();
                Err(anyhow::anyhow!(error))
            }
            Err(error) => {
                let _ = join.join();
                Err(anyhow::anyhow!(
                    "egress proxy listener startup channel: {error}"
                ))
            }
        }
    }

    /// Stage a policy without making it visible to new connections. This lets
    /// callers update the namespace redirect first and then activate the new
    /// policy, preserving the old policy throughout the transition.
    pub(crate) fn prepare(&self, host_interaction_ip: Ipv4Addr, policy: &SandboxNetworkPolicy) {
        self.pending
            .lock()
            .expect("egress proxy pending lock poisoned")
            .insert(
                host_interaction_ip,
                ActiveEgressPolicy {
                    policy: policy.clone(),
                },
            );
    }

    pub(crate) fn activate(&self, host_interaction_ip: Ipv4Addr) {
        let pending = self
            .pending
            .lock()
            .expect("egress proxy pending lock poisoned")
            .remove(&host_interaction_ip);
        if let Some(policy) = pending {
            self.active
                .write()
                .expect("egress proxy active lock poisoned")
                .insert(host_interaction_ip, policy);
        }
    }

    pub(crate) fn discard_pending(&self, host_interaction_ip: Ipv4Addr) {
        self.pending
            .lock()
            .expect("egress proxy pending lock poisoned")
            .remove(&host_interaction_ip);
    }

    pub(crate) fn remove(&self, host_interaction_ip: Ipv4Addr) {
        self.pending
            .lock()
            .expect("egress proxy pending lock poisoned")
            .remove(&host_interaction_ip);
        self.active
            .write()
            .expect("egress proxy active lock poisoned")
            .remove(&host_interaction_ip);
        self.stop_listener(host_interaction_ip);
        self.stop_connections(host_interaction_ip);
    }

    pub(crate) fn has_active(&self, host_interaction_ip: Ipv4Addr) -> bool {
        self.active
            .read()
            .expect("egress proxy active lock poisoned")
            .contains_key(&host_interaction_ip)
    }

    pub(crate) fn shutdown(&self) {
        let listeners = std::mem::take(
            &mut *self
                .listeners
                .lock()
                .expect("egress proxy listener lock poisoned"),
        );
        for (_, listener) in listeners {
            let _ = listener.stop.send(());
            let _ = listener.join.join();
        }
        let hosts = self
            .connections
            .lock()
            .expect("egress proxy connection lock poisoned")
            .keys()
            .copied()
            .collect::<Vec<_>>();
        for host_interaction_ip in hosts {
            self.stop_connections(host_interaction_ip);
        }
        self.resolver.shutdown();
    }

    fn stop_listener(&self, host_interaction_ip: Ipv4Addr) {
        if let Some(listener) = self
            .listeners
            .lock()
            .expect("egress proxy listener lock poisoned")
            .remove(&host_interaction_ip)
        {
            let _ = listener.stop.send(());
            let _ = listener.join.join();
        }
    }

    fn active_policy(&self, host_interaction_ip: Ipv4Addr) -> Option<ActiveEgressPolicy> {
        self.active
            .read()
            .expect("egress proxy active lock poisoned")
            .get(&host_interaction_ip)
            .cloned()
    }

    fn register_connection(
        &self,
        host_interaction_ip: Ipv4Addr,
        client: &TcpStream,
    ) -> std::io::Result<(usize, Arc<AtomicBool>)> {
        let id = self.next_connection_id.fetch_add(1, Ordering::Relaxed);
        let cancel = Arc::new(AtomicBool::new(false));
        let sockets = Arc::new(Mutex::new(ConnectionSockets {
            client: client.try_clone()?,
            upstream: None,
            cancel: Arc::clone(&cancel),
            closed: false,
        }));
        self.connections
            .lock()
            .expect("egress proxy connection lock poisoned")
            .entry(host_interaction_ip)
            .or_insert_with(|| ConnectionState {
                sockets: HashMap::new(),
                joins: HashMap::new(),
            })
            .sockets
            .insert(id, sockets);
        Ok((id, cancel))
    }

    fn register_join(
        &self,
        host_interaction_ip: Ipv4Addr,
        id: usize,
        join: JoinHandle<()>,
    ) -> Option<JoinHandle<()>> {
        let mut connections = self
            .connections
            .lock()
            .expect("egress proxy connection lock poisoned");
        let Some(state) = connections.get_mut(&host_interaction_ip) else {
            return Some(join);
        };
        state.joins.insert(id, join);
        None
    }

    fn set_upstream_connection(
        &self,
        host_interaction_ip: Ipv4Addr,
        id: usize,
        upstream: &TcpStream,
    ) {
        let entry = self.connections.lock().ok().and_then(|connections| {
            connections
                .get(&host_interaction_ip)?
                .sockets
                .get(&id)
                .cloned()
        });
        let Some(entry) = entry else { return };
        let Ok(upstream) = upstream.try_clone() else {
            return;
        };
        let Ok(mut sockets) = entry.lock() else {
            return;
        };
        if sockets.closed {
            let _ = upstream.shutdown(Shutdown::Both);
        } else {
            sockets.upstream = Some(upstream);
        }
    }

    fn unregister_connection(&self, host_interaction_ip: Ipv4Addr, id: usize) {
        if let Ok(mut connections) = self.connections.lock() {
            if let Some(state) = connections.get_mut(&host_interaction_ip) {
                state.sockets.remove(&id);
            }
        }
    }

    fn reap_finished(&self, host_interaction_ip: Ipv4Addr) {
        let joins = self
            .connections
            .lock()
            .ok()
            .and_then(|mut connections| {
                let state = connections.get_mut(&host_interaction_ip)?;
                let finished = state
                    .joins
                    .iter()
                    .filter_map(|(id, join)| join.is_finished().then_some(*id))
                    .collect::<Vec<_>>();
                Some(
                    finished
                        .into_iter()
                        .filter_map(|id| state.joins.remove(&id))
                        .collect::<Vec<_>>(),
                )
            })
            .unwrap_or_default();
        for join in joins {
            let _ = join.join();
        }
    }

    fn stop_connections(&self, host_interaction_ip: Ipv4Addr) {
        let state = self
            .connections
            .lock()
            .expect("egress proxy connection lock poisoned")
            .remove(&host_interaction_ip);
        let Some(state) = state else { return };
        for sockets in state.sockets.values() {
            if let Ok(mut sockets) = sockets.lock() {
                sockets.closed = true;
                sockets.cancel.store(true, Ordering::Release);
                let _ = sockets.client.shutdown(Shutdown::Both);
                if let Some(upstream) = sockets.upstream.as_ref() {
                    let _ = upstream.shutdown(Shutdown::Both);
                }
            }
        }
        for join in state.joins.into_values() {
            let _ = join.join();
        }
    }
}

impl Drop for EgressProxy {
    fn drop(&mut self) {
        let listeners = std::mem::take(
            self.listeners
                .get_mut()
                .expect("egress proxy listener lock poisoned"),
        );
        for (_, listener) in listeners {
            let _ = listener.stop.send(());
            let _ = listener.join.join();
        }
        let hosts = self
            .connections
            .get_mut()
            .expect("egress proxy connection lock poisoned")
            .keys()
            .copied()
            .collect::<Vec<_>>();
        for host_interaction_ip in hosts {
            self.stop_connections(host_interaction_ip);
        }
    }
}

fn run_namespace_listener(
    listener: TcpListener,
    stop_rx: mpsc::Receiver<()>,
    proxy: Arc<EgressProxy>,
    host_interaction_ip: Ipv4Addr,
) {
    loop {
        proxy.reap_finished(host_interaction_ip);
        if stop_rx.try_recv().is_ok() {
            break;
        }
        match listener.accept() {
            Ok((stream, _peer)) => {
                let permit = match proxy.active_connections.fetch_update(
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                    |active| (active < MAX_CONNECTIONS_PER_PROXY).then_some(active + 1),
                ) {
                    Ok(_) => ConnectionPermit {
                        active: Arc::clone(&proxy.active_connections),
                    },
                    Err(_) => {
                        warn!(%host_interaction_ip, "egress proxy connection limit reached");
                        drop(stream);
                        continue;
                    }
                };
                let (connection_id, cancel) = match proxy
                    .register_connection(host_interaction_ip, &stream)
                {
                    Ok(connection) => connection,
                    Err(error) => {
                        warn!(%host_interaction_ip, %error, "clone egress proxy connection failed");
                        drop(permit);
                        drop(stream);
                        continue;
                    }
                };
                let handler_proxy = Arc::clone(&proxy);
                let join = thread::Builder::new()
                    .name("agentenv-egress-proxy-conn".to_string())
                    .spawn(move || {
                        let _permit = permit;
                        handle_connection(
                            stream,
                            host_interaction_ip,
                            connection_id,
                            cancel,
                            handler_proxy,
                        );
                    });
                match join {
                    Ok(join) => {
                        if let Some(join) =
                            proxy.register_join(host_interaction_ip, connection_id, join)
                        {
                            let _ = join.join();
                        }
                    }
                    Err(error) => {
                        proxy.unregister_connection(host_interaction_ip, connection_id);
                        warn!(%host_interaction_ip, %error, "spawn egress proxy connection handler failed");
                        // Dropping the stream rejects this connection and dropping
                        // the permit makes the bounded handler count recover.
                    }
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(5));
            }
            Err(error) => {
                warn!(error = %error, "egress proxy accept failed");
                thread::sleep(Duration::from_millis(50));
            }
        }
    }
}

fn handle_connection(
    mut client: TcpStream,
    host_interaction_ip: Ipv4Addr,
    connection_id: usize,
    cancel: Arc<AtomicBool>,
    proxy: Arc<EgressProxy>,
) {
    let _registration = ConnectionRegistration {
        proxy: Arc::clone(&proxy),
        host_interaction_ip,
        connection_id,
    };
    let Some(active_policy) = proxy.active_policy(host_interaction_ip) else {
        return;
    };
    if cancel.load(Ordering::Acquire) {
        return;
    }

    let _ = client.set_read_timeout(Some(PREFACE_TIMEOUT));
    let mut preface = Vec::with_capacity(4096);
    let mut scratch = [0_u8; 4096];
    let (original_ip, original_port) = match original_destination(&client) {
        Ok(destination) => destination,
        Err(error) => {
            debug!(error = %error, %host_interaction_ip, "egress proxy could not read original destination");
            return;
        }
    };
    let original_addr = SocketAddr::new(IpAddr::V4(original_ip), original_port);

    // E2B keeps an explicit CIDR grant usable even when the same policy also
    // has domain rules. Connect directly to that original destination; only
    // the domain branch needs protocol inspection and trusted DNS resolution.
    let (hostname, upstream) = if active_policy.policy.is_ip_allowed(original_ip) {
        (None, original_addr)
    } else {
        let hostname = loop {
            match parse_protocol_host(&preface, original_port) {
                Ok(Some(host)) => break host,
                Ok(None) if preface.len() >= MAX_PREFACE_BYTES => return,
                Ok(None) => {}
                Err(error) => {
                    debug!(%error, %host_interaction_ip, "egress proxy could not inspect connection");
                    return;
                }
            }
            match client.read(&mut scratch) {
                Ok(0) => return,
                Ok(read) => preface.extend_from_slice(&scratch[..read]),
                Err(_) => return,
            }
        };
        let upstream = match select_upstream(
            &active_policy.policy,
            &proxy.resolver,
            &hostname,
            original_addr,
            Some(&cancel),
        ) {
            UpstreamDecision::Forward(upstream) => upstream,
            UpstreamDecision::Deny => {
                debug!(
                    %host_interaction_ip,
                    hostname = %hostname,
                    original_ip = %original_ip,
                    original_port,
                    "egress proxy denied connection"
                );
                return;
            }
            UpstreamDecision::Unavailable => {
                debug!(
                    %host_interaction_ip,
                    hostname = %hostname,
                    original_ip = %original_ip,
                    original_port,
                    "egress proxy could not select an eligible upstream"
                );
                return;
            }
        };
        (Some(hostname), upstream)
    };
    if cancel.load(Ordering::Acquire) {
        return;
    }

    let mut upstream_stream = match TcpStream::connect_timeout(&upstream, UPSTREAM_TIMEOUT) {
        Ok(stream) => stream,
        Err(error) => {
            debug!(
                %host_interaction_ip,
                ?hostname,
                %upstream,
                %error,
                "egress proxy upstream connect failed"
            );
            return;
        }
    };
    if cancel.load(Ordering::Acquire) {
        let _ = upstream_stream.shutdown(Shutdown::Both);
        return;
    }
    proxy.set_upstream_connection(host_interaction_ip, connection_id, &upstream_stream);
    let _ = upstream_stream.set_read_timeout(None);
    let _ = client.set_read_timeout(None);
    if let Err(error) = upstream_stream.write_all(&preface) {
        debug!(
            %host_interaction_ip,
            ?hostname,
            %upstream,
            %error,
            "egress proxy upstream preface write failed"
        );
        return;
    }
    relay(client, upstream_stream);
}

struct ConnectionRegistration {
    proxy: Arc<EgressProxy>,
    host_interaction_ip: Ipv4Addr,
    connection_id: usize,
}

impl Drop for ConnectionRegistration {
    fn drop(&mut self) {
        self.proxy
            .unregister_connection(self.host_interaction_ip, self.connection_id);
    }
}

/// Applies the proxy-backed capabilities in the active policy and selects the
/// upstream connection. Domain matches are resolved in the host namespace and
/// connected to the selected trusted address, matching E2B's trusted-resolution
/// semantics. IP/CIDR matches continue to use the guest's original destination.
fn select_upstream(
    policy: &SandboxNetworkPolicy,
    resolver: &HostNetResolver,
    hostname: &str,
    original_destination: SocketAddr,
    cancel: Option<&AtomicBool>,
) -> UpstreamDecision {
    let SocketAddr::V4(original_destination) = original_destination else {
        return UpstreamDecision::Unavailable;
    };
    // E2B treats allowOut entries additively: an explicitly allowed CIDR is
    // still valid when domain interception is enabled. Check that grant before
    // requiring a Host/SNI value, which also supports direct-IP HTTPS.
    if policy.is_ip_allowed(*original_destination.ip()) {
        return UpstreamDecision::Forward(SocketAddr::V4(original_destination));
    }
    if policy.has_domain_allow_rules() {
        if hostname.is_empty() {
            return UpstreamDecision::Deny;
        }
        if !policy.is_domain_allowed(hostname, None) {
            return UpstreamDecision::Deny;
        }
        return resolve_trusted_upstream(
            policy,
            resolver,
            hostname,
            original_destination.port(),
            cancel,
        )
        .map_or(UpstreamDecision::Unavailable, UpstreamDecision::Forward);
    }
    UpstreamDecision::Deny
}

fn resolve_trusted_upstream(
    policy: &SandboxNetworkPolicy,
    resolver: &HostNetResolver,
    hostname: &str,
    port: u16,
    cancel: Option<&AtomicBool>,
) -> Option<SocketAddr> {
    // The resolver worker stays in the host namespace, so guest-controlled
    // /etc/hosts, DNS configuration, and routes cannot redirect this lookup.
    resolver
        .resolve(hostname, port, cancel)?
        .into_iter()
        .find(|address| {
            matches!(address, SocketAddr::V4(address)
            if policy.is_domain_allowed(hostname, Some(*address.ip())))
        })
}

fn relay(mut client: TcpStream, mut upstream: TcpStream) {
    let mut client_reader = match client.try_clone() {
        Ok(reader) => reader,
        Err(_) => return,
    };
    let mut upstream_writer = match upstream.try_clone() {
        Ok(writer) => writer,
        Err(_) => return,
    };
    let forward = thread::spawn(move || {
        let _ = std::io::copy(&mut client_reader, &mut upstream_writer);
        let _ = upstream_writer.shutdown(Shutdown::Write);
    });
    let _ = std::io::copy(&mut upstream, &mut client);
    // Match the stream-copy contract used by mature async proxy runtimes:
    // close only the completed direction so a peer can still drain data in
    // the opposite direction before the relay exits.
    let _ = client.shutdown(Shutdown::Write);
    let _ = upstream.shutdown(Shutdown::Read);
    let _ = forward.join();
}

fn parse_protocol_host(preface: &[u8], original_port: u16) -> Result<Option<String>> {
    if original_port == 443 || preface.first() == Some(&22) {
        return parse_tls_sni(preface);
    }
    parse_http_host(preface)
}

fn parse_http_host(preface: &[u8]) -> Result<Option<String>> {
    let mut headers = [httparse::EMPTY_HEADER; 64];
    let mut request = httparse::Request::new(&mut headers);
    match request.parse(preface).context("parse HTTP request")? {
        Status::Partial => Ok(None),
        Status::Complete(_) => Ok(request.headers.iter().find_map(|header| {
            (header.name.eq_ignore_ascii_case("host")).then(|| normalize_host(header.value))
        })),
    }
}

fn normalize_host(value: &[u8]) -> String {
    let value = String::from_utf8_lossy(value);
    let host = value.trim().trim_end_matches('.');
    host.rsplit_once(':')
        .filter(|(host, port)| !host.contains(']') && port.parse::<u16>().is_ok())
        .map_or_else(
            || host.to_ascii_lowercase(),
            |(host, _)| host.to_ascii_lowercase(),
        )
}

fn parse_tls_sni(preface: &[u8]) -> Result<Option<String>> {
    if preface.first() != Some(&22) {
        return Ok(None);
    }
    // TLS permits a ClientHello handshake message to span multiple records.
    // Parse record envelopes first and accumulate their handshake payloads so
    // SNI extraction does not depend on the sender's record boundaries.
    let mut records = preface;
    let mut handshake = Vec::new();
    loop {
        let (remaining, record) = match parse_tls_raw_record(records) {
            Ok(parsed) => parsed,
            Err(tls_parser::Err::Incomplete(_)) => return Ok(None),
            Err(error) => return Err(anyhow::anyhow!("parse TLS record: {error:?}")),
        };
        records = remaining;
        if record.hdr.record_type == TlsRecordType::Handshake {
            handshake.extend_from_slice(record.data);
            loop {
                if handshake.len() < 4 {
                    break;
                }
                let message_len =
                    u32::from_be_bytes([0, handshake[1], handshake[2], handshake[3]]) as usize;
                let message_size = 4 + message_len;
                if handshake.len() < message_size {
                    break;
                }
                let message = handshake.drain(..message_size).collect::<Vec<_>>();
                if message[0] != 1 {
                    continue;
                }
                let (_, message) = parse_tls_handshake_msg_client_hello(&message[4..])
                    .map_err(|error| anyhow::anyhow!("parse TLS ClientHello: {error:?}"))?;
                let tls_parser::TlsMessageHandshake::ClientHello(hello) = message else {
                    return Ok(Some(String::new()));
                };
                let Some(extensions) = hello.ext else {
                    return Ok(Some(String::new()));
                };
                let (_, extensions) = tls_parser::parse_tls_client_hello_extensions(extensions)
                    .map_err(|error| {
                        anyhow::anyhow!("parse TLS ClientHello extensions: {error:?}")
                    })?;
                for extension in extensions {
                    if let tls_parser::TlsExtension::SNI(names) = extension {
                        if let Some((tls_parser::SNIType::HostName, name)) =
                            names.into_iter().next()
                        {
                            return Ok(Some(String::from_utf8_lossy(name).to_ascii_lowercase()));
                        }
                    }
                }
                return Ok(Some(String::new()));
            }
        }
        if records.is_empty() {
            return Ok(None);
        }
    }
}

fn original_destination(stream: &TcpStream) -> Result<(Ipv4Addr, u16)> {
    let mut address = std::mem::MaybeUninit::<libc::sockaddr_in>::zeroed();
    let mut length = std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t;
    // SAFETY: address and length point to valid writable storage for getsockopt.
    let result = unsafe {
        libc::getsockopt(
            stream.as_raw_fd(),
            libc::SOL_IP,
            SO_ORIGINAL_DST,
            address.as_mut_ptr().cast(),
            &mut length,
        )
    };
    if result != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    if usize::try_from(length).unwrap_or(0) < std::mem::size_of::<libc::sockaddr_in>() {
        anyhow::bail!("SO_ORIGINAL_DST returned a short sockaddr_in ({length} bytes)");
    }
    // SAFETY: getsockopt returned success and initialized a complete sockaddr_in.
    let address = unsafe { address.assume_init() };
    if address.sin_family as libc::c_int != libc::AF_INET {
        anyhow::bail!(
            "SO_ORIGINAL_DST returned unexpected address family {}",
            address.sin_family
        );
    }
    let ip = Ipv4Addr::from(u32::from_be(address.sin_addr.s_addr));
    Ok((ip, u16::from_be(address.sin_port)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::{BaseSandboxNetworkPolicy, SandboxNetworkEgressPolicy};

    #[test]
    fn http_host_is_parsed() {
        let host = parse_http_host(b"GET / HTTP/1.1\r\nHost: Example.com:443\r\n\r\n")
            .unwrap()
            .unwrap();
        assert_eq!(host, "example.com");
    }

    #[test]
    fn tls_sni_is_parsed() {
        let hostname = b"example.com";
        let mut client_hello = vec![
            0x03, 0x03, // ClientHello version
        ];
        client_hello.extend_from_slice(&[0; 32]);
        client_hello.extend_from_slice(&[
            0x00, // session id length
            0x00, 0x02, 0x00, 0x2f, // one cipher suite
            0x01, 0x00, // one compression method
            0x00, 0x14, // extensions length
            0x00, 0x00, 0x00, 0x10, // server_name extension
            0x00, 0x0e, // server name list length
            0x00, // host_name
            0x00, 0x0b, // hostname length
        ]);
        client_hello.extend_from_slice(hostname);

        let mut preface = vec![0x16, 0x03, 0x03, 0x00, 0x43, 0x01, 0x00, 0x00, 0x3f];
        preface.extend_from_slice(&client_hello);

        assert_eq!(
            parse_tls_sni(&preface).unwrap().as_deref(),
            Some("example.com")
        );

        let payload = &preface[5..];
        let split = 20;
        let mut fragmented = vec![0x16, 0x03, 0x03, 0x00, split as u8];
        fragmented.extend_from_slice(&payload[..split]);
        let second_len = (payload.len() - split) as u16;
        fragmented.extend_from_slice(&[0x16, 0x03, 0x03]);
        fragmented.extend_from_slice(&second_len.to_be_bytes());
        fragmented.extend_from_slice(&payload[split..]);
        assert_eq!(
            parse_tls_sni(&fragmented).unwrap().as_deref(),
            Some("example.com")
        );
    }

    #[test]
    fn staged_replacement_keeps_old_policy_until_activation() {
        let proxy = EgressProxy::new();
        let ip = Ipv4Addr::new(10, 11, 0, 1);
        let old = SandboxNetworkPolicy::new(
            true,
            BaseSandboxNetworkPolicy::Deny,
            SandboxNetworkEgressPolicy::new(
                Some(vec!["old.example.com".to_string()]),
                Some(vec!["0.0.0.0/0".to_string()]),
            )
            .unwrap(),
        );
        let new = SandboxNetworkPolicy::new(
            true,
            BaseSandboxNetworkPolicy::Deny,
            SandboxNetworkEgressPolicy::new(
                Some(vec!["new.example.com".to_string()]),
                Some(vec!["0.0.0.0/0".to_string()]),
            )
            .unwrap(),
        );
        proxy.prepare(ip, &old);
        proxy.activate(ip);
        proxy.prepare(ip, &new);
        assert_eq!(proxy.active_policy(ip).unwrap().policy, old);
        proxy.activate(ip);
        assert_eq!(proxy.active_policy(ip).unwrap().policy, new);
    }

    #[test]
    fn domain_policy_denies_connections_without_a_hostname() {
        let policy = SandboxNetworkPolicy::new(
            true,
            BaseSandboxNetworkPolicy::Allow,
            SandboxNetworkEgressPolicy::new(
                Some(vec!["example.com".to_string()]),
                Some(vec!["0.0.0.0/0".to_string()]),
            )
            .unwrap(),
        );
        let resolver = HostNetResolver::new();

        assert!(matches!(
            select_upstream(
                &policy,
                &resolver,
                "",
                SocketAddr::from(([203, 0, 113, 10], 443)),
                None,
            ),
            UpstreamDecision::Deny
        ));
    }

    #[test]
    fn mixed_domain_and_cidr_policy_keeps_cidr_grants() {
        let policy = SandboxNetworkPolicy::new(
            true,
            BaseSandboxNetworkPolicy::Deny,
            SandboxNetworkEgressPolicy::new(
                Some(vec!["example.com".to_string(), "8.8.8.8".to_string()]),
                Some(vec!["0.0.0.0/0".to_string()]),
            )
            .unwrap(),
        );
        let resolver = HostNetResolver::new();

        assert!(matches!(
            select_upstream(
                &policy,
                &resolver,
                "",
                SocketAddr::from(([8, 8, 8, 8], 443)),
                None,
            ),
            UpstreamDecision::Forward(SocketAddr::V4(destination))
                if destination.ip() == &Ipv4Addr::new(8, 8, 8, 8)
                    && destination.port() == 443
        ));
    }
}
