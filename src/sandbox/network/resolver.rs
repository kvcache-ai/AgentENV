use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use nix::sched::{setns, CloneFlags};
use tracing::warn;

use super::slot::host_ns_fd;

const RESOLVER_QUEUE_CAPACITY: usize = 128;
const RESOLVER_STOP_POLL: Duration = Duration::from_millis(100);
const RESOLVER_RESPONSE_TIMEOUT: Duration = Duration::from_secs(30);
const RESOLVER_WORKERS: usize = 16;

struct ResolveRequest {
    hostname: String,
    port: u16,
    response: mpsc::SyncSender<Option<Vec<SocketAddr>>>,
}

/// Resolver worker that remains in the host network namespace for its entire
/// lifetime. Connection handlers submit hostnames instead of changing their
/// own namespace for every DNS lookup.
pub(super) struct HostNetResolver {
    requests: Mutex<Option<mpsc::SyncSender<ResolveRequest>>>,
    stopped: AtomicBool,
    joins: Mutex<Vec<JoinHandle<()>>>,
}

impl HostNetResolver {
    pub(super) fn new() -> Self {
        let (request_tx, request_rx): (
            mpsc::SyncSender<ResolveRequest>,
            mpsc::Receiver<ResolveRequest>,
        ) = mpsc::sync_channel(RESOLVER_QUEUE_CAPACITY);
        let request_rx = Arc::new(Mutex::new(request_rx));
        let mut joins = Vec::with_capacity(RESOLVER_WORKERS);
        for worker_id in 0..RESOLVER_WORKERS {
            let request_rx = Arc::clone(&request_rx);
            let join = match thread::Builder::new()
                .name(format!("agentenv-egress-dns-resolver-{worker_id}"))
                .spawn(move || {
                    let host_namespace_ready = match setns(host_ns_fd(), CloneFlags::CLONE_NEWNET) {
                        Ok(()) => true,
                        Err(error) => {
                            warn!(%error, "egress proxy resolver could not enter host network namespace");
                            false
                        }
                    };
                    loop {
                        let request = match request_rx
                            .lock()
                            .expect("egress proxy resolver request lock poisoned")
                            .recv_timeout(RESOLVER_STOP_POLL)
                        {
                            Ok(request) => request,
                            Err(mpsc::RecvTimeoutError::Timeout) => continue,
                            Err(mpsc::RecvTimeoutError::Disconnected) => break,
                        };
                        let resolved = if host_namespace_ready {
                            match (request.hostname.as_str(), request.port).to_socket_addrs() {
                                Ok(addresses) => Some(addresses.collect::<Vec<_>>()),
                                Err(error) => {
                                    warn!(hostname = %request.hostname, %error, "egress proxy DNS lookup failed");
                                    None
                                }
                            }
                        } else {
                            None
                        };
                        let _ = request.response.send(resolved);
                    }
                })
            {
                Ok(join) => join,
                Err(error) => {
                    // NetworkManager is a process-global infallible singleton;
                    // degrade DNS-backed proxy rules to unavailable instead of
                    // crashing the service when the host cannot create a worker.
                    warn!(%error, "spawn egress proxy host DNS resolver failed; domain resolution is unavailable");
                    break;
                }
            };
            joins.push(join);
        }
        let requests = (!joins.is_empty()).then_some(request_tx);
        Self {
            requests: Mutex::new(requests),
            stopped: AtomicBool::new(false),
            joins: Mutex::new(joins),
        }
    }

    pub(super) fn resolve(&self, hostname: &str, port: u16) -> Option<Vec<SocketAddr>> {
        if self.stopped.load(Ordering::Acquire) {
            return None;
        }
        let (response_tx, response_rx) = mpsc::sync_channel(1);
        let requests = self
            .requests
            .lock()
            .expect("egress proxy resolver request lock poisoned")
            .as_ref()?
            .clone();
        requests
            .try_send(ResolveRequest {
                hostname: hostname.to_string(),
                port,
                response: response_tx,
            })
            .ok()?;
        response_rx
            .recv_timeout(RESOLVER_RESPONSE_TIMEOUT)
            .ok()
            .flatten()
    }

    pub(super) fn shutdown(&self) {
        self.stopped.store(true, Ordering::Release);
        let _ = self
            .requests
            .lock()
            .expect("egress proxy resolver request lock poisoned")
            .take();
        let joins = std::mem::take(
            &mut *self
                .joins
                .lock()
                .expect("egress proxy resolver join lock poisoned"),
        );
        // A libc resolver call is not cancellable. Join workers that have
        // already observed shutdown, but detach one that is still in DNS so
        // network teardown cannot hang indefinitely.
        for join in joins {
            if join.is_finished() {
                let _ = join.join();
            }
        }
    }
}

impl Drop for HostNetResolver {
    fn drop(&mut self) {
        self.shutdown();
    }
}
