use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc as std_mpsc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use hickory_resolver::config::{LookupIpStrategy, ResolverOpts};
use hickory_resolver::net::runtime::TokioRuntimeProvider;
use hickory_resolver::TokioResolver;
use nix::sched::{setns, CloneFlags};
use tokio::sync::{mpsc, oneshot, Semaphore};
use tokio::task::JoinSet;
use tracing::{debug, warn};

use super::slot::host_ns_fd;

const RESOLVER_QUEUE_CAPACITY: usize = 128;
const RESOLVER_MAX_IN_FLIGHT: usize = 16;
const RESOLVER_STOP_POLL: Duration = Duration::from_millis(100);
const RESOLVER_DNS_TIMEOUT: Duration = Duration::from_secs(5);
const RESOLVER_RESPONSE_TIMEOUT: Duration = Duration::from_secs(30);

struct ResolveRequest {
    hostname: String,
    port: u16,
    response: std_mpsc::SyncSender<Option<Vec<SocketAddr>>>,
}

#[derive(Default)]
struct ResolverLifecycle {
    requests: Option<mpsc::Sender<ResolveRequest>>,
    shutdown: Option<oneshot::Sender<()>>,
    join: Option<JoinHandle<()>>,
}

/// Resolver worker that remains in the host network namespace for its entire
/// lifetime. DNS requests use an async resolver with bounded network timeouts
/// so shutdown can cancel work instead of waiting on libc's non-cancellable
/// resolver calls.
pub(super) struct HostNetResolver {
    lifecycle: Mutex<ResolverLifecycle>,
    stopped: AtomicBool,
}

impl HostNetResolver {
    pub(super) fn new() -> Self {
        let (request_tx, request_rx) = mpsc::channel(RESOLVER_QUEUE_CAPACITY);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let lifecycle = match thread::Builder::new()
            .name("agentenv-egress-dns-resolver".to_string())
            .spawn(move || run_resolver_thread(request_rx, shutdown_rx))
        {
            Ok(join) => ResolverLifecycle {
                requests: Some(request_tx),
                shutdown: Some(shutdown_tx),
                join: Some(join),
            },
            Err(error) => {
                warn!(%error, "spawn egress proxy host DNS resolver failed; domain resolution is unavailable");
                ResolverLifecycle::default()
            }
        };

        Self {
            lifecycle: Mutex::new(lifecycle),
            stopped: AtomicBool::new(false),
        }
    }

    pub(super) fn resolve(
        &self,
        hostname: &str,
        port: u16,
        cancel: Option<&AtomicBool>,
    ) -> Option<Vec<SocketAddr>> {
        if self.stopped.load(Ordering::Acquire) {
            return None;
        }

        let (response_tx, response_rx) = std_mpsc::sync_channel(1);
        let requests = self
            .lifecycle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .requests
            .as_ref()?
            .clone();
        requests
            .try_send(ResolveRequest {
                hostname: hostname.to_string(),
                port,
                response: response_tx,
            })
            .ok()?;

        let deadline = Instant::now() + RESOLVER_RESPONSE_TIMEOUT;
        loop {
            if self.stopped.load(Ordering::Acquire)
                || cancel.is_some_and(|cancel| cancel.load(Ordering::Acquire))
            {
                return None;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return None;
            }
            match response_rx.recv_timeout(remaining.min(RESOLVER_STOP_POLL)) {
                Ok(resolved) => return resolved,
                Err(std_mpsc::RecvTimeoutError::Timeout) => continue,
                Err(std_mpsc::RecvTimeoutError::Disconnected) => return None,
            }
        }
    }

    pub(super) fn shutdown(&self) {
        if self.stopped.swap(true, Ordering::AcqRel) {
            return;
        }

        let (shutdown, join) = {
            let mut lifecycle = self
                .lifecycle
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            lifecycle.requests.take();
            (lifecycle.shutdown.take(), lifecycle.join.take())
        };

        if let Some(shutdown) = shutdown {
            let _ = shutdown.send(());
        }
        if let Some(join) = join {
            let _ = join.join();
        }
    }
}

impl Drop for HostNetResolver {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn run_resolver_thread(
    mut requests: mpsc::Receiver<ResolveRequest>,
    shutdown: oneshot::Receiver<()>,
) {
    if let Err(error) = setns(host_ns_fd(), CloneFlags::CLONE_NEWNET) {
        warn!(%error, "egress proxy resolver could not enter host network namespace");
        return;
    }

    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            warn!(%error, "build egress proxy DNS resolver runtime failed");
            return;
        }
    };

    let mut builder = match TokioResolver::builder(TokioRuntimeProvider::default()) {
        Ok(builder) => builder,
        Err(error) => {
            warn!(%error, "read host DNS resolver configuration failed");
            return;
        }
    };
    let options: &mut ResolverOpts = builder.options_mut();
    options.ip_strategy = LookupIpStrategy::Ipv4Only;
    options.timeout = RESOLVER_DNS_TIMEOUT;
    options.attempts = 1;
    options.num_concurrent_reqs = 1;
    options.max_active_requests = RESOLVER_MAX_IN_FLIGHT;
    let resolver = match builder.build() {
        Ok(resolver) => resolver,
        Err(error) => {
            warn!(%error, "build host DNS resolver failed");
            return;
        }
    };

    runtime.block_on(run_resolver(resolver, &mut requests, shutdown));
}

async fn run_resolver(
    resolver: TokioResolver,
    requests: &mut mpsc::Receiver<ResolveRequest>,
    mut shutdown: oneshot::Receiver<()>,
) {
    let permits = std::sync::Arc::new(Semaphore::new(RESOLVER_MAX_IN_FLIGHT));
    let mut tasks = JoinSet::new();

    loop {
        tokio::select! {
            biased;
            _ = &mut shutdown => break,
            request = requests.recv() => {
                let Some(request) = request else { break };
                let resolver = resolver.clone();
                let permits = std::sync::Arc::clone(&permits);
                tasks.spawn(async move {
                    let Ok(_permit) = permits.acquire_owned().await else {
                        return;
                    };
                    let resolved = resolver
                        .lookup_ip(request.hostname.as_str())
                        .await
                        .ok()
                        .map(|lookup| {
                            lookup
                            .iter()
                                .filter_map(|ip| {
                                    matches!(ip, IpAddr::V4(_))
                                        .then_some(SocketAddr::new(ip, request.port))
                                })
                                .collect::<Vec<_>>()
                        });
                    let _ = request.response.send(resolved);
                });
            }
            Some(result) = tasks.join_next(), if !tasks.is_empty() => {
                if let Err(error) = result {
                    debug!(%error, "egress proxy DNS lookup task failed");
                }
            }
        }
    }

    tasks.abort_all();
    while let Some(result) = tasks.join_next().await {
        if let Err(error) = result {
            debug!(%error, "egress proxy DNS lookup task stopped during shutdown");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn shutdown_is_idempotent_and_releases_waiters() {
        let resolver = Arc::new(HostNetResolver::new());
        let lookup_resolver = Arc::clone(&resolver);
        let (result_tx, result_rx) = std_mpsc::sync_channel(1);
        let lookup = thread::spawn(move || {
            let _ = result_tx.send(lookup_resolver.resolve("resolver-shutdown.invalid", 443, None));
        });

        resolver.shutdown();
        resolver.shutdown();

        assert!(result_rx.recv_timeout(Duration::from_secs(1)).is_ok());
        lookup.join().unwrap();
        assert!(resolver.resolve("example.com", 443, None).is_none());
    }

    #[test]
    fn poisoned_lifecycle_lock_does_not_break_shutdown() {
        let resolver = Arc::new(HostNetResolver::new());
        let poisoned = Arc::clone(&resolver);
        let _ = thread::spawn(move || {
            let _guard = poisoned.lifecycle.lock().unwrap();
            panic!("poison resolver lifecycle lock for test");
        })
        .join();

        resolver.shutdown();
    }
}
