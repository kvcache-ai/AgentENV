use super::DEFAULT_TIMEOUT_SECS;
use crate::client::Client;
use crate::grpc::{build_start_request, pty_envs, StartOpts, Transport};
use crate::pty::{terminal_size, RawModeGuard};
use anyhow::Result;
use clap::Args as ClapArgs;
use envd::process::{
    process_event, process_input, process_selector, stream_input_request, ConnectRequest,
    ConnectResponse, ProcessInput, ProcessSelector, Pty, SendInputRequest, StartResponse,
    StreamInputRequest, UpdateRequest,
};
use futures::{channel::mpsc, Stream, StreamExt};
use std::io::{Read, Write};
use std::pin::Pin;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

const EXIT_SANDBOX_PAUSED: i32 = 130;
const WATCHDOG_INTERVAL: Duration = Duration::from_secs(1);
const PAUSE_STATE_PROBE_INTERVAL: Duration = Duration::from_secs(5);
const INPUT_OUTPUT_STALL_TIMEOUT: Duration = Duration::from_millis(500);
// agentenv proxy has PROXY_REQUEST_BODY_IDLE_TIMEOUT of 30s
const STREAM_INPUT_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);
const STREAM_INPUT_RECONNECT_DELAY: Duration = Duration::from_millis(100);
const INPUT_STATE_POLL_INTERVAL: Duration = Duration::from_millis(50);
const OUTPUT_IDLE_TIMEOUT: Duration = Duration::from_millis(500);
const OUTPUT_RECONNECT_DELAY: Duration = Duration::from_millis(500);
const OUTPUT_RECONNECT_TIMEOUT: Duration = Duration::from_millis(800);
const OUTPUT_RECONNECT_GRACE: Duration = Duration::from_millis(750);
const RECONNECT_PROBE_TIMEOUT: Duration = Duration::from_millis(200);
const RECONNECT_PAUSE_JOIN_TIMEOUT: Duration = Duration::from_millis(450);
const SEND_INPUT_TIMEOUT: Duration = Duration::from_millis(500);
const RESIZE_POLL_INTERVAL: Duration = Duration::from_millis(250);
const SANDBOX_LOST_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(ClapArgs)]
pub struct Args {
    #[arg(add = crate::commands::completion::add_active_sandbox_candidates())]
    sandbox_id: String,
}

pub fn run(args: Args) -> Result<()> {
    let client = Client::from_env()?;
    let rt = super::tokio_rt()?;
    let code = rt.block_on(attach(&client, &args.sandbox_id))?;
    std::process::exit(code);
}

pub(crate) async fn attach(client: &Client, sandbox_id: &str) -> Result<i32> {
    let connect_client = client.clone();
    let connect_sandbox_id = sandbox_id.to_owned();
    let sandbox = tokio::task::spawn_blocking(move || {
        connect_client.connect_sandbox(&connect_sandbox_id, DEFAULT_TIMEOUT_SECS)
    })
    .await??;
    let state = SessionState::new();
    let _keepalive_task = super::keepalive::spawn_periodic(
        client.clone(),
        sandbox_id.to_owned(),
        Arc::clone(&state.activity_generation),
    );
    let (cols, rows) = terminal_size();
    let transport = Arc::new(client.transport(sandbox_id, sandbox.envd_access_token.as_deref())?);
    let mut last_error = None;
    let mut started = None;
    for shell in ["/bin/bash", "/bin/sh"] {
        let req = build_start_request(StartOpts {
            cmd: shell,
            args: Vec::new(),
            envs: pty_envs(),
            pty: Some((cols, rows)),
            stdin: true,
        });
        let mut stream = match transport
            .server_stream::<_, StartResponse>("Start", req)
            .await
        {
            Ok(stream) => stream,
            Err(err) => {
                last_error = Some(err);
                continue;
            }
        };
        match wait_for_pid(&mut stream).await {
            Ok(pid) => {
                started = Some((stream, pid));
                break;
            }
            Err(err) => {
                last_error = Some(err);
            }
        }
    }
    let (stream, pid) = started.ok_or_else(|| {
        last_error.unwrap_or_else(|| anyhow::anyhow!("no shell candidates configured"))
    })?;
    let raw = RawModeGuard::enter()?;
    let selector = ProcessSelector {
        selector: Some(process_selector::Selector::Pid(pid)),
    };

    let (stdin_tx, stdin_rx) = mpsc::unbounded();
    let (reconnect_tx, reconnect_rx) = mpsc::unbounded();
    let (reconnect_ack_tx, reconnect_ack_rx) = mpsc::unbounded();
    let (input_activity_tx, input_activity_rx) = mpsc::unbounded();

    spawn_stdin_pump(stdin_tx, state.clone());
    let input_task = maintain_stream_input(
        transport.clone(),
        selector.clone(),
        stdin_rx,
        state.clone(),
        input_activity_tx,
    );
    let output_task = maintain_output_stream(
        transport.clone(),
        selector.clone(),
        stream,
        state.clone(),
        reconnect_rx,
        reconnect_ack_tx,
    );
    let resize_task = resize_loop(transport.clone(), selector, state.clone());
    let watchdog_task = watchdog_loop(
        client.clone(),
        sandbox_id.to_string(),
        transport,
        state.clone(),
        reconnect_tx,
        reconnect_ack_rx,
        input_activity_rx,
    );

    tokio::pin!(input_task);
    tokio::pin!(output_task);
    tokio::pin!(resize_task);
    tokio::pin!(watchdog_task);

    let end = tokio::select! {
        end = &mut output_task => end,
        end = &mut input_task => end,
        end = &mut resize_task => end,
        end = &mut watchdog_task => end,
    };

    state.stop();
    drop(raw);
    Ok(exit_code(end, sandbox_id))
}

fn exit_code(end: SessionEnd, sandbox_id: &str) -> i32 {
    match end {
        SessionEnd::Clean => 0,
        SessionEnd::Exited(code) => {
            print_notice(&format!(
                "Terminal session ended. Re-run `aenv cn {sandbox_id}` to reconnect."
            ));
            code
        }
        SessionEnd::Paused => {
            print_notice(&format!(
                "Sandbox {sandbox_id} has been paused. Run `aenv resume {sandbox_id}` to resume it."
            ));
            EXIT_SANDBOX_PAUSED
        }
        SessionEnd::Deleted => {
            print_notice(&format!("Sandbox {sandbox_id} no longer exists."));
            EXIT_SANDBOX_PAUSED
        }
        SessionEnd::Lost => {
            print_notice(&format!(
                "Terminal session lost contact with sandbox {sandbox_id}. Re-run `aenv cn {sandbox_id}` to reconnect."
            ));
            EXIT_SANDBOX_PAUSED
        }
    }
}

fn print_notice(message: &str) {
    let mut stderr = std::io::stderr().lock();
    let _ = write!(stderr, "\r\x1b[2K{message}\r\n");
    let _ = stderr.flush();
}

enum SessionEnd {
    Clean,
    Exited(i32),
    Paused,
    Deleted,
    Lost,
}

#[derive(Clone)]
struct SessionState {
    done: Arc<AtomicBool>,
    reconnect_generation: Arc<AtomicU64>,
    activity_generation: Arc<AtomicU64>,
    output_activity: Arc<AtomicU64>,
    recovery_mode: Arc<AtomicBool>,
}

impl SessionState {
    fn new() -> Self {
        Self {
            done: Arc::new(AtomicBool::new(false)),
            reconnect_generation: Arc::new(AtomicU64::new(0)),
            activity_generation: Arc::new(AtomicU64::new(0)),
            output_activity: Arc::new(AtomicU64::new(0)),
            recovery_mode: Arc::new(AtomicBool::new(false)),
        }
    }

    fn stop(&self) {
        self.done.store(true, Ordering::Relaxed);
    }

    fn is_done(&self) -> bool {
        self.done.load(Ordering::Relaxed)
    }

    fn generation(&self) -> u64 {
        self.reconnect_generation.load(Ordering::Relaxed)
    }

    fn bump_generation(&self) {
        self.reconnect_generation.fetch_add(1, Ordering::Relaxed);
    }

    fn record_activity(&self) {
        self.activity_generation.fetch_add(1, Ordering::Relaxed);
    }

    fn output_generation(&self) -> u64 {
        self.output_activity.load(Ordering::Relaxed)
    }

    fn record_output(&self) {
        self.output_activity.fetch_add(1, Ordering::Relaxed);
        self.recovery_mode.store(false, Ordering::Relaxed);
    }

    fn finish_recovery(&self) {
        self.recovery_mode.store(false, Ordering::Relaxed);
    }

    fn in_recovery(&self) -> bool {
        self.recovery_mode.load(Ordering::Relaxed)
    }

    fn request_recovery_reconnect(&self, reconnect_tx: &mpsc::UnboundedSender<()>) {
        self.recovery_mode.store(true, Ordering::Relaxed);
        self.bump_generation();
        let _ = reconnect_tx.unbounded_send(());
    }
}

async fn wait_for_pid<S>(stream: &mut S) -> Result<u32>
where
    S: StreamExt<Item = Result<StartResponse>> + Unpin,
{
    while let Some(msg) = stream.next().await {
        let msg = msg?;
        if let Some(process_event::Event::Start(start)) = msg.event.and_then(|w| w.event) {
            return Ok(start.pid);
        }
    }
    anyhow::bail!("stream closed before start event")
}

fn pty_input(payload: Vec<u8>) -> StreamInputRequest {
    StreamInputRequest {
        event: Some(stream_input_request::Event::Data(
            stream_input_request::DataEvent {
                input: Some(ProcessInput {
                    input: Some(process_input::Input::Pty(payload)),
                }),
            },
        )),
    }
}

fn keepalive_input() -> StreamInputRequest {
    StreamInputRequest {
        event: Some(stream_input_request::Event::Keepalive(
            stream_input_request::KeepAlive {},
        )),
    }
}

fn send_input_request(selector: ProcessSelector, payload: Vec<u8>) -> SendInputRequest {
    SendInputRequest {
        process: Some(selector),
        input: Some(ProcessInput {
            input: Some(process_input::Input::Pty(payload)),
        }),
    }
}

fn spawn_stdin_pump(stdin_tx: mpsc::UnboundedSender<Vec<u8>>, state: SessionState) {
    std::thread::spawn(move || {
        let mut stdin = std::io::stdin();
        let mut buf = [0u8; 4096];
        while !state.is_done() {
            match stdin.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => {
                    if stdin_tx.unbounded_send(buf[..n].to_vec()).is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    });
}

async fn maintain_stream_input(
    transport: Arc<Transport>,
    selector: ProcessSelector,
    mut stdin_rx: mpsc::UnboundedReceiver<Vec<u8>>,
    state: SessionState,
    input_activity_tx: mpsc::UnboundedSender<u64>,
) -> SessionEnd {
    while !state.is_done() {
        if state.in_recovery() {
            while state.in_recovery() && !state.is_done() {
                tokio::select! {
                    Some(payload) = stdin_rx.next() => {
                        send_recovery_input(
                            &transport,
                            selector.clone(),
                            payload,
                            &mut stdin_rx,
                        ).await;
                    }
                    _ = tokio::time::sleep(INPUT_STATE_POLL_INTERVAL) => {}
                }
            }
            continue;
        }

        let generation = state.generation();
        let (stream_tx, stream_rx) = mpsc::unbounded();
        let _ = stream_tx.unbounded_send(StreamInputRequest {
            event: Some(stream_input_request::Event::Start(
                stream_input_request::StartEvent {
                    process: Some(selector.clone()),
                },
            )),
        });

        let stream = transport
            .client_stream::<_, envd::process::StreamInputResponse, _>("StreamInput", stream_rx);
        tokio::pin!(stream);

        let mut keepalive = tokio::time::interval(STREAM_INPUT_KEEPALIVE_INTERVAL);

        loop {
            tokio::select! {
                result = &mut stream => {
                    if state.is_done() {
                        return SessionEnd::Clean;
                    }
                    if result.is_err() {
                        tokio::time::sleep(STREAM_INPUT_RECONNECT_DELAY).await;
                    }
                    break;
                }
                Some(payload) = stdin_rx.next() => {
                    state.record_activity();
                    if state.in_recovery() {
                        send_recovery_input(
                            &transport,
                            selector.clone(),
                            payload,
                            &mut stdin_rx,
                        ).await;
                        break;
                    }

                    if should_expect_output(&payload) {
                        let _ = input_activity_tx.unbounded_send(state.output_generation());
                    }
                    route_input(&transport, selector.clone(), payload, &stream_tx).await;
                }
                _ = tokio::time::sleep(INPUT_STATE_POLL_INTERVAL) => {
                    if state.generation() != generation || state.in_recovery() {
                        break;
                    }
                }
                _ = keepalive.tick() => {
                    if stream_tx.unbounded_send(keepalive_input()).is_err() {
                        break;
                    }
                }
            }
        }
    }
    SessionEnd::Clean
}

fn should_expect_output(payload: &[u8]) -> bool {
    payload
        .iter()
        .any(|byte| matches!(*byte, 0x20..=0x7e) || *byte >= 0x80)
}

async fn send_recovery_input(
    transport: &Transport,
    selector: ProcessSelector,
    payload: Vec<u8>,
    stdin_rx: &mut mpsc::UnboundedReceiver<Vec<u8>>,
) {
    let req = send_input_request(selector, payload);
    let _ = tokio::time::timeout(
        SEND_INPUT_TIMEOUT,
        transport.unary::<_, envd::process::SendInputResponse>("SendInput", req),
    )
    .await;

    while stdin_rx.try_recv().is_ok() {}
}

async fn route_input(
    transport: &Transport,
    selector: ProcessSelector,
    payload: Vec<u8>,
    stream_tx: &mpsc::UnboundedSender<StreamInputRequest>,
) {
    if stream_tx.unbounded_send(pty_input(payload.clone())).is_ok() {
        return;
    }

    let req = send_input_request(selector, payload);
    let _ = tokio::time::timeout(
        SEND_INPUT_TIMEOUT,
        transport.unary::<_, envd::process::SendInputResponse>("SendInput", req),
    )
    .await;
}

enum OutputStream {
    Start(Pin<Box<dyn Stream<Item = Result<StartResponse>> + Send>>),
    Connect(Pin<Box<dyn Stream<Item = Result<ConnectResponse>> + Send>>),
}

async fn maintain_output_stream(
    transport: Arc<Transport>,
    selector: ProcessSelector,
    initial: impl Stream<Item = Result<StartResponse>> + Send + 'static,
    state: SessionState,
    mut reconnect_rx: mpsc::UnboundedReceiver<()>,
    reconnect_ack_tx: mpsc::UnboundedSender<()>,
) -> SessionEnd {
    let mut stream = Some(OutputStream::Start(Box::pin(initial)));
    let mut generation = state.generation();
    let mut reconnect_grace_until: Option<Instant> = None;

    while !state.is_done() {
        let Some(current_stream) = stream.as_mut() else {
            if let Some(next) = reconnect_output(&transport, selector.clone()).await {
                stream = Some(next);
                generation = state.generation();
                state.finish_recovery();
                reconnect_grace_until = Some(arm_output_reconnect_grace(&mut reconnect_rx));
                let _ = reconnect_ack_tx.unbounded_send(());
            }
            continue;
        };

        match drain_output_once(
            current_stream,
            &mut reconnect_rx,
            &state,
            &mut reconnect_grace_until,
        )
        .await
        {
            OutputDrain::Exited(code) => return SessionEnd::Exited(code),
            OutputDrain::Disconnected => {
                state.bump_generation();
                stream = reconnect_output(&transport, selector.clone()).await;
                if stream.is_some() {
                    generation = state.generation();
                    state.finish_recovery();
                    reconnect_grace_until = Some(arm_output_reconnect_grace(&mut reconnect_rx));
                    let _ = reconnect_ack_tx.unbounded_send(());
                }
            }
            OutputDrain::ReconnectRequested => {
                stream = reconnect_output(&transport, selector.clone()).await;
                if stream.is_some() {
                    generation = state.generation();
                    state.finish_recovery();
                    reconnect_grace_until = Some(arm_output_reconnect_grace(&mut reconnect_rx));
                    let _ = reconnect_ack_tx.unbounded_send(());
                }
            }
            OutputDrain::Idle => {
                let current = state.generation();
                if current == generation {
                    continue;
                }
                if reconnect_grace_until.is_some_and(|until| Instant::now() < until) {
                    generation = current;
                    continue;
                }
                stream = reconnect_output(&transport, selector.clone()).await;
                if stream.is_some() {
                    generation = current;
                    state.finish_recovery();
                    reconnect_grace_until = Some(arm_output_reconnect_grace(&mut reconnect_rx));
                    let _ = reconnect_ack_tx.unbounded_send(());
                }
            }
        }
    }

    SessionEnd::Clean
}

fn arm_output_reconnect_grace(reconnect_rx: &mut mpsc::UnboundedReceiver<()>) -> Instant {
    while reconnect_rx.try_recv().is_ok() {}
    Instant::now() + OUTPUT_RECONNECT_GRACE
}

enum OutputDrain {
    Exited(i32),
    Disconnected,
    ReconnectRequested,
    Idle,
}

async fn open_output_connection(
    transport: &Transport,
    selector: ProcessSelector,
) -> Result<Pin<Box<dyn Stream<Item = Result<ConnectResponse>> + Send>>> {
    transport
        .server_stream::<_, ConnectResponse>(
            "Connect",
            ConnectRequest {
                process: Some(selector),
            },
        )
        .await
}

async fn reconnect_output(
    transport: &Transport,
    selector: ProcessSelector,
) -> Option<OutputStream> {
    tokio::time::sleep(OUTPUT_RECONNECT_DELAY).await;
    let connect = open_output_connection(transport, selector);
    match tokio::time::timeout(OUTPUT_RECONNECT_TIMEOUT, connect).await {
        Ok(Ok(next)) => Some(OutputStream::Connect(next)),
        Ok(Err(_)) | Err(_) => None,
    }
}

async fn drain_output_once(
    stream: &mut OutputStream,
    reconnect_rx: &mut mpsc::UnboundedReceiver<()>,
    state: &SessionState,
    reconnect_grace_until: &mut Option<Instant>,
) -> OutputDrain {
    let mut reconnect_closed = false;

    loop {
        let next_event = async {
            match stream {
                OutputStream::Start(stream) => stream
                    .next()
                    .await
                    .map(|result| result.map(|resp| resp.event)),
                OutputStream::Connect(stream) => stream
                    .next()
                    .await
                    .map(|result| result.map(|resp| resp.event)),
            }
        };
        tokio::pin!(next_event);

        let next = tokio::select! {
            signal = reconnect_rx.next(), if !reconnect_closed => {
                if signal.is_none() {
                    reconnect_closed = true;
                    continue;
                }
                if reconnect_grace_until.is_some_and(|until| Instant::now() < until) {
                    while reconnect_rx.try_recv().is_ok() {}
                    continue;
                }
                return OutputDrain::ReconnectRequested;
            }
            event = tokio::time::timeout(OUTPUT_IDLE_TIMEOUT, &mut next_event) => event,
        };

        let event = match next {
            Ok(Some(Ok(event))) => event,
            Ok(Some(Err(_))) | Ok(None) => return OutputDrain::Disconnected,
            Err(_) => return OutputDrain::Idle,
        };
        state.finish_recovery();

        match write_output_event(event) {
            OutputWrite::End(end) => return end,
            OutputWrite::Continue { visible_output } => {
                if visible_output {
                    state.record_activity();
                    state.record_output();
                    *reconnect_grace_until = None;
                }
            }
        }
    }
}

enum OutputWrite {
    Continue { visible_output: bool },
    End(OutputDrain),
}

fn write_output_event(event: Option<envd::process::ProcessEvent>) -> OutputWrite {
    let Some(evt) = event.and_then(|w| w.event) else {
        return OutputWrite::Continue {
            visible_output: false,
        };
    };
    match evt {
        process_event::Event::Data(data) => match data.output {
            Some(process_event::data_event::Output::Pty(bytes))
            | Some(process_event::data_event::Output::Stdout(bytes)) => {
                let visible_output = !bytes.is_empty();
                let mut stdout = std::io::stdout().lock();
                if stdout
                    .write_all(&bytes)
                    .and_then(|_| stdout.flush())
                    .is_err()
                {
                    return OutputWrite::End(OutputDrain::Disconnected);
                }
                OutputWrite::Continue { visible_output }
            }
            _ => OutputWrite::Continue {
                visible_output: false,
            },
        },
        process_event::Event::End(end) => OutputWrite::End(OutputDrain::Exited(end.exit_code)),
        _ => OutputWrite::Continue {
            visible_output: false,
        },
    }
}

async fn resize_loop(
    transport: Arc<Transport>,
    selector: ProcessSelector,
    state: SessionState,
) -> SessionEnd {
    let mut last = terminal_size();
    while !state.is_done() {
        tokio::time::sleep(RESIZE_POLL_INTERVAL).await;
        let current = terminal_size();
        if current == last {
            continue;
        }
        last = current;
        let (cols, rows) = current;
        let req = UpdateRequest {
            process: Some(selector.clone()),
            pty: Some(Pty {
                size: Some(envd::process::pty::Size { cols, rows }),
            }),
        };
        let _ = transport
            .unary::<_, envd::process::UpdateResponse>("Update", req)
            .await;
    }
    SessionEnd::Clean
}

async fn watchdog_loop(
    client: Client,
    sandbox_id: String,
    transport: Arc<Transport>,
    state: SessionState,
    reconnect_tx: mpsc::UnboundedSender<()>,
    mut reconnect_ack_rx: mpsc::UnboundedReceiver<()>,
    mut input_activity_rx: mpsc::UnboundedReceiver<u64>,
) -> SessionEnd {
    let mut previous_health = ProbeStatus::Healthy;
    let mut recovered_health = false;
    let mut reconnect_in_flight = false;
    let mut quiet_until: Option<Instant> = None;
    let mut unhealthy_since: Option<Instant> = None;
    let mut pending_input: Option<InputStallProbe> = None;
    let mut next_pause_probe = Instant::now() + PAUSE_STATE_PROBE_INTERVAL;

    while !state.is_done() {
        tokio::time::sleep(WATCHDOG_INTERVAL).await;

        while reconnect_ack_rx.try_recv().is_ok() {
            reconnect_in_flight = false;
            quiet_until = Some(Instant::now() + OUTPUT_RECONNECT_GRACE);
            if let (Some(probe), Some(until)) = (&mut pending_input, quiet_until) {
                probe.deadline = probe.deadline.max(until);
            }
        }

        if quiet_until.is_some_and(|until| Instant::now() >= until) {
            quiet_until = None;
        }

        while let Ok(output_generation) = input_activity_rx.try_recv() {
            if state.output_generation() != output_generation {
                pending_input = None;
                continue;
            }
            pending_input = Some(InputStallProbe {
                output_generation,
                deadline: Instant::now() + INPUT_OUTPUT_STALL_TIMEOUT,
            });
        }

        match envd_ready_probe(Arc::clone(&transport)).await {
            Ok(true) => {
                unhealthy_since = None;
                if matches!(previous_health, ProbeStatus::Unhealthy) {
                    recovered_health = true;
                }
                previous_health = ProbeStatus::Healthy;
            }
            Ok(false) | Err(_) => {
                let now = Instant::now();
                let since = unhealthy_since.get_or_insert(now);
                if now.duration_since(*since) >= SANDBOX_LOST_TIMEOUT {
                    return SessionEnd::Lost;
                }
                previous_health = ProbeStatus::Unhealthy;
                recovered_health = false;
            }
        }

        if recovered_health && quiet_until.is_none() {
            request_reconnect_once(
                &state,
                &reconnect_tx,
                &mut reconnect_in_flight,
                &mut recovered_health,
            );
        }

        if let Some(probe) = &mut pending_input {
            if state.output_generation() != probe.output_generation {
                pending_input = None;
                reconnect_in_flight = false;
                state.recovery_mode.store(false, Ordering::Relaxed);
            } else if Instant::now() >= probe.deadline {
                if let Some(until) = quiet_until {
                    probe.deadline = until;
                } else {
                    request_reconnect_once(
                        &state,
                        &reconnect_tx,
                        &mut reconnect_in_flight,
                        &mut recovered_health,
                    );
                    probe.deadline = Instant::now() + INPUT_OUTPUT_STALL_TIMEOUT;
                }
            }
        }

        if Instant::now() >= next_pause_probe {
            next_pause_probe = Instant::now() + PAUSE_STATE_PROBE_INTERVAL;
            let pause_client = client.clone();
            let pause_sandbox_id = sandbox_id.clone();
            let pause_probe = tokio::task::spawn_blocking(move || {
                pause_client.sandbox_state_with_timeout(&pause_sandbox_id, RECONNECT_PROBE_TIMEOUT)
            });
            match tokio::time::timeout(RECONNECT_PAUSE_JOIN_TIMEOUT, pause_probe).await {
                Ok(Ok(Ok(Some(state)))) if state == "paused" => return SessionEnd::Paused,
                Ok(Ok(Ok(None))) => return SessionEnd::Deleted,
                _ => {}
            }
        }
    }

    SessionEnd::Clean
}

fn request_reconnect_once(
    state: &SessionState,
    reconnect_tx: &mpsc::UnboundedSender<()>,
    reconnect_in_flight: &mut bool,
    recovered_health: &mut bool,
) {
    if *reconnect_in_flight {
        return;
    }
    state.request_recovery_reconnect(reconnect_tx);
    *reconnect_in_flight = true;
    *recovered_health = false;
}

async fn envd_ready_probe(transport: Arc<Transport>) -> Result<bool> {
    match tokio::time::timeout(RECONNECT_PROBE_TIMEOUT, transport.ready()).await {
        Ok(Ok(())) => Ok(true),
        Ok(Err(_)) | Err(_) => Ok(false),
    }
}

#[derive(Copy, Clone)]
struct InputStallProbe {
    output_generation: u64,
    deadline: Instant,
}

#[derive(Copy, Clone)]
enum ProbeStatus {
    Healthy,
    Unhealthy,
}

#[cfg(test)]
mod tests {
    use super::*;
    use envd::process::ProcessEvent;
    use futures::stream;

    fn start_response(event: process_event::Event) -> StartResponse {
        StartResponse {
            event: Some(ProcessEvent { event: Some(event) }),
        }
    }

    #[tokio::test]
    async fn output_keepalive_events_keep_recovery_from_sticking() {
        let responses = stream::iter([
            Ok(start_response(process_event::Event::Keepalive(
                process_event::KeepAlive {},
            ))),
            Ok(start_response(process_event::Event::End(
                process_event::EndEvent {
                    exit_code: 7,
                    exited: true,
                    status: String::new(),
                    error: None,
                },
            ))),
        ]);
        let mut stream = OutputStream::Start(Box::pin(responses));
        let (_reconnect_tx, mut reconnect_rx) = mpsc::unbounded();
        let state = SessionState::new();
        state.recovery_mode.store(true, Ordering::Relaxed);
        let mut reconnect_grace_until = None;

        let drained = drain_output_once(
            &mut stream,
            &mut reconnect_rx,
            &state,
            &mut reconnect_grace_until,
        )
        .await;

        assert!(matches!(drained, OutputDrain::Exited(7)));
        assert!(!state.in_recovery());
    }

    #[tokio::test]
    async fn output_stream_disconnect_requests_reconnect_even_without_health_failure() {
        let responses = stream::empty();
        let mut stream = OutputStream::Start(Box::pin(responses));
        let (_reconnect_tx, mut reconnect_rx) = mpsc::unbounded();
        let state = SessionState::new();
        let mut reconnect_grace_until = None;

        let drained = drain_output_once(
            &mut stream,
            &mut reconnect_rx,
            &state,
            &mut reconnect_grace_until,
        )
        .await;

        assert!(matches!(drained, OutputDrain::Disconnected));
    }

    #[test]
    fn printable_input_stall_probe_requests_reconnect_after_timeout() {
        assert!(should_expect_output(b"ls\n"));
        assert!(!should_expect_output(b"\r"));

        let state = SessionState::new();
        let (reconnect_tx, mut reconnect_rx) = mpsc::unbounded();
        let mut reconnect_in_flight = false;
        let mut recovered_health = false;
        let mut probe = InputStallProbe {
            output_generation: state.output_generation(),
            deadline: Instant::now() - Duration::from_millis(1),
        };

        if state.output_generation() == probe.output_generation && Instant::now() >= probe.deadline
        {
            request_reconnect_once(
                &state,
                &reconnect_tx,
                &mut reconnect_in_flight,
                &mut recovered_health,
            );
            probe.deadline = Instant::now() + INPUT_OUTPUT_STALL_TIMEOUT;
        }

        assert!(state.in_recovery());
        assert!(reconnect_in_flight);
        assert!(reconnect_rx.try_recv().is_ok());
        assert!(probe.deadline > Instant::now());
    }
}
