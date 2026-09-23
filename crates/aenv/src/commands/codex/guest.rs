//! Guest commands and service lifecycle, controlled by the Rust CLI.

use super::{runtime::WORKSPACE, socket::Proxy};
use crate::client::{sandboxes::Sandbox, Client};
use crate::grpc::{build_start_request, StartOpts, Transport};
use anyhow::{bail, ensure, Context, Result};
use envd::process::{
    process_event::{self, data_event},
    process_selector, ListRequest, ListResponse, ProcessInfo, ProcessSelector, SendSignalRequest,
    SendSignalResponse, Signal, StartRequest, StartResponse,
};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::Write;
use std::path::Path;
use std::time::{Duration, Instant};

const RECORDS: &str = "/codex-home/services.json";

// Keep the session leader alive until it has signalled its own group. Its
// membership prevents this PGID from being recycled before either signal.
// envd owns this supervisor; the client addresses it by a unique startup tag.
const SERVICE_SUPERVISOR: &str = r#"exec >> "$1" 2>&1
shift
cleanup() {
    trap '' TERM
    trap - EXIT
    kill -s TERM -- "-$$"
    sleep 5
    kill -s KILL -- "-$$"
}
trap cleanup EXIT TERM
"$@" &
wait "$!"
"#;

pub(super) struct Guest<'a> {
    pub client: &'a Client,
    pub sandbox: &'a Sandbox,
}

struct Output {
    code: i32,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ServiceProcess {
    pid: u32,
    started: u64,
}

#[derive(Debug)]
struct ProcessStat {
    pid: u32,
    group: u32,
    session: u32,
    started: u64,
    state: char,
}

impl Guest<'_> {
    fn transport(&self) -> Result<Transport> {
        self.client.transport(
            &self.sandbox.sandbox_id,
            self.sandbox.envd_access_token.as_deref(),
        )
    }

    async fn output(&self, cmd: &str, args: &[&str]) -> Result<Output> {
        tokio::time::timeout(Duration::from_secs(60), async {
            let request = build_start_request(StartOpts {
                cmd,
                args: args.iter().map(|s| (*s).into()).collect(),
                envs: Default::default(),
                pty: None,
                stdin: false,
            });
            let mut stream = self
                .transport()?
                .server_stream::<_, StartResponse>("Start", request)
                .await?;
            let (mut stdout, mut stderr) = (Vec::new(), Vec::new());
            while let Some(response) = stream.next().await {
                match response?.event.and_then(|event| event.event) {
                    Some(process_event::Event::Data(data)) => {
                        match data.output {
                            Some(data_event::Output::Stdout(bytes)) => stdout.extend(bytes),
                            Some(data_event::Output::Stderr(bytes)) => stderr.extend(bytes),
                            _ => (),
                        }
                        ensure!(
                            stdout.len() + stderr.len() <= 64 * 1024 * 1024,
                            "Sandbox command output exceeds 64 MiB"
                        );
                    }
                    Some(process_event::Event::End(end)) => {
                        return Ok(Output {
                            code: end.exit_code,
                            stdout,
                            stderr,
                        })
                    }
                    _ => (),
                }
            }
            bail!("Sandbox command disconnected before completion")
        })
        .await
        .context("Sandbox command timed out")?
    }

    pub async fn command(&self, cmd: &str, args: &[&str]) -> Result<Vec<u8>> {
        let output = self.output(cmd, args).await?;
        ensure!(
            output.code == 0,
            "Sandbox command failed ({}): {}",
            output.code,
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(output.stdout)
    }

    pub async fn exec(&self, command: &str) -> Result<Vec<u8>> {
        self.command("bash", &["-c", command]).await
    }
    pub async fn text(&self, command: &str) -> Result<String> {
        Ok(String::from_utf8(self.exec(command).await?)?)
    }

    pub async fn upload(&self, local: &Path, remote: &str, show_progress: bool) -> Result<()> {
        let size = if show_progress {
            local.metadata()?.len()
        } else {
            0
        };
        let progress = crate::progress::TransferProgress::new("Upload", size)?;
        self.client
            .files(&self.sandbox.sandbox_id)?
            .upload(local, remote, Some("user"), &progress)
            .await?;
        progress.finish();
        Ok(())
    }

    pub async fn write(&self, remote: &str, bytes: &[u8]) -> Result<()> {
        let mut temporary = tempfile::NamedTempFile::new()?;
        temporary.write_all(bytes)?;
        self.upload(temporary.path(), remote, false).await
    }

    async fn read_records(&self) -> Result<Vec<ServiceProcess>> {
        let files = self.client.files(&self.sandbox.sandbox_id)?;
        if files.stat(RECORDS, Some("user")).await?.is_none() {
            return Ok(vec![]);
        }
        let mut response = files.download(RECORDS, Some("user")).await?;
        let mut bytes = Vec::new();
        while let Some(chunk) =
            tokio::time::timeout(Duration::from_secs(10), response.chunk()).await??
        {
            bytes.extend(chunk);
            ensure!(bytes.len() <= 4096, "Invalid Codex service records");
        }
        let records: Vec<ServiceProcess> =
            serde_json::from_slice(&bytes).context("Invalid Codex service records")?;
        ensure!(
            records.len() <= 2 && records.iter().all(|p| p.pid > 1 && p.started > 0),
            "Invalid Codex service records"
        );
        Ok(records)
    }

    async fn write_records(&self, records: &[ServiceProcess]) -> Result<()> {
        // Publish atomically so an interrupted upload cannot corrupt recovery.
        self.write(&format!("{RECORDS}.tmp"), &serde_json::to_vec(records)?)
            .await?;
        self.command("mv", &["--", &format!("{RECORDS}.tmp"), RECORDS])
            .await?;
        Ok(())
    }

    pub async fn download(&self, remote: &str, local: &Path) -> Result<()> {
        let response = self
            .client
            .files(&self.sandbox.sandbox_id)?
            .download(remote, Some("user"))
            .await?;
        let progress = crate::progress::TransferProgress::new(
            "Download",
            response.content_length().unwrap_or(0),
        )?;
        super::super::download::save_stream(response.bytes_stream(), local, false, &progress)
            .await?;
        progress.finish();
        Ok(())
    }

    async fn processes(&self) -> Result<Vec<ProcessStat>> {
        // A process can disappear while cat runs. Parse the available records
        // in Rust; the presence of PID 1 distinguishes that race from failure.
        let output = self
            .output("bash", &["-c", "cat /proc/[0-9]*/stat 2>/dev/null"])
            .await?;
        let processes = String::from_utf8_lossy(&output.stdout)
            .lines()
            .map(parse_stat)
            .collect::<Result<Vec<_>>>()?;
        ensure!(
            processes.iter().any(|p| p.pid == 1),
            "Cannot inspect sandbox processes"
        );
        Ok(processes)
    }

    async fn stop_service(&self, tag: &str) -> Result<()> {
        let result = tokio::time::timeout(
            Duration::from_secs(10),
            self.transport()?.unary::<_, SendSignalResponse>(
                "SendSignal",
                SendSignalRequest {
                    process: Some(ProcessSelector {
                        selector: Some(process_selector::Selector::Tag(tag.into())),
                    }),
                    signal: Signal::Sigterm as i32,
                },
            ),
        )
        .await?;
        if let Err(error) = result {
            // A supervisor can exit between List and SendSignal. Never fall
            // back to its numeric PID, which may now name an unrelated task.
            let managed: ListResponse = tokio::time::timeout(
                Duration::from_secs(10),
                self.transport()?.unary("List", ListRequest {}),
            )
            .await??;
            if managed
                .processes
                .iter()
                .any(|p| p.tag.as_deref() == Some(tag))
            {
                return Err(error).context("Cannot stop Codex service supervisor");
            }
        }
        Ok(())
    }

    pub async fn stop_services(&self) -> Result<()> {
        let mut records = self.read_records().await?;
        // Recover the narrow window between envd starting a service and this
        // client publishing its group record (e.g. an interrupted startup).
        let managed: ListResponse = tokio::time::timeout(
            Duration::from_secs(10),
            self.transport()?.unary("List", ListRequest {}),
        )
        .await??;
        let processes = self.processes().await?;
        for process in managed.processes.iter().filter(|p| owned_service(p)) {
            if let Some(leader) = processes
                .iter()
                .find(|p| p.pid == process.pid && p.group == p.pid && p.session == p.pid)
            {
                records.retain(|p| p.pid != leader.pid);
                records.push(ServiceProcess {
                    pid: leader.pid,
                    started: leader.started,
                });
            }
            self.stop_service(process.tag.as_deref().unwrap()).await?;
        }
        if records.is_empty() {
            return Ok(());
        }
        // /proc snapshots are only used to wait for shutdown, never to select
        // signal targets. The supervisor performs TERM/KILL escalation while
        // it still owns the group, including when its service exits first.
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if live_groups(&records, &self.processes().await?).is_empty() {
                self.write_records(&[]).await?;
                return Ok(());
            }
            if Instant::now() >= deadline {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        bail!("Codex services did not stop through their supervisors; refusing PID-based cleanup; sandbox retained for recovery")
    }

    pub async fn start_services(&self, token: &str) -> Result<()> {
        self.stop_services().await?;
        let files = self.client.files(&self.sandbox.sandbox_id)?;
        match files.stat("/codex-home/ui", Some("user")).await? {
            Some(entry) => ensure!(
                entry.r#type == envd::filesystem::FileType::Directory as i32,
                "/codex-home/ui must be a directory"
            ),
            None => files.make_dir("/codex-home/ui").await?,
        }
        self.write("/codex-home/ui-token", token.as_bytes()).await?;
        self.command("chmod", &["600", "/codex-home/ui-token"])
            .await?;
        let mut records = Vec::new();
        let proxy = Proxy::new(self.client.base_url(), self.sandbox)?;
        for (name, port, home, args) in [
            (
                "executor",
                4501,
                "/codex-home",
                vec!["exec-server", "--listen", "ws://0.0.0.0:4501"],
            ),
            (
                "ui",
                4502,
                "/codex-home/ui",
                vec![
                    "app-server",
                    "--listen",
                    "ws://0.0.0.0:4502",
                    "--ws-auth",
                    "capability-token",
                    "--ws-token-file",
                    "/codex-home/ui-token",
                ],
            ),
        ] {
            let request = service_request(name, home, &args);
            let tag = request.tag.clone().unwrap();
            let mut stream = self
                .transport()?
                .server_stream::<_, StartResponse>("Start", request)
                .await?;
            let pid = tokio::time::timeout(Duration::from_secs(10), async {
                while let Some(response) = stream.next().await {
                    match response?.event.and_then(|event| event.event) {
                        Some(process_event::Event::Start(start)) => return Ok(start.pid),
                        Some(process_event::Event::End(_)) => {
                            bail!("Codex {name} exited before startup")
                        }
                        _ => (),
                    }
                }
                bail!("Codex {name} did not start")
            })
            .await??;
            // Non-PTY envd starts are not process-group leaders. setsid execs
            // in place; verify this rather than assuming its PID is the group.
            let deadline = Instant::now() + Duration::from_secs(5);
            let record = loop {
                let processes = self.processes().await?;
                let leader = processes.iter().find(|p| p.pid == pid).with_context(|| {
                    format!("Codex {name} exited; inspect /codex-home/{name}.log")
                })?;
                if pid > 1 && leader.group == pid && leader.session == pid {
                    break ServiceProcess {
                        pid,
                        started: leader.started,
                    };
                }
                ensure!(
                    Instant::now() < deadline,
                    "Codex service did not create its own process group"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            };
            records.push(record);
            if let Err(error) = self.write_records(&records).await {
                let _ = self.stop_service(&tag).await;
                return Err(error);
            }
            // envd owns the process; dropping the output stream does not stop it.
            drop(stream);
            let deadline = Instant::now() + Duration::from_secs(20);
            loop {
                let alive = self
                    .processes()
                    .await?
                    .iter()
                    .any(|p| p.pid == pid && p.state != 'Z' && p.state != 'X');
                ensure!(alive, "Codex {name} exited; inspect /codex-home/{name}.log");
                let bearer = if port == 4502 { Some(token) } else { None };
                if matches!(
                    tokio::time::timeout(Duration::from_secs(1), proxy.check(port, bearer)).await,
                    Ok(Ok(()))
                ) {
                    break;
                }
                ensure!(
                    Instant::now() < deadline,
                    "Codex {name} did not become ready; inspect /codex-home/{name}.log"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
        Ok(())
    }
}

fn service_request(name: &str, home: &str, args: &[&str]) -> StartRequest {
    let mut command = vec![
        "/usr/bin/env".into(),
        "-i".into(),
        "HOME=/home/user".into(),
        "PATH=/usr/local/bin:/usr/bin:/bin".into(),
        format!("CODEX_HOME={home}"),
        "/bin/bash".into(),
        "-c".into(),
        SERVICE_SUPERVISOR.into(),
        "aenv-codex".into(),
        format!("/codex-home/{name}.log"),
        "codex".into(),
    ];
    command.extend(args.iter().map(|s| (*s).into()));
    let mut request = build_start_request(StartOpts {
        cmd: "/usr/bin/setsid",
        args: command,
        envs: HashMap::new(),
        pty: None,
        stdin: false,
    });
    request.process.as_mut().unwrap().cwd = Some(WORKSPACE.into());
    request.tag = Some(format!("aenv-codex-{name}-{}", uuid::Uuid::new_v4()));
    request
}

fn owned_service(process: &ProcessInfo) -> bool {
    process.tag.as_deref().is_some_and(|tag| {
        tag.strip_prefix("aenv-codex-executor-")
            .or_else(|| tag.strip_prefix("aenv-codex-ui-"))
            .is_some_and(|id| uuid::Uuid::parse_str(id).is_ok())
    }) && process.config.as_ref().is_some_and(|config| {
        config.cmd == "/usr/bin/setsid"
            && config.cwd.as_deref() == Some(WORKSPACE)
            && config.args.first().is_some_and(|arg| arg == "/usr/bin/env")
            && config.args.get(5).is_some_and(|arg| arg == "/bin/bash")
            && config
                .args
                .get(7)
                .is_some_and(|arg| arg == SERVICE_SUPERVISOR)
            && config.args.get(8).is_some_and(|arg| arg == "aenv-codex")
    })
}

fn parse_stat(line: &str) -> Result<ProcessStat> {
    let (head, tail) = line
        .rsplit_once(") ")
        .context("Invalid sandbox process record")?;
    let (pid, _) = head
        .split_once(" (")
        .context("Missing sandbox process ID")?;
    let fields: Vec<_> = tail.split_whitespace().collect();
    ensure!(fields.len() >= 20, "Truncated sandbox process record");
    Ok(ProcessStat {
        pid: pid.parse()?,
        group: fields[2].parse()?,
        session: fields[3].parse()?,
        started: fields[19].parse()?,
        state: fields[0].chars().next().context("Missing process state")?,
    })
}

fn live_groups(records: &[ServiceProcess], processes: &[ProcessStat]) -> Vec<u32> {
    records
        .iter()
        .filter(|record| {
            if processes
                .iter()
                .any(|p| p.pid == record.pid && p.started != record.started)
            {
                return false;
            }
            processes.iter().any(|p| {
                p.group == record.pid && p.session == record.pid && p.state != 'Z' && p.state != 'X'
            })
        })
        .map(|record| record.pid)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn service_uses_clean_environment_and_owns_its_process_group() {
        let request = service_request(
            "executor",
            "/codex-home",
            &["exec-server", "--listen", "ws://0.0.0.0:4501"],
        );
        let process = request.process.unwrap();
        assert_eq!(process.cmd, "/usr/bin/setsid");
        assert_eq!(&process.args[..2], ["/usr/bin/env", "-i"]);
        assert!(process.envs.is_empty());
        assert_eq!(process.cwd.as_deref(), Some(WORKSPACE));
        assert!(request.pty.is_none());
        assert_eq!(request.stdin, Some(false));
        assert_ne!(
            request.tag,
            service_request("executor", "/codex-home", &[]).tag
        );
        let mut info = ProcessInfo {
            config: Some(process),
            pid: 42,
            tag: request.tag,
        };
        assert!(owned_service(&info));
        info.config.as_mut().unwrap().args[7] = "exec codex exec-server".into();
        assert!(!owned_service(&info));
        info.config.as_mut().unwrap().args[7] = SERVICE_SUPERVISOR.into();
        info.tag = Some("aenv-codex-executor".into());
        assert!(!owned_service(&info));
        info.tag = Some("unrelated".into());
        assert!(!owned_service(&info));
    }
    fn stat(pid: u32, group: u32, session: u32, started: u64, state: char) -> ProcessStat {
        ProcessStat {
            pid,
            group,
            session,
            started,
            state,
        }
    }
    #[test]
    fn shutdown_wait_includes_orphans_but_ignores_reused_leaders_and_zombies() {
        let records = [ServiceProcess {
            pid: 42,
            started: 100,
        }];
        assert_eq!(
            live_groups(&records, &[stat(43, 42, 42, 101, 'S')]),
            vec![42]
        );
        assert!(live_groups(&records, &[stat(43, 42, 42, 101, 'Z')]).is_empty());
        assert!(live_groups(&records, &[stat(42, 42, 42, 200, 'S')]).is_empty());
        assert!(live_groups(&records, &[stat(43, 42, 99, 101, 'S')]).is_empty());
    }

    #[tokio::test]
    async fn stale_service_tag_never_falls_back_to_a_reused_pid() -> Result<()> {
        use prost::Message;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let old_tag = service_request("executor", "/codex-home", &[]).tag.unwrap();
        let replacement = service_request("executor", "/codex-home", &[]);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let client = Client::new(&format!("http://{}", listener.local_addr()?), "")?;
        let expected_tag = old_tag.clone();
        let server = tokio::spawn(async move {
            for method in ["SendSignal", "List"] {
                let (mut socket, _) = listener.accept().await?;
                let mut bytes = Vec::new();
                let header_end = loop {
                    bytes.push(socket.read_u8().await?);
                    if bytes.ends_with(b"\r\n\r\n") {
                        break bytes.len();
                    }
                };
                let headers = std::str::from_utf8(&bytes)?;
                assert!(headers.starts_with(&format!("POST /process.Process/{method} ")));
                let size: usize = headers
                    .lines()
                    .find_map(|line| {
                        line.to_ascii_lowercase()
                            .strip_prefix("content-length: ")
                            .map(str::to_owned)
                    })
                    .unwrap_or_else(|| "0".into())
                    .parse()?;
                bytes.resize(header_end + size, 0);
                socket.read_exact(&mut bytes[header_end..]).await?;
                let (status, content_type, body) = if method == "SendSignal" {
                    let request = SendSignalRequest::decode(&bytes[header_end..])?;
                    assert_eq!(request.signal, Signal::Sigterm as i32);
                    assert_eq!(
                        request.process.unwrap().selector,
                        Some(process_selector::Selector::Tag(expected_tag.clone()))
                    );
                    (
                        "404 Not Found",
                        "application/json",
                        br#"{"code":"not_found","message":"supervisor exited"}"#.to_vec(),
                    )
                } else {
                    (
                        "200 OK",
                        "application/proto",
                        ListResponse {
                            // The old numeric PID is now a different service.
                            processes: vec![ProcessInfo {
                                pid: 42,
                                tag: replacement.tag.clone(),
                                config: replacement.process.clone(),
                            }],
                        }
                        .encode_to_vec(),
                    )
                };
                socket.write_all(format!(
                    "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len()
                ).as_bytes()).await?;
                socket.write_all(&body).await?;
            }
            Ok::<_, anyhow::Error>(())
        });
        let sandbox = Sandbox {
            sandbox_id: "test-sandbox".into(),
            envd_access_token: None,
            traffic_access_token: None,
        };
        let guest = Guest {
            client: &client,
            sandbox: &sandbox,
        };
        tokio::time::timeout(Duration::from_secs(5), guest.stop_service(&old_tag)).await??;
        server.await??;
        Ok(())
    }

    async fn check_supervisor_cleanup(terminate: bool) -> Result<()> {
        let directory = tempfile::tempdir()?;
        let log = directory.path().join("service.log");
        let mut unrelated = tokio::process::Command::new("sleep")
            .arg("30")
            .kill_on_drop(true)
            .spawn()?;
        let service = if terminate {
            "trap '' TERM; echo $$; exec sleep 30"
        } else {
            "trap '' TERM; sleep 30 & echo $!"
        };
        let mut command = tokio::process::Command::new("/bin/bash");
        command
            .args(["-c", SERVICE_SUPERVISOR, "test-supervisor"])
            .arg(&log)
            .args(["/bin/bash", "-c", service])
            .kill_on_drop(true);
        // Isolate only these test-owned processes. The supervisor must be the
        // session/group leader, just as it is after setsid in the guest.
        unsafe {
            command.pre_exec(|| {
                if libc::setsid() < 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let mut supervisor = command.spawn()?;
        let pid = supervisor.id().unwrap();
        let descendant = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Ok(contents) = std::fs::read_to_string(&log) {
                    if let Some(pid) = contents
                        .lines()
                        .next()
                        .and_then(|line| line.parse::<u32>().ok())
                    {
                        break pid;
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await?;
        let leader = parse_stat(&std::fs::read_to_string(format!("/proc/{pid}/stat"))?)?;
        assert_eq!((leader.group, leader.session), (pid, pid));
        if terminate {
            // This is our unreaped child, so its PID cannot have been reused.
            ensure!(unsafe { libc::kill(pid as i32, libc::SIGTERM) } == 0);
        }
        tokio::time::timeout(Duration::from_secs(10), supervisor.wait()).await??;
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let alive = std::fs::read_to_string(format!("/proc/{descendant}/stat"))
                    .ok()
                    .and_then(|line| parse_stat(&line).ok())
                    .is_some_and(|p| p.group == pid && p.state != 'Z' && p.state != 'X');
                if !alive {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await?;
        assert!(unrelated.try_wait()?.is_none());
        unrelated.kill().await?;
        Ok(())
    }

    #[tokio::test]
    async fn supervisor_escalates_for_term_ignoring_children() -> Result<()> {
        check_supervisor_cleanup(true).await
    }

    #[tokio::test]
    async fn supervisor_cleans_orphans_after_service_exits() -> Result<()> {
        check_supervisor_cleanup(false).await
    }
    #[test]
    fn proc_stat_allows_spaces_and_parentheses_in_command_names() -> Result<()> {
        let fields = format!("S 1 42 42 {} 123", ["0"; 15].join(" "));
        let parsed = parse_stat(&format!("42 (a command ) name) {fields}"))?;
        assert_eq!(
            (parsed.pid, parsed.group, parsed.session, parsed.started),
            (42, 42, 42, 123)
        );
        assert!(parse_stat("42 (short) S 1").is_err());
        Ok(())
    }
}
