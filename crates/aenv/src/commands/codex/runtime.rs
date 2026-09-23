//! Configuration, native Codex processes, and sandbox session lifecycle.

use super::{guest::Guest, project, socket, Action, Setup, Start, TurnOptions};
use crate::client::{handle_status, sandboxes::Sandbox, Client};
use anyhow::{bail, ensure, Context, Result};
use directories::{BaseDirs, ProjectDirs};
use serde::{Deserialize, Serialize};
use serde_json::json;
use shell_util::shell_quote;
use std::fs::{self, File, OpenOptions};
use std::io::{self, IsTerminal, Write};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Component, Path, PathBuf};
use std::process::Stdio;
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::process::{Child, Command};

pub(super) const WORKSPACE: &str = "/workspace/project";
const VERSION: &str = "0.155.1";
const TTL: u32 = 300;

fn dirs() -> Result<ProjectDirs> {
    ProjectDirs::from("", "", "aenv").context("Cannot determine aenv directories")
}
fn config_path() -> Result<PathBuf> {
    Ok(dirs()?.config_dir().join("codex.json"))
}

pub(super) fn write_private(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path.parent().context("Missing file parent")?;
    fs::create_dir_all(parent)?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    temporary.write_all(bytes)?;
    temporary.as_file().sync_all()?;
    temporary.persist(path)?;
    Ok(())
}

fn private_dir(path: &Path) -> Result<()> {
    fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(path)?;
    Ok(())
}

#[derive(Serialize, Deserialize)]
#[serde(default)]
struct Config {
    template: String,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            template: "codex-executor".into(),
        }
    }
}
impl Config {
    fn load() -> Result<Self> {
        match fs::read(config_path()?) {
            Ok(bytes) => Ok(serde_json::from_slice(&bytes)?),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(Self::default()),
            Err(error) => Err(error.into()),
        }
    }
    fn validate(&self) -> Result<()> {
        ensure!(
            !self.template.trim().is_empty(),
            "Template name cannot be empty"
        );
        Ok(())
    }
}

fn codex_binary() -> Result<PathBuf> {
    let binary = PathBuf::from(std::env::var_os("CODEX_BIN").unwrap_or_else(|| "codex".into()));
    let output = std::process::Command::new(&binary)
        .arg("--version")
        .output()
        .with_context(|| {
            format!("Install Codex: npm install -g @openai/codex@{VERSION} (or set CODEX_BIN)")
        })?;
    ensure!(
        output.status.success()
            && String::from_utf8_lossy(&output.stdout).trim() == format!("codex-cli {VERSION}"),
        "This integration requires Codex {VERSION} on the client and in the template"
    );
    Ok(binary)
}

fn codex_home() -> Result<PathBuf> {
    let home = match std::env::var_os("CODEX_HOME") {
        Some(path) => absolute(Path::new(&path))?,
        None => BaseDirs::new()
            .context("Cannot find home directory")?
            .home_dir()
            .join(".codex"),
    };
    check_codex_home(&home)?;
    Ok(home)
}

fn check_codex_home(home: &Path) -> Result<()> {
    // Codex 0.155.1 gives this file precedence over CODEX_EXEC_SERVER_URL.
    let environments = home.join("environments.toml");
    ensure!(
        !environments.try_exists()?,
        "Codex {VERSION} cannot override {} for an AgentENV session. Use a CODEX_HOME without environments.toml and configure/login with Codex there first.",
        environments.display()
    );
    Ok(())
}

fn setup(args: Setup) -> Result<()> {
    let client = Client::from_env()?;
    let mut config = Config::load()?;
    if let Some(template) = args.template {
        config.template = template;
    }
    config.validate()?;
    let mut alias = reqwest::Url::parse("https://unused.invalid/")?;
    alias.path_segments_mut().unwrap().push(&config.template);
    match client
        .get(&format!("/templates/aliases{}", alias.path()))
        .call()
    {
        Ok(_) => println!("Using existing template: {}", config.template),
        Err(ureq::Error::Status(404, _)) => {
            println!("Building the bundled executor template (first use only)...");
            let context = tempfile::Builder::new()
                .prefix("aenv-codex-build-")
                .tempdir()?;
            fs::write(
                context.path().join("Dockerfile"),
                include_str!("assets/Dockerfile"),
            )?;
            super::super::build::codex_template(
                context.path().to_owned(),
                config.template.clone(),
            )?;
        }
        Err(error) => {
            handle_status(Err(error))?;
        }
    }
    write_private(&config_path()?, &serde_json::to_vec_pretty(&config)?)?;
    println!("Template setting saved to {}.", config_path()?.display());
    println!("Ready. Run: aenv codex start /path/to/project");
    Ok(())
}

#[derive(Serialize, Deserialize)]
struct Manifest {
    source: PathBuf,
    api_url: String,
    status: String,
    sandbox_id: String,
    #[serde(default)]
    baseline: Option<String>,
}
impl Manifest {
    fn save(&self, session: &Path) -> Result<()> {
        write_private(
            &session.join("session.json"),
            &serde_json::to_vec_pretty(self)?,
        )?;
        File::open(session)?.sync_all()?;
        Ok(())
    }

    fn ensure_resumable(&self, session: &Path) -> Result<()> {
        ensure!(
            self.status != "deletion-pending",
            "Files were saved and sandbox deletion has started; run `aenv codex finish {}` to complete cleanup",
            shell_quote(&session.to_string_lossy())
        );
        ensure!(
            !["saved", "setup-failed"].contains(&self.status.as_str()),
            "This sandbox was deleted; start a new session from the saved project"
        );
        Ok(())
    }
}

fn lock(session: &Path) -> Result<File> {
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o600)
        .open(session.join(".lock"))?;
    file.try_lock()
        .context("This session is already open in another aenv process")?;
    Ok(file)
}

fn existing(client: &Client, path: &Path) -> Result<(PathBuf, Manifest, File)> {
    let session = path.canonicalize().context("Session directory not found")?;
    let lock = lock(&session)?;
    let manifest: Manifest = serde_json::from_slice(&fs::read(session.join("session.json"))?)?;
    ensure!(
        manifest.api_url == client.base_url(),
        "Run `aenv auth` against the deployment used by this session"
    );
    Ok((session, manifest, lock))
}

/// Resolve existing ancestors too, so --output cannot enter the source through
/// a symlink or a yet-to-be-created nested directory.
fn absolute(path: &Path) -> Result<PathBuf> {
    let mut result = std::env::current_dir()?;
    for component in path.components() {
        match component {
            Component::RootDir => result = PathBuf::from("/"),
            Component::CurDir => (),
            Component::ParentDir => {
                result.pop();
            }
            Component::Normal(name) => {
                result.push(name);
                if result.symlink_metadata().is_ok() {
                    result = result.canonicalize()?;
                }
            }
            _ => bail!("Unsupported path"),
        }
    }
    Ok(result)
}

pub(super) fn run(action: Action) -> Result<()> {
    // Session logs and archives can contain private project data.
    unsafe {
        libc::umask(0o077);
    }
    if let Action::Setup(args) = action {
        return setup(args);
    }
    if matches!(action, Action::Start(_) | Action::Resume(_)) {
        ensure!(
            io::stdin().is_terminal() && io::stdout().is_terminal(),
            "Run `aenv codex start` or `resume` in an interactive terminal"
        );
    }
    let client = Client::from_env()?;
    let runtime = super::super::tokio_rt()?;
    match action {
        Action::Start(args) => start(&client, &runtime, args),
        Action::Resume(args) => {
            let (session, mut manifest, _lock) = existing(&client, &args.session)?;
            manifest.ensure_resumable(&session)?;
            let binary = codex_binary()?;
            let codex_home = codex_home()?;
            let sandbox = client.connect_sandbox(&manifest.sandbox_id, TTL)?;
            println!("Resuming session: {}", session.display());
            runtime.block_on(run_session(
                Session {
                    client: &client,
                    codex_home: &codex_home,
                    binary: &binary,
                    path: &session,
                    manifest: &mut manifest,
                    sandbox: &sandbox,
                },
                &args.turn,
                None,
            ))
        }
        Action::Finish { session } => {
            let (session, mut manifest, _lock) = existing(&client, &session)?;
            finish(&client, &runtime, &session, &mut manifest)
        }
        Action::Setup(_) => unreachable!(),
    }
}

fn finish(
    client: &Client,
    runtime: &tokio::runtime::Runtime,
    session: &Path,
    manifest: &mut Manifest,
) -> Result<()> {
    match manifest.status.as_str() {
        "saved" => {
            println!(
                "Already saved: {}",
                fs::read_to_string(session.join("latest.txt"))?.trim()
            );
            return Ok(());
        }
        "deletion-pending" => return delete_saved_sandbox(client, session, manifest),
        _ => (),
    }
    ensure!(
        manifest.baseline.is_some(),
        "This session has no imported project to save"
    );
    let sandbox = client.connect_sandbox(&manifest.sandbox_id, TTL)?;
    let _lease = Lease::new(client, &sandbox.sandbox_id);
    runtime.block_on(save_and_close(client, session, manifest, &sandbox, true))
}

fn delete_saved_sandbox(client: &Client, session: &Path, manifest: &mut Manifest) -> Result<()> {
    let latest = fs::read_to_string(session.join("latest.txt"))?;
    let result = (|| {
        // Persist the completed export before the irreversible request. Keep this
        // state on any error: DELETE may succeed even if its response is lost.
        manifest.status = "deletion-pending".into();
        manifest.save(session)?;
        match client
            .delete(&format!("/sandboxes/{}", manifest.sandbox_id))
            .call()
        {
            Ok(_) | Err(ureq::Error::Status(404, _)) => (),
            Err(error) => {
                handle_status(Err(error))?;
            }
        }
        manifest.status = "saved".into();
        manifest.save(session)
    })();
    result.with_context(|| {
        format!(
            "Files saved at {}; run `aenv codex finish {}` to complete cleanup",
            latest.trim(),
            shell_quote(&session.to_string_lossy())
        )
    })?;
    println!(
        "Saved your files at {} and deleted the sandbox.",
        latest.trim()
    );
    Ok(())
}

fn start(client: &Client, runtime: &tokio::runtime::Runtime, args: Start) -> Result<()> {
    let binary = codex_binary()?;
    let codex_home = codex_home()?;
    let mut config = Config::load()?;
    if let Some(template) = args.template {
        config.template = template;
    }
    config.validate()?;
    let project = args
        .project
        .canonicalize()
        .context("Project directory not found")?;
    let home = BaseDirs::new()
        .context("Cannot find home directory")?
        .home_dir()
        .canonicalize()?;
    ensure!(
        project.is_dir() && project != home && project != Path::new("/"),
        "Choose a project directory, not your home directory or filesystem root"
    );
    let output = absolute(
        &args
            .output
            .unwrap_or(dirs()?.data_dir().join("codex/sessions")),
    )?;
    ensure!(
        !output.starts_with(&project),
        "Choose an --output directory outside your project"
    );
    let session = output.join(format!(
        "{}-{}",
        chrono::Local::now().format("%Y%m%d-%H%M%S"),
        &uuid::Uuid::new_v4().simple().to_string()[..8]
    ));
    private_dir(&session)?;
    let _lock = lock(&session)?;
    println!("Session and saved results: {}", session.display());
    let archive = session.join("input.tar.gz");
    let count = project::pack(&project, &archive)?;
    println!("Uploading {count} project entries; your original directory stays unchanged.");
    let sandbox: Sandbox = handle_status(client.post("/sandboxes").send_json(json!({
        "templateID":config.template,"timeout":TTL,"secure":true,"autoPause":true,
        "network":{"allowPublicTraffic":false},"metadata":{"purpose":"codex-project"}
    })))?
    .into_json()?;
    let mut manifest = Manifest {
        source: project,
        api_url: client.base_url().into(),
        status: "preparing".into(),
        sandbox_id: sandbox.sandbox_id.clone(),
        baseline: None,
    };
    if let Err(error) = manifest.save(&session) {
        let _ = client.delete_sandbox(&sandbox.sandbox_id);
        return Err(error);
    }
    println!("Sandbox: {}", sandbox.sandbox_id);
    runtime.block_on(run_session(
        Session {
            client,
            codex_home: &codex_home,
            binary: &binary,
            path: &session,
            manifest: &mut manifest,
            sandbox: &sandbox,
        },
        &args.turn,
        Some(&archive),
    ))
}

struct Lease {
    stop: Option<mpsc::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
    error: Arc<Mutex<Option<String>>>,
}
impl Lease {
    fn new(client: &Client, sid: &str) -> Self {
        let (stop, receiver) = mpsc::channel();
        let error = Arc::new(Mutex::new(None));
        let (api, sid, errors) = (client.clone(), sid.to_owned(), error.clone());
        let thread = std::thread::spawn(move || {
            while receiver.recv_timeout(Duration::from_secs(30))
                == Err(mpsc::RecvTimeoutError::Timeout)
            {
                if let Err(error) = handle_status(
                    api.post(&format!("/sandboxes/{sid}/timeout"))
                        .timeout(Duration::from_secs(30))
                        .send_json(json!({"timeout":TTL})),
                ) {
                    *errors.lock().unwrap() =
                        Some(format!("Sandbox lease renewal failed: {error:#}"));
                    break;
                }
            }
        });
        Self {
            stop: Some(stop),
            thread: Some(thread),
            error,
        }
    }
    fn check(&self) -> Result<()> {
        if let Some(error) = self.error.lock().unwrap().as_ref() {
            bail!("{error}");
        }
        Ok(())
    }
}
impl Drop for Lease {
    fn drop(&mut self) {
        self.stop.take();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

struct Session<'a> {
    client: &'a Client,
    codex_home: &'a Path,
    binary: &'a Path,
    path: &'a Path,
    manifest: &'a mut Manifest,
    sandbox: &'a Sandbox,
}

fn preserve(client: &Client, session: &Path, manifest: &mut Manifest) -> Result<()> {
    match client.pause_sandbox(&manifest.sandbox_id) {
        Ok(()) => {
            manifest.status = "paused".into();
            println!("Sandbox paused.");
        }
        Err(error) => {
            manifest.status = "recovery-needed".into();
            eprintln!("Could not pause sandbox: {error:#}");
        }
    }
    manifest.save(session)?;
    println!(
        "Continue: aenv codex resume {}",
        shell_quote(&session.to_string_lossy())
    );
    println!(
        "Save and delete sandbox: aenv codex finish {}",
        shell_quote(&session.to_string_lossy())
    );
    Ok(())
}

async fn save_and_close(
    client: &Client,
    session: &Path,
    manifest: &mut Manifest,
    sandbox: &Sandbox,
    delete: bool,
) -> Result<()> {
    let guest = Guest { client, sandbox };
    let saved = async {
        guest.stop_services().await?;
        project::save(
            &guest,
            manifest
                .baseline
                .as_deref()
                .context("No project baseline")?,
            session,
        )
        .await?;
        Ok::<_, anyhow::Error>(())
    }
    .await;
    if !delete || saved.is_err() {
        preserve(client, session, manifest)?;
    }
    saved?;
    if delete {
        delete_saved_sandbox(client, session, manifest)?;
    }
    Ok(())
}

async fn run_session(
    session: Session<'_>,
    options: &TurnOptions,
    archive: Option<&Path>,
) -> Result<()> {
    let guest = Guest {
        client: session.client,
        sandbox: session.sandbox,
    };
    let lease = Lease::new(session.client, &session.sandbox.sandbox_id);
    let mut live: Option<Live> = None;
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let work = async {
        if let Some(archive) = archive {
            session.manifest.baseline = Some(project::import(&guest, archive).await?);
        }
        session.manifest.status = "running".into();
        session.manifest.save(session.path)?;
        ensure!(
            guest.text("codex --version").await?.trim() == format!("codex-cli {VERSION}"),
            "Rebuild the executor template with Codex {VERSION}"
        );
        let token = uuid::Uuid::new_v4().simple().to_string();
        guest.start_services(&token).await?;
        live = Some(Live::start(&session, token).await?);
        println!("Connected. Commands and file operations run in your sandbox.");
        live.as_mut()
            .unwrap()
            .native_ui(&session, options, &lease)
            .await
    };
    let mut result = tokio::select! {
        result = work => result,
        _ = terminate.recv() => Err(anyhow::anyhow!("Session interrupted")),
    };
    if let Some(mut live) = live.take() {
        if let Err(error) = live.stop().await {
            if result.is_ok() {
                result = Err(error);
            } else {
                eprintln!("Stopping Codex: {error:#}");
            }
        }
    }
    if session.manifest.baseline.is_some() {
        let saved = save_and_close(
            session.client,
            session.path,
            session.manifest,
            session.sandbox,
            false,
        )
        .await;
        if let Err(error) = &saved {
            eprintln!("Automatic save failed; recover with `aenv codex finish`: {error:#}");
        }
        result = result.and(saved);
    } else {
        session.client.delete_sandbox(&session.sandbox.sandbox_id)?;
        session.manifest.status = "setup-failed".into();
        session.manifest.save(session.path)?;
    }
    result
}

async fn stop_process(process: &mut Child) -> Result<()> {
    if process.try_wait()?.is_some() {
        return Ok(());
    }
    if let Some(pid) = process.id() {
        unsafe {
            libc::kill(pid as i32, libc::SIGTERM);
        }
    }
    if tokio::time::timeout(Duration::from_secs(10), process.wait())
        .await
        .is_err()
    {
        process.kill().await?;
    }
    Ok(())
}

struct Live {
    process: Child,
    rpc: socket::Rpc,
    events: socket::SharedEvents,
    ui_path: PathBuf,
    _services: Vec<socket::Service>,
    _directory: tempfile::TempDir,
}
impl Live {
    async fn start(session: &Session<'_>, token: String) -> Result<Self> {
        let proxy = socket::Proxy::new(session.client.base_url(), session.sandbox)?;
        let (relay_url, relay) = proxy.relay().await?;
        let directory = tempfile::Builder::new()
            .prefix("aenv-codex-")
            .tempdir_in("/tmp")?;
        let harness = directory.path().join("harness.sock");
        let ui_path = directory.path().join("ui.sock");
        let log = OpenOptions::new()
            .create(true)
            .append(true)
            .mode(0o600)
            .open(session.path.join("codex.log"))?;
        let mut process = Command::new(session.binary)
            .args(["app-server", "--strict-config", "--listen"])
            .arg(format!("unix://{}", harness.display()))
            .args([
                "-c",
                "approval_policy=\"never\"",
                "-c",
                "sandbox_mode=\"danger-full-access\"",
            ])
            .env("CODEX_HOME", session.codex_home)
            .env("CODEX_EXEC_SERVER_URL", relay_url)
            .env_remove("CODEX_EXEC_SERVER_NOISE_REGISTRY_URL")
            .env_remove("CODEX_EXEC_SERVER_NOISE_ENVIRONMENT_ID")
            .env_remove("CODEX_EXEC_SERVER_NOISE_AUTH_TOKEN")
            .env_remove("CODEX_EXEC_SERVER_NOISE_CHATGPT_ACCOUNT_ID")
            .current_dir(session.path)
            .stdin(Stdio::null())
            .stdout(log.try_clone()?)
            .stderr(log)
            .kill_on_drop(true)
            .spawn()?;
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut rpc = loop {
            ensure!(
                process.try_wait()?.is_none(),
                "Codex app-server exited; inspect the session's codex.log"
            );
            ensure!(
                Instant::now() < deadline,
                "Codex app-server did not start; inspect codex.log"
            );
            match socket::Rpc::connect(&harness).await {
                Ok(rpc) => break rpc,
                Err(_) if !harness.exists() => tokio::time::sleep(Duration::from_millis(100)).await,
                Err(error) => return Err(error),
            }
        };
        rpc.call("environment/info", json!({"environmentId":"remote"}))
            .await?;
        let events = socket::Events::new(session.path)?;
        let gateway = socket::gateway(&ui_path, harness, proxy, token, events.clone()).await?;
        Ok(Self {
            process,
            rpc,
            events,
            ui_path,
            _services: vec![relay, gateway],
            _directory: directory,
        })
    }

    async fn stop(&mut self) -> Result<()> {
        let interrupted = async {
            let loaded = self.rpc.call("thread/loaded/list", json!({})).await?;
            for id in loaded["data"]
                .as_array()
                .context("Invalid loaded threads")?
            {
                let thread =
                    self.rpc.call("thread/read", json!({"threadId":id})).await?["thread"].clone();
                if thread["ephemeral"] != true && thread["status"]["type"] == "active" {
                    let turns = self
                        .rpc
                        .call("thread/turns/list", json!({"threadId":id,"limit":100}))
                        .await?;
                    for turn in turns["data"].as_array().context("Invalid thread turns")? {
                        if turn["status"] == "inProgress" {
                            self.rpc
                                .call("turn/interrupt", json!({"threadId":id,"turnId":turn["id"]}))
                                .await?;
                        }
                    }
                }
                self.rpc
                    .call("thread/backgroundTerminals/clean", json!({"threadId":id}))
                    .await?;
            }
            Ok::<_, anyhow::Error>(())
        }
        .await;
        stop_process(&mut self.process).await?;
        interrupted
    }

    async fn native_ui(
        &mut self,
        session: &Session<'_>,
        options: &TurnOptions,
        lease: &Lease,
    ) -> Result<()> {
        let mut command = Command::new(session.binary);
        let thread_id = self.events.lock().unwrap().thread_id.clone();
        if thread_id.is_some() {
            command.arg("resume");
        }
        command.args([
            "--remote",
            &format!("unix://{}", self.ui_path.display()),
            "-C",
            WORKSPACE,
        ]);
        if let Some(model) = &options.model {
            command.args(["-m", model]);
        }
        if let Some(id) = &thread_id {
            command.arg(id);
        } else {
            command.args(["-a", "never", "-s", "danger-full-access"]);
        }
        command
            .env("CODEX_HOME", session.codex_home)
            .env_remove("CODEX_EXEC_SERVER_URL")
            .current_dir(session.path)
            .kill_on_drop(true);
        let _terminal = crate::pty::RawModeGuard::enter()?;
        let mut process = command.spawn()?;
        let result = async {
            loop {
                if let Some(status) = process.try_wait()? {
                    ensure!(status.success(), "Codex exited with {status}");
                    return Ok(());
                }
                lease.check()?;
                {
                    let events = self.events.lock().unwrap();
                    if let Some(error) = &events.error {
                        bail!("Codex connection failed: {error}");
                    }
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
        .await;
        stop_process(&mut process).await?;
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn saved_session() -> Result<(tempfile::TempDir, Manifest)> {
        let session = tempfile::tempdir()?;
        let saved = session.path().join("saves/0001");
        fs::create_dir_all(saved.join("project"))?;
        fs::write(saved.join("project/result.txt"), "saved work")?;
        write_private(
            &session.path().join("latest.txt"),
            saved.to_str().unwrap().as_bytes(),
        )?;
        let manifest = Manifest {
            source: "/source".into(),
            api_url: "http://unused.invalid".into(),
            status: "paused".into(),
            sandbox_id: "test-sandbox".into(),
            baseline: Some("baseline".into()),
        };
        manifest.save(session.path())?;
        Ok((session, manifest))
    }

    fn read_manifest(session: &Path) -> Result<Manifest> {
        Ok(serde_json::from_slice(&fs::read(
            session.join("session.json"),
        )?)?)
    }

    fn deletion_server(
        session: &Path,
        responses: Vec<Option<&'static str>>,
        before_reply: impl Fn(usize, &Path) -> Result<()> + Send + 'static,
    ) -> Result<(Client, std::thread::JoinHandle<Result<()>>)> {
        use std::io::BufRead;
        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let client = Client::new_with_timeouts(
            &format!("http://{}", listener.local_addr()?),
            "",
            Duration::from_secs(2),
            Duration::from_secs(2),
        )?;
        let session = session.to_owned();
        let server = std::thread::spawn(move || {
            for (index, response) in responses.into_iter().enumerate() {
                let deadline = Instant::now() + Duration::from_secs(5);
                let (mut stream, _) = loop {
                    match listener.accept() {
                        Ok(connection) => break connection,
                        Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                            ensure!(Instant::now() < deadline, "Missing deletion request");
                            std::thread::sleep(Duration::from_millis(10));
                        }
                        Err(error) => return Err(error.into()),
                    }
                };
                stream.set_read_timeout(Some(Duration::from_secs(2)))?;
                let mut reader = io::BufReader::new(&mut stream);
                let mut line = String::new();
                reader.read_line(&mut line)?;
                ensure!(
                    line.starts_with("DELETE /sandboxes/test-sandbox "),
                    "Finish must not reconnect or export after deletion starts: {line}"
                );
                loop {
                    line.clear();
                    ensure!(reader.read_line(&mut line)? > 0, "Incomplete request");
                    if line == "\r\n" {
                        break;
                    }
                }
                ensure!(
                    read_manifest(&session)?.status == "deletion-pending",
                    "Deletion must have a persisted recovery state"
                );
                before_reply(index, &session)?;
                if let Some(status) = response {
                    write!(
                        stream,
                        "HTTP/1.1 {status}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    )?;
                }
                // None simulates a successful DELETE whose response is lost.
            }
            Ok(())
        });
        Ok((client, server))
    }

    #[test]
    fn finish_retries_failed_deletion_without_reconnecting() -> Result<()> {
        let (session, mut manifest) = saved_session()?;
        let (client, server) = deletion_server(
            session.path(),
            vec![
                Some("500 Internal Server Error"),
                Some("403 Forbidden"),
                Some("204 No Content"),
            ],
            |_, _| Ok(()),
        )?;
        let runtime = super::super::super::tokio_rt()?;
        let error = delete_saved_sandbox(&client, session.path(), &mut manifest).unwrap_err();
        assert!(error.to_string().contains("saves/0001"));
        assert!(error.to_string().contains("aenv codex finish"));
        manifest = read_manifest(session.path())?;
        assert_eq!(manifest.status, "deletion-pending");
        assert!(manifest.ensure_resumable(session.path()).is_err());
        assert!(finish(&client, &runtime, session.path(), &mut manifest).is_err());
        manifest = read_manifest(session.path())?;
        assert_eq!(manifest.status, "deletion-pending");
        finish(&client, &runtime, session.path(), &mut manifest)?;
        server.join().unwrap()?;
        manifest = read_manifest(session.path())?;
        assert_eq!(manifest.status, "saved");
        finish(&client, &runtime, session.path(), &mut manifest)?;
        assert_eq!(
            fs::read_to_string(session.path().join("saves/0001/project/result.txt"))?,
            "saved work"
        );
        Ok(())
    }

    #[test]
    fn finish_recovers_when_delete_response_is_lost() -> Result<()> {
        let (session, mut manifest) = saved_session()?;
        let (client, server) =
            deletion_server(session.path(), vec![None, Some("404 Not Found")], |_, _| {
                Ok(())
            })?;
        assert!(delete_saved_sandbox(&client, session.path(), &mut manifest).is_err());
        manifest = read_manifest(session.path())?;
        assert_eq!(manifest.status, "deletion-pending");
        finish(
            &client,
            &super::super::super::tokio_rt()?,
            session.path(),
            &mut manifest,
        )?;
        server.join().unwrap()?;
        assert_eq!(read_manifest(session.path())?.status, "saved");
        Ok(())
    }

    #[test]
    fn finish_recovers_when_final_manifest_write_fails() -> Result<()> {
        let (session, mut manifest) = saved_session()?;
        let (client, server) = deletion_server(
            session.path(),
            vec![Some("204 No Content"), Some("404 Not Found")],
            |index, session| {
                if index == 0 {
                    // Preserve the durable pending record while forcing the
                    // post-DELETE replacement to fail, even when run as root.
                    fs::rename(session.join("session.json"), session.join("pending.json"))?;
                    fs::create_dir(session.join("session.json"))?;
                }
                Ok(())
            },
        )?;
        assert!(delete_saved_sandbox(&client, session.path(), &mut manifest).is_err());
        fs::remove_dir(session.path().join("session.json"))?;
        fs::rename(
            session.path().join("pending.json"),
            session.path().join("session.json"),
        )?;
        manifest = read_manifest(session.path())?;
        assert_eq!(manifest.status, "deletion-pending");
        finish(
            &client,
            &super::super::super::tokio_rt()?,
            session.path(),
            &mut manifest,
        )?;
        server.join().unwrap()?;
        assert_eq!(read_manifest(session.path())?.status, "saved");
        Ok(())
    }

    #[test]
    fn finish_does_not_delete_without_persisting_pending_state() -> Result<()> {
        let (session, mut manifest) = saved_session()?;
        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let client = Client::new_with_timeouts(
            &format!("http://{}", listener.local_addr()?),
            "",
            Duration::from_millis(100),
            Duration::from_millis(100),
        )?;
        fs::remove_file(session.path().join("session.json"))?;
        fs::create_dir(session.path().join("session.json"))?;
        assert!(delete_saved_sandbox(&client, session.path(), &mut manifest).is_err());
        assert_eq!(
            listener.accept().unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        Ok(())
    }

    #[test]
    fn setup_keeps_only_template_and_ignores_legacy_model_settings() -> Result<()> {
        let config: Config = serde_json::from_value(json!({
            "template":"existing", "model":"old-model", "base_url":"invalid", "api_key":"old-key"
        }))?;
        config.validate()?;
        assert_eq!(
            serde_json::to_value(config)?,
            json!({"template":"existing"})
        );
        Config::default().validate()?;
        Ok(())
    }
    #[test]
    fn conflicting_execution_config_is_rejected_without_changing_it() -> Result<()> {
        let root = tempfile::tempdir()?;
        check_codex_home(root.path())?;
        let path = root.path().join("environments.toml");
        fs::write(&path, "include_local=true")?;
        assert!(check_codex_home(root.path()).is_err());
        assert_eq!(fs::read_to_string(path)?, "include_local=true");
        Ok(())
    }
    #[test]
    fn session_lock_and_private_atomic_files() -> Result<()> {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir()?;
        let held = lock(root.path())?;
        assert!(lock(root.path()).is_err());
        drop(held);
        assert!(lock(root.path()).is_ok());
        let path = root.path().join("config");
        write_private(&path, b"secret")?;
        write_private(&path, b"replacement")?;
        assert_eq!(fs::metadata(&path)?.permissions().mode() & 0o777, 0o600);
        assert_eq!(fs::read(path)?, b"replacement");
        Ok(())
    }
    #[test]
    fn output_paths_resolve_existing_symlinks() -> Result<()> {
        let root = tempfile::tempdir()?;
        fs::create_dir(root.path().join("project"))?;
        std::os::unix::fs::symlink(root.path().join("project"), root.path().join("link"))?;
        assert_eq!(
            absolute(&root.path().join("link/new/subdir"))?,
            root.path().join("project/new/subdir")
        );
        Ok(())
    }
}
