use std::collections::{BTreeMap, HashSet};
use std::fs::{self, DirBuilder, OpenOptions};
use std::io::Write;
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, BufReader};
use tokio::process::Command;
use tokio::time::{sleep, timeout_at, Instant};

use crate::process::{self, PATH};
use crate::{ComposePlan, MAX_PLAN_BYTES};

const WORKDIR: &str = "/var/lib/agentenv-compose";

fn environment(document: &Value) -> BTreeMap<String, String> {
    let mut environment: BTreeMap<_, _> = [
        ("PATH", PATH),
        ("HOME", "/root"),
        ("DOCKER_HOST", "unix:///var/run/docker.sock"),
        ("COMPOSE_ANSI", "never"),
    ]
    .into_iter()
    .map(|(key, value)| (key.to_owned(), value.to_owned()))
    .collect();
    if let Some(services) = document.get("services").and_then(Value::as_object) {
        for service in services.values() {
            if let Some(variables) = service.get("environment").and_then(Value::as_object) {
                for (key, value) in variables {
                    if value.is_null() {
                        environment.remove(key);
                    }
                }
            }
        }
    }
    environment
}

pub(crate) fn mountpoints() -> Result<HashSet<PathBuf>> {
    Ok(fs::read_to_string("/proc/self/mountinfo")?
        .lines()
        .filter_map(|line| line.split_whitespace().nth(4))
        // Only fixed paths without mountinfo escapes are queried by this runtime.
        .map(PathBuf::from)
        .collect())
}

fn validate(
    plan: &ComposePlan,
    mounts: &HashSet<PathBuf>,
    device: impl Fn(&Path) -> Result<u64>,
) -> Result<()> {
    anyhow::ensure!(
        plan.compose.get("services").is_some_and(Value::is_object),
        "Compose services must be an object"
    );
    let root_device = device(Path::new("/"))?;
    let mut devices = HashSet::new();
    for service in &plan.services {
        let mount = &service.mount_path;
        let path = Path::new(mount);
        anyhow::ensure!(
            mounts.contains(path),
            "service drive is not mounted: {mount}"
        );
        let dev = device(path)?;
        anyhow::ensure!(
            dev != root_device && devices.insert(dev),
            "service drive is not isolated: {mount}"
        );
        anyhow::ensure!(service.config.is_object(), "source image config is missing");
    }
    Ok(())
}

fn device(path: &Path) -> Result<u64> {
    let metadata =
        fs::symlink_metadata(path).with_context(|| format!("stat {}", path.display()))?;
    anyhow::ensure!(
        metadata.is_dir() && !metadata.file_type().is_symlink(),
        "service drive is not a directory mount: {}",
        path.display()
    );
    Ok(metadata.dev())
}

fn command(program: &str, environment: &BTreeMap<String, String>) -> Command {
    // Resolve independently of the sanitized environment: a null service PATH
    // must stay absent during Compose's second interpolation pass.
    let mut command = Command::new(format!("/usr/local/bin/{program}"));
    command.current_dir(WORKDIR).env_clear().envs(environment);
    command
}

fn write_private_json(path: &Path, value: &impl serde::Serialize) -> Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    serde_json::to_writer(&mut file, value)?;
    file.flush()?;
    Ok(())
}

async fn read_plan(reader: impl AsyncRead + Unpin) -> Result<ComposePlan> {
    let mut reader = BufReader::new(reader.take((MAX_PLAN_BYTES + 1) as u64));
    let mut input = Vec::new();
    // A newline completes the frame; envd's stdin stream does not need EOF.
    reader.read_until(b'\n', &mut input).await?;
    anyhow::ensure!(input.len() <= MAX_PLAN_BYTES, "Compose plan exceeds 4 MiB");
    serde_json::from_slice(&input).context("invalid Compose startup plan")
}

async fn start(plan: ComposePlan, deadline: Instant) -> Result<()> {
    validate(&plan, &mountpoints()?, device)?;
    DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(WORKDIR)?;
    let env = environment(&Value::Null);
    loop {
        let output = process::run(
            command("docker", &env).args(["info", "--format", "{{json .}}"]),
            false,
        )
        .await?;
        if output.status.success() {
            let info: Value =
                serde_json::from_slice(&output.stdout).context("decode Docker info")?;
            anyhow::ensure!(
                info.get("Driver").and_then(Value::as_str) == Some("plain"),
                "Docker must use the plain snapshotter"
            );
            break;
        }
        anyhow::ensure!(
            deadline.saturating_duration_since(Instant::now()) > Duration::from_millis(200),
            "Docker did not become ready: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        sleep(Duration::from_millis(200)).await;
    }
    // Registration updates shared configuration and must remain serial.
    for service in &plan.services {
        let metadata = Path::new(WORKDIR).join(format!("{}.json", service.drive_id));
        write_private_json(&metadata, &service.config)?;
        process::run(
            command("plain-snapshotter", &env)
                .args(["register", "--image-metadata"])
                .arg(metadata)
                .args([&service.local_image, &service.mount_path]),
            true,
        )
        .await?;
    }
    let compose_file = Path::new(WORKDIR).join("compose.json");
    write_private_json(&compose_file, &plan.compose)?;
    let wait_seconds = deadline
        .saturating_duration_since(Instant::now())
        .as_secs_f64()
        .ceil()
        .max(1.0)
        .to_string();
    process::run(
        command("docker", &environment(&plan.compose))
            .args([
                "compose",
                "--project-name",
                "aenv",
                "--env-file",
                "/dev/null",
                "--file",
            ])
            .arg(compose_file)
            .args([
                "up",
                "--detach",
                "--no-build",
                "--pull",
                "never",
                "--wait",
                "--wait-timeout",
                &wait_seconds,
            ]),
        true,
    )
    .await?;
    Ok(())
}

pub async fn run(reader: impl AsyncRead + Unpin, budget: Duration) -> Result<()> {
    let deadline = Instant::now()
        .checked_add(budget)
        .context("invalid startup timeout")?;
    process::until_signal(async {
        timeout_at(deadline, async {
            start(read_plan(reader).await?, deadline).await
        })
        .await
        .context("Compose startup deadline exceeded")?
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ComposeService;
    use serde_json::json;
    use tokio::io::AsyncWriteExt;

    fn plan() -> ComposePlan {
        ComposePlan {
            compose: json!({"services": {"web": {}}}),
            services: (0..2)
                .map(|i| ComposeService {
                    name: format!("web{i}"),
                    image: "busybox:1.37".into(),
                    drive_id: format!("compose_{i}"),
                    mount_path: format!("/mnt/compose_{i}"),
                    local_image: format!("aenv-compose/service-{i}:local"),
                    config: json!({}),
                })
                .collect(),
        }
    }

    #[test]
    fn rejects_unmounted_or_shared_drives() {
        let mut plan = plan();
        let mounts = plan
            .services
            .iter()
            .map(|s| PathBuf::from(&s.mount_path))
            .collect();
        assert!(validate(&plan, &HashSet::new(), |_| Ok(1))
            .unwrap_err()
            .to_string()
            .contains("not mounted"));
        assert!(validate(&plan, &mounts, |_| Ok(1))
            .unwrap_err()
            .to_string()
            .contains("not isolated"));
        let device = |path: &Path| Ok(if path == Path::new("/") { 1 } else { 2 });
        assert!(validate(&plan, &mounts, device)
            .unwrap_err()
            .to_string()
            .contains("not isolated"));
        plan.services[0].mount_path = "/elsewhere".into();
        assert!(validate(&plan, &mounts, device)
            .unwrap_err()
            .to_string()
            .contains("not mounted"));
    }

    #[test]
    fn validates_isolated_drives_and_image_config() {
        let mut plan = plan();
        let mounts = plan
            .services
            .iter()
            .map(|s| PathBuf::from(&s.mount_path))
            .collect();
        let device = |path: &Path| {
            Ok(match path.to_str().unwrap() {
                "/" => 1,
                "/mnt/compose_0" => 2,
                _ => 3,
            })
        };
        validate(&plan, &mounts, device).unwrap();
        plan.services[0].config = Value::Null;
        assert!(validate(&plan, &mounts, device)
            .unwrap_err()
            .to_string()
            .contains("image config"));
    }

    #[test]
    fn unresolved_variables_stay_unset() {
        let env = environment(
            &json!({"services": {"web": {"environment": {"HOME": null, "PATH": null}}}}),
        );
        assert!(!env.contains_key("HOME"));
        assert!(!env.contains_key("PATH"));
        assert_eq!(env.len(), 2);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn framed_input_does_not_require_eof() {
        let (mut writer, reader) = tokio::io::duplex(4096);
        let mut frame = serde_json::to_vec(&plan()).unwrap();
        frame.push(b'\n');
        writer.write_all(&frame).await.unwrap();
        assert_eq!(
            timeout_at(Instant::now() + Duration::from_secs(1), read_plan(reader))
                .await
                .unwrap()
                .unwrap()
                .services
                .len(),
            2
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn rejects_oversized_input_and_times_out_missing_frame() {
        assert!(read_plan(&vec![b' '; MAX_PLAN_BYTES + 1][..])
            .await
            .unwrap_err()
            .to_string()
            .contains("exceeds 4 MiB"));
        let (_writer, reader) = tokio::io::duplex(128);
        assert!(run(reader, Duration::from_millis(10))
            .await
            .unwrap_err()
            .to_string()
            .contains("deadline exceeded"));
    }

    #[test]
    fn metadata_is_private_and_not_overwritten() {
        use std::os::unix::fs::PermissionsExt;
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("config.json");
        write_private_json(&path, &json!({"Env": ["VALUE=private"]})).unwrap();
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert!(write_private_json(&path, &json!({})).is_err());
    }
}
