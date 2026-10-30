//! Sandboxes keep running while the ublk daemon crashes under them.
//!
//! This is its own test binary because it kills the process-wide ublk
//! daemon, which would disturb other tests sharing it. Set
//! `AENV_UBLK_CRASH_SEED` to replay a crash sequence, and
//! `AENV_UBLK_CRASH_SANDBOXES` / `AENV_UBLK_CRASH_ROUNDS` to scale it.

mod common;

use std::time::{Duration, Instant};

use agentenv::cfg::ConfigManager;
use agentenv::sandbox::{FirecrackerSandbox, SandboxExecutor, SandboxLaunchConfig};
use agentenv::snapshot::RunnableSnapshot;
use anyhow::{bail, ensure, Context, Result};
use futures::future::try_join_all;
use tempfile::tempdir;
use uuid::Uuid;

/// How many sandboxes launched mid-run may join the starting ones.
const EXTRA_SANDBOXES: usize = 4;
const RECOVERY_TIMEOUT: Duration = Duration::from_secs(90);
/// Bounds every sandbox operation, so a hang fails the test with its name.
const OP_TIMEOUT: Duration = Duration::from_secs(180);

/// Rewrites 16 files in a loop. Each file is synced before its checksum, and
/// the checksum before both replace the previous pair, so after the loop
/// stops every pair must match. Any failed step is logged to `errors`. The
/// short pause leaves CPU for many sandboxes per host while keeping writes in
/// flight most of the time.
const WRITER: &str = r#"
cd /stress
n=0
while [ ! -e stop ]; do
  n=$((n + 1)); f=f$((n % 16))
  if head -c 262144 /dev/urandom > $f.new && sync $f.new &&
     sha256sum < $f.new > $f.sum.new && sync $f.sum.new &&
     mv $f.new $f && mv $f.sum.new $f.sum; then
    echo $n > progress
  else
    echo "iteration $n failed" >> errors
  fi
  sleep 0.1
done
touch done
"#;

/// Rereads part of the image's lower layers from the device.
const READER: &str = r#"
while [ ! -e /stress/stop ]; do
  find /usr/lib -type f -size -256k 2>/dev/null | head -n 500 | xargs cat > /dev/null 2>&1
  echo 3 > /proc/sys/vm/drop_caches 2>/dev/null
  sleep 1
done
"#;

/// Stops the workload and checks it from the device, bypassing the page
/// cache, plus the guest kernel log for block or filesystem errors.
const VERIFY: &str = r#"
cd /stress && touch stop
while [ ! -e done ]; do sleep 0.1; done
if [ -e errors ]; then cat errors; exit 1; fi
for s in f*.sum; do
  f=${s%.sum}
  [ "$(dd if=$f iflag=direct bs=262144 status=none | sha256sum)" = "$(cat $s)" ] ||
    { echo "$f does not match its checksum"; exit 1; }
done
if dmesg 2>/dev/null | grep -iE 'I/O error|blk_update_request|EXT4-fs error|Remounting filesystem read-only|Aborting journal'; then
  exit 1
fi
cat progress
"#;

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0 >> 33
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    fn millis(&mut self, min: u64, max: u64) -> Duration {
        Duration::from_millis(min + self.below(max - min + 1))
    }
}

fn env_u64(name: &str) -> Option<u64> {
    let value = std::env::var(name).ok()?;
    match value.strip_prefix("0x") {
        Some(hex) => u64::from_str_radix(hex, 16).ok(),
        None => value.parse().ok(),
    }
}

async fn bounded<T>(what: &str, op: impl std::future::Future<Output = Result<T>>) -> Result<T> {
    tokio::time::timeout(OP_TIMEOUT, op)
        .await
        .with_context(|| format!("{what} did not finish within {OP_TIMEOUT:?}"))?
}

async fn sh(sandbox: &FirecrackerSandbox, script: &str) -> Result<String> {
    let output = bounded("guest script", sandbox.run_command("sh", &["-c", script])).await?;
    ensure!(
        output.exit_code == 0,
        "guest script failed ({}): {}{}",
        output.exit_code,
        output.stdout,
        output.stderr
    );
    Ok(output.stdout)
}

async fn start_workload(sandbox: &FirecrackerSandbox) -> Result<()> {
    sh(
        sandbox,
        &format!(
            "rm -rf /stress && mkdir /stress && cat > /stress/writer.sh <<'EOF'\n{WRITER}\nEOF\n\
             cat > /stress/reader.sh <<'EOF'\n{READER}\nEOF\n\
             setsid sh /stress/writer.sh < /dev/null > /dev/null 2>&1 &\n\
             setsid sh /stress/reader.sh < /dev/null > /dev/null 2>&1 &"
        ),
    )
    .await?;
    Ok(())
}

async fn progress(sandbox: &FirecrackerSandbox) -> Result<u64> {
    let output = bounded(
        "reading guest progress",
        sandbox.run_command("cat", &["/stress/progress"]),
    )
    .await?;
    Ok(output.stdout.trim().parse().unwrap_or(0))
}

/// Wait until every sandbox's writer completes more synced iterations.
async fn wait_all_progress(sandboxes: &[FirecrackerSandbox]) -> Result<()> {
    try_join_all(
        sandboxes
            .iter()
            .enumerate()
            .map(|(index, sandbox)| async move {
                let deadline = Instant::now() + RECOVERY_TIMEOUT;
                let start = progress(sandbox).await?;
                while progress(sandbox).await? < start + 2 {
                    if Instant::now() > deadline {
                        bail!("sandbox {index} writes stalled at iteration {start}");
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Ok(())
            }),
    )
    .await?;
    Ok(())
}

async fn launch(runnable: &RunnableSnapshot) -> Result<FirecrackerSandbox> {
    let mut sandbox = FirecrackerSandbox::from_snapshot(runnable, &SandboxLaunchConfig::default())?;
    bounded("sandbox start", sandbox.start()).await?;
    start_workload(&sandbox).await?;
    Ok(sandbox)
}

/// Pid of a ublk daemon serving this process's socket, other than `except`.
fn find_daemon_pid(except: &[i32]) -> Option<i32> {
    let socket = &ConfigManager::global_config().ublk.daemon_socket_path;
    let socket = socket.to_str()?;
    std::fs::read_dir("/proc")
        .ok()?
        .filter_map(|entry| entry.ok()?.file_name().to_str()?.parse::<i32>().ok())
        .find(|pid| {
            !except.contains(pid)
                && std::fs::read(format!("/proc/{pid}/cmdline")).is_ok_and(|cmdline| {
                    let cmdline = String::from_utf8_lossy(&cmdline);
                    cmdline.contains("uvm-ublk-daemon") && cmdline.contains(socket)
                })
        })
}

struct Killer(Vec<i32>);

impl Killer {
    fn kill(&mut self) -> Result<()> {
        let pid = find_daemon_pid(&self.0).context("no ublk daemon to kill")?;
        // The process may already be gone.
        let _ = nix::sys::signal::kill(
            nix::unistd::Pid::from_raw(pid),
            nix::sys::signal::Signal::SIGKILL,
        );
        self.0.push(pid);
        Ok(())
    }

    /// Kill the next daemon to appear within `window`, if any: usually the
    /// replacement while it recovers devices.
    async fn kill_next(&mut self, window: Duration) -> Result<()> {
        let deadline = Instant::now() + window;
        while Instant::now() < deadline {
            if find_daemon_pid(&self.0).is_some() {
                return self.kill();
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
enum Chaos {
    Kill,
    KillTwice,
    /// Kill the daemon while a sandbox launches.
    KillDuringLaunch,
    /// Pause and resume a sandbox after the daemon was replaced.
    PauseResume,
}

#[tokio::test(flavor = "multi_thread")]
async fn sandboxes_survive_random_ublk_daemon_crashes() -> Result<()> {
    common::setup().await;
    let seed = env_u64("AENV_UBLK_CRASH_SEED").unwrap_or_else(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    });
    let count = env_u64("AENV_UBLK_CRASH_SANDBOXES").unwrap_or(8) as usize;
    let rounds = env_u64("AENV_UBLK_CRASH_ROUNDS").unwrap_or(8);
    eprintln!(
        "ublk crash sequence: AENV_UBLK_CRASH_SEED={seed:#x} sandboxes={count} rounds={rounds}"
    );
    let mut rng = Rng(seed);

    let store = tempdir()?;
    let (builder, snapshot_manager, _) = common::snapshot_test_parts(store.path());
    let template = builder
        .build_and_publish(
            &snapshot_manager,
            common::default_rootfs_template_build_spec()
                .alias(format!("ublk-crash-{}", Uuid::now_v7()))
                .resources(1, 256),
        )
        .await?;
    let runnable = snapshot_manager.resolve_runnable(template).await?;

    // Launched together, they share the template's memory device and each
    // owns a runtime rootfs device.
    let mut sandboxes = try_join_all((0..count).map(|_| launch(&runnable))).await?;
    wait_all_progress(&sandboxes).await?;

    let mut killer = Killer(Vec::new());
    for round in 0..rounds {
        let action = match rng.below(7) {
            0..=2 => Chaos::Kill,
            3 | 4 => Chaos::KillTwice,
            5 => Chaos::KillDuringLaunch,
            _ => Chaos::PauseResume,
        };
        eprintln!("round {round}: {action:?} ({} sandboxes)", sandboxes.len());
        tokio::time::sleep(rng.millis(200, 2000)).await;
        match action {
            Chaos::Kill => killer.kill()?,
            Chaos::KillTwice => {
                killer.kill()?;
                killer.kill_next(rng.millis(0, 500)).await?;
            }
            Chaos::KillDuringLaunch => {
                let delay = rng.millis(0, 1500);
                let (launched, killed) = tokio::join!(launch(&runnable), async {
                    tokio::time::sleep(delay).await;
                    killer.kill()
                });
                killed?;
                // A launch racing the crash may fail; it must not disturb
                // the other sandboxes.
                match launched {
                    Ok(sandbox) if sandboxes.len() < count + EXTRA_SANDBOXES => {
                        sandboxes.push(sandbox)
                    }
                    Ok(mut sandbox) => bounded("sandbox stop", sandbox.stop()).await?,
                    Err(err) => eprintln!("  launch racing a crash failed: {err:#}"),
                }
            }
            Chaos::PauseResume => {
                killer.kill()?;
                wait_all_progress(&sandboxes).await?;
                let index = rng.below(sandboxes.len() as u64) as usize;
                let mut sandbox = sandboxes.swap_remove(index);
                let before = progress(&sandbox).await?;
                let snapshot = bounded("sandbox pause", sandbox.pause()).await?;
                bounded("sandbox stop", sandbox.stop()).await?;
                let resumed = bounded(
                    "sandbox resume",
                    FirecrackerSandbox::resume_from_snapshot_config(&snapshot),
                )
                .await?;
                ensure!(
                    progress(&resumed).await? >= before,
                    "resumed sandbox lost writes"
                );
                sandboxes.push(resumed);
            }
        }
        wait_all_progress(&sandboxes)
            .await
            .with_context(|| format!("after round {round} ({action:?}), seed {seed:#x}"))?;
    }

    for (index, sandbox) in sandboxes.iter_mut().enumerate() {
        let iterations = sh(sandbox, VERIFY)
            .await
            .with_context(|| format!("verify sandbox {index}, seed {seed:#x}"))?;
        eprintln!("sandbox {index}: {} verified iterations", iterations.trim());
        bounded("sandbox stop", sandbox.stop()).await?;
    }
    Ok(())
}
