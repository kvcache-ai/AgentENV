// The benchmark uses a custom main (harness = false). Include it under the
// standard test harness to exercise preparation and error propagation.
#[allow(dead_code)]
mod snapshot_benchmark {
    include!("../benches/snapshot_benchmark.rs");
    use std::collections::VecDeque;
    use std::sync::Mutex;

    #[derive(Default)]
    struct CommandReply {
        exit_code: i32,
    }

    struct ScriptedGuest {
        replies: Mutex<VecDeque<CommandReply>>,
        calls: Mutex<Vec<(String, Vec<String>)>>,
    }

    impl ScriptedGuest {
        fn new(replies: Vec<CommandReply>) -> Self {
            Self {
                replies: Mutex::new(replies.into()),
                calls: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait::async_trait(?Send)]
    impl SandboxExecutor for ScriptedGuest {
        fn executor(&self) -> Result<agentenv::sandbox::Executor> {
            anyhow::bail!("scripted guest uses the command override")
        }
        async fn run_command(&self, cmd: &str, args: &[&str]) -> Result<ProcessOutput> {
            self.calls.lock().unwrap().push((
                cmd.to_owned(),
                args.iter().map(|arg| (*arg).to_owned()).collect(),
            ));
            let reply = self.replies.lock().unwrap().pop_front().unwrap();
            Ok(ProcessOutput {
                stdout: "fixture stdout".into(),
                stderr: "fixture stderr".into(),
                exit_code: reply.exit_code,
            })
        }
    }

    #[tokio::test]
    async fn dirty_memory_resizes_tmpfs_before_writing_one_gib() {
        let guest = ScriptedGuest::new(vec![CommandReply::default(), CommandReply::default()]);
        dirty_1g_mem(&guest).await.unwrap();
        assert_eq!(
            *guest.calls.lock().unwrap(),
            vec![
                (
                    "mount".into(),
                    vec!["-o".into(), "remount,size=1024M".into(), "/dev/shm".into()]
                ),
                (
                    "dd".into(),
                    vec![
                        "if=/dev/zero".into(),
                        "of=/dev/shm/bench_1g".into(),
                        "bs=1M".into(),
                        "count=1024".into()
                    ]
                )
            ]
        );
    }

    #[tokio::test]
    async fn dirty_memory_does_not_write_when_tmpfs_resize_fails() {
        let guest = ScriptedGuest::new(vec![CommandReply { exit_code: 1 }]);
        let error = dirty_1g_mem(&guest).await.unwrap_err();
        assert!(error
            .to_string()
            .contains("resize guest /dev/shm exited with 1"));
        assert_eq!(guest.calls.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn dirty_memory_propagates_dd_failure_after_tmpfs_resize() {
        let guest =
            ScriptedGuest::new(vec![CommandReply::default(), CommandReply { exit_code: 1 }]);
        let error = dirty_1g_mem(&guest).await.unwrap_err();
        assert!(error
            .to_string()
            .contains("dd /dev/shm/bench_1g exited with 1"));
        assert_eq!(guest.calls.lock().unwrap().len(), 2);
    }

    #[test]
    fn failed_preparation_stops_sandbox_before_returning_original_error() {
        for cleanup_fails in [false, true] {
            let mut stops = 0;
            let error = prepare_snapshot_workload(
                &mut stops,
                |_| anyhow::bail!("dd exited with 1: No space left on device"),
                |stops| {
                    *stops += 1;
                    anyhow::ensure!(!cleanup_fails, "injected stop failure");
                    Ok(())
                },
            )
            .unwrap_err();
            assert_eq!(stops, 1);
            assert_eq!(
                error.root_cause().to_string(),
                "dd exited with 1: No space left on device"
            );
            assert_eq!(
                format!("{error:#}").contains("injected stop failure"),
                cleanup_fails
            );
        }
    }

    #[test]
    fn successful_preparation_keeps_sandbox_running_for_sampling() {
        prepare_snapshot_workload(
            &mut (),
            |_| Ok(()),
            |_| panic!("successful preparation must not stop the sandbox before sampling"),
        )
        .unwrap();
    }

    fn run_dd(destination: &str) -> ProcessOutput {
        let output = std::process::Command::new("dd")
            .args(["if=/dev/zero", destination, "bs=512", "count=1"])
            .output()
            .expect("run dd fixture");
        ProcessOutput {
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
            exit_code: output.status.code().expect("dd exited without a signal"),
        }
    }

    #[test]
    fn dd_write_failure_prevents_snapshot_sampling() {
        let output = run_dd("of=/dev/full");
        assert_ne!(output.exit_code, 0);
        assert!(!output.stderr.is_empty());
        for command in ["dd /tmp/bench_1g", "dd /dev/shm/bench_1g"] {
            let mut measured = false;
            let error = run_default_benchmark("snapshot_creation", &[], || {
                ensure_guest_command_success(command, &output)?;
                measured = true;
                Ok(vec![Duration::from_millis(1)])
            })
            .unwrap_err();
            assert!(!measured);
            let cause = error.root_cause().to_string();
            assert!(cause.contains(command));
            assert!(cause.contains(&format!("exited with {}", output.exit_code)));
            assert!(cause.contains(&format!("stderr={:?}", output.stderr)));
        }
    }

    #[test]
    fn successful_dd_accepts_statistics_on_stderr() {
        let output = run_dd("of=/dev/null");
        assert_eq!(output.exit_code, 0);
        assert!(!output.stderr.is_empty());
        ensure_guest_command_success("dd", &output).unwrap();
    }

    #[test]
    fn failed_disk_preparation_commands_preserve_diagnostics() {
        let output = ProcessOutput {
            stdout: "partial output".into(),
            stderr: "permission denied".into(),
            exit_code: 2,
        };
        for command in ["sync", "drop guest caches"] {
            let error = ensure_guest_command_success(command, &output).unwrap_err();
            let message = error.to_string();
            assert!(message.contains(command));
            assert!(message.contains("exited with 2"));
            assert!(message.contains("partial output"));
            assert!(message.contains("permission denied"));
        }
    }

    #[test]
    fn failed_benchmark_propagates_error_instead_of_reporting_a_skip() {
        let error = run_default_benchmark("snapshot_resume", &[], || {
            anyhow::bail!("injected restore failure")
        })
        .unwrap_err();
        assert_eq!(error.to_string(), "run snapshot_resume");
        assert_eq!(error.root_cause().to_string(), "injected restore failure");
    }

    #[test]
    fn unmatched_benchmark_does_not_execute_workload() {
        let ran = run_default_benchmark("snapshot_resume", &["snapshot_creation".into()], || {
            panic!("filtered workload must not execute")
        })
        .unwrap();
        assert!(!ran);
    }

    #[test]
    fn successful_benchmark_reports_samples() {
        let mut calls = 0;
        let ran = run_default_benchmark("snapshot_resume", &[], || {
            calls += 1;
            Ok(vec![Duration::from_millis(1)])
        })
        .unwrap();
        assert!(ran);
        assert_eq!(calls, 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires KVM, ublk, runtime assets, capabilities, and isolated AENV_TEST_STATE_DIR"]
    async fn real_vm_dd_before_after() -> Result<()> {
        let state_dir = PathBuf::from(
            std::env::var_os("AENV_TEST_STATE_DIR").context("set AENV_TEST_STATE_DIR")?,
        );
        let config = agentenv::cfg::ConfigManager::init_global()?.config();
        anyhow::ensure!(
            config.home_path == state_dir.join("home")
                && config.runtime_path == state_dir.join("run"),
            "VM reproduction requires home and runtime paths under AENV_TEST_STATE_DIR"
        );
        agentenv::setup::ensure_environment(config, None).await?;
        let mut sandbox = setup_sandbox_inner(HEAVY_MEM_SIZE_MIB).await?;
        let result = async {
            let environment = sandbox
                .run_command(
                    "sh",
                    &[
                        "-c",
                        "uname -r; grep MemTotal /proc/meminfo; df -B1 /dev/shm",
                    ],
                )
                .await?;
            ensure_guest_command_success("inspect guest", &environment)?;

            // Original preparation: successful RPC, but its exit code was ignored.
            let before = sandbox
                .run_command(
                    "dd",
                    &[
                        "if=/dev/zero",
                        "of=/dev/shm/bench_1g",
                        "bs=1M",
                        "count=1024",
                    ],
                )
                .await?;
            let before_size = sandbox
                .run_command("stat", &["-c", "%s", "/dev/shm/bench_1g"])
                .await?;
            ensure_guest_command_success("stat original workload", &before_size)?;
            let before_bytes: u64 = before_size.stdout.trim().parse()?;
            anyhow::ensure!(before.exit_code != 0, "original dd unexpectedly succeeded");
            anyhow::ensure!(
                before_bytes < 1024 * 1024 * 1024,
                "original dd wrote a full GiB"
            );

            let removed = sandbox.run_command("rm", &["/dev/shm/bench_1g"]).await?;
            ensure_guest_command_success("remove partial workload", &removed)?;

            // Exercise the production fixture, including remount and exit-code checks.
            dirty_1g_mem(&sandbox).await?;
            let after_size = sandbox
                .run_command("stat", &["-c", "%s", "/dev/shm/bench_1g"])
                .await?;
            ensure_guest_command_success("stat fixed workload", &after_size)?;
            let after_bytes: u64 = after_size.stdout.trim().parse()?;
            anyhow::ensure!(
                after_bytes == 1024 * 1024 * 1024,
                "fixed dd did not write a full GiB"
            );
            let capacity = sandbox.run_command("df", &["-B1", "/dev/shm"]).await?;
            ensure_guest_command_success("inspect fixed tmpfs", &capacity)?;
            println!(
                "VM_DD_REPRO {}",
                serde_json::json!({
                    "vcpu_count": 1,
                    "memory_mib": HEAVY_MEM_SIZE_MIB,
                    "before": {
                        "environment": environment.stdout,
                        "dd_exit_code": before.exit_code,
                        "dd_stderr": before.stderr,
                        "file_bytes": before_bytes,
                    },
                    "after": {
                        "dd_exit_code": 0,
                        "file_bytes": after_bytes,
                        "capacity": capacity.stdout,
                    },
                })
            );
            Ok::<(), anyhow::Error>(())
        }
        .await;
        let cleanup = sandbox.stop().await;
        let daemon_cleanup = UblkDeviceManager::global().shutdown_daemon().await;
        result?;
        cleanup?;
        daemon_cleanup
    }
}
