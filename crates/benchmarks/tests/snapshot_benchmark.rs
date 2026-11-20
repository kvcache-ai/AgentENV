// The benchmark uses a custom main (harness = false). Include it under the
// standard test harness to exercise its hardware-independent runner.
#[allow(dead_code)]
mod snapshot_benchmark {
    include!("../benches/snapshot_benchmark.rs");

    use agentenv::sandbox::ProcessOutput;
    use std::collections::VecDeque;
    use std::sync::atomic::AtomicUsize;
    use std::sync::Mutex;

    #[derive(Default)]
    struct CommandReply {
        start_delay: Duration,
        execution_delay: Duration,
        exit_code: i32,
    }

    struct ScriptedGuest {
        replies: Mutex<VecDeque<CommandReply>>,
        calls: Mutex<Vec<(String, Vec<String>)>>,
        completed: AtomicUsize,
    }

    impl ScriptedGuest {
        fn new(replies: Vec<CommandReply>) -> Self {
            Self {
                replies: Mutex::new(replies.into()),
                calls: Mutex::new(Vec::new()),
                completed: AtomicUsize::new(0),
            }
        }
    }

    #[test]
    fn criterion_only_validates_resume_command_for_selected_benchmarks() {
        const CHILD_FILTER: &str = "AENV_TEST_RESUME_CRITERION_FILTER";
        if let Ok(filter) = std::env::var(CHILD_FILTER) {
            let output = tempfile::tempdir().unwrap();
            let mut criterion = Criterion::default()
                .with_filter(filter)
                .output_directory(output.path());
            bench_snapshot_resume_to_command(&mut criterion);
            return;
        }

        // Isolate the invalid command from other tests without changing the
        // parent process environment. A selected case must still reject it.
        for (filter, succeeds) in [
            ("^unrelated_benchmark$", true),
            ("^snapshot_resume_to_command$", false),
        ] {
            let state = tempfile::tempdir().unwrap();
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "snapshot_benchmark::criterion_only_validates_resume_command_for_selected_benchmarks",
                    "--nocapture",
                ])
                .env(CHILD_FILTER, filter)
                .env(RESUME_COMMAND_ENV, "")
                .env("AENV_CONFIG_PATH", state.path().join("missing.toml"))
                .output()
                .unwrap();
            let stderr = String::from_utf8_lossy(&output.stderr);
            assert_eq!(output.status.success(), succeeds, "{filter}: {stderr}");
            if !succeeds {
                assert!(stderr.contains("AENV_BENCH_RESUME_COMMAND must not be empty"));
            }
        }
    }

    #[test]
    fn failed_sample_command_stops_sample_and_warm_sandbox() {
        for keep_warm in [false, true] {
            for timeout in [false, true] {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .start_paused(true)
                    .build()
                    .unwrap();
                let mut replies = Vec::new();
                if keep_warm {
                    replies.push(CommandReply::default());
                }
                replies.push(CommandReply {
                    start_delay: if timeout {
                        Duration::from_secs(120)
                    } else {
                        Duration::ZERO
                    },
                    exit_code: 7,
                    ..Default::default()
                });
                let guest = ScriptedGuest::new(replies);
                let mut resumed = 0;
                let mut stopped = Vec::new();
                let error = collect_resume_command_samples(
                    keep_warm,
                    3,
                    || {
                        resumed += 1;
                        Ok(resumed)
                    },
                    |_| rt.block_on(run_resume_command_inner(&guest, "workload")),
                    |sandboxes| {
                        stopped.extend(sandboxes);
                        Ok(())
                    },
                )
                .unwrap_err();
                assert!(format!("{error:#}").contains(if timeout {
                    "timed out after 60s"
                } else {
                    "exited with 7"
                }));
                assert_eq!(resumed, if keep_warm { 2 } else { 1 });
                assert_eq!(stopped, if keep_warm { vec![2, 1] } else { vec![1] });
                assert_eq!(guest.calls.lock().unwrap().len(), resumed);
            }
        }
    }

    #[test]
    fn warmup_or_sample_resume_failure_stops_retained_sandbox() {
        for fail_warmup in [false, true] {
            let mut resumed = 0;
            let mut commands = 0;
            let mut stopped = Vec::new();
            let error = collect_resume_command_samples(
                true,
                3,
                || {
                    resumed += 1;
                    anyhow::ensure!(resumed == 1, "sample resume failed");
                    Ok(resumed)
                },
                |_| {
                    commands += 1;
                    anyhow::ensure!(!fail_warmup, "warmup command failed");
                    Ok(())
                },
                |sandboxes| {
                    stopped.extend(sandboxes);
                    Ok(())
                },
            )
            .unwrap_err();
            assert!(format!("{error:#}").contains(if fail_warmup {
                "warmup command failed"
            } else {
                "sample resume failed"
            }));
            assert_eq!(commands, 1);
            assert_eq!(stopped, vec![1]);
        }
    }

    #[test]
    fn sample_cleanup_failure_still_stops_warm_sandbox_and_preserves_primary_error() {
        for fail_command in [false, true] {
            let mut resumed = 0;
            let mut stopped = Vec::new();
            let error = collect_resume_command_samples(
                true,
                3,
                || {
                    resumed += 1;
                    Ok(resumed)
                },
                |sandbox| {
                    anyhow::ensure!(!fail_command || *sandbox == 1, "sample RPC failed");
                    Ok(())
                },
                |sandboxes| {
                    let sample = sandboxes == vec![2];
                    stopped.extend(sandboxes);
                    anyhow::ensure!(!sample, "sample cleanup failed");
                    Ok(())
                },
            )
            .unwrap_err();
            assert!(format!("{error:#}").contains(if fail_command {
                "sample RPC failed"
            } else {
                "sample cleanup failed"
            }));
            assert_eq!(stopped, vec![2, 1]);
            assert_eq!(resumed, 2);
        }
    }

    #[async_trait::async_trait(?Send)]
    impl SandboxExecutor for ScriptedGuest {
        fn executor(&self) -> Result<agentenv::sandbox::Executor> {
            anyhow::bail!("scripted guest uses the command overrides")
        }

        async fn run_command(&self, cmd: &str, args: &[&str]) -> Result<ProcessOutput> {
            self.run_command_with_opts(cmd, args, &ProcessOpts::default())
                .await
        }

        async fn run_command_with_opts(
            &self,
            cmd: &str,
            args: &[&str],
            opts: &ProcessOpts,
        ) -> Result<ProcessOutput> {
            self.calls.lock().unwrap().push((
                cmd.to_owned(),
                args.iter().map(|arg| (*arg).to_owned()).collect(),
            ));
            let reply = self.replies.lock().unwrap().pop_front().unwrap();
            // Match Executor's boundary: ProcessOpts starts timing only after
            // startup. The benchmark must separately bound this first await.
            tokio::time::sleep(reply.start_delay).await;
            let completion = async {
                tokio::time::sleep(reply.execution_delay).await;
                self.completed.fetch_add(1, Ordering::SeqCst);
                Ok(ProcessOutput {
                    stdout: "fixture stdout".into(),
                    stderr: "fixture stderr".into(),
                    exit_code: reply.exit_code,
                })
            };
            match opts.timeout {
                Some(timeout) => tokio::time::timeout(timeout, completion)
                    .await
                    .context("scripted process completion timed out")?,
                None => completion.await,
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn resume_command_timeout_covers_waiting_for_start_event() {
        let guest = ScriptedGuest::new(vec![CommandReply {
            start_delay: Duration::from_secs(120),
            ..Default::default()
        }]);
        let start = tokio::time::Instant::now();
        let error = run_resume_command_inner(&guest, "echo ready")
            .await
            .unwrap_err();
        assert_eq!(start.elapsed(), RESUME_COMMAND_TIMEOUT);
        assert!(error.to_string().contains("including process startup"));
        tokio::time::advance(Duration::from_secs(120)).await;
        assert_eq!(guest.completed.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn resume_command_start_and_completion_share_one_timeout_budget() {
        let guest = ScriptedGuest::new(vec![CommandReply {
            start_delay: Duration::from_secs(40),
            execution_delay: Duration::from_secs(40),
            ..Default::default()
        }]);
        let start = tokio::time::Instant::now();
        let error = run_resume_command_inner(&guest, "echo ready")
            .await
            .unwrap_err();
        assert_eq!(start.elapsed(), RESUME_COMMAND_TIMEOUT);
        assert!(error.to_string().contains("including process startup"));
        assert_eq!(guest.completed.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn resume_command_completes_within_deadline_and_checks_exit_code() {
        for exit_code in [0, 7] {
            let guest = ScriptedGuest::new(vec![CommandReply {
                start_delay: Duration::from_secs(5),
                execution_delay: Duration::from_secs(10),
                exit_code,
            }]);
            let start = tokio::time::Instant::now();
            let result = run_resume_command_inner(&guest, "echo ready").await;
            assert_eq!(start.elapsed(), Duration::from_secs(15));
            assert_eq!(guest.completed.load(Ordering::SeqCst), 1);
            if exit_code == 0 {
                result.unwrap();
            } else {
                assert!(result.unwrap_err().to_string().contains("exited with 7"));
            }
        }
    }
}
