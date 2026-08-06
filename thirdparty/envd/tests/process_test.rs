use anyhow::Result;
use envd::process::{
    process_event, ListRequest, ProcessClient, ProcessConfig, ProcessInput, ProcessSelector,
    SendInputRequest, SendSignalRequest, Signal, StartRequest,
};
use futures_util::StreamExt;
use tonic::Request;

mod common;
use common::TEST_ENVD_ADDR;

const TEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Verifies that the process list is initially empty.
#[tokio::test]
async fn test_list_process() -> Result<()> {
    let mut client = ProcessClient::connect(TEST_ENVD_ADDR, None).await?;
    let request = Request::new(ListRequest {});

    let response = client.list(request).await?;

    println!("Response: {:?}", response);
    // Expect no processes running initially
    assert_eq!(response.into_inner().processes.len(), 0);

    Ok(())
}

/// Verifies starting a process, capturing its output, and checking the exit code.
/// This test runs a simple bash script that echoes messages and sleeps.
#[tokio::test]
async fn test_start_process() -> Result<()> {
    tokio::time::timeout(TEST_TIMEOUT, async {
        // Connect to the server
        let mut client = ProcessClient::connect(TEST_ENVD_ADDR, None).await?;

        // Configure the process request: run /bin/bash validation script
        let request = Request::new(StartRequest {
            process: Some(ProcessConfig {
                cmd: "/bin/bash".to_string(),
                args: vec![
                    "-c".to_string(),
                    "echo Hello from Rust!; sleep 1; echo Done!".to_string(),
                ],
                envs: Default::default(),
                cwd: None,
            }),
            pty: None,
            tag: None,
            stdin: Some(false),
        });

        let mut response_stream = client.start(request).await?.into_inner();
        let mut output_buffer = String::new();

        // Consume the stream of events from the process
        while let Some(response) = response_stream.next().await {
            let resp = match response {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("RPC Error: {:?}", e);
                    continue;
                }
            };

            let event_wrapper = match resp.event {
                Some(w) => w,
                None => continue,
            };

            match event_wrapper.event {
                Some(process_event::Event::Start(start_event)) => {
                    println!("Process started with PID: {}", start_event.pid);
                }
                Some(process_event::Event::Data(data_event)) => match data_event.output {
                    Some(process_event::data_event::Output::Stdout(bytes)) => {
                        let s = String::from_utf8_lossy(&bytes);
                        print!("{}", s);
                        output_buffer.push_str(&s); // Collect stdout for verification
                    }
                    Some(process_event::data_event::Output::Stderr(bytes)) => {
                        eprint!("{}", String::from_utf8_lossy(&bytes));
                    }
                    Some(process_event::data_event::Output::Pty(bytes)) => {
                        print!("{}", String::from_utf8_lossy(&bytes));
                    }
                    None => {}
                },
                Some(process_event::Event::End(end_event)) => {
                    println!("\nProcess exited with code: {}", end_event.exit_code);
                    // Assert normal termination
                    assert_eq!(end_event.exit_code, 0);
                    break;
                }
                _ => {}
            }
        }

        // Verify expected output was received
        assert!(output_buffer.contains("Hello from Rust!"));
        assert!(output_buffer.contains("Done!"));

        Ok(())
    })
    .await
    .map_err(|e| anyhow::anyhow!("Test timed out: {}", e))?
}

/// Verifies interactive process: Start cat, SendInput, verify output.
#[tokio::test]
async fn test_process_interactive() -> Result<()> {
    tokio::time::timeout(TEST_TIMEOUT, async {
        let mut client = ProcessClient::connect(TEST_ENVD_ADDR, None).await?;

        // Start `cat`
        let request = Request::new(StartRequest {
            process: Some(ProcessConfig {
                cmd: "cat".to_string(),
                args: vec![],
                envs: Default::default(),
                cwd: None,
            }),
            pty: None,
            tag: None,
            stdin: Some(true),
        });

        let mut response_stream = client.start(request).await?.into_inner();
        let mut pid = 0;

        // Wait for Start event
        while let Some(Ok(resp)) = response_stream.next().await {
            if let Some(event_wrapper) = resp.event {
                if let Some(process_event::Event::Start(start_event)) = event_wrapper.event {
                    pid = start_event.pid;
                    println!("Started cat with PID: {}", pid);
                    break;
                }
            }
        }
        assert_ne!(pid, 0);

        let process_selector = ProcessSelector {
            selector: Some(envd::process::process_selector::Selector::Pid(pid)),
        };

        // Send Input: "hello\n"
        let mut input_client = client.clone();
        let input_msg = "hello_interactive";

        // We send data + newline because cat might be line-buffered
        let data_to_send = format!("{}\n", input_msg);

        input_client
            .send_input(Request::new(SendInputRequest {
                process: Some(process_selector.clone()),
                input: Some(ProcessInput {
                    input: Some(envd::process::process_input::Input::Stdin(
                        data_to_send.as_bytes().to_vec(),
                    )),
                }),
            }))
            .await?;

        // Read loop
        let mut output_received = false;
        while let Some(response) = response_stream.next().await {
            let resp = match response {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("Stream error: {:?}", e);
                    break;
                }
            };

            let event_wrapper = match resp.event {
                Some(w) => w,
                None => continue,
            };

            match event_wrapper.event {
                Some(process_event::Event::Data(data)) => {
                    let Some(process_event::data_event::Output::Stdout(bytes)) = data.output else {
                        continue;
                    };

                    let s = String::from_utf8_lossy(&bytes);
                    print!("Received stdout: {}", s);
                    if !s.contains(input_msg) {
                        continue;
                    }

                    // Kill process to finish test
                    let _ = input_client
                        .send_signal(Request::new(SendSignalRequest {
                            process: Some(process_selector.clone()),
                            signal: Signal::Sigterm as i32,
                        }))
                        .await;

                    // We break after signal, but we might receive End event next
                    // Let's rely on receiving output to set flag, then loop will eventually end or we break here.
                    output_received = true;
                    break;
                }
                Some(process_event::Event::End(_)) => {
                    break;
                }
                _ => {}
            }
        }

        assert!(output_received);
        Ok(())
    })
    .await
    .map_err(|e| anyhow::anyhow!("Test timed out: {}", e))?
}
