use std::{net::SocketAddr, time::Duration};

use crate::template::logs::{BuildLogLevel, BuildLogger};
use anyhow::{bail, Context, Result};
use tonic::transport::{Channel, Endpoint};

mod proto {
    tonic::include_proto!("moby.buildkit.v1");
}

pub(crate) fn build_image_name(build_id: &str) -> String {
    format!("aenv-build:{build_id}")
}

pub(crate) struct BuildkitHistory {
    client: proto::control_client::ControlClient<Channel>,
}

impl BuildkitHistory {
    pub(crate) async fn connect(address: SocketAddr) -> Result<Self> {
        let channel = Endpoint::from_shared(format!("http://{address}"))?
            .connect_timeout(Duration::from_secs(10))
            .connect()
            .await
            .context("connect to builder history")?;
        Ok(Self {
            client: proto::control_client::ControlClient::new(channel),
        })
    }

    /// Replay completed records as well as live events, including after reconnect.
    /// Cache seeds contain old history; only this build's unique exporter name counts.
    pub(crate) async fn wait_for_image(
        &self,
        build_id: &str,
        logger: &BuildLogger,
    ) -> Result<String> {
        let image_name = build_image_name(build_id);
        let mut collectors = tokio::task::JoinSet::new();
        let mut collecting = false;
        loop {
            let result = async {
                let mut stream = self
                    .client
                    .clone()
                    .listen_build_history(proto::BuildHistoryRequest::default())
                    .await?
                    .into_inner();
                while let Some(event) = stream.message().await? {
                    let ours = event.record.as_ref().is_some_and(|record| {
                        record.exporters.iter().any(|exporter| {
                            exporter.r#type == "image"
                                && exporter.attrs.get("name") == Some(&image_name)
                        })
                    });
                    if ours && !collecting {
                        let client = self.client.clone();
                        let reference = event.record.as_ref().unwrap().r#ref.clone();
                        let logger = logger.clone();
                        collectors
                            .spawn(async move { collect_status(client, reference, logger).await });
                        collecting = true;
                    }
                    if ours && event.r#type == 1 {
                        match tokio::time::timeout(Duration::from_secs(10), collectors.join_next())
                            .await
                        {
                            Ok(Some(Ok(Ok(())))) => {}
                            other => logger.log(
                                BuildLogLevel::Warn,
                                None,
                                format!("BuildKit log collection did not finish: {other:?}"),
                            ),
                        }
                    }
                    if let Some(digest) = completed_image(event, &image_name)? {
                        return Ok(digest);
                    }
                }
                Err(tonic::Status::unavailable("build history stream closed").into())
            }
            .await;
            let retry = result
                .as_ref()
                .err()
                .and_then(|error: &anyhow::Error| error.downcast_ref::<tonic::Status>())
                .is_some_and(|status| {
                    matches!(
                        status.code(),
                        tonic::Code::Unavailable
                            | tonic::Code::Cancelled
                            | tonic::Code::DeadlineExceeded
                    )
                });
            if !retry {
                return result;
            }
            // The owning build's deadline and cancellation also bound reconnects.
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }
}

#[derive(Default)]
struct VertexProgress {
    started: bool,
    finished: bool,
    cached: bool,
    error: Option<String>,
}

fn vertex_updates(
    vertex: &proto::Vertex,
    progress: &mut VertexProgress,
) -> Vec<(BuildLogLevel, String)> {
    let mut updates = Vec::new();
    if vertex.started.is_some() && !progress.started && !progress.finished {
        progress.started = true;
        updates.push((BuildLogLevel::Info, "started".to_owned()));
    }
    if !vertex.error.is_empty() {
        if progress.error.as_deref() != Some(&vertex.error) {
            progress.error = Some(vertex.error.clone());
            updates.push((BuildLogLevel::Error, vertex.error.clone()));
        }
        progress.finished = true;
    } else if vertex.cached {
        if !progress.cached {
            progress.cached = true;
            updates.push((BuildLogLevel::Info, "cached".to_owned()));
        }
        progress.finished = true;
    } else if vertex.completed.is_some() && !progress.finished {
        progress.finished = true;
        updates.push((BuildLogLevel::Info, "completed".to_owned()));
    }
    updates
}

/// Status replays both active and historical solves. Keep occurrence counts so
/// reconnects do not duplicate logs, including repeated identical output.
async fn collect_status(
    mut client: proto::control_client::ControlClient<Channel>,
    reference: String,
    logger: BuildLogger,
) -> Result<()> {
    use std::collections::HashMap;
    let mut seen = HashMap::new();
    let mut vertices = HashMap::new();
    for attempt in 0..4 {
        let mut occurrences = HashMap::new();
        let result = async {
            let mut stream = client
                .status(proto::StatusRequest {
                    r#ref: reference.clone(),
                })
                .await?
                .into_inner();
            while let Some(status) = stream.message().await? {
                for vertex in status.vertexes {
                    if vertex.started.is_none() && vertex.completed.is_none() {
                        continue;
                    }
                    if !vertices.contains_key(&vertex.digest) && vertices.len() >= 32_768 {
                        continue;
                    }
                    let progress = vertices.entry(vertex.digest.clone()).or_default();
                    for (level, phase) in vertex_updates(&vertex, progress) {
                        logger.log(level, None, format!("{}: {phase}", vertex.name));
                    }
                }
                for entry in status.logs {
                    if seen.len() >= 32_768 {
                        continue;
                    }
                    let timestamp = entry.timestamp.map(|t| (t.seconds, t.nanos));
                    // Hash message bytes rather than retaining a second copy of output.
                    use sha2::Digest;
                    let key = (
                        entry.vertex,
                        timestamp,
                        entry.stream,
                        <[u8; 32]>::from(sha2::Sha256::digest(&entry.msg)),
                    );
                    let count = occurrences.entry(key.clone()).or_insert(0usize);
                    *count += 1;
                    let previous = seen.entry(key).or_insert(0);
                    if *count > *previous {
                        logger.output(None, entry.stream == 2, &entry.msg);
                        *previous = *count;
                    }
                }
                // Warnings are emitted only on the initial subscription; replayed
                // process output and vertex errors above retain their severity.
                if attempt == 0 {
                    for warning in status.warnings {
                        logger.log(
                            BuildLogLevel::Warn,
                            None,
                            String::from_utf8_lossy(&warning.short),
                        );
                    }
                }
            }
            Ok::<_, anyhow::Error>(())
        }
        .await;
        match result {
            Ok(()) => return Ok(()),
            Err(error) if attempt == 3 => return Err(error),
            Err(_) => tokio::time::sleep(Duration::from_millis(250)).await,
        }
    }
    unreachable!()
}

fn completed_image(event: proto::BuildHistoryEvent, image_name: &str) -> Result<Option<String>> {
    if event.r#type != 1 {
        return Ok(None);
    }
    let Some(record) = event.record else {
        return Ok(None);
    };
    let Some(exporter_index) = record.exporters.iter().position(|exporter| {
        exporter.r#type == "image"
            && exporter
                .attrs
                .get("name")
                .is_some_and(|name| name == image_name)
    }) else {
        return Ok(None);
    };
    if let Some(error) = record.error.filter(|error| error.code != 0) {
        bail!("BuildKit build {} failed: {}", record.r#ref, error.message);
    }
    // BuildKit records exported descriptors by exporter index. ExporterResponse
    // contains frontend metadata and need not contain containerimage.digest.
    let descriptor = record
        .result
        .as_ref()
        .and_then(|result| result.results.get(&(exporter_index as i64)))
        .context("completed BuildKit build has no image descriptor")?;
    super::validate_digest(&descriptor.digest)?;
    Ok(Some(descriptor.digest.clone()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use proto::{
        control_server::{Control, ControlServer},
        BuildHistoryEvent, BuildHistoryRecord, BuildResultInfo, Descriptor, Exporter,
    };
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    #[test]
    fn vertex_progress_does_not_regress_or_repeat_replayed_states() {
        let timestamp = || prost_types::Timestamp {
            seconds: 1,
            nanos: 0,
        };
        let mut progress = VertexProgress::default();
        let completed = proto::Vertex {
            digest: "vertex".into(),
            name: "load .dockerignore".into(),
            started: Some(timestamp()),
            completed: Some(timestamp()),
            ..Default::default()
        };
        assert_eq!(
            vertex_updates(&completed, &mut progress),
            vec![
                (BuildLogLevel::Info, "started".to_owned()),
                (BuildLogLevel::Info, "completed".to_owned()),
            ]
        );
        let replayed_start = proto::Vertex {
            digest: "vertex".into(),
            name: "load .dockerignore".into(),
            started: Some(timestamp()),
            ..Default::default()
        };
        assert!(vertex_updates(&replayed_start, &mut progress).is_empty());
        assert!(vertex_updates(&completed, &mut progress).is_empty());
    }

    fn completed(name: &str) -> BuildHistoryEvent {
        BuildHistoryEvent {
            r#type: 1,
            record: Some(BuildHistoryRecord {
                r#ref: "solve-id".into(),
                exporters: vec![Exporter {
                    r#type: "image".into(),
                    attrs: [("name".into(), name.into())].into(),
                }],
                result: Some(BuildResultInfo {
                    results: [(
                        0,
                        Descriptor {
                            digest: crate::digest::sha256_digest(b"image"),
                        },
                    )]
                    .into(),
                }),
                error: None,
            }),
        }
    }

    #[test]
    fn only_successful_completion_of_the_expected_image_is_published() {
        let name = build_image_name("current");
        assert!(completed_image(completed("aenv-build:cached"), &name)
            .unwrap()
            .is_none());
        for kind in [0, 2] {
            let mut event = completed(&name);
            event.r#type = kind;
            assert!(completed_image(event, &name).unwrap().is_none());
        }
        let mut failed = completed(&name);
        failed.record.as_mut().unwrap().error = Some(proto::BuildError {
            code: 2,
            message: "RUN failed".into(),
        });
        assert!(completed_image(failed, &name)
            .unwrap_err()
            .to_string()
            .contains("RUN failed"));
        for digest in [None, Some("sha256:invalid")] {
            let mut event = completed(&name);
            let result = event.record.as_mut().unwrap().result.as_mut().unwrap();
            result.results.clear();
            if let Some(digest) = digest {
                result.results.insert(
                    0,
                    Descriptor {
                        digest: digest.into(),
                    },
                );
            }
            assert!(completed_image(event, &name).is_err());
        }
        let mut event = completed(&name);
        let record = event.record.as_mut().unwrap();
        record.exporters.insert(
            0,
            Exporter {
                r#type: "local".into(),
                attrs: Default::default(),
            },
        );
        let results = &mut record.result.as_mut().unwrap().results;
        let image = results.remove(&0).unwrap();
        results.insert(1, image);
        results.insert(
            0,
            Descriptor {
                digest: crate::digest::sha256_digest(b"other"),
            },
        );
        assert_eq!(
            completed_image(event, &name).unwrap(),
            Some(crate::digest::sha256_digest(b"image"))
        );
        assert_eq!(
            completed_image(completed(&name), &name).unwrap(),
            Some(crate::digest::sha256_digest(b"image"))
        );
    }

    struct History(Arc<AtomicUsize>, Arc<AtomicUsize>);
    #[tonic::async_trait]
    impl Control for History {
        type StatusStream =
            futures::stream::Iter<std::vec::IntoIter<Result<proto::StatusResponse, tonic::Status>>>;
        async fn status(
            &self,
            request: tonic::Request<proto::StatusRequest>,
        ) -> Result<tonic::Response<Self::StatusStream>, tonic::Status> {
            assert_eq!(request.get_ref().r#ref, "solve-id");
            let output = proto::VertexLog {
                vertex: "vertex".into(),
                timestamp: Some(prost_types::Timestamp {
                    seconds: 1,
                    nanos: 2,
                }),
                stream: 2,
                msg: b"output\n".to_vec(),
            };
            let mut events = vec![Ok(proto::StatusResponse {
                logs: vec![output.clone(), output],
                ..Default::default()
            })];
            if self.1.fetch_add(1, Ordering::SeqCst) == 0 {
                events.push(Err(tonic::Status::unavailable("status disconnected")));
            } else {
                events.push(Ok(proto::StatusResponse {
                    logs: vec![proto::VertexLog {
                        vertex: "vertex".into(),
                        timestamp: Some(prost_types::Timestamp {
                            seconds: 2,
                            nanos: 0,
                        }),
                        stream: 1,
                        msg: b"last\n".to_vec(),
                    }],
                    ..Default::default()
                }));
            }
            Ok(tonic::Response::new(futures::stream::iter(events)))
        }
        type ListenBuildHistoryStream =
            futures::stream::Iter<std::vec::IntoIter<Result<BuildHistoryEvent, tonic::Status>>>;
        async fn listen_build_history(
            &self,
            request: tonic::Request<proto::BuildHistoryRequest>,
        ) -> Result<tonic::Response<Self::ListenBuildHistoryStream>, tonic::Status> {
            assert!(!request.get_ref().active_only);
            assert!(!request.get_ref().early_exit);
            let events = if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
                vec![
                    Ok(completed("aenv-build:cached")),
                    Err(tonic::Status::unavailable("connection lost")),
                ]
            } else {
                vec![
                    Ok(completed("aenv-build:cached")),
                    Ok(completed(&build_image_name("current"))),
                ]
            };
            Ok(tonic::Response::new(futures::stream::iter(events)))
        }
    }

    #[tokio::test]
    async fn reconnect_replays_completion_without_a_client_submission() -> Result<()> {
        let calls = Arc::new(AtomicUsize::new(0));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let incoming = futures::stream::unfold(listener, |listener| async {
            Some((listener.accept().await.map(|(socket, _)| socket), listener))
        });
        let status_calls = Arc::new(AtomicUsize::new(0));
        let service = ControlServer::new(History(calls.clone(), status_calls.clone()));
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        });
        let client = BuildkitHistory::connect(address).await?;
        let root = tempfile::tempdir()?;
        let repository = crate::snapshot::repository::backends::PosixFsBackend::new(
            crate::snapshot::repository::backends::PosixFsBackendConfig {
                root: root.path().join("repository"),
                cache_root: Some(root.path().join("cache")),
                runtime_cache_root: None,
            },
        )?
        .repository();
        let logs = crate::template::logs::BuildLogs::default();
        let id = crate::snapshot::SnapshotId::generate();
        let session = logs.start(id.clone(), repository);
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            client.wait_for_image("current", &session.logger),
        )
        .await;
        server.abort();
        assert_eq!(result??, crate::digest::sha256_digest(b"image"));
        let entries = logs.temporary(&id).unwrap();
        session.finish().await?;
        assert!(logs.temporary(&id).is_none());
        assert_eq!(
            entries
                .iter()
                .map(|entry| entry.message.as_str())
                .collect::<Vec<_>>(),
            vec!["output", "output", "last"]
        );
        assert_eq!(entries[0].level, BuildLogLevel::Warn);
        assert_eq!(entries[2].level, BuildLogLevel::Info);
        assert_eq!(status_calls.load(Ordering::SeqCst), 2);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        Ok(())
    }
}
