use std::{net::SocketAddr, time::Duration};

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
    pub(crate) async fn wait_for_image(&self, build_id: &str) -> Result<String> {
        let image_name = build_image_name(build_id);
        loop {
            let result = async {
                let mut stream = self
                    .client
                    .clone()
                    .listen_build_history(proto::BuildHistoryRequest::default())
                    .await?
                    .into_inner();
                while let Some(event) = stream.message().await? {
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

    struct History(Arc<AtomicUsize>);
    #[tonic::async_trait]
    impl Control for History {
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
        let service = ControlServer::new(History(calls.clone()));
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        });
        let client = BuildkitHistory::connect(address).await?;
        let result =
            tokio::time::timeout(Duration::from_secs(5), client.wait_for_image("current")).await;
        server.abort();
        assert_eq!(result??, crate::digest::sha256_digest(b"image"));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        Ok(())
    }
}
