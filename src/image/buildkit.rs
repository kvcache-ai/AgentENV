//! Read-only access to BuildKit's content store. Image data stays on the node;
//! the build client supplies only the digest returned by the image exporter.

use std::{net::SocketAddr, path::Path, time::Duration};

use anyhow::{ensure, Context, Result};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tonic::transport::{Channel, Endpoint};

mod proto {
    tonic::include_proto!("containerd.services.content.v1");
}

pub(crate) const BUILDKIT_PORT: u16 = 1234;
pub(crate) const MAX_METADATA_BYTES: u64 = 4 * 1024 * 1024;
pub(crate) const MAX_IMAGE_BYTES: u64 = 64 * 1024 * 1024 * 1024;

#[derive(Clone)]
pub(crate) struct BuildkitContent {
    client: proto::content_client::ContentClient<Channel>,
}

pub(crate) fn validate_digest(digest: &str) -> Result<()> {
    ensure!(
        digest.strip_prefix("sha256:").is_some_and(|hex| {
            hex.len() == 64
                && hex
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        }),
        "expected a lowercase sha256 image digest"
    );
    Ok(())
}

impl BuildkitContent {
    pub(crate) async fn connect(address: SocketAddr) -> Result<Self> {
        let channel = Endpoint::from_shared(format!("http://{address}"))?
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(60))
            .connect()
            .await
            .context("connect to builder content store")?;
        Ok(Self {
            client: proto::content_client::ContentClient::new(channel),
        })
    }

    pub(crate) async fn metadata(&self, digest: &str) -> Result<Vec<u8>> {
        let mut bytes = Vec::new();
        self.copy(digest, MAX_METADATA_BYTES, &mut bytes).await?;
        Ok(bytes)
    }

    pub(crate) async fn download(&self, digest: &str, size: u64, path: &Path) -> Result<()> {
        ensure!(size <= MAX_IMAGE_BYTES, "image layer exceeds 64 GiB");
        let mut file = tokio::fs::File::create(path).await?;
        let actual = self.copy(digest, size, &mut file).await?;
        ensure!(
            actual == size,
            "content size mismatch for {digest}: expected {size}, got {actual}"
        );
        file.flush().await?;
        Ok(())
    }

    async fn copy(
        &self,
        digest: &str,
        limit: u64,
        output: &mut (impl AsyncWrite + Unpin),
    ) -> Result<u64> {
        validate_digest(digest)?;
        let mut stream = self
            .client
            .clone()
            .read(proto::ReadContentRequest {
                digest: digest.to_owned(),
                offset: 0,
                size: 0,
            })
            .await?
            .into_inner();
        let mut hash = Sha256::new();
        let mut offset = 0u64;
        while let Some(chunk) =
            tokio::time::timeout(Duration::from_secs(60), stream.message()).await??
        {
            ensure!(
                chunk.offset >= 0 && chunk.offset as u64 == offset,
                "unexpected content offset"
            );
            offset = offset
                .checked_add(chunk.data.len() as u64)
                .context("content size overflow")?;
            ensure!(offset <= limit, "content exceeds byte limit {limit}");
            hash.update(&chunk.data);
            output.write_all(&chunk.data).await?;
        }
        ensure!(
            format!("sha256:{:x}", hash.finalize()) == digest,
            "content digest mismatch for {digest}"
        );
        Ok(offset)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proto::{
        content_server::{Content, ContentServer},
        ReadContentRequest, ReadContentResponse,
    };

    struct Store(Vec<(i64, Vec<u8>)>);

    #[tonic::async_trait]
    impl Content for Store {
        type ReadStream =
            futures::stream::Iter<std::vec::IntoIter<Result<ReadContentResponse, tonic::Status>>>;

        async fn read(
            &self,
            _request: tonic::Request<ReadContentRequest>,
        ) -> Result<tonic::Response<Self::ReadStream>, tonic::Status> {
            Ok(tonic::Response::new(futures::stream::iter(
                self.0
                    .iter()
                    .map(|(offset, data)| {
                        Ok(ReadContentResponse {
                            offset: *offset,
                            data: data.clone(),
                        })
                    })
                    .collect::<Vec<_>>(),
            )))
        }
    }

    async fn store(chunks: Vec<(i64, Vec<u8>)>) -> (BuildkitContent, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let incoming = futures::stream::unfold(listener, |listener| async {
            Some((listener.accept().await.map(|(socket, _)| socket), listener))
        });
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(ContentServer::new(Store(chunks)))
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        });
        (BuildkitContent::connect(address).await.unwrap(), server)
    }

    #[tokio::test]
    async fn reads_verified_chunks_and_rejects_corrupt_truncated_or_oversized_content() {
        let digest = crate::digest::sha256_digest(b"abcdef");
        let (client, server) = store(vec![(0, b"abc".to_vec()), (3, b"def".to_vec())]).await;
        assert_eq!(client.metadata(&digest).await.unwrap(), b"abcdef");
        let work = tempfile::tempdir().unwrap();
        let blob = work.path().join("blob");
        client.download(&digest, 6, &blob).await.unwrap();
        assert_eq!(tokio::fs::read(&blob).await.unwrap(), b"abcdef");
        assert!(client
            .download(&digest, 5, &blob)
            .await
            .unwrap_err()
            .to_string()
            .contains("byte limit"));
        assert!(client
            .download(&digest, 7, &blob)
            .await
            .unwrap_err()
            .to_string()
            .contains("size mismatch"));
        assert!(client
            .metadata(&crate::digest::sha256_digest(b"wrong"))
            .await
            .unwrap_err()
            .to_string()
            .contains("digest mismatch"));
        server.abort();
        for chunks in [
            vec![(0, b"abc".to_vec())],
            vec![(1, b"abcdef".to_vec())],
            vec![(-1, b"abcdef".to_vec())],
            vec![(0, b"abc".to_vec()), (4, b"def".to_vec())],
        ] {
            let (client, server) = store(chunks).await;
            assert!(client.metadata(&digest).await.is_err());
            server.abort();
        }
    }

    #[test]
    fn content_digests_are_canonical_and_cannot_contain_paths() {
        assert!(validate_digest(&format!("sha256:{}", "ab".repeat(32))).is_ok());
        for digest in [
            "sha256:../file",
            "sha512:abc",
            "sha256:",
            &format!("sha256:{}", "AB".repeat(32)),
        ] {
            assert!(validate_digest(digest).is_err());
        }
    }
}
