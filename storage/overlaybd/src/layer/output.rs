use std::sync::Arc;

use anyhow::Result;

use crate::compression::zfile::{CompressArgs, CompressOptions, ZFileCompactWriter};
use crate::config::{CompressionAlgorithm, OverlaybdCompactOutput};
use crate::io::virtual_file::{
    DigestCompactWriter, VirtualFile, DIGEST_COMPACT_WRITER_BUFFER_SIZE,
};
use crate::lsmt::file::CommitArgs;

fn compression_args(algorithm: CompressionAlgorithm, workers: usize) -> CompressArgs {
    let algorithm = match algorithm {
        CompressionAlgorithm::Lz4 => CompressOptions::LZ4,
        CompressionAlgorithm::Zstd => CompressOptions::ZSTD,
    };
    let mut args = CompressArgs::new(CompressOptions::new(
        algorithm,
        CompressOptions::DEFAULT_BLOCK_SIZE,
        0,
    ));
    args.workers = workers.max(1);
    args
}

/// Configure the layer output, optionally describing its final physical bytes.
/// Digest-tracked output must be empty and exclusively owned by this write.
pub async fn create_commit_args(
    output: Arc<dyn VirtualFile>,
    mode: OverlaybdCompactOutput,
    compute_digest: bool,
) -> Result<CommitArgs> {
    Ok(match mode {
        OverlaybdCompactOutput::Raw if compute_digest => CommitArgs::from_writer(
            DigestCompactWriter::new(DIGEST_COMPACT_WRITER_BUFFER_SIZE, output),
        ),
        OverlaybdCompactOutput::Raw => CommitArgs::new(output),
        OverlaybdCompactOutput::ZFile { algorithm, workers } => {
            let compression = compression_args(algorithm, workers);
            let writer = if compute_digest {
                ZFileCompactWriter::new_with_digest(output, &compression).await?
            } else {
                ZFileCompactWriter::new(output, &compression).await?
            };
            CommitArgs::from_writer(Arc::new(writer))
        }
    })
}
