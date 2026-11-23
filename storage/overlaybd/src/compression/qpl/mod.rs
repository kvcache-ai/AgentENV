//! zfile codec backed by Intel QPL (`CompressOptions::QPL`): one raw DEFLATE
//! stream per block.

mod job;
mod pool;

use anyhow::{ensure, Context, Result};

use self::job::{Job, QplError};
use self::pool::{Entry, QplJobPool};
use super::zfile::{CompressArgs, CompressOptions, Compressor};

pub(crate) struct QplCompressor {
    max_dst_size: usize,
    src_blk_size: usize,
}

impl QplCompressor {
    pub(crate) fn new(args: &CompressArgs) -> Result<Self> {
        ensure!(
            args.opt.algo == CompressOptions::QPL,
            "compression type invalid, expected QPL"
        );
        ensure!(args.opt.block_size != 0, "block_size must be > 0");
        ensure!(
            args.opt.block_size <= CompressOptions::DEFAULT_BLOCK_SIZE,
            "QPL block_size {} exceeds DEFAULT_BLOCK_SIZE {}, which sizes the bounce buffers",
            args.opt.block_size,
            CompressOptions::DEFAULT_BLOCK_SIZE
        );
        let src_blk_size = usize::try_from(args.opt.block_size).context("invalid block_size")?;
        let max_dst_size = usize::try_from(job::qpl_compress_bound(args.opt.block_size))
            .context("invalid QPL output bound")?;
        // Codecs are built at setup time, so the pool's one-time prewarm
        // happens here rather than on the first compress or decompress.
        pool::global();

        Ok(Self {
            max_dst_size,
            src_blk_size,
        })
    }
}

impl Compressor for QplCompressor {
    fn max_dst_size(&self) -> usize {
        self.max_dst_size
    }

    fn src_block_size(&self) -> usize {
        self.src_blk_size
    }

    fn compress(&mut self, src: &[u8], dst: &mut [u8]) -> Result<usize> {
        run_block(pool::global(), Job::compress, src, dst, self.max_dst_size)
    }

    fn decompress(&self, src: &[u8], dst: &mut [u8]) -> Result<usize> {
        run_block(pool::global(), Job::decompress, src, dst, self.src_blk_size)
    }
}

/// `Job::compress` or `Job::decompress`.
type BlockOp = fn(&mut Job, &[u8], &mut [u8]) -> Result<usize, QplError>;

/// Runs one block through `op` with a job from `pool`, producing at most
/// `out_len` bytes. Hardware jobs read `src` in place but write into the
/// entry's pre-faulted bounce buffer, which is then copied to `dst`.
fn run_block(
    pool: &QplJobPool,
    op: BlockOp,
    src: &[u8],
    dst: &mut [u8],
    out_len: usize,
) -> Result<usize> {
    ensure!(
        dst.len() >= out_len,
        "destination buffer too small ({}), expected at least {}",
        dst.len(),
        out_len
    );
    let dst = &mut dst[..out_len];
    let mut batch = pool.take_batch(1)?;
    let result = match &mut batch.entries_mut()[0] {
        Entry::Hardware { job, bounce } => {
            let out = &mut bounce.as_mut_slice()[..out_len];
            op(job, src, out).inspect(|&len| {
                dst[..len].copy_from_slice(&out[..len]);
            })
        }
        Entry::Software { job } => op(job, src, dst),
    };
    match result {
        Ok(len) => Ok(len),
        Err(error) => {
            batch.discard(&[0]);
            Err(error.into())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::zfile::create_compressor;
    use super::*;

    fn qpl_args() -> CompressArgs {
        CompressArgs::new(CompressOptions::new(
            CompressOptions::QPL,
            CompressOptions::DEFAULT_BLOCK_SIZE,
            0,
        ))
    }

    #[test]
    fn round_trips() {
        let mut compressor = create_compressor(&qpl_args()).expect("create");
        let block: Vec<u8> = b"AgentENV zfile block compressed by QPL. "
            .iter()
            .cycle()
            .take(4096)
            .copied()
            .collect();
        let mut compressed = vec![0; compressor.max_dst_size()];
        let len = compressor
            .compress(&block, &mut compressed)
            .expect("compress");

        let mut out = vec![0; compressor.src_block_size()];
        let out_len = compressor
            .decompress(&compressed[..len], &mut out)
            .expect("decompress");
        assert_eq!(&out[..out_len], block);
    }
}
