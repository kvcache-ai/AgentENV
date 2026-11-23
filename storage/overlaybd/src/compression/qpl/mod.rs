//! zfile codec backed by Intel QPL (`CompressOptions::QPL`): one raw DEFLATE
//! stream per block.

mod job;
mod pool;

use std::ops::Range;

use anyhow::{bail, ensure, Context, Result};

use self::job::{BatchItem, Job, Op, Path, QplError};
use self::pool::{Entry, QplJobPool};
use super::zfile::{CompressArgs, CompressOptions, Compressor};

/// Blocks per batch on the hardware path, if the job pool is that large.
const HW_NBATCH: usize = 32;

/// Sets the job pool size of this process; see `zfile::set_qpl_job_pool_size`.
pub(crate) fn set_job_pool_size(size: usize) -> Result<()> {
    pool::set_size(size)
}

pub(crate) struct QplCompressor {
    max_dst_size: usize,
    src_blk_size: usize,
    nbatch: usize,
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
        let pool = pool::global();
        let nbatch = match pool.path() {
            Path::Hardware => HW_NBATCH.min(pool.capacity()),
            // A software job runs to completion inside `qpl_submit_job`.
            Path::Software => 1,
        };

        Ok(Self {
            max_dst_size,
            src_blk_size,
            nbatch,
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

    fn nbatch(&self) -> usize {
        self.nbatch
    }

    fn compress_batch(
        &mut self,
        src: &[u8],
        src_chunk_len: &[usize],
        dst: &mut [u8],
        dst_chunk_len: &mut [usize],
        n: usize,
    ) -> Result<()> {
        if n == 0 {
            return Ok(());
        }
        ensure!(
            src_chunk_len.len() >= n && dst_chunk_len.len() >= n,
            "chunk length arrays are smaller than n"
        );
        let slot = dst.len() / n;
        ensure!(
            slot >= self.max_dst_size,
            "destination chunk capacity ({slot}) smaller than max_dst_size ({})",
            self.max_dst_size
        );
        let mut srcs = Vec::with_capacity(n);
        let mut offset = 0usize;
        for &len in &src_chunk_len[..n] {
            let end = offset.checked_add(len).context("source offset overflow")?;
            ensure!(end <= src.len(), "source chunk exceeds source buffer");
            srcs.push(&src[offset..end]);
            offset = end;
        }
        let mut dsts: Vec<&mut [u8]> = dst.chunks_exact_mut(slot).take(n).collect();
        run_blocks(
            pool::global(),
            Op::Compress,
            self.nbatch,
            &srcs,
            &mut dsts,
            self.max_dst_size,
            &mut dst_chunk_len[..n],
        )
    }

    fn decompress_batch(&self, src: &[u8], blocks: &[Range<usize>], dst: &mut [u8]) -> Result<()> {
        let block_size = self.src_blk_size;
        let needed = blocks
            .len()
            .checked_mul(block_size)
            .context("destination size overflow")?;
        ensure!(
            dst.len() >= needed,
            "destination buffer too small ({}), expected at least {needed}",
            dst.len()
        );
        let srcs = blocks
            .iter()
            .map(|block| {
                src.get(block.clone())
                    .context("block range exceeds source buffer")
            })
            .collect::<Result<Vec<_>>>()?;
        let mut dsts: Vec<&mut [u8]> = dst
            .chunks_exact_mut(block_size)
            .take(blocks.len())
            .collect();
        let mut lens = vec![0; blocks.len()];
        run_blocks(
            pool::global(),
            Op::Decompress,
            self.nbatch,
            &srcs,
            &mut dsts,
            block_size,
            &mut lens,
        )?;
        if let Some(&n) = lens.iter().find(|&&n| n != block_size) {
            bail!("unexpected decompressed size: got {n}, expected {block_size}");
        }
        Ok(())
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

/// Runs `op` on every block, `nbatch` blocks at a time on one set of at most
/// `nbatch` jobs taken once for the whole call, producing at most `out_len`
/// bytes per block and storing each output length in `lens`. Jobs that fail
/// are discarded and the first error is returned.
fn run_blocks(
    pool: &QplJobPool,
    op: Op,
    nbatch: usize,
    srcs: &[&[u8]],
    dsts: &mut [&mut [u8]],
    out_len: usize,
    lens: &mut [usize],
) -> Result<()> {
    for dst in dsts.iter() {
        ensure!(
            dst.len() >= out_len,
            "destination buffer too small ({}), expected at least {}",
            dst.len(),
            out_len
        );
    }
    if srcs.is_empty() {
        return Ok(());
    }
    let mut batch = pool.take_batch(srcs.len().min(nbatch))?;
    let chunks = srcs
        .chunks(nbatch)
        .zip(dsts.chunks_mut(nbatch))
        .zip(lens.chunks_mut(nbatch));
    for ((srcs, dsts), lens) in chunks {
        let mut items: Vec<BatchItem<'_>> = batch
            .entries_mut()
            .iter_mut()
            .zip(srcs)
            .zip(dsts.iter_mut())
            .map(|((entry, &src), dst)| match entry {
                Entry::Hardware { job, bounce } => BatchItem {
                    job,
                    src,
                    dst: &mut bounce.as_mut_slice()[..out_len],
                },
                Entry::Software { job } => BatchItem {
                    job,
                    src,
                    dst: &mut dst[..out_len],
                },
            })
            .collect();
        let results = job::run_batch(op, &mut items);
        drop(items);

        let mut first_error = None;
        let mut failed = Vec::new();
        for (i, (((entry, result), dst), len)) in batch
            .entries_mut()
            .iter_mut()
            .zip(results)
            .zip(dsts.iter_mut())
            .zip(lens.iter_mut())
            .enumerate()
        {
            match result {
                Ok(n) => {
                    if let Entry::Hardware { bounce, .. } = entry {
                        dst[..n].copy_from_slice(&bounce.as_mut_slice()[..n]);
                    }
                    *len = n;
                }
                Err(error) => {
                    failed.push(i);
                    first_error.get_or_insert(error);
                }
            }
        }
        if let Some(error) = first_error {
            batch.discard(&failed);
            return Err(error.into());
        }
    }
    Ok(())
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

    #[test]
    fn compress_batch_round_trips() {
        let mut compressor = create_compressor(&qpl_args()).expect("create");
        let block_size = compressor.src_block_size();
        let slot = compressor.max_dst_size();
        // Several full batches and a short one.
        let n = compressor.nbatch() * 2 + 3;
        let src: Vec<u8> = b"AgentENV zfile block compressed by QPL. "
            .iter()
            .cycle()
            .take(n * block_size)
            .enumerate()
            .map(|(i, &byte)| byte ^ (i / block_size) as u8)
            .collect();
        let mut dst = vec![0; n * slot];
        let mut lens = vec![0; n];
        compressor
            .compress_batch(&src, &vec![block_size; n], &mut dst, &mut lens, n)
            .expect("compress_batch");

        let blocks: Vec<_> = lens
            .iter()
            .enumerate()
            .map(|(i, &len)| i * slot..i * slot + len)
            .collect();
        let mut decoded = vec![0; n * block_size];
        compressor
            .decompress_batch(&dst, &blocks, &mut decoded)
            .expect("decompress_batch");
        assert!(decoded == src, "batch decode differs from the source");
    }
}
