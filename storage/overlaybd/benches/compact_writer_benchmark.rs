use anyhow::Result;
use criterion::{Criterion, Throughput};
use overlaybd::backend::local::LocalFile;
use overlaybd::virtual_file::VirtualFile;
use overlaybd::zfile::{CompressArgs, CompressOptions, ZFileCompactWriter};
use rand::{rngs::StdRng, Rng, SeedableRng};
use std::sync::Arc;
use std::time::{Duration, Instant};
use storage_util::CompactWriter;

const INPUT_SIZE: usize = 64 * 1024 * 1024;
const BATCH_SIZE: usize = 512 * 1024;

#[derive(Clone, Copy, PartialEq)]
enum DataKind {
    Random,
    Mixed,
    Compressible,
}

fn source_bytes(kind: DataKind) -> Vec<u8> {
    let mut data = vec![0u8; INPUT_SIZE];
    let mut rng = StdRng::seed_from_u64(0x5eed_5eed_0000_0001);
    rng.fill_bytes(&mut data);
    if kind != DataKind::Random {
        let tile = data[..64].to_vec();
        for (block, chunk) in data.chunks_mut(4096).enumerate() {
            if kind == DataKind::Compressible || block % 2 == 0 {
                for (offset, byte) in chunk.iter_mut().enumerate() {
                    *byte = tile[offset % tile.len()];
                }
            }
        }
    }
    data
}

async fn run_once(
    source: &[u8],
    algo: u8,
    workers: usize,
    output: &std::path::Path,
) -> Result<u64> {
    let file: Arc<dyn VirtualFile> = Arc::new(LocalFile::new(output)?);
    let args = CompressArgs {
        opt: CompressOptions::new(algo, 4096, 0),
        overwrite_header: false,
        workers,
    };
    let writer = ZFileCompactWriter::new(file.clone(), &args).await?;
    for (index, chunk) in source.chunks(BATCH_SIZE).enumerate() {
        CompactWriter::write(
            &writer,
            Box::new(chunk.to_vec()),
            (index * BATCH_SIZE) as u64,
            chunk.len(),
        )
        .await?;
    }
    CompactWriter::finalize(&writer).await?;
    file.size().await
}

fn main() {
    let mut criterion = Criterion::default().configure_from_args();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build benchmark runtime");
    let output = std::path::PathBuf::from("/tmp/aenv-compact-writer-benchmark.commit");
    let filter = std::env::var("AENV_ZFILE_CASE").ok();
    let mut group = criterion.benchmark_group("compact_writer");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(2));
    for (algo, algo_name) in [
        (CompressOptions::LZ4, "lz4"),
        (CompressOptions::ZSTD, "zstd"),
    ] {
        for (kind, kind_name) in [
            (DataKind::Random, "random"),
            (DataKind::Mixed, "mixed"),
            (DataKind::Compressible, "compressible"),
        ] {
            let source = source_bytes(kind);
            for workers in [1, 2, 4, 8] {
                let case = format!("{algo_name}/{kind_name}/w{workers}");
                if filter.as_ref().is_some_and(|wanted| wanted != &case) {
                    continue;
                }
                group.throughput(Throughput::Bytes(source.len() as u64));
                group.bench_function(case.clone(), |b| {
                    b.iter_custom(|iterations| {
                        let mut artifact_size = 0;
                        let start = Instant::now();
                        for _ in 0..iterations {
                            artifact_size = runtime
                                .block_on(run_once(&source, algo, workers, &output))
                                .expect("compact-writer benchmark iteration");
                        }
                        println!(
                            "{case} compression_ratio={:.6}",
                            artifact_size as f64 / source.len() as f64
                        );
                        start.elapsed()
                    });
                });
            }
        }
    }
    group.finish();
    let _ = std::fs::remove_file(output);
}
