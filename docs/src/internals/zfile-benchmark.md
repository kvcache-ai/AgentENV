# zfile Codec Benchmark

`storage/overlaybd/benches/zfile_read_benchmark.rs` is a Criterion benchmark for
the zfile read and compress paths. It compares the codecs used for snapshot
layers (LZ4, ZSTD, and QPL/IAA with the `qpl` feature).

## Groups

| Group | Data | What it measures |
| --- | --- | --- |
| `throughput` | in-memory zfile, 4 KiB blocks | `read_at_into` latency/throughput: 4 KiB random QD1, 128 KiB sequential QD1, 4 KiB random QD16 on one shared handle |
| `block_size` | in-memory zfile | 4 KiB random reads with 4 KiB vs 64 KiB zfile blocks |
| `compress_throughput` | in-memory | full `zfile_compress` pipeline, plus compression ratio of the complete artifact |

Cases named `*_real_*` run only when
`ZFILE_BENCH_DATASET` names an external real-data file (not one of the built-in
synthetic datasets below). QPL cases run only with `--features qpl`.

## Why the real-data cases

The built-in datasets are generated from fixed seeds:

- `random`: pseudo-random bytes, effectively incompressible;
- `compressible`: a short repeating tile;
- `mixed`: alternating 4 KiB chunks of the two.

Neither extreme looks like real image or memory data, so these datasets cannot
show how the codecs actually differ:

- On random data, every codec stores near-literal blocks, so decompression is
  close to a `memcpy`. On the compressible tile, every codec gets a near-zero
  ratio.
- `mixed` only reflects the 50/50 split: every codec lands near a ratio of 0.5.

Results from one host (Xeon 6767P, Silesia corpus as the real-data file) show
this. On synthetic data LZ4 and ZSTD look the same; on real data they are 4x
apart in read latency and clearly different in ratio:

| Case | LZ4 | ZSTD |
| --- | ---: | ---: |
| `random4k_qd1` read (synthetic random) | 434 ns | 447 ns |
| `real_random4k_qd1` read | 1.46 µs | 6.21 µs |
| `mixed_bs4k` ratio (synthetic) | 0.515 | 0.514 |
| `real_bs4k` ratio | 0.582 | 0.426 |

The real-data cases run the same read patterns and compress pipeline on a real
file. Any file representative of the target workload works. The suggested input
is the concatenated [Silesia corpus](https://sun.aei.polsl.pl/~sdeor/index.php?page=silesia):
about 200 MiB of text, executables, databases, XML, and images. The file is
truncated to whole 64 KiB units and loaded once.

## Running

```bash
# Suggested real-data file: the concatenated Silesia corpus
git clone --depth 1 https://github.com/zlib-ng/corpora
cat corpora/silesia/* > silesia-all

# LZ4/ZSTD only
ZFILE_BENCH_DATASET=$PWD/silesia-all cargo bench -p overlaybd --bench zfile_read

# Include QPL (build requirements: scripts/build-with-qpl.sh)
ZFILE_BENCH_DATASET=$PWD/silesia-all cargo bench -p overlaybd --features qpl --bench zfile_read
```

The zlib-ng mirror stores `dickens` and `webster` with LF line endings, so
`silesia-all` built from it is about 1 MiB smaller than the official Silesia
zip. Use the same source file when comparing runs.
