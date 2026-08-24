use agentenv::orchestrator::{
    InMemoryMetadataStore, MetadataStore, SandboxListFilter, SandboxMetadata, SandboxState,
};
use agentenv::types::{SandboxId, SandboxResources};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::collections::HashMap;
use std::time::{Duration, SystemTime};
use tokio::runtime::Runtime;
use tokio::sync::RwLock;
use uuid::Uuid;

const DEFAULT_SAMPLE_SIZE: usize = 10;
const DEFAULT_WARM_UP_TIME: Duration = Duration::from_millis(50);
const DEFAULT_MEASUREMENT_TIME: Duration = Duration::from_millis(250);
const FULL_SAMPLE_SIZE: usize = 20;

struct NaiveStore {
    inner: RwLock<HashMap<SandboxId, SandboxMetadata>>,
}

impl NaiveStore {
    fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
        }
    }

    async fn add(&self, metadata: SandboxMetadata) {
        self.inner.write().await.insert(metadata.id, metadata);
    }

    async fn list_filtered(&self, filter: SandboxListFilter) -> Vec<SandboxMetadata> {
        let states_filter = filter.states;
        let user_metadata_filter = filter.user_metadata;

        self.inner
            .read()
            .await
            .values()
            .filter(|metadata| {
                let state_matches = states_filter
                    .as_ref()
                    .is_none_or(|states| states.contains(&metadata.state));
                let user_metadata_matches =
                    user_metadata_filter.as_ref().is_none_or(|required_pairs| {
                        metadata
                            .user_metadata
                            .as_ref()
                            .is_some_and(|actual_metadata| {
                                required_pairs
                                    .iter()
                                    .all(|(k, v)| actual_metadata.get(k) == Some(v))
                            })
                    });
                state_matches && user_metadata_matches
            })
            .cloned()
            .collect()
    }

    async fn list_expired(&self, now: SystemTime) -> Vec<SandboxMetadata> {
        self.inner
            .read()
            .await
            .values()
            .filter(|metadata| metadata.is_expired(now))
            .cloned()
            .collect()
    }
}

fn bench_orchestrator_store_list(c: &mut Criterion) {
    let rt = Runtime::new().expect("create Tokio runtime for benchmarks");
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(10_000);
    let dataset = benchmark_dataset(now, 100_000);
    let dataset_len = dataset.len() as u64;

    let naive = NaiveStore::new();
    let current_store = InMemoryMetadataStore::new();
    rt.block_on(async {
        for metadata in dataset.iter().cloned() {
            naive.add(metadata.clone()).await;
            current_store
                .add(metadata)
                .await
                .expect("seed current store");
        }
    });

    let mut group = c.benchmark_group("orchestrator_store");
    group.throughput(Throughput::Elements(dataset_len));

    group.bench_with_input(
        BenchmarkId::new("list_expired", "naive_scanning"),
        &now,
        |b, now| {
            b.to_async(&rt).iter(|| naive.list_expired(*now));
        },
    );
    group.bench_with_input(
        BenchmarkId::new("list_expired", "current_store"),
        &now,
        |b, now| {
            b.to_async(&rt).iter(|| async {
                current_store
                    .list_expired(*now)
                    .await
                    .expect("list expired")
            });
        },
    );

    let running_filter = SandboxListFilter {
        states: Some(vec![SandboxState::Running]),
        ..SandboxListFilter::matches_all()
    };
    group.bench_with_input(
        BenchmarkId::new("list_filtered_running", "naive_scanning"),
        &running_filter,
        |b, filter| {
            b.to_async(&rt).iter(|| naive.list_filtered(filter.clone()));
        },
    );
    group.bench_with_input(
        BenchmarkId::new("list_filtered_running", "current_store"),
        &running_filter,
        |b, filter| {
            b.to_async(&rt).iter(|| async {
                current_store
                    .list_filtered(filter.clone())
                    .await
                    .expect("list filtered")
            });
        },
    );

    group.finish();
}

fn bench_orchestrator_store_cas(c: &mut Criterion) {
    let rt = Runtime::new().expect("create Tokio runtime for benchmarks");
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(10_000);

    let cas_store = InMemoryMetadataStore::new();
    let cas_id = SandboxId::from_uuid(Uuid::from_u128(200_001));
    let cas_metadata = SandboxMetadata {
        id: cas_id,
        state: SandboxState::Running,
        created_at: now,
        user_metadata: Some(HashMap::from([
            ("team".to_string(), "alpha".to_string()),
            ("cas_tag".to_string(), "v1".to_string()),
        ])),
        ..Default::default()
    };
    rt.block_on(async {
        cas_store
            .add(cas_metadata.clone())
            .await
            .expect("seed cas benchmark store");
    });

    let expected_running = vec![SandboxState::Running];
    let expected_paused = vec![SandboxState::Paused];

    let mut cas_group = c.benchmark_group("orchestrator_store_cas");
    cas_group.throughput(Throughput::Elements(1));

    cas_group.bench_with_input(
        BenchmarkId::new("update_state_if_state", "success"),
        &cas_id,
        |b, sandbox_id| {
            b.to_async(&rt).iter(|| async {
                cas_store
                    .update_state_if_state(
                        sandbox_id,
                        SandboxState::Running,
                        expected_running.as_slice(),
                    )
                    .await
                    .expect("update_state_if_state success");
            });
        },
    );
    cas_group.bench_with_input(
        BenchmarkId::new("update_state_if_state", "state_conflict"),
        &cas_id,
        |b, sandbox_id| {
            b.to_async(&rt).iter(|| async {
                cas_store
                    .update_state_if_state(
                        sandbox_id,
                        SandboxState::Running,
                        expected_paused.as_slice(),
                    )
                    .await
                    .expect_err("update_state_if_state should conflict");
            });
        },
    );

    cas_group.bench_with_input(
        BenchmarkId::new("update_if_state", "success"),
        &cas_metadata,
        |b, metadata| {
            b.to_async(&rt).iter(|| async {
                cas_store
                    .update_if_state(&metadata.id, expected_running.as_slice(), |current| {
                        *current = (*metadata).clone();
                    })
                    .await
                    .expect("update_if_state success");
            });
        },
    );
    cas_group.bench_with_input(
        BenchmarkId::new("update_if_state", "state_conflict"),
        &cas_metadata,
        |b, metadata| {
            b.to_async(&rt).iter(|| async {
                cas_store
                    .update_if_state(&metadata.id, expected_paused.as_slice(), |current| {
                        *current = (*metadata).clone();
                    })
                    .await
                    .expect_err("update_if_state should conflict");
            });
        },
    );

    cas_group.finish();
}

fn bench_orchestrator_store_update(c: &mut Criterion) {
    let rt = Runtime::new().expect("create Tokio runtime for benchmarks");
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(10_000);

    let update_store = InMemoryMetadataStore::new();
    let update_id = SandboxId::from_uuid(Uuid::from_u128(200_002));
    let update_metadata = SandboxMetadata {
        id: update_id,
        state: SandboxState::Running,
        created_at: now,
        user_metadata: Some(HashMap::from([("team".to_string(), "alpha".to_string())])),
        ..Default::default()
    };
    let update_payload = SandboxMetadata {
        id: update_id,
        state: SandboxState::Running,
        created_at: now,
        user_metadata: Some(HashMap::from([
            ("team".to_string(), "alpha".to_string()),
            ("update_tag".to_string(), "v2".to_string()),
        ])),
        ..Default::default()
    };
    let missing_update_payload = SandboxMetadata {
        id: SandboxId::from_uuid(Uuid::from_u128(999_999)),
        state: SandboxState::Running,
        created_at: now,
        ..Default::default()
    };

    rt.block_on(async {
        update_store
            .add(update_metadata)
            .await
            .expect("seed update benchmark store");
    });

    let mut update_group = c.benchmark_group("orchestrator_store_update");
    update_group.throughput(Throughput::Elements(1));

    update_group.bench_with_input(
        BenchmarkId::new("update", "success"),
        &update_payload,
        |b, metadata| {
            b.to_async(&rt).iter(|| async {
                update_store
                    .update(metadata.clone())
                    .await
                    .expect("update success");
            });
        },
    );
    update_group.bench_with_input(
        BenchmarkId::new("update", "not_found"),
        &missing_update_payload,
        |b, metadata| {
            b.to_async(&rt).iter(|| async {
                update_store
                    .update(metadata.clone())
                    .await
                    .expect_err("update should fail for missing sandbox");
            });
        },
    );

    update_group.finish();
}

fn bench_metrics_snapshot_resource_scan(c: &mut Criterion) {
    const SANDBOX_COUNT: usize = 1000;

    let rt = Runtime::new().expect("create Tokio runtime for benchmarks");
    let store = InMemoryMetadataStore::new();
    rt.block_on(async {
        for sandbox_index in 0..SANDBOX_COUNT {
            let user_metadata = (0..16)
                .map(|metadata_index| {
                    (
                        format!("bench-key-{sandbox_index}-{metadata_index}"),
                        format!(
                            "bench-value-{sandbox_index}-{metadata_index}-{}",
                            "x".repeat(128)
                        ),
                    )
                })
                .collect::<HashMap<_, _>>();

            store
                .add(SandboxMetadata {
                    id: SandboxId::new(),
                    state: SandboxState::Running,
                    resources: SandboxResources {
                        cpu_count: 1,
                        memory_mib: 128,
                        disk_size_mib: 0,
                    },
                    user_metadata: Some(user_metadata),
                    ..Default::default()
                })
                .await
                .expect("seed metrics snapshot benchmark store");
        }
    });

    let mut group = c.benchmark_group("orchestrator_metrics");
    group.throughput(Throughput::Elements(SANDBOX_COUNT as u64));
    group.bench_function("metrics_snapshot_resource_scan_1000", |b| {
        b.to_async(&rt).iter(|| async {
            let mut running_sandbox_count = 0u32;
            let mut starting_sandbox_count = 0u32;
            let mut allocated_cpu = 0u32;
            let mut allocated_memory_bytes = 0u64;
            store
                .list_with_callback(|metadata| {
                    let contributes_resources = metadata.state != SandboxState::Paused;
                    if matches!(
                        metadata.state,
                        SandboxState::Running
                            | SandboxState::Pausing
                            | SandboxState::Snapshotting
                            | SandboxState::Forking
                            | SandboxState::Killing
                    ) {
                        running_sandbox_count = running_sandbox_count.saturating_add(1);
                    }
                    if matches!(
                        metadata.state,
                        SandboxState::Creating | SandboxState::Resuming
                    ) {
                        starting_sandbox_count = starting_sandbox_count.saturating_add(1);
                    }
                    if contributes_resources {
                        allocated_cpu = allocated_cpu.saturating_add(metadata.resources.cpu_count);
                        allocated_memory_bytes = allocated_memory_bytes
                            .saturating_add(u64::from(metadata.resources.memory_mib) * 1024 * 1024);
                    }
                })
                .await
                .expect("scan metrics benchmark store");
            std::hint::black_box((
                running_sandbox_count,
                starting_sandbox_count,
                allocated_cpu,
                allocated_memory_bytes,
            ));
        });
    });
    group.finish();
}

fn benchmark_dataset(now: SystemTime, count: usize) -> Vec<SandboxMetadata> {
    let mut out = Vec::with_capacity(count);
    for idx in 0..count {
        let state = match idx % 100 {
            0 => SandboxState::Running,
            1..=9 => SandboxState::Paused,
            10..=14 => SandboxState::Pausing,
            15..=19 => SandboxState::Resuming,
            _ => SandboxState::Creating,
        };
        let expires_at = match idx % 200 {
            0 => Some(now - Duration::from_secs(1)),
            1..=19 => Some(now + Duration::from_secs(3600 + idx as u64)),
            _ => None,
        };

        let user_metadata = (idx % 5 == 0).then(|| {
            HashMap::from([
                ("team".to_string(), "alpha".to_string()),
                ("group".to_string(), format!("g-{}", idx % 10)),
            ])
        });

        out.push(SandboxMetadata {
            id: SandboxId::from_uuid(Uuid::from_u128(idx as u128 + 1)),
            state,
            created_at: now - Duration::from_secs(600),
            timeout: expires_at.and_then(|deadline| deadline.duration_since(now).ok()),
            expires_at,
            user_metadata,
            ..Default::default()
        });
    }
    out
}

fn criterion_config() -> Criterion {
    if std::env::var_os("AENV_BENCH_FULL").is_some() {
        Criterion::default().sample_size(FULL_SAMPLE_SIZE)
    } else {
        Criterion::default()
            .sample_size(DEFAULT_SAMPLE_SIZE)
            .warm_up_time(DEFAULT_WARM_UP_TIME)
            .measurement_time(DEFAULT_MEASUREMENT_TIME)
    }
}

criterion_group! {
    name = benches;
    config = criterion_config();
    targets =
        bench_orchestrator_store_list,
        bench_orchestrator_store_cas,
        bench_orchestrator_store_update,
        bench_metrics_snapshot_resource_scan
}
criterion_main!(benches);
