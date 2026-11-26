-- =============================================================================
-- Section 1: Shared Layer Storage & GC Substrate (layers, layer_heads, gc_pins)
--            and Unified Snapshot Directory (Sandbox, Template, Builder)
-- =============================================================================

CREATE TABLE layers (
    uuid                TEXT PRIMARY KEY NOT NULL CHECK (uuid <> '00000000-0000-0000-0000-000000000000'),
    managed_parent_uuid TEXT REFERENCES layers(uuid),
    digest              TEXT NOT NULL CHECK (digest <> ''),
    state               TEXT NOT NULL CHECK (state IN ('available', 'deleting', 'deleted'))
);

CREATE TABLE layer_heads (
    owner_kind TEXT NOT NULL CHECK (owner_kind IN ('snapshot', 'volume', 'image')),
    owner_id   TEXT NOT NULL CHECK (owner_id <> ''),
    layer_uuid TEXT NOT NULL REFERENCES layers(uuid),
    released   BOOLEAN NOT NULL DEFAULT FALSE CHECK (released IN (FALSE, TRUE)),
    PRIMARY KEY (owner_kind, owner_id, layer_uuid)
);

-- A released pin may precede its target registration.
CREATE TABLE gc_pins (
    pin_id        TEXT PRIMARY KEY NOT NULL,
    target_kind   TEXT NOT NULL CHECK (target_kind IN ('snapshot', 'volume', 'image')),
    target_id     TEXT NOT NULL CHECK (target_id <> ''),
    owner_node_id TEXT,
    released      BOOLEAN NOT NULL DEFAULT FALSE CHECK (released IN (FALSE, TRUE))
);

CREATE TABLE snapshots (
    snapshot_id       TEXT PRIMARY KEY NOT NULL,
    source_kind       TEXT NOT NULL CHECK (source_kind IN ('sandbox', 'template', 'builder')),
    visibility        TEXT NOT NULL DEFAULT 'public' CHECK (visibility IN ('public', 'internal')),
    alias             TEXT CHECK (alias IS NULL OR trim(alias) <> ''),
    state             TEXT NOT NULL CHECK (state IN ('active', 'retired', 'reaped', 'reserved')),
    placement         TEXT CHECK (placement IN ('local', 'distributed')),
    owner_node_id     TEXT,
    source_sandbox_id TEXT,
    created_at        BIGINT NOT NULL,
    updated_at        BIGINT NOT NULL,
    CHECK (
        (state IN ('active', 'retired') AND placement IS NOT NULL)
        OR (state IN ('reaped', 'reserved') AND placement IS NULL)
    ),
    CHECK (
        (placement = 'local' AND source_kind = 'sandbox' AND alias IS NULL AND owner_node_id IS NOT NULL)
        OR (placement = 'distributed' AND owner_node_id IS NULL)
        OR (placement IS NULL AND owner_node_id IS NULL)
    ),
    CHECK (
        (source_kind = 'builder' AND visibility = 'internal')
        OR (source_kind IN ('sandbox', 'template') AND visibility = 'public')
    ),
    CHECK (state = 'active' OR alias IS NULL)
);

CREATE INDEX live_children ON layers (managed_parent_uuid) WHERE state IN ('available', 'deleting');
CREATE INDEX deleting_layers ON layers (uuid) WHERE state = 'deleting';
CREATE INDEX live_layers_by_digest ON layers (digest) WHERE state IN ('available', 'deleting');

CREATE INDEX active_layer_heads ON layer_heads (layer_uuid) WHERE NOT released;
CREATE INDEX released_layer_heads ON layer_heads (owner_kind, owner_id, layer_uuid) WHERE released;

CREATE INDEX active_gc_pins ON gc_pins (target_kind, target_id) WHERE NOT released;

CREATE UNIQUE INDEX snapshots_unique_alias ON snapshots (visibility, alias)
    WHERE alias IS NOT NULL;
CREATE INDEX snapshots_active_order ON snapshots (source_kind, created_at DESC, snapshot_id ASC)
    WHERE state = 'active' AND visibility = 'public';
CREATE INDEX snapshots_active_source_order ON snapshots (
    source_sandbox_id,
    created_at DESC,
    snapshot_id ASC
) WHERE state = 'active' AND source_kind = 'sandbox';
CREATE INDEX snapshots_retired ON snapshots (snapshot_id) WHERE state = 'retired';

-- =============================================================================
-- Section 2: Volumes (with BuildCache), Published Images & Tags, Build Jobs
-- =============================================================================

CREATE TABLE volumes (
    volume_id           TEXT PRIMARY KEY NOT NULL CHECK (volume_id <> ''),
    name                TEXT NOT NULL CHECK (name <> ''),
    purpose             TEXT NOT NULL DEFAULT 'user'
                        CHECK (purpose IN ('user', 'build_cache_work', 'build_cache_seed', 'build_cache_retired')),
    mode                TEXT NOT NULL CHECK (mode IN ('exclusive', 'read_only')),
    size_mb             BIGINT NOT NULL CHECK (size_mb > 0),
    status              TEXT NOT NULL CHECK (status IN ('ready', 'uploading', 'failed')),
    backing_layers_json TEXT NOT NULL DEFAULT '[]',
    reserved_by         TEXT,
    ro_mounts_json      TEXT NOT NULL DEFAULT '[]',
    created_at          BIGINT NOT NULL,
    updated_at          BIGINT NOT NULL,
    CHECK (mode = 'exclusive' OR reserved_by IS NULL),
    CHECK (purpose <> 'build_cache_seed' OR mode = 'read_only')
);

CREATE UNIQUE INDEX volumes_unique_name ON volumes (name);
CREATE UNIQUE INDEX volumes_single_build_cache_seed ON volumes (purpose)
    WHERE purpose = 'build_cache_seed';
CREATE INDEX volumes_user_order ON volumes (volume_id ASC) WHERE purpose = 'user';
CREATE INDEX volumes_build_cache_retired ON volumes (volume_id ASC)
    WHERE purpose = 'build_cache_retired';

CREATE TABLE images (
    digest         TEXT PRIMARY KEY NOT NULL CHECK (digest LIKE 'sha256:%'),
    schema_version INTEGER NOT NULL DEFAULT 1,
    os             TEXT NOT NULL DEFAULT 'linux',
    architecture   TEXT NOT NULL CHECK (architecture IN ('amd64', 'arm64')),
    payload_json   TEXT NOT NULL,
    retired        BOOLEAN NOT NULL DEFAULT FALSE CHECK (retired IN (FALSE, TRUE)),
    created_at     BIGINT NOT NULL
);

CREATE TABLE image_tags (
    name                   TEXT NOT NULL CHECK (name <> ''),
    tag                    TEXT NOT NULL CHECK (tag <> ''),
    image_digest           TEXT NOT NULL REFERENCES images(digest),
    source_manifest_digest TEXT,
    created_at             BIGINT NOT NULL,
    updated_at             BIGINT NOT NULL,
    PRIMARY KEY (name, tag)
);
CREATE INDEX image_tags_by_digest ON image_tags (image_digest);

CREATE TABLE build_jobs (
    build_id            TEXT PRIMARY KEY NOT NULL,
    kind                TEXT NOT NULL CHECK (kind IN ('template', 'image')),
    status              TEXT NOT NULL CHECK (status IN ('waiting', 'building', 'ready', 'error')),
    target_json         TEXT NOT NULL,
    result_snapshot_id  TEXT REFERENCES snapshots(snapshot_id),
    result_image_digest TEXT,
    error_message       TEXT,
    error_step          TEXT,
    started_at          BIGINT,
    finished_at         BIGINT,
    created_at          BIGINT NOT NULL,
    updated_at          BIGINT NOT NULL,
    CHECK (
        (kind = 'template' AND result_image_digest IS NULL)
        OR (kind = 'image' AND result_snapshot_id IS NULL)
    )
);
CREATE INDEX build_jobs_active ON build_jobs (status, created_at DESC)
    WHERE status IN ('waiting', 'building');
