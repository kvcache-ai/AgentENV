-- =============================================================================
-- Section 1: Shared Layer Storage & GC Substrate (layers, layer_heads, snapshot_pins)
--            and Unified Snapshot Directory (Sandbox, Template, Builder)
-- =============================================================================

CREATE TABLE layers (
    uuid                UUID PRIMARY KEY CHECK (uuid <> '00000000-0000-0000-0000-000000000000'),
    managed_parent_uuid UUID REFERENCES layers(uuid),
    digest              TEXT NOT NULL CHECK (digest <> ''),
    state               TEXT NOT NULL CHECK (state IN ('available', 'deleting', 'deleted'))
);

CREATE TABLE layer_heads (
    owner_kind TEXT NOT NULL CHECK (owner_kind IN ('snapshot', 'volume', 'image')),
    owner_id   TEXT NOT NULL CHECK (owner_id <> ''),
    layer_uuid UUID NOT NULL REFERENCES layers(uuid),
    released   BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (owner_kind, owner_id, layer_uuid)
);

-- A released pin may precede snapshot registration.
CREATE TABLE snapshot_pins (
    pin_id        UUID PRIMARY KEY,
    snapshot_id   UUID NOT NULL,
    owner_node_id TEXT,
    released      BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE snapshots (
    snapshot_id       UUID PRIMARY KEY,
    source_kind       TEXT NOT NULL CHECK (source_kind IN ('sandbox', 'template')),
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
    CHECK (state = 'active' OR alias IS NULL)
);

CREATE INDEX live_children ON layers (managed_parent_uuid) WHERE state IN ('available', 'deleting');
CREATE INDEX deleting_layers ON layers (uuid) WHERE state = 'deleting';
CREATE INDEX live_layers_by_digest ON layers (digest) WHERE state IN ('available', 'deleting');

CREATE INDEX active_layer_heads ON layer_heads (layer_uuid) WHERE NOT released;
CREATE INDEX released_layer_heads ON layer_heads (owner_kind, owner_id, layer_uuid) WHERE released;

CREATE INDEX active_snapshot_pins ON snapshot_pins (snapshot_id) WHERE NOT released;

CREATE UNIQUE INDEX snapshots_unique_alias ON snapshots (alias)
    WHERE alias IS NOT NULL;
CREATE INDEX snapshots_active_order ON snapshots (source_kind, created_at DESC, snapshot_id ASC)
    WHERE state = 'active';
CREATE INDEX snapshots_active_source_order ON snapshots (
    source_sandbox_id,
    created_at DESC,
    snapshot_id ASC
) WHERE state = 'active' AND source_kind = 'sandbox';
CREATE INDEX snapshots_retired ON snapshots (snapshot_id) WHERE state = 'retired';

-- =============================================================================
-- Section 2: Volumes, Published Images & Tags, Build Jobs
-- =============================================================================

CREATE TABLE volumes (
    volume_id           TEXT PRIMARY KEY CHECK (volume_id <> ''),
    name                TEXT NOT NULL CHECK (name <> ''),
    mode                TEXT NOT NULL CHECK (mode IN ('exclusive', 'read_only')),
    size_mb             BIGINT NOT NULL CHECK (size_mb > 0),
    status              TEXT NOT NULL CHECK (status IN ('ready', 'uploading', 'failed')),
    backing_layers_json TEXT NOT NULL DEFAULT '[]',
    reserved_by         TEXT,
    ro_mounts_json      TEXT NOT NULL DEFAULT '[]',
    created_at          BIGINT NOT NULL,
    updated_at          BIGINT NOT NULL,
    CHECK (mode = 'exclusive' OR reserved_by IS NULL)
);

CREATE UNIQUE INDEX volumes_unique_name ON volumes (name);

CREATE TABLE images (
    digest       TEXT PRIMARY KEY CHECK (digest LIKE 'sha256:%'),
    payload_json TEXT NOT NULL,
    created_at   BIGINT NOT NULL
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

CREATE TABLE build_jobs (
    build_id            UUID PRIMARY KEY,
    status              TEXT NOT NULL CHECK (status IN ('waiting', 'building', 'ready', 'error')),
    target_json         TEXT NOT NULL,
    result_image_digest TEXT,
    error_message       TEXT,
    error_step          TEXT,
    started_at          BIGINT,
    finished_at         BIGINT,
    created_at          BIGINT NOT NULL,
    updated_at          BIGINT NOT NULL
);
