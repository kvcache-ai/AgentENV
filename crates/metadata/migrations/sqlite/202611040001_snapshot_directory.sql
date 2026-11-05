CREATE TABLE snapshots (
    snapshot_id TEXT PRIMARY KEY NOT NULL,
    alias TEXT UNIQUE CHECK (alias IS NULL OR trim(alias) <> ''),
    snapshot_type TEXT NOT NULL CHECK (snapshot_type IN ('none', 'local', 'distributed')),
    committed_digest TEXT CHECK (committed_digest IS NULL OR trim(committed_digest) <> ''),
    metadata TEXT CHECK (metadata IS NULL OR (json_valid(metadata) AND json_type(metadata) = 'object')),
    retired BOOLEAN NOT NULL DEFAULT FALSE CHECK (retired IN (FALSE, TRUE)),
    created_at BIGINT NOT NULL
);

CREATE TABLE layers (
    uuid TEXT PRIMARY KEY NOT NULL CHECK (uuid <> '00000000-0000-0000-0000-000000000000'),
    managed_parent_uuid TEXT REFERENCES layers(uuid),
    digest TEXT NOT NULL CHECK (digest <> ''),
    state TEXT NOT NULL CHECK (state IN ('available', 'deleting', 'deleted'))
);

CREATE TABLE snapshot_heads (
    snapshot_id TEXT NOT NULL REFERENCES snapshots(snapshot_id),
    layer_uuid TEXT NOT NULL REFERENCES layers(uuid),
    PRIMARY KEY (snapshot_id, layer_uuid)
);

-- A released pin may precede its snapshot registration.
CREATE TABLE snapshot_pins (
    pin_id TEXT PRIMARY KEY NOT NULL,
    snapshot_id TEXT NOT NULL,
    released BOOLEAN NOT NULL DEFAULT FALSE CHECK (released IN (FALSE, TRUE))
);

CREATE INDEX snapshots_order ON snapshots (created_at DESC, snapshot_id ASC);
CREATE INDEX retired_snapshots ON snapshots (snapshot_id) WHERE retired;
CREATE INDEX live_children ON layers (managed_parent_uuid) WHERE state IN ('available', 'deleting');
CREATE INDEX available_layers ON layers (uuid) WHERE state = 'available';
CREATE INDEX deleting_layers ON layers (uuid) WHERE state = 'deleting';
CREATE INDEX heads_by_layer ON snapshot_heads (layer_uuid);
CREATE INDEX active_pins ON snapshot_pins (snapshot_id) WHERE NOT released;
