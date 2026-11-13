CREATE TABLE snapshots (
    snapshot_id UUID PRIMARY KEY,
    alias TEXT UNIQUE CHECK (alias IS NULL OR trim(alias) <> ''),
    snapshot_type TEXT NOT NULL CHECK (snapshot_type IN ('none', 'local', 'distributed')),
    owner_node_id TEXT,
    source_sandbox_id TEXT,
    retired BOOLEAN NOT NULL DEFAULT FALSE,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL
);

CREATE TABLE layers (
    uuid UUID PRIMARY KEY CHECK (uuid <> '00000000-0000-0000-0000-000000000000'),
    managed_parent_uuid UUID REFERENCES layers(uuid),
    digest TEXT NOT NULL CHECK (digest <> ''),
    collectible BOOLEAN NOT NULL DEFAULT FALSE,
    state TEXT NOT NULL CHECK (state IN ('available', 'deleting', 'deleted'))
);

CREATE TABLE snapshot_heads (
    snapshot_id UUID NOT NULL REFERENCES snapshots(snapshot_id),
    layer_uuid UUID NOT NULL REFERENCES layers(uuid),
    PRIMARY KEY (snapshot_id, layer_uuid)
);

-- A released pin may precede its snapshot registration.
CREATE TABLE snapshot_pins (
    pin_id UUID PRIMARY KEY,
    snapshot_id UUID NOT NULL,
    released BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE INDEX snapshots_order ON snapshots (created_at DESC, snapshot_id ASC)
    WHERE NOT retired AND snapshot_type <> 'none';
CREATE INDEX snapshots_source_order ON snapshots (
    source_sandbox_id,
    created_at DESC,
    snapshot_id ASC
) WHERE NOT retired AND snapshot_type <> 'none';
CREATE INDEX retired_snapshots ON snapshots (created_at, snapshot_id)
    WHERE retired = TRUE AND snapshot_type IN ('local', 'distributed');
CREATE INDEX live_children ON layers (managed_parent_uuid) WHERE state IN ('available', 'deleting');
CREATE INDEX available_layers ON layers (uuid) WHERE state = 'available';
CREATE INDEX heads_by_layer ON snapshot_heads (layer_uuid);
CREATE INDEX active_pins ON snapshot_pins (snapshot_id) WHERE NOT released;
