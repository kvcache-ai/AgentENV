use agentenv_domain::{
    BuildCacheState, BuildErrorReason, BuildJobRecord, BuildTarget, ImageTag, ImageTagRecord,
    PublishedImage, ResourceKind, SnapshotId, SnapshotListFilter, SnapshotManifest,
    SnapshotMetadata, SnapshotVisibility, SnapshotVolume, VolumeRecord, VolumeRecordPage,
};
use tarpc::context::Context;
use uuid::Uuid;

use crate::{GcReport, MetaResult, MetaServer};

#[tarpc::service]
pub trait Meta {
    async fn health() -> MetaResult<()>;

    // Snapshots
    async fn get_snapshot(reference: String) -> MetaResult<Option<SnapshotMetadata>>;
    async fn get_snapshot_with_visibility(
        reference: String,
        visibility: SnapshotVisibility,
    ) -> MetaResult<Option<SnapshotMetadata>>;
    async fn list_snapshots(filter: SnapshotListFilter) -> MetaResult<Vec<SnapshotMetadata>>;
    async fn register_snapshot(
        metadata: SnapshotMetadata,
        volume_snapshots: Vec<SnapshotVolume>,
        manifest: SnapshotManifest,
        restored_layers: Vec<String>,
    ) -> MetaResult<SnapshotMetadata>;
    async fn promote_snapshot(
        metadata: SnapshotMetadata,
        volume_snapshots: Vec<SnapshotVolume>,
        manifest: SnapshotManifest,
        restored_layers: Vec<String>,
    ) -> MetaResult<Option<SnapshotMetadata>>;
    async fn retire_snapshot(id: SnapshotId) -> MetaResult<()>;

    // Shared pins & GC
    async fn pin_snapshot(
        id: SnapshotId,
        pin_id: Uuid,
        owner_node_id: Option<String>,
    ) -> MetaResult<Option<SnapshotMetadata>>;
    async fn pin_resource(
        kind: ResourceKind,
        target_id: String,
        pin_id: Uuid,
        owner_node_id: Option<String>,
    ) -> MetaResult<bool>;
    async fn release_pin(kind: ResourceKind, target_id: String, pin_id: Uuid) -> MetaResult<()>;
    async fn collect_garbage(limit: u32) -> MetaResult<GcReport>;

    // Volumes & Build Cache
    async fn get_volume(reference: String) -> MetaResult<Option<VolumeRecord>>;
    async fn list_volumes_page(
        after_volume_id: Option<String>,
        limit: u32,
        include_internal: bool,
    ) -> MetaResult<VolumeRecordPage>;
    async fn create_volume(
        record: VolumeRecord,
        from_volume_id: Option<String>,
        restored_layers: Vec<String>,
    ) -> MetaResult<VolumeRecord>;
    async fn update_volume(
        record: VolumeRecord,
        restored_layers: Vec<String>,
    ) -> MetaResult<VolumeRecord>;
    async fn delete_volume(volume_id: String) -> MetaResult<()>;
    async fn reserve_volume(volume_id: String, owner: String) -> MetaResult<Option<String>>;
    async fn reserve_read_only_volume(volume_id: String, owner: String) -> MetaResult<()>;
    async fn replace_volume_owner(
        volume_id: String,
        from: String,
        to: Option<String>,
    ) -> MetaResult<()>;
    async fn get_build_cache_state() -> MetaResult<BuildCacheState>;
    async fn replace_build_cache_head(volume_id: String) -> MetaResult<BuildCacheState>;

    // Images & Tags
    async fn put_image(image: PublishedImage, restored_layers: Vec<String>) -> MetaResult<String>;
    async fn get_image(digest: String) -> MetaResult<Option<PublishedImage>>;
    async fn put_image_tag(
        tag: ImageTag,
        digest: String,
        source_manifest_digest: Option<String>,
    ) -> MetaResult<()>;
    async fn get_image_tag(tag: ImageTag) -> MetaResult<Option<ImageTagRecord>>;

    // Build Jobs
    async fn create_build_job(
        build_id: SnapshotId,
        target: BuildTarget,
    ) -> MetaResult<BuildJobRecord>;
    async fn get_build_job(build_id: SnapshotId) -> MetaResult<Option<BuildJobRecord>>;
    async fn start_build_job(build_id: SnapshotId) -> MetaResult<bool>;
    async fn finish_build_job(
        build_id: SnapshotId,
        result_snapshot_id: Option<SnapshotId>,
        result_image_digest: Option<String>,
    ) -> MetaResult<Option<BuildJobRecord>>;
    async fn fail_build_job(
        build_id: SnapshotId,
        reason: BuildErrorReason,
    ) -> MetaResult<Option<BuildJobRecord>>;
    async fn delete_build_job(
        build_id: SnapshotId,
        reserve_snapshot_tombstone: bool,
    ) -> MetaResult<bool>;
}

macro_rules! forward_meta {
    ($(fn $name:ident($($arg:ident: $ty:ty),* $(,)?) -> $ret:ty => ($($call:expr),* $(,)?);)*) => {
        $(
            async fn $name(self, _: Context, $($arg: $ty),*) -> $ret {
                Box::pin(MetaServer::$name(&self, $($call),*)).await
            }
        )*
    };
}

impl Meta for MetaServer {
    async fn health(self, _: Context) -> MetaResult<()> {
        Ok(())
    }

    async fn collect_garbage(self, _: Context, limit: u32) -> MetaResult<GcReport> {
        // Keep claimed layer deletions running when an operator disconnects.
        let active = self.active_collections.clone().read_owned().await;
        tokio::spawn(async move {
            let _active = active;
            MetaServer::collect_garbage(&self, limit).await
        })
        .await
        .map_err(|error| crate::MetaError::backend("finish collection", error))?
    }

    forward_meta! {
        fn get_snapshot(reference: String) -> MetaResult<Option<SnapshotMetadata>>
            => (&reference);
        fn get_snapshot_with_visibility(reference: String, visibility: SnapshotVisibility) -> MetaResult<Option<SnapshotMetadata>>
            => (&reference, visibility);
        fn list_snapshots(filter: SnapshotListFilter) -> MetaResult<Vec<SnapshotMetadata>>
            => (filter);
        fn register_snapshot(
            metadata: SnapshotMetadata,
            volume_snapshots: Vec<SnapshotVolume>,
            manifest: SnapshotManifest,
            restored_layers: Vec<String>,
        ) -> MetaResult<SnapshotMetadata>
            => (metadata, &volume_snapshots, manifest, &restored_layers);
        fn promote_snapshot(
            metadata: SnapshotMetadata,
            volume_snapshots: Vec<SnapshotVolume>,
            manifest: SnapshotManifest,
            restored_layers: Vec<String>,
        ) -> MetaResult<Option<SnapshotMetadata>>
            => (metadata, &volume_snapshots, manifest, &restored_layers);
        fn retire_snapshot(id: SnapshotId) -> MetaResult<()>
            => (id);
        fn pin_snapshot(id: SnapshotId, pin_id: Uuid, owner_node_id: Option<String>) -> MetaResult<Option<SnapshotMetadata>>
            => (id, pin_id, owner_node_id);
        fn pin_resource(kind: ResourceKind, target_id: String, pin_id: Uuid, owner_node_id: Option<String>) -> MetaResult<bool>
            => (kind, &target_id, pin_id, owner_node_id);
        fn release_pin(kind: ResourceKind, target_id: String, pin_id: Uuid) -> MetaResult<()>
            => (kind, &target_id, pin_id);
        fn get_volume(reference: String) -> MetaResult<Option<VolumeRecord>>
            => (&reference);
        fn list_volumes_page(after_volume_id: Option<String>, limit: u32, include_internal: bool) -> MetaResult<VolumeRecordPage>
            => (after_volume_id.as_deref(), limit, include_internal);
        fn create_volume(record: VolumeRecord, from_volume_id: Option<String>, restored_layers: Vec<String>) -> MetaResult<VolumeRecord>
            => (record, from_volume_id.as_deref(), &restored_layers);
        fn update_volume(record: VolumeRecord, restored_layers: Vec<String>) -> MetaResult<VolumeRecord>
            => (record, &restored_layers);
        fn delete_volume(volume_id: String) -> MetaResult<()>
            => (&volume_id);
        fn reserve_volume(volume_id: String, owner: String) -> MetaResult<Option<String>>
            => (&volume_id, &owner);
        fn reserve_read_only_volume(volume_id: String, owner: String) -> MetaResult<()>
            => (&volume_id, &owner);
        fn replace_volume_owner(volume_id: String, from: String, to: Option<String>) -> MetaResult<()>
            => (&volume_id, &from, to.as_deref());
        fn get_build_cache_state() -> MetaResult<BuildCacheState>
            => ();
        fn replace_build_cache_head(volume_id: String) -> MetaResult<BuildCacheState>
            => (&volume_id);
        fn put_image(image: PublishedImage, restored_layers: Vec<String>) -> MetaResult<String>
            => (&image, &restored_layers);
        fn get_image(digest: String) -> MetaResult<Option<PublishedImage>>
            => (&digest);
        fn put_image_tag(tag: ImageTag, digest: String, source_manifest_digest: Option<String>) -> MetaResult<()>
            => (&tag, &digest, source_manifest_digest.as_deref());
        fn get_image_tag(tag: ImageTag) -> MetaResult<Option<ImageTagRecord>>
            => (&tag);
        fn create_build_job(build_id: SnapshotId, target: BuildTarget) -> MetaResult<BuildJobRecord>
            => (&build_id, &target);
        fn get_build_job(build_id: SnapshotId) -> MetaResult<Option<BuildJobRecord>>
            => (&build_id);
        fn start_build_job(build_id: SnapshotId) -> MetaResult<bool>
            => (&build_id);
        fn finish_build_job(
            build_id: SnapshotId,
            result_snapshot_id: Option<SnapshotId>,
            result_image_digest: Option<String>,
        ) -> MetaResult<Option<BuildJobRecord>>
            => (&build_id, result_snapshot_id.as_ref(), result_image_digest.as_deref());
        fn fail_build_job(build_id: SnapshotId, reason: BuildErrorReason) -> MetaResult<Option<BuildJobRecord>>
            => (&build_id, &reason);
        fn delete_build_job(build_id: SnapshotId, reserve_snapshot_tombstone: bool) -> MetaResult<bool>
            => (&build_id, reserve_snapshot_tombstone);
    }
}
