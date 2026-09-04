mod gc;
mod graph;
mod service;
mod source_config;
mod store;

#[cfg(test)]
mod mock;

pub(crate) use service::ImageCacheService;

pub(crate) use store::{
    local_image_services_from_app_config, local_image_services_from_global_config,
    CachedImageConfig, OverlaybdLayerLocation, OverlaybdLayerStore, RuntimeImageOwner,
    RuntimeImageRefs, SourceImageStore,
};

#[cfg(test)]
pub(crate) mod test_support {
    pub(crate) use super::mock::RecordingRuntimeImageRefs;
    pub(crate) use super::service::ImageCacheService;
    pub(crate) use super::store::test_local_image_services_from_service;
}
