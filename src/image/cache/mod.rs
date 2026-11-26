mod gc;
mod graph;
mod service;
mod source_config;
mod store;

pub(crate) use service::ImageCacheService;

#[cfg(test)]
pub(crate) use store::OverlaybdLayerLocation;
pub(crate) use store::{
    attach_local_layer_locations, local_image_services_from_app_config,
    local_image_services_from_global_config, CachedImageConfig, OverlaybdLayerStore,
    RuntimeImageOwner, RuntimeImageRefs, SourceImageStore,
};

#[cfg(test)]
pub(crate) mod test_support {
    pub(crate) use super::service::ImageCacheService;
    pub(crate) use super::store::test_local_image_services_from_service;
}
