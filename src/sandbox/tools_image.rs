use std::path::PathBuf;

use anyhow::Result;

use crate::cfg::AppConfig;
use crate::image::ImageResolver;

/// Returns the overlaybd image config of the tools drive for `version`,
/// installing it from the tools OCI image when it is not on disk yet.
/// `None` means the sandbox attaches the installed tools drive file instead.
pub(crate) async fn resolve_tools_image(
    config: &AppConfig,
    version: &str,
) -> Result<Option<PathBuf>> {
    crate::deps::prepare_tools_image(config, version, |image_ref| async move {
        Ok(ImageResolver::new(config)
            .resolve_tools(&image_ref)
            .await?
            .map(|resolved| resolved.overlaybd_config_path))
    })
    .await
}
