//! The Compose guest image source in images/compose, embedded so `aenv compose
//! up` can build the image or template with the server's builder without a
//! checkout.

use crate::client::templates::{build_status, Template};
use crate::client::Client;
use anyhow::{bail, Context, Result};
use std::path::Path;
use tempfile::TempDir;

macro_rules! source {
    ($($path:literal),* $(,)?) => {
        &[$((
            $path,
            include_bytes!(concat!(env!("CARGO_MANIFEST_DIR"), "/../../images/compose/", $path))
                as &[u8],
        )),*]
    };
}

const SOURCE: &[(&str, &[u8])] = source!(
    ".dockerignore",
    "Dockerfile",
    "containerd.toml",
    "daemon.json",
    "init",
    "plain-snapshotter/go.mod",
    "plain-snapshotter/go.sum",
    "plain-snapshotter/main.go",
    "plain-snapshotter/snapshotter.go",
);

/// FNV-1a over every file's path, length and contents, so a changed source
/// gets a new image name.
fn fingerprint() -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for (path, data) in SOURCE {
        let len = (data.len() as u64).to_le_bytes();
        for &byte in path.as_bytes().iter().chain(&[0]).chain(&len).chain(*data) {
            hash = (hash ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3);
        }
    }
    hash
}

/// Image name, as `aenv build --image --name` takes it.
fn image_name() -> String {
    format!("aenv-compose:{:016x}", fingerprint())
}

/// Image reference for cold starts.
pub(super) fn image_reference() -> String {
    format!("aenv/{}", image_name())
}

/// The server reports an unknown image tag as `image tag '...' was not found`.
pub(super) fn is_missing(error: &anyhow::Error) -> bool {
    let message = error.to_string();
    message.contains(&image_name()) && message.contains("was not found")
}

fn write_source(dir: &Path) -> Result<()> {
    for (path, data) in SOURCE {
        let path = dir.join(path);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&path, data).with_context(|| format!("write {}", path.display()))?;
    }
    Ok(())
}

fn source_dir() -> Result<TempDir> {
    let context = tempfile::Builder::new()
        .prefix("aenv-compose-image-")
        .tempdir()?;
    write_source(context.path())?;
    Ok(context)
}

/// Builds the image with the server's builder, reporting progress on stderr.
pub(super) fn build() -> Result<()> {
    let context = source_dir()?;
    super::super::build::compose_guest(context.path().to_owned(), image_name(), true)
}

/// Template name for warm starts.
fn template_name() -> String {
    format!("aenv-compose-{:016x}", fingerprint())
}

fn find_template(client: &Client, name: &str) -> Result<Option<Template>> {
    let templates = client.list_templates()?;
    Ok(templates.into_iter().find(|template| {
        let mut names = template.names.iter().chain(&template.aliases);
        names.any(|n| n == name)
    }))
}

/// Whether the template is ready, or `false` after a failed build. A build
/// that is still running is an error, since its alias blocks another build.
fn is_ready(template: &Template, name: &str) -> Result<bool> {
    match template.build_status.as_deref() {
        None | Some(build_status::READY) => Ok(true),
        Some(build_status::ERROR) => Ok(false),
        Some(_) => bail!(
            "the bundled template {name} is still building; \
             check it with `aenv template watch {name}` and retry"
        ),
    }
}

/// Returns the bundled template, building it with the server's builder on
/// first use.
pub(super) fn ensure_template(client: &Client) -> Result<String> {
    let name = template_name();
    if let Some(template) = find_template(client, &name)? {
        if is_ready(&template, &name)? {
            return Ok(name);
        }
        // A failed build keeps the alias, which would reject a new build.
        eprintln!("Removing the failed bundled template {name}...");
        client.delete_template(&template.template_id)?;
    }
    eprintln!("Building the bundled Compose template (first use only)...");
    let context = source_dir()?;
    let built = super::super::build::compose_guest(context.path().to_owned(), name.clone(), false);
    if let Err(error) = built {
        // A concurrent first run may have published the same template.
        match find_template(client, &name)? {
            Some(template) if is_ready(&template, &name)? => {}
            _ => return Err(error),
        }
    }
    Ok(name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn embeds_every_image_source_file() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../images/compose");
        let mut files: Vec<String> = walkdir::WalkDir::new(&root)
            .into_iter()
            .map(Result::unwrap)
            .filter(|entry| entry.file_type().is_file())
            .map(|entry| {
                let path = entry.path().strip_prefix(&root).unwrap();
                path.to_str().unwrap().replace('\\', "/")
            })
            .filter(|path| !path.ends_with("README.md") && !path.ends_with(".gitignore"))
            .collect();
        files.sort();
        let mut embedded: Vec<&str> = SOURCE.iter().map(|(path, _)| *path).collect();
        embedded.sort();
        assert_eq!(files, embedded);
    }

    #[test]
    fn names_the_image_by_source_fingerprint() {
        let reference = image_reference();
        let tag = reference.strip_prefix("aenv/aenv-compose:").unwrap();
        assert_eq!(tag.len(), 16);
        assert!(tag.bytes().all(|byte| byte.is_ascii_hexdigit()));
        assert_eq!(reference, image_reference());
        assert_eq!(template_name(), format!("aenv-compose-{tag}"));
    }

    #[test]
    fn detects_only_a_missing_managed_image() {
        let missing = anyhow::anyhow!("HTTP 400: image tag '{}' was not found", image_name());
        assert!(is_missing(&missing));
        assert!(!is_missing(&anyhow::anyhow!(
            "HTTP 400: image tag 'aenv-compose:0000000000000000' was not found"
        )));
        assert!(!is_missing(&anyhow::anyhow!("HTTP 500: {}", image_name())));
    }

    #[test]
    fn rebuilds_failed_templates_and_waits_for_running_builds() {
        let template = |status: Option<&str>| -> Template {
            serde_json::from_value(serde_json::json!({
                "templateID": "t", "buildID": "t", "buildStatus": status
            }))
            .unwrap()
        };
        assert!(is_ready(&template(Some("ready")), "x").unwrap());
        assert!(is_ready(&template(None), "x").unwrap());
        assert!(!is_ready(&template(Some("error")), "x").unwrap());
        for status in ["waiting", "building"] {
            let error = is_ready(&template(Some(status)), "x").unwrap_err();
            assert!(error.to_string().contains("still building"), "{error}");
        }
    }

    #[test]
    fn writes_the_build_context() {
        let dir = tempfile::tempdir().unwrap();
        write_source(dir.path()).unwrap();
        for (path, data) in SOURCE {
            assert_eq!(std::fs::read(dir.path().join(path)).unwrap(), *data);
        }
    }
}
