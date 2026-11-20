use std::collections::HashSet;

use super::{ImageError, ImageResult};
use crate::types::{ImageName, ImageTag};

fn invalid_image_ref<T>(message: impl Into<String>) -> ImageResult<T> {
    Err(ImageError::InvalidReference {
        reason: message.into(),
    })
}

pub(crate) fn image_ref_candidates(
    input: &str,
    search_registries: &[String],
    allowed_registries: Option<&[String]>,
) -> ImageResult<Vec<String>> {
    let image = input.trim();
    if image.is_empty() {
        return invalid_image_ref("image reference must be non-empty");
    }

    let (name, suffix) = split_name_suffix(image);
    validate_image_ref_parts(image, name, suffix)?;

    let candidates = if has_registry_host(name) {
        vec![format!("{name}{suffix}")]
    } else {
        if search_registries.is_empty() {
            return invalid_image_ref(format!(
                "cannot resolve unqualified image ref '{image}': search registry list is empty"
            ));
        }

        let mut candidates = Vec::with_capacity(search_registries.len());
        let mut seen = HashSet::with_capacity(search_registries.len());
        for registry in search_registries {
            let registry = validate_registry_search_prefix(registry)?;
            let repository = if registry == "docker.io" && !name.contains('/') {
                format!("docker.io/library/{name}")
            } else {
                format!("{registry}/{name}")
            };
            let candidate = format!("{repository}{suffix}");
            if seen.insert(candidate.clone()) {
                candidates.push(candidate);
            }
        }
        candidates
    };

    enforce_allowed_registries(image, candidates, allowed_registries)
}

/// Drop any candidate whose registry host is not in `allowed_registries`.
/// `None` (the key is unset) imposes no restriction. `Some(list)` enables the
/// whitelist: candidates outside the list are dropped, and an empty list
/// therefore rejects every candidate. If filtering removes every candidate, the
/// reference is rejected as [`ImageError::InvalidReference`].
fn enforce_allowed_registries(
    image: &str,
    candidates: Vec<String>,
    allowed_registries: Option<&[String]>,
) -> ImageResult<Vec<String>> {
    let Some(allowed_registries) = allowed_registries else {
        return Ok(candidates);
    };

    let permitted: Vec<String> = candidates
        .into_iter()
        .filter(|candidate| {
            registry_host_of(candidate)
                .is_some_and(|host| allowed_registries.iter().any(|allowed| allowed == host))
        })
        .collect();

    if permitted.is_empty() {
        return invalid_image_ref(format!(
            "image reference '{image}' is not from an allowed registry; \
             allowed registries: [{}]",
            allowed_registries.join(", ")
        ));
    }

    Ok(permitted)
}

/// The tag that names the published copy of `candidate`, a fully-qualified
/// reference from [`image_ref_candidates`], such as
/// `docker.io/library/python:3.12`. `None` when it is pinned by digest or
/// does not form a valid name.
pub(crate) fn pulled_image_tag(candidate: &str) -> Option<ImageTag> {
    let (name, suffix) = split_name_suffix(candidate);
    let tag = suffix.strip_prefix(':')?;
    registry_host_of(name)?;
    ImageTag::new(&ImageName::parse_pulled(name).ok()?, tag).ok()
}

/// Extract the registry host from a fully-qualified image reference, returning
/// `None` when the leading segment does not look like a registry host.
pub(crate) fn registry_host_of(image_ref: &str) -> Option<&str> {
    let (host, _) = image_ref.split_once('/')?;
    is_registry_host(host).then_some(host)
}

fn has_registry_host(input: &str) -> bool {
    let image = input.trim();
    let Some((host, _)) = image.split_once('/') else {
        return false;
    };
    is_registry_host(host)
}

fn is_registry_host(input: &str) -> bool {
    input.contains('.') || input.contains(':') || input == "localhost"
}

fn validate_registry_search_prefix(input: &str) -> ImageResult<&str> {
    let registry = input;
    if registry.is_empty() {
        return invalid_image_ref("image resolver search registry entry must be non-empty");
    }
    if registry.contains("://") {
        return invalid_image_ref(format!(
            "image resolver search registry entry must not include a URL scheme: {input}"
        ));
    }
    if registry.contains('@') {
        return invalid_image_ref(format!(
            "image resolver search registry entry must not include a digest: {registry}"
        ));
    }
    let host = registry.split('/').next().unwrap_or(registry);
    if !is_registry_host(host) {
        return invalid_image_ref(format!(
            "image resolver search registry entry must start with a registry host: {registry}"
        ));
    }
    if registry
        .split('/')
        .skip(1)
        .any(|segment| segment.contains(':'))
    {
        return invalid_image_ref(format!(
            "image resolver search registry entry must not include an image tag: {registry}"
        ));
    }
    Ok(registry)
}

fn validate_image_ref_parts(image: &str, name: &str, suffix: &str) -> ImageResult<()> {
    if name.is_empty() {
        return invalid_image_ref(format!(
            "image reference has an empty repository name: {image}"
        ));
    }
    if name.contains('@') {
        return invalid_image_ref(format!(
            "image reference has invalid digest syntax: {image}"
        ));
    }
    if let Some(tag) = suffix.strip_prefix(':') {
        if tag.is_empty() {
            return invalid_image_ref(format!("image reference has an empty tag: {image}"));
        }
        if tag.contains('@') {
            return invalid_image_ref(format!("image reference has invalid tag syntax: {image}"));
        }
    } else if let Some(digest) = suffix.strip_prefix('@') {
        if digest.is_empty() {
            return invalid_image_ref(format!("image reference has an empty digest: {image}"));
        }
        if digest.contains('@') {
            return invalid_image_ref(format!(
                "image reference has invalid digest syntax: {image}"
            ));
        }
    }
    Ok(())
}

fn split_name_suffix(image: &str) -> (&str, &str) {
    let last_slash = image.rfind('/').map(|idx| idx + 1).unwrap_or(0);
    let leaf = &image[last_slash..];
    if let Some(at_idx) = leaf.find('@') {
        // OCI refs may include both tag and digest (`repo:tag@sha256:...`).
        // The digest is authoritative, so drop the tag and keep only `repo@digest`.
        let digest_split = last_slash + at_idx;
        let name_split = leaf[..at_idx]
            .find(':')
            .map(|tag_idx| last_slash + tag_idx)
            .unwrap_or(digest_split);
        return (&image[..name_split], &image[digest_split..]);
    }
    if let Some(idx) = leaf.find(':') {
        let split = last_slash + idx;
        return (&image[..split], &image[split..]);
    }
    (image, ":latest")
}

#[cfg(test)]
mod tests {
    use super::{image_ref_candidates, pulled_image_tag};

    #[test]
    fn pulled_image_tags_name_images_by_their_full_reference() {
        let tag = |candidate: &str| pulled_image_tag(candidate).map(|t| t.to_string());
        for candidate in [
            "docker.io/library/python:3.12",
            "registry.example.com/base/ubuntu:latest",
            "ghcr.io/team/app:v1",
        ] {
            assert_eq!(tag(candidate).as_deref(), Some(candidate));
        }
        let long = format!("docker.io/{}:v1", "a".repeat(241));
        for candidate in [
            "docker.io/library/python@sha256:abc",
            "localhost:5000/app:v1",
            "docker.io/Team/app:v1",
            long.as_str(),
        ] {
            assert_eq!(tag(candidate), None, "{candidate} should not be published");
        }
    }

    fn registries(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    fn candidates(input: &str, search: &[&str]) -> super::ImageResult<Vec<String>> {
        image_ref_candidates(input, &registries(search), None)
    }

    #[test]
    fn image_ref_candidates_searches_unqualified_shortnames_in_order() {
        assert_eq!(
            candidates("ubuntu:24.04", &["docker.io", "ghcr.io"]).unwrap(),
            vec![
                "docker.io/library/ubuntu:24.04".to_string(),
                "ghcr.io/ubuntu:24.04".to_string(),
            ]
        );
    }

    #[test]
    fn image_ref_candidates_preserves_namespace_for_unqualified_refs() {
        assert_eq!(
            candidates("e2b/code-interpreter:latest", &["docker.io", "ghcr.io"]).unwrap(),
            vec![
                "docker.io/e2b/code-interpreter:latest".to_string(),
                "ghcr.io/e2b/code-interpreter:latest".to_string(),
            ]
        );
    }

    #[test]
    fn image_ref_candidates_skips_search_for_fully_qualified_refs() {
        assert_eq!(
            candidates(
                "registry.internal:5000/team/app@sha256:abc",
                &["docker.io", "ghcr.io"]
            )
            .unwrap(),
            vec!["registry.internal:5000/team/app@sha256:abc".to_string()]
        );
    }

    #[test]
    fn image_ref_candidates_strips_tag_when_digest_is_present() {
        assert_eq!(
            candidates(
                "registry.internal:5000/team/app:v1@sha256:abc",
                &["docker.io", "ghcr.io"]
            )
            .unwrap(),
            vec!["registry.internal:5000/team/app@sha256:abc".to_string()]
        );
        assert_eq!(
            candidates("ubuntu:24.04@sha256:abc", &["docker.io"]).unwrap(),
            vec!["docker.io/library/ubuntu@sha256:abc".to_string()]
        );
    }

    #[test]
    fn image_ref_candidates_supports_registry_namespace_prefixes() {
        assert_eq!(
            candidates("ubuntu", &["registry.example.com/base"]).unwrap(),
            vec!["registry.example.com/base/ubuntu:latest".to_string()]
        );
    }

    #[test]
    fn image_ref_candidates_deduplicates_candidates_without_changing_order() {
        assert_eq!(
            candidates("ubuntu", &["docker.io", "docker.io", "ghcr.io"]).unwrap(),
            vec![
                "docker.io/library/ubuntu:latest".to_string(),
                "ghcr.io/ubuntu:latest".to_string(),
            ]
        );
    }

    #[test]
    fn image_ref_candidates_rejects_search_prefix_without_registry_host() {
        let err =
            candidates("ubuntu", &["myregistry"]).expect_err("hostless search prefix should fail");

        assert!(err.to_string().contains("registry host"));
    }

    #[test]
    fn image_ref_candidates_allows_fully_qualified_ref_in_allowlist() {
        let allowed = registries(&["registry.example.com", "ghcr.io"]);
        assert_eq!(
            image_ref_candidates("registry.example.com/team/app:tag", &[], Some(&allowed)).unwrap(),
            vec!["registry.example.com/team/app:tag".to_string()]
        );
    }

    #[test]
    fn image_ref_candidates_rejects_fully_qualified_ref_not_in_allowlist() {
        let allowed = registries(&["registry.example.com"]);
        let err = image_ref_candidates("ghcr.io/team/app:tag", &[], Some(&allowed))
            .expect_err("host outside allowlist should be rejected");

        assert!(err.to_string().contains("not from an allowed registry"));
    }

    #[test]
    fn image_ref_candidates_filters_shortname_expansions_to_allowlist() {
        let allowed = registries(&["ghcr.io"]);
        // docker.io candidate is filtered out; only the ghcr.io one survives.
        assert_eq!(
            image_ref_candidates(
                "ubuntu",
                &registries(&["docker.io", "ghcr.io"]),
                Some(&allowed)
            )
            .unwrap(),
            vec!["ghcr.io/ubuntu:latest".to_string()]
        );
    }

    #[test]
    fn image_ref_candidates_rejects_shortname_when_no_candidate_is_allowed() {
        let allowed = registries(&["registry.example.com"]);
        let err = image_ref_candidates(
            "ubuntu",
            &registries(&["docker.io", "ghcr.io"]),
            Some(&allowed),
        )
        .expect_err("no candidate in allowlist should be rejected");

        assert!(err.to_string().contains("not from an allowed registry"));
    }

    #[test]
    fn image_ref_candidates_empty_allowlist_rejects_every_registry() {
        // An explicit empty list is a meaningful "deny all", distinct from None.
        let err = image_ref_candidates("ghcr.io/team/app:tag", &[], Some(&[]))
            .expect_err("empty allowlist should reject all hosts");

        assert!(err.to_string().contains("not from an allowed registry"));
    }

    #[test]
    fn image_ref_candidates_unset_allowlist_imposes_no_restriction() {
        assert_eq!(
            image_ref_candidates("ghcr.io/team/app:tag", &[], None).unwrap(),
            vec!["ghcr.io/team/app:tag".to_string()]
        );
    }
}
