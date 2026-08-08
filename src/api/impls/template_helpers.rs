use agentenv_http_server::models;

use crate::cfg::ConfigManager;
use crate::snapshot::repository::build_files::is_valid_build_files_hash;
use crate::snapshot::{SnapshotAlias, SnapshotId, SnapshotRecord};
use crate::template::TemplateBuildSpec;
use crate::types::SandboxResources;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum TemplateBuildStartBaseSource {
    DefaultImage,
    Image(String),
    Template(SnapshotAlias),
}

fn resolve_resources(
    body: &models::TemplateBuildRequestV3,
) -> std::result::Result<SandboxResources, models::Error> {
    let config = ConfigManager::global_config();
    let cpu_count = body.cpu_count.unwrap_or(config.machine.vcpu_count);
    let memory_mib = body.memory_mb.unwrap_or(config.machine.mem_size_mib);
    if cpu_count == 0 || memory_mib == 0 {
        return Err(models::Error::new(
            400,
            "cpuCount and memoryMB must be greater than 0".to_string(),
        ));
    }
    Ok(SandboxResources {
        cpu_count,
        memory_mib,
        disk_size_mib: 0,
    })
}

pub(super) fn template_build_record_from_v3_request(
    body: &models::TemplateBuildRequestV3,
    id: SnapshotId,
    alias: &str,
) -> Result<SnapshotRecord, models::Error> {
    if alias.contains(':') {
        return Err(models::Error::new(
            400,
            "template name tags are not supported yet".to_string(),
        ));
    }
    if body.tags.as_ref().is_some_and(|tags| !tags.is_empty()) {
        return Err(models::Error::new(
            400,
            "template tags are not supported yet".to_string(),
        ));
    }

    let alias =
        SnapshotAlias::parse(alias).map_err(|err| models::Error::new(400, err.to_string()))?;
    let resources = resolve_resources(body)?;

    Ok(SnapshotRecord::template_waiting(id, Some(alias), resources))
}

pub(super) fn template_build_spec_from_start_request(
    body: &models::TemplateBuildStartV2,
    alias: Option<&SnapshotAlias>,
    resources: SandboxResources,
) -> Result<TemplateBuildSpec, models::Error> {
    let mut spec = TemplateBuildSpec::new().resources(resources.cpu_count, resources.memory_mib);
    if let Some(alias) = alias {
        spec = spec.alias(alias.to_string());
    }
    if let Some(start_cmd) = body.start_cmd.as_ref() {
        spec = spec.start_cmd(start_cmd.clone());
    }
    if let Some(ready_cmd) = body.ready_cmd.as_ref() {
        spec = spec.ready_cmd(ready_cmd.clone());
    }

    for step in body.steps.as_deref().unwrap_or_default() {
        spec = apply_e2b_template_step(spec, step)?;
    }

    Ok(spec)
}

pub(super) fn template_build_start_base_source(
    body: &models::TemplateBuildStartV2,
) -> Result<TemplateBuildStartBaseSource, models::Error> {
    let from_image = body
        .from_image
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let from_template = body
        .from_template
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());

    match (from_image, from_template) {
        (Some(_), Some(_)) => Err(models::Error::new(
            400,
            "cannot specify both fromImage and fromTemplate".to_string(),
        )),
        (Some(image), None) => Ok(TemplateBuildStartBaseSource::Image(image.to_string())),
        (None, Some(template)) => SnapshotAlias::parse(template)
            .map(TemplateBuildStartBaseSource::Template)
            .map_err(|err| models::Error::new(400, err.to_string())),
        (None, None) => Ok(TemplateBuildStartBaseSource::DefaultImage),
    }
}

/// Upper bound on a COPY/ADD source pattern. The pattern is matched against
/// every entry name of an uploaded archive, so its length directly bounds the
/// per-entry matching cost.
const MAX_COPY_SRC_BYTES: usize = 4096;

fn apply_e2b_template_step(
    mut spec: TemplateBuildSpec,
    step: &models::TemplateStep,
) -> Result<TemplateBuildSpec, models::Error> {
    let args = step.args.as_deref().unwrap_or_default();
    let step_type = step.r_type.to_ascii_uppercase();
    let carries_files_hash = step
        .files_hash
        .as_deref()
        .map(str::trim)
        .is_some_and(|hash| !hash.is_empty());
    if carries_files_hash && !matches!(step_type.as_str(), "COPY" | "ADD") {
        return Err(models::Error::new(
            400,
            format!(
                "{} template step must not carry a filesHash; only COPY and ADD consume build context archives",
                step.r_type
            ),
        ));
    }
    match step_type.as_str() {
        "RUN" => {
            let Some(cmd) = args.first().filter(|cmd| !cmd.trim().is_empty()) else {
                return Err(models::Error::new(
                    400,
                    "RUN template step requires a command argument".to_string(),
                ));
            };
            spec = spec.run(cmd.clone());
        }
        "ENV" | "ARG" => {
            if args.is_empty() || !args.len().is_multiple_of(2) {
                return Err(models::Error::new(
                    400,
                    format!("{} template step requires key/value arguments", step.r_type),
                ));
            }
            for pair in args.chunks(2) {
                let key = pair[0].trim();
                if key.is_empty() {
                    return Err(models::Error::new(
                        400,
                        format!("{} template step requires a non-empty key", step.r_type),
                    ));
                }
                spec = spec.env(key.to_string(), pair[1].clone());
            }
        }
        "WORKDIR" => {
            let Some(path) = args.first().filter(|path| !path.trim().is_empty()) else {
                return Err(models::Error::new(
                    400,
                    "WORKDIR template step requires a path argument".to_string(),
                ));
            };
            spec = spec.workdir(path.clone());
        }
        "USER" => {
            let Some(value) = args.first().filter(|v| !v.trim().is_empty()) else {
                return Err(models::Error::new(
                    400,
                    "USER step requires a value".to_string(),
                ));
            };
            spec = spec.user(value.clone());
        }
        "EXPOSE" => {
            let Some(port) = args.first().filter(|p| !p.trim().is_empty()) else {
                return Err(models::Error::new(
                    400,
                    "EXPOSE step requires a port argument".to_string(),
                ));
            };
            spec = spec.exposed_port(port.clone());
        }
        "VOLUME" => {
            let Some(path) = args.first().filter(|p| !p.trim().is_empty()) else {
                return Err(models::Error::new(
                    400,
                    "VOLUME step requires a path argument".to_string(),
                ));
            };
            spec = spec.volume(path.clone());
        }
        "LABEL" => {
            if args.len() < 2 || !args.len().is_multiple_of(2) {
                return Err(models::Error::new(
                    400,
                    "LABEL step requires key/value arguments".to_string(),
                ));
            }
            for pair in args.chunks(2) {
                let key = pair[0].trim();
                if key.is_empty() {
                    return Err(models::Error::new(
                        400,
                        "LABEL step requires a non-empty key".to_string(),
                    ));
                }
                spec = spec.label(key.to_string(), pair[1].clone());
            }
        }
        // The E2B SDK resolves ADD like COPY client-side (local files only)
        // and sends both with a filesHash referencing the uploaded archive.
        "COPY" | "ADD" => {
            let Some(files_hash) = step
                .files_hash
                .as_deref()
                .map(str::trim)
                .filter(|hash| !hash.is_empty())
            else {
                return Err(models::Error::new(
                    400,
                    format!(
                        "{} template steps require a filesHash referencing an uploaded build context archive",
                        step.r_type
                    ),
                ));
            };
            // The hash is an opaque cache key that becomes part of an archive
            // path, so it must be shape-checked before the build starts.
            if !is_valid_build_files_hash(files_hash) {
                return Err(models::Error::new(
                    400,
                    format!(
                        "{} template step filesHash '{files_hash}' is not a valid build files hash",
                        step.r_type
                    ),
                ));
            }
            let src = args
                .first()
                .map(|value| value.trim())
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    models::Error::new(
                        400,
                        format!("{} template step requires a source argument", step.r_type),
                    )
                })?;
            if src.len() > MAX_COPY_SRC_BYTES {
                return Err(models::Error::new(
                    400,
                    format!(
                        "{} template step source argument exceeds {MAX_COPY_SRC_BYTES} bytes",
                        step.r_type
                    ),
                ));
            }
            let dest = args
                .get(1)
                .map(|value| value.trim())
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    models::Error::new(
                        400,
                        format!(
                            "{} template step requires a destination argument",
                            step.r_type
                        ),
                    )
                })?;
            let user = args
                .get(2)
                .map(|value| value.trim())
                .filter(|value| !value.is_empty())
                .map(str::to_string);
            let mode = args
                .get(3)
                .map(|value| value.trim())
                .filter(|value| !value.is_empty())
                .map(|value| {
                    let mode = u32::from_str_radix(value, 8).map_err(|_| {
                        models::Error::new(
                            400,
                            format!(
                                "{} template step mode '{value}' is not a valid octal mode",
                                step.r_type
                            ),
                        )
                    })?;
                    if mode > 0o7777 {
                        return Err(models::Error::new(
                            400,
                            format!(
                                "{} template step mode '{value}' exceeds the maximum octal mode 7777",
                                step.r_type
                            ),
                        ));
                    }
                    Ok(mode)
                })
                .transpose()?;
            spec = spec.copy(src, dest, files_hash, user, mode);
        }
        other => {
            return Err(models::Error::new(
                400,
                format!("template step type {other} is not supported"),
            ));
        }
    }

    Ok(spec)
}

#[cfg(test)]
mod tests {
    use super::{
        apply_e2b_template_step, template_build_start_base_source, TemplateBuildStartBaseSource,
        MAX_COPY_SRC_BYTES,
    };
    use crate::template::TemplateBuildSpec;
    use agentenv_http_server::models;

    const HASH: &str = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08";

    fn step(r_type: &str, args: &[&str]) -> models::TemplateStep {
        let mut step = models::TemplateStep::new(r_type.to_string());
        step.args = Some(args.iter().map(|arg| (*arg).to_string()).collect());
        step
    }

    fn copy_step(args: &[&str]) -> models::TemplateStep {
        let mut step = step("COPY", args);
        step.files_hash = Some(HASH.to_string());
        step
    }

    fn apply(step: &models::TemplateStep) -> Result<TemplateBuildSpec, models::Error> {
        apply_e2b_template_step(TemplateBuildSpec::new(), step)
    }

    #[test]
    fn start_base_source_defaults_when_not_specified() {
        let body = models::TemplateBuildStartV2::new();
        let source = template_build_start_base_source(&body).expect("source should parse");
        assert_eq!(source, TemplateBuildStartBaseSource::DefaultImage);
    }

    #[test]
    fn start_base_source_rejects_image_and_template_together() {
        let mut body = models::TemplateBuildStartV2::new();
        body.from_image = Some("ubuntu:24.04".to_string());
        body.from_template = Some("base-template".to_string());

        let err = template_build_start_base_source(&body).expect_err("source should fail");
        assert_eq!(err.code, 400);
        assert_eq!(
            err.message,
            "cannot specify both fromImage and fromTemplate"
        );
    }

    #[test]
    fn start_base_source_parses_template_alias() {
        let mut body = models::TemplateBuildStartV2::new();
        body.from_template = Some("base-template".to_string());

        let source = template_build_start_base_source(&body).expect("source should parse");
        assert_eq!(
            source,
            TemplateBuildStartBaseSource::Template(
                crate::snapshot::SnapshotAlias::parse("base-template").expect("alias should parse")
            )
        );
    }

    #[test]
    fn step_type_is_matched_case_insensitively() {
        apply(&step("workdir", &["/app"])).expect("lowercase step type should apply");
    }

    #[test]
    fn files_hash_outside_copy_and_add_is_rejected() {
        let mut run = step("RUN", &["echo hi"]);
        run.files_hash = Some(HASH.to_string());

        let err = apply(&run).expect_err("a RUN step must not carry a filesHash");
        assert_eq!(err.code, 400);
        assert!(err.message.contains("filesHash"), "{}", err.message);

        // A blank filesHash stays acceptable: the SDK omits it as an empty
        // string for non-COPY steps.
        run.files_hash = Some("  ".to_string());
        apply(&run).expect("a blank filesHash should be ignored");
    }

    #[test]
    fn copy_step_rejects_out_of_range_mode() {
        apply(&copy_step(&["src", "/dest", "", "0755"])).expect("a valid mode should apply");

        let err = apply(&copy_step(&["src", "/dest", "", "10000"]))
            .expect_err("a mode above 7777 should be rejected");
        assert_eq!(err.code, 400);
        assert!(err.message.contains("10000"), "{}", err.message);
    }

    #[test]
    fn copy_step_rejects_a_malformed_files_hash() {
        let mut copy = copy_step(&["src", "/dest"]);
        copy.files_hash = Some("../../etc/passwd".to_string());

        let err = apply(&copy).expect_err("a malformed filesHash should be rejected");
        assert_eq!(err.code, 400);
        assert!(err.message.contains("../../etc/passwd"), "{}", err.message);
    }

    #[test]
    fn copy_step_rejects_an_oversized_source_pattern() {
        let oversized = "a".repeat(MAX_COPY_SRC_BYTES + 1);
        let err = apply(&copy_step(&[oversized.as_str(), "/dest"]))
            .expect_err("an oversized source should be rejected");
        assert_eq!(err.code, 400);
        assert!(
            err.message.contains(&MAX_COPY_SRC_BYTES.to_string()),
            "{}",
            err.message
        );
    }
}
