use agentenv_http_server::models;

use crate::cfg::ConfigManager;
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

    for (index, step) in body.steps.as_deref().unwrap_or_default().iter().enumerate() {
        let appended_from = spec.step_count();
        spec = apply_e2b_template_step(spec, step)?;
        spec = spec.stamp_source_steps(appended_from, index + 1);
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

fn apply_e2b_template_step(
    mut spec: TemplateBuildSpec,
    step: &models::TemplateStep,
) -> Result<TemplateBuildSpec, models::Error> {
    if step
        .files_hash
        .as_ref()
        .is_some_and(|hash| !hash.trim().is_empty())
    {
        return Err(models::Error::new(
            400,
            "template build filesHash/COPY support is not implemented yet".to_string(),
        ));
    }

    let args = step.args.as_deref().unwrap_or_default();
    match step.r_type.to_ascii_uppercase().as_str() {
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
        "COPY" | "ADD" => {
            return Err(models::Error::new(
                400,
                format!("{} template steps are not supported yet", step.r_type),
            ));
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
        template_build_spec_from_start_request, template_build_start_base_source,
        TemplateBuildStartBaseSource,
    };
    use crate::types::SandboxResources;
    use agentenv_http_server::models;

    #[test]
    fn expanded_env_pairs_share_their_client_step_number() {
        let mut body = models::TemplateBuildStartV2::new();
        body.steps = Some(vec![
            models::TemplateStep {
                r_type: "ENV".to_string(),
                args: Some(vec![
                    "A".to_string(),
                    "1".to_string(),
                    "B".to_string(),
                    "2".to_string(),
                ]),
                files_hash: None,
                force: None,
            },
            models::TemplateStep {
                r_type: "RUN".to_string(),
                args: Some(vec!["false".to_string()]),
                files_hash: None,
                force: None,
            },
        ]);
        let spec = template_build_spec_from_start_request(&body, None, SandboxResources::default())
            .expect("spec should parse");
        let sources: Vec<Option<usize>> =
            spec.steps().iter().map(|step| step.source_step).collect();
        // One ENV request with two pairs expands to two internal steps; both
        // must report as client step 1, and the RUN as client step 2.
        assert_eq!(sources, vec![Some(1), Some(1), Some(2)]);
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
}
