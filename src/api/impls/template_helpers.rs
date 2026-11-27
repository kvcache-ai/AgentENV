use agentenv_http_server::models;

use super::error::ApiError;
use crate::cfg::ConfigManager;
use crate::snapshot::{BuildTarget, SnapshotAlias};
use crate::template::{TemplateBuildBaseSource, TemplateBuildSpec};
use crate::types::SandboxResources;

fn resolve_resources(
    body: &models::TemplateBuildRequestV3,
) -> std::result::Result<SandboxResources, ApiError> {
    let config = ConfigManager::global_config();
    let cpu_count = body.cpu_count.unwrap_or(config.machine.vcpu_count);
    let memory_mib = body.memory_mb.unwrap_or(config.machine.mem_size_mib);
    if cpu_count == 0 || memory_mib == 0 {
        return Err(ApiError::bad_request(
            "cpuCount and memoryMB must be greater than 0",
        ));
    }
    Ok(SandboxResources {
        cpu_count,
        memory_mib,
        disk_size_mib: 0,
    })
}

pub(super) fn template_build_target_from_v3_request(
    body: &models::TemplateBuildRequestV3,
    alias: &str,
) -> Result<BuildTarget, ApiError> {
    if alias.contains(':') {
        return Err(ApiError::bad_request(
            "template name tags are not supported yet",
        ));
    }
    if body.tags.as_ref().is_some_and(|tags| !tags.is_empty()) {
        return Err(ApiError::bad_request("template tags are not supported yet"));
    }

    let alias =
        SnapshotAlias::parse(alias).map_err(|err| ApiError::bad_request(err.to_string()))?;
    let resources = resolve_resources(body)?;

    Ok(BuildTarget::Template {
        alias: Some(alias),
        resources,
    })
}

pub(super) fn template_build_spec_from_start_request(
    body: &models::TemplateBuildStartV2,
    alias: Option<&SnapshotAlias>,
    resources: SandboxResources,
) -> Result<TemplateBuildSpec, ApiError> {
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

pub(super) fn template_registry_auth(
    body: &models::TemplateBuildStartV2,
) -> Result<Option<crate::image::oci_image::RegistryAuth>, ApiError> {
    use crate::image::oci_image::RegistryAuth;
    let Some(registry) = body.from_image_registry.as_ref() else {
        return Ok(None);
    };
    if body
        .from_image
        .as_deref()
        .is_none_or(|image| image.trim().is_empty())
        || body
            .from_template
            .as_deref()
            .is_some_and(|template| !template.trim().is_empty())
    {
        return Err(ApiError::bad_request(
            "fromImageRegistry requires fromImage and cannot be used with fromTemplate",
        ));
    }
    let auth = match registry {
        models::FromImageRegistry::GeneralRegistry(value) => RegistryAuth::General {
            username: value.username.clone(),
            password: value.password.clone(),
        },
        models::FromImageRegistry::GcpRegistry(value) => RegistryAuth::Gcp {
            service_account_json: value.service_account_json.clone(),
        },
        models::FromImageRegistry::AwsRegistry(value) => RegistryAuth::Aws {
            access_key_id: value.aws_access_key_id.clone(),
            secret_access_key: value.aws_secret_access_key.clone(),
            region: value.aws_region.clone(),
        },
    };
    auth.validate()
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
    Ok(Some(auth))
}

pub(super) fn template_build_start_base_source(
    body: &models::TemplateBuildStartV2,
) -> Result<TemplateBuildBaseSource, ApiError> {
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
        (Some(_), Some(_)) => Err(ApiError::bad_request(
            "cannot specify both fromImage and fromTemplate",
        )),
        (Some(image), None) => Ok(TemplateBuildBaseSource::Image(image.to_string())),
        (None, Some(template)) => SnapshotAlias::parse(template)
            .map(TemplateBuildBaseSource::Template)
            .map_err(|err| ApiError::bad_request(err.to_string())),
        (None, None) => Ok(TemplateBuildBaseSource::DefaultImage),
    }
}

fn apply_e2b_template_step(
    mut spec: TemplateBuildSpec,
    step: &models::TemplateStep,
) -> Result<TemplateBuildSpec, ApiError> {
    if step
        .files_hash
        .as_ref()
        .is_some_and(|hash| !hash.trim().is_empty())
    {
        return Err(ApiError::bad_request(
            "template build filesHash/COPY support is not implemented yet",
        ));
    }

    let args = step.args.as_deref().unwrap_or_default();
    match step.r_type.to_ascii_uppercase().as_str() {
        "RUN" => {
            let Some(cmd) = args.first().filter(|cmd| !cmd.trim().is_empty()) else {
                return Err(ApiError::bad_request(
                    "RUN template step requires a command argument",
                ));
            };
            spec = spec.run(cmd.clone());
        }
        "ENV" | "ARG" => {
            if args.is_empty() || !args.len().is_multiple_of(2) {
                return Err(ApiError::bad_request(format!(
                    "{} template step requires key/value arguments",
                    step.r_type
                )));
            }
            for pair in args.chunks(2) {
                let key = pair[0].trim();
                if key.is_empty() {
                    return Err(ApiError::bad_request(format!(
                        "{} template step requires a non-empty key",
                        step.r_type
                    )));
                }
                spec = spec.env(key.to_string(), pair[1].clone());
            }
        }
        "WORKDIR" => {
            let Some(path) = args.first().filter(|path| !path.trim().is_empty()) else {
                return Err(ApiError::bad_request(
                    "WORKDIR template step requires a path argument",
                ));
            };
            spec = spec.workdir(path.clone());
        }
        "USER" => {
            let Some(value) = args.first().filter(|v| !v.trim().is_empty()) else {
                return Err(ApiError::bad_request("USER step requires a value"));
            };
            spec = spec.user(value.clone());
        }
        "EXPOSE" => {
            let Some(port) = args.first().filter(|p| !p.trim().is_empty()) else {
                return Err(ApiError::bad_request(
                    "EXPOSE step requires a port argument",
                ));
            };
            spec = spec.exposed_port(port.clone());
        }
        "VOLUME" => {
            let Some(path) = args.first().filter(|p| !p.trim().is_empty()) else {
                return Err(ApiError::bad_request(
                    "VOLUME step requires a path argument",
                ));
            };
            spec = spec.volume(path.clone());
        }
        "LABEL" => {
            if args.len() < 2 || !args.len().is_multiple_of(2) {
                return Err(ApiError::bad_request(
                    "LABEL step requires key/value arguments",
                ));
            }
            for pair in args.chunks(2) {
                let key = pair[0].trim();
                if key.is_empty() {
                    return Err(ApiError::bad_request("LABEL step requires a non-empty key"));
                }
                spec = spec.label(key.to_string(), pair[1].clone());
            }
        }
        "COPY" | "ADD" => {
            return Err(ApiError::bad_request(format!(
                "{} template steps are not supported yet",
                step.r_type
            )));
        }
        other => {
            return Err(ApiError::bad_request(format!(
                "template step type {other} is not supported"
            )));
        }
    }

    Ok(spec)
}

#[cfg(test)]
mod tests {
    use super::{
        template_build_spec_from_start_request, template_build_start_base_source,
        template_registry_auth,
    };
    use crate::template::TemplateBuildBaseSource;
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
        assert_eq!(source, TemplateBuildBaseSource::DefaultImage);
    }

    #[test]
    fn start_base_source_rejects_image_and_template_together() {
        let mut body = models::TemplateBuildStartV2::new();
        body.from_image = Some("ubuntu:24.04".to_string());
        body.from_template = Some("base-template".to_string());

        let err = template_build_start_base_source(&body).expect_err("source should fail");
        assert_eq!(err.status_code(), 400);
        assert_eq!(
            err.message(),
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
            TemplateBuildBaseSource::Template(
                crate::snapshot::SnapshotAlias::parse("base-template").expect("alias should parse")
            )
        );
    }

    #[test]
    fn registry_auth_wire_types_round_trip_and_validate() {
        for registry in [
            serde_json::json!({"type":"registry", "username":"reader", "password":"secret"}),
            serde_json::json!({"type":"aws", "awsAccessKeyId":"key", "awsSecretAccessKey":"secret", "awsRegion":"us-east-1"}),
            serde_json::json!({"type":"gcp", "serviceAccountJson":"{\"type\":\"service_account\"}"}),
        ] {
            let request = serde_json::json!({"fromImage":"registry.example/team/base:v1", "fromImageRegistry":registry});
            let body: models::TemplateBuildStartV2 =
                serde_json::from_value(request.clone()).unwrap();
            assert!(template_registry_auth(&body).unwrap().is_some());
            assert_eq!(serde_json::to_value(body).unwrap(), request);
        }
    }

    #[test]
    fn registry_auth_rejects_invalid_sources_and_credentials() {
        for source in [
            serde_json::json!({}),
            serde_json::json!({"fromImage":" "}),
            serde_json::json!({"fromTemplate":"base"}),
        ] {
            let mut request = source;
            request["fromImageRegistry"] =
                serde_json::json!({"type":"registry", "username":"reader", "password":"secret"});
            let body = serde_json::from_value(request).unwrap();
            assert_eq!(
                template_registry_auth(&body).err().unwrap().status_code(),
                400
            );
        }
        for registry in [
            serde_json::json!({"type":"registry", "username":"reader", "password":""}),
            serde_json::json!({"type":"aws", "awsAccessKeyId":"key", "awsSecretAccessKey":"secret", "awsRegion":"bad/region"}),
            serde_json::json!({"type":"gcp", "serviceAccountJson":"not-json-secret"}),
        ] {
            let body = serde_json::from_value(serde_json::json!({"fromImage":"registry.example/base", "fromImageRegistry":registry})).unwrap();
            let error = template_registry_auth(&body).err().unwrap();
            assert_eq!(error.status_code(), 400);
            assert!(!error.message().contains("secret"));
        }
        for registry in [
            serde_json::json!({"type":"unknown", "username":"reader", "password":"secret"}),
            serde_json::json!({"username":"reader", "password":"secret"}),
            serde_json::json!({"type":"registry", "username":"reader"}),
        ] {
            assert!(serde_json::from_value::<models::TemplateBuildStartV2>(serde_json::json!({"fromImage":"registry.example/base", "fromImageRegistry":registry})).is_err());
        }
        assert!(template_registry_auth(&models::TemplateBuildStartV2::new())
            .unwrap()
            .is_none());
    }
}
