use std::path::{Path, PathBuf};
use std::process::Stdio;

use anyhow::{Context, Result};
use serde::Serialize;
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tracing::{debug, info};
use uuid::Uuid;

/// Handle for invoking overlaybd CLI tools.
pub struct OverlaybdTools {
    create_bin: PathBuf,
    apply_bin: PathBuf,
    commit_bin: PathBuf,
}

pub(crate) struct CreateLayerRequest {
    pub data_file: PathBuf,
    pub index_file: PathBuf,
    /// Virtual size in GiB – passed directly to `overlaybd-create`.
    pub virtual_size_gib: u64,
}

struct ApplyLayerRequest {
    input_layer_path: PathBuf,
    image_config_path: PathBuf,
    global_config_path: PathBuf,
    mkfs: bool,
}

pub(crate) struct CommitLayerRequest {
    pub data_file: PathBuf,
    pub index_file: PathBuf,
    pub commit_file: PathBuf,
    pub uuid: Option<Uuid>,
    pub parent_uuid: Option<Uuid>,
}

pub struct ConvertLayerRequest {
    pub work_dir: PathBuf,
    pub input_layer_path: PathBuf,
    pub global_config_path: PathBuf,
    pub virtual_size_gib: u64,
    pub mkfs: bool,
    pub lower_layers: Vec<PathBuf>,
    pub uuid: Option<Uuid>,
    pub parent_uuid: Option<Uuid>,
}

#[derive(Debug)]
pub struct ConvertLayerResult {
    /// Final sealed overlaybd lower layer produced by the conversion.
    pub layer_commit: PathBuf,
}

#[derive(Debug)]
pub(crate) struct StepSummary {
    pub stdout: String,
    pub stderr: String,
}

#[derive(Debug, thiserror::Error)]
pub enum ToolExecutionError {
    #[error("overlaybd tool binary not found at {path}")]
    MissingBinary { path: PathBuf },
    #[error("{reason}")]
    InvalidRequest { reason: String },
    #[error("tool failed: {command_line}\nexit code: {exit_code:?}\nstderr: {stderr}")]
    NonZeroExit {
        command_line: String,
        exit_code: Option<i32>,
        stdout: String,
        stderr: String,
    },
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

const UPPER_DATA: &str = "upper.data";
const UPPER_INDEX: &str = "upper.index";
const IMAGE_JSON: &str = "image.json";
const LAYER_COMMIT: &str = "layer.commit";
const RESULT_TXT: &str = "result.txt";

impl OverlaybdTools {
    pub fn from_bin_dir(bin_dir: impl Into<PathBuf>) -> Self {
        let bin_dir = bin_dir.into();
        Self {
            create_bin: bin_dir.join("overlaybd-create"),
            apply_bin: bin_dir.join("overlaybd-apply"),
            commit_bin: bin_dir.join("overlaybd-commit"),
        }
    }

    pub fn from_overlaybd_install_root(root: impl Into<PathBuf>) -> Self {
        let root = root.into();
        Self::from_bin_dir(root.join("bin"))
    }

    pub(crate) async fn create_writable_layer(
        &self,
        req: &CreateLayerRequest,
        work_dir: &Path,
    ) -> Result<StepSummary, ToolExecutionError> {
        self.ensure_binary_exists(&self.create_bin)?;
        let mut cmd = self.base_command(&self.create_bin, work_dir);
        cmd.arg(&req.data_file)
            .arg(&req.index_file)
            .arg(req.virtual_size_gib.to_string());
        run(cmd).await
    }

    async fn apply_oci_layer(
        &self,
        req: &ApplyLayerRequest,
        work_dir: &Path,
    ) -> Result<StepSummary, ToolExecutionError> {
        self.ensure_binary_exists(&self.apply_bin)?;

        // Overlaybd-apply as of 1.0.16 does not support decompressing
        // zstd-compressed tar layers. The support landed in master:
        // https://github.com/containerd/overlaybd/commit/222aa97867b314bb9f466ca69d4f70fa8dcb449b
        // however, the newest development build (as of 2026.05.21) have other issues (segmentation fault).
        // As a workaround, when the input layer is zstd-compressed, we pipe it
        // through `zstd -d` first so overlaybd-apply always receives a plain
        // tar stream. Detect this from the file magic instead of trusting the
        // OCI mediaType: some builders/registries mislabel zstd layers as
        // gzip (for example Docker mediaType `rootfs.diff.tar.gzip` with a
        // `28 b5 2f fd` zstd frame). This is a temporary measure — once
        // overlaybd-apply reliably handles tar+zstd natively this branch can be
        // removed.
        let is_zstd = layer_uses_zstd_decompression(&req.input_layer_path).await?;
        let zstd_process = if is_zstd {
            debug!(
                input = %req.input_layer_path.display(),
                "input layer is zstd-compressed; piping through `zstd -d`"
            );
            let mut zstd_cmd = Command::new("zstd");
            zstd_cmd
                .arg("-dc")
                .arg(&req.input_layer_path)
                .stdout(Stdio::piped())
                .stderr(Stdio::piped());
            let command_line = format!("{zstd_cmd:?}");
            let child = zstd_cmd.spawn().map_err(|e| {
                ToolExecutionError::Other(
                    anyhow::Error::new(e).context("spawn `zstd -d` for layer decompression"),
                )
            })?;
            Some((command_line, child))
        } else {
            None
        };

        let mut cmd = self.base_command(&self.apply_bin, work_dir);
        cmd.arg(format!(
            "--service_config_path={}",
            req.global_config_path.display()
        ));
        if req.mkfs {
            cmd.arg("--mkfs");
        }

        let zstd_process = if let Some((zstd_command_line, mut zstd_child)) = zstd_process {
            let zstd_stdout: Stdio = zstd_child
                .stdout
                .take()
                .expect("zstd child stdout must be piped")
                .try_into()
                .map_err(|e| {
                    ToolExecutionError::Other(
                        anyhow::Error::new(e).context("convert zstd child stdout to stdin"),
                    )
                })?;
            let zstd_stderr = zstd_child.stderr.take().map(|mut stderr| {
                tokio::spawn(async move {
                    let mut buffer = Vec::new();
                    stderr.read_to_end(&mut buffer).await.map(|_| buffer)
                })
            });
            cmd.arg("/dev/stdin").stdin(zstd_stdout);
            Some((zstd_command_line, zstd_child, zstd_stderr))
        } else {
            cmd.arg(&req.input_layer_path);
            None
        };
        cmd.arg(&req.image_config_path);

        let apply_result = run(cmd).await;

        if let Some((zstd_command_line, mut zstd_child, zstd_stderr)) = zstd_process {
            let zstd_status = zstd_child.wait().await.map_err(|e| {
                ToolExecutionError::Other(
                    anyhow::Error::new(e)
                        .context(format!("wait for layer decompressor: {zstd_command_line}")),
                )
            })?;
            let zstd_stderr = match zstd_stderr {
                Some(task) => {
                    let buffer = task.await.map_err(|e| {
                        ToolExecutionError::Other(anyhow::Error::new(e).context(format!(
                            "join stderr reader for layer decompressor: {zstd_command_line}"
                        )))
                    })?;
                    buffer
                        .map(|buffer| String::from_utf8_lossy(&buffer).into_owned())
                        .map_err(|e| {
                            ToolExecutionError::Other(anyhow::Error::new(e).context(format!(
                                "read stderr from layer decompressor: {zstd_command_line}"
                            )))
                        })?
                }
                None => String::new(),
            };
            if !zstd_status.success() {
                return Err(ToolExecutionError::NonZeroExit {
                    command_line: zstd_command_line,
                    exit_code: zstd_status.code(),
                    stdout: String::new(),
                    stderr: zstd_stderr,
                });
            }
        }

        apply_result
    }

    pub(crate) async fn commit_writable_layer(
        &self,
        req: &CommitLayerRequest,
        work_dir: &Path,
    ) -> Result<StepSummary, ToolExecutionError> {
        self.ensure_binary_exists(&self.commit_bin)?;
        let mut cmd = self.base_command(&self.commit_bin, work_dir);
        if let Some(uuid) = req.uuid {
            cmd.arg("--uuid").arg(uuid.to_string());
        }
        if let Some(parent_uuid) = req.parent_uuid {
            cmd.arg("--parent-uuid").arg(parent_uuid.to_string());
        }
        cmd.arg(&req.data_file)
            .arg(&req.index_file)
            .arg(&req.commit_file);
        run(cmd).await
    }

    /// Convert one local OCI tar layer into a sealed overlaybd lower layer.
    ///
    /// On success, the build-time artifacts (`upper.data`, `upper.index`,
    /// `image.json`, `result.txt`) are removed and only `layer.commit` is
    /// returned to the caller. On failure, intermediate files are retained to
    /// help debugging.
    pub async fn convert_local_oci_layer_to_overlaybd(
        &self,
        req: &ConvertLayerRequest,
    ) -> Result<ConvertLayerResult, ToolExecutionError> {
        let work_dir = &req.work_dir;

        let upper_data = work_dir.join(UPPER_DATA);
        let upper_index = work_dir.join(UPPER_INDEX);
        let image_json = work_dir.join(IMAGE_JSON);
        let layer_commit = work_dir.join(LAYER_COMMIT);
        let result_txt = work_dir.join(RESULT_TXT);

        std::fs::create_dir_all(work_dir).map_err(|e| {
            ToolExecutionError::Other(
                anyhow::Error::new(e).context(format!("create work_dir {}", work_dir.display())),
            )
        })?;

        // Reject reusing a dirty work directory. The check now runs after the
        // directory is created so it applies uniformly whether `work_dir`
        // existed before or not.
        for path in [
            &upper_data,
            &upper_index,
            &image_json,
            &layer_commit,
            &result_txt,
        ] {
            if path.exists() {
                return Err(ToolExecutionError::InvalidRequest {
                    reason: format!(
                        "target file already exists: {}; provide a clean work_dir",
                        path.display()
                    ),
                });
            }
        }

        info!(work_dir = %work_dir.display(), "overlaybd-create");
        self.create_writable_layer(
            &CreateLayerRequest {
                data_file: upper_data.clone(),
                index_file: upper_index.clone(),
                virtual_size_gib: req.virtual_size_gib,
            },
            work_dir,
        )
        .await?;

        write_image_json(
            &image_json,
            &req.lower_layers,
            &upper_data,
            &upper_index,
            &result_txt,
        )?;

        info!(work_dir = %work_dir.display(), "overlaybd-apply");
        self.apply_oci_layer(
            &ApplyLayerRequest {
                input_layer_path: req.input_layer_path.clone(),
                image_config_path: image_json.clone(),
                global_config_path: req.global_config_path.clone(),
                mkfs: req.mkfs,
            },
            work_dir,
        )
        .await?;

        info!(work_dir = %work_dir.display(), "overlaybd-commit");
        self.commit_writable_layer(
            &CommitLayerRequest {
                data_file: upper_data,
                index_file: upper_index,
                commit_file: layer_commit.clone(),
                uuid: req.uuid,
                parent_uuid: req.parent_uuid,
            },
            work_dir,
        )
        .await?;

        cleanup_conversion_artifacts(&image_json, &result_txt, work_dir)?;

        Ok(ConvertLayerResult { layer_commit })
    }

    fn base_command(&self, bin: &Path, work_dir: &Path) -> Command {
        let mut cmd = Command::new(bin);
        cmd.current_dir(work_dir);
        cmd
    }

    fn ensure_binary_exists(&self, bin: &Path) -> Result<(), ToolExecutionError> {
        match std::fs::metadata(bin) {
            Ok(metadata) if metadata.is_file() => Ok(()),
            Ok(_) => Err(ToolExecutionError::MissingBinary {
                path: bin.to_path_buf(),
            }),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                Err(ToolExecutionError::MissingBinary {
                    path: bin.to_path_buf(),
                })
            }
            Err(error) => Err(ToolExecutionError::Other(
                anyhow::Error::new(error).context(format!(
                    "inspect bundled overlaybd binary {}",
                    bin.display()
                )),
            )),
        }
    }
}

async fn run(mut cmd: Command) -> Result<StepSummary, ToolExecutionError> {
    let command_line = format!("{cmd:?}");
    debug!(cmd = %command_line, "exec");

    let output = cmd.output().await.map_err(|e| {
        ToolExecutionError::Other(anyhow::Error::new(e).context(command_line.clone()))
    })?;

    let summary = StepSummary {
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
    };

    if !output.status.success() {
        return Err(ToolExecutionError::NonZeroExit {
            command_line,
            exit_code: output.status.code(),
            stdout: summary.stdout,
            stderr: summary.stderr,
        });
    }

    Ok(summary)
}

async fn layer_uses_zstd_decompression(
    input_layer_path: &Path,
) -> Result<bool, ToolExecutionError> {
    let magic = read_layer_magic(input_layer_path).await?;
    Ok(magic.is_some_and(is_zstd_magic))
}

async fn read_layer_magic(input_layer_path: &Path) -> Result<Option<[u8; 4]>, ToolExecutionError> {
    let mut file = tokio::fs::File::open(input_layer_path).await.map_err(|e| {
        ToolExecutionError::Other(anyhow::Error::new(e).context(format!(
            "open input OCI layer for magic detection {}",
            input_layer_path.display()
        )))
    })?;
    let mut magic = [0u8; 4];
    let bytes_read = file.read(&mut magic).await.map_err(|e| {
        ToolExecutionError::Other(anyhow::Error::new(e).context(format!(
            "read input OCI layer magic {}",
            input_layer_path.display()
        )))
    })?;
    Ok((bytes_read == magic.len()).then_some(magic))
}

fn is_zstd_magic(magic: [u8; 4]) -> bool {
    const ZSTD_FRAME_MAGIC: [u8; 4] = [0x28, 0xb5, 0x2f, 0xfd];
    if magic == ZSTD_FRAME_MAGIC {
        return true;
    }

    // Zstandard skippable frames have magic numbers 0x184D2A50..=0x184D2A5F
    // encoded little-endian on disk.
    let value = u32::from_le_bytes(magic);
    (0x184D2A50..=0x184D2A5F).contains(&value)
}

fn write_image_json(
    path: &Path,
    lower_layers: &[PathBuf],
    upper_data: &Path,
    upper_index: &Path,
    result_file: &Path,
) -> Result<(), ToolExecutionError> {
    #[derive(Serialize)]
    struct ImageConfig {
        lowers: Vec<Lower>,
        upper: Upper,
        #[serde(rename = "resultFile")]
        result_file: String,
    }
    #[derive(Serialize)]
    struct Lower {
        file: String,
    }
    #[derive(Serialize)]
    struct Upper {
        data: String,
        index: String,
    }

    let lowers = lower_layers
        .iter()
        .map(|path| {
            Ok(Lower {
                file: path_to_json_string(path)?,
            })
        })
        .collect::<Result<Vec<_>, ToolExecutionError>>()?;

    let config = ImageConfig {
        lowers,
        upper: Upper {
            data: path_to_json_string(upper_data)?,
            index: path_to_json_string(upper_index)?,
        },
        result_file: path_to_json_string(result_file)?,
    };

    let json = serde_json::to_string_pretty(&config).context("serialize image.json")?;
    std::fs::write(path, json)
        .with_context(|| format!("write {}", path.display()))
        .map_err(ToolExecutionError::Other)
}

fn cleanup_conversion_artifacts(
    image_json: &Path,
    result_txt: &Path,
    work_dir: &Path,
) -> Result<(), ToolExecutionError> {
    let paths = [
        (work_dir.join(UPPER_DATA), false),
        (work_dir.join(UPPER_INDEX), false),
        (image_json.to_path_buf(), false),
        (result_txt.to_path_buf(), true),
    ];

    for (path, allow_missing) in paths {
        match std::fs::remove_file(&path) {
            Ok(()) => {}
            Err(error) if allow_missing && error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(ToolExecutionError::Other(
                    anyhow::Error::new(error)
                        .context(format!("remove conversion artifact {}", path.display())),
                ));
            }
        }
    }

    Ok(())
}

fn path_to_json_string(path: &Path) -> Result<String, ToolExecutionError> {
    path.to_str()
        .map(str::to_owned)
        .ok_or_else(|| ToolExecutionError::InvalidRequest {
            reason: format!(
                "path contains non-UTF-8 bytes and cannot be serialized into overlaybd image config: {}",
                path.display()
            ),
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
    use tempfile::tempdir;

    #[test]
    fn write_image_json_empty_lowers() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("image.json");
        write_image_json(
            &path,
            &[],
            Path::new("/work/upper.data"),
            Path::new("/work/upper.index"),
            Path::new("/work/result.txt"),
        )
        .unwrap();

        let content: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(content["lowers"].as_array().unwrap().len(), 0);
        assert_eq!(content["upper"]["data"], "/work/upper.data");
        assert_eq!(content["upper"]["index"], "/work/upper.index");
        assert_eq!(content["resultFile"], "/work/result.txt");
    }

    #[test]
    fn write_image_json_multiple_lowers() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("image.json");
        write_image_json(
            &path,
            &[
                PathBuf::from("/a/layer0.commit"),
                PathBuf::from("/b/layer1.commit"),
            ],
            Path::new("/work/upper.data"),
            Path::new("/work/upper.index"),
            Path::new("/work/result.txt"),
        )
        .unwrap();

        let content: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        let lowers = content["lowers"].as_array().unwrap();
        assert_eq!(lowers.len(), 2);
        assert_eq!(lowers[0]["file"], "/a/layer0.commit");
        assert_eq!(lowers[1]["file"], "/b/layer1.commit");
    }

    #[test]
    fn convert_rejects_existing_target_files() {
        let tmp = tempdir().unwrap();
        let work_dir = tmp.path().join("work");
        std::fs::create_dir_all(&work_dir).unwrap();
        std::fs::write(work_dir.join("upper.data"), b"existing").unwrap();

        let tools = OverlaybdTools::from_bin_dir("/nonexistent");
        let req = ConvertLayerRequest {
            work_dir,
            input_layer_path: PathBuf::from("/input.tar"),
            global_config_path: PathBuf::from("/global.json"),
            virtual_size_gib: 64,
            mkfs: true,
            lower_layers: vec![],
            uuid: None,
            parent_uuid: None,
        };

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let err = rt.block_on(tools.convert_local_oci_layer_to_overlaybd(&req));
        let err = err.unwrap_err();
        assert!(err.to_string().contains("already exists"));
    }

    #[test]
    fn convert_runs_fake_tool_chain_in_order() {
        let tmp = tempdir().unwrap();
        let work_dir = tmp.path().join("work");
        let bin_dir = tmp.path().join("bin");
        let log_path = tmp.path().join("tool.log");
        fs::create_dir_all(&bin_dir).unwrap();

        for tool in ["overlaybd-create", "overlaybd-apply", "overlaybd-commit"] {
            write_script(
                &bin_dir.join(tool),
                &format!(
                    "#!/bin/sh\nprintf '%s\\n' \"$0 $*\" >> '{}'\nprintf 'LD_LIBRARY_PATH=%s\\n' \"${{LD_LIBRARY_PATH:-}}\" >> '{}'\nif [ \"$(basename \"$0\")\" = \"overlaybd-create\" ]; then\n  : > \"$1\"\n  : > \"$2\"\nfi\nif [ \"$(basename \"$0\")\" = \"overlaybd-apply\" ]; then\n  printf 'result' > result.txt\nfi\nif [ \"$(basename \"$0\")\" = \"overlaybd-commit\" ]; then\n  last=\"\"\n  for arg in \"$@\"; do last=\"$arg\"; done\n  printf 'commit' > \"$last\"\nfi\nprintf '%s stdout\\n' \"$(basename \"$0\")\"\nprintf '%s stderr\\n' \"$(basename \"$0\")\" >&2\n",
                    log_path.display(),
                    log_path.display()
                ),
            );
        }

        let input_layer_path = tmp.path().join("layer.tar");
        fs::write(&input_layer_path, b"not-zstd-layer").unwrap();

        let tools = OverlaybdTools::from_bin_dir(&bin_dir);
        let req = ConvertLayerRequest {
            work_dir: work_dir.clone(),
            input_layer_path,
            global_config_path: tmp.path().join("global.json"),
            virtual_size_gib: 64,
            mkfs: true,
            lower_layers: vec![
                PathBuf::from("/lower/a.commit"),
                PathBuf::from("/lower/b.commit"),
            ],
            uuid: Some(Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap()),
            parent_uuid: Some(Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee").unwrap()),
        };

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = rt
            .block_on(tools.convert_local_oci_layer_to_overlaybd(&req))
            .unwrap();

        let ConvertLayerResult { layer_commit } = result;
        assert_eq!(layer_commit, work_dir.join(LAYER_COMMIT));
        assert!(layer_commit.exists());
        assert!(
            !work_dir.join(UPPER_DATA).exists(),
            "upper.data should be cleaned after successful conversion"
        );
        assert!(
            !work_dir.join(UPPER_INDEX).exists(),
            "upper.index should be cleaned after successful conversion"
        );
        assert!(
            !work_dir.join(IMAGE_JSON).exists(),
            "image.json should be cleaned after successful conversion"
        );
        assert!(
            !work_dir.join(RESULT_TXT).exists(),
            "result.txt should be cleaned after successful conversion"
        );

        let log = fs::read_to_string(&log_path).unwrap();
        assert!(log.contains("overlaybd-create"));
        assert!(log.contains("overlaybd-apply"));
        assert!(log.contains("overlaybd-commit"));
        assert!(log.contains("--uuid 11111111-2222-3333-4444-555555555555"));
        assert!(log.contains("--parent-uuid aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"));
        assert!(log.contains(&format!(
            "--service_config_path={}",
            req.global_config_path.display()
        )));
        assert!(log.contains("--mkfs"));
    }

    #[test]
    fn convert_reports_non_zero_exit_with_captured_output() {
        let tmp = tempdir().unwrap();
        let work_dir = tmp.path().join("work");
        let bin_dir = tmp.path().join("bin");
        fs::create_dir_all(&bin_dir).unwrap();

        write_script(
            &bin_dir.join("overlaybd-create"),
            "#!/bin/sh\necho create-stdout\necho create-stderr >&2\nexit 23\n",
        );
        write_script(&bin_dir.join("overlaybd-apply"), "#!/bin/sh\nexit 0\n");
        write_script(&bin_dir.join("overlaybd-commit"), "#!/bin/sh\nexit 0\n");

        let tools = OverlaybdTools::from_bin_dir(&bin_dir);
        let req = ConvertLayerRequest {
            work_dir,
            input_layer_path: tmp.path().join("layer.tar"),
            global_config_path: tmp.path().join("global.json"),
            virtual_size_gib: 1,
            mkfs: false,
            lower_layers: vec![],
            uuid: None,
            parent_uuid: None,
        };

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let err = rt
            .block_on(tools.convert_local_oci_layer_to_overlaybd(&req))
            .expect_err("command should fail");

        match err {
            ToolExecutionError::NonZeroExit {
                exit_code,
                stdout,
                stderr,
                ..
            } => {
                assert_eq!(exit_code, Some(23));
                assert!(stdout.contains("create-stdout"));
                assert!(stderr.contains("create-stderr"));
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn convert_reports_missing_binary_explicitly() {
        let tmp = tempdir().unwrap();
        let tools = OverlaybdTools::from_bin_dir(tmp.path());
        let req = ConvertLayerRequest {
            work_dir: tmp.path().join("work"),
            input_layer_path: tmp.path().join("layer.tar"),
            global_config_path: tmp.path().join("global.json"),
            virtual_size_gib: 1,
            mkfs: false,
            lower_layers: vec![],
            uuid: None,
            parent_uuid: None,
        };

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let err = rt
            .block_on(tools.convert_local_oci_layer_to_overlaybd(&req))
            .expect_err("missing binary should fail");

        match err {
            ToolExecutionError::MissingBinary { path } => {
                assert!(path.ends_with("overlaybd-create"));
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn layer_decompression_detects_zstd_by_magic() {
        let tmp = tempdir().unwrap();
        let layer = tmp.path().join("zstd-layer.blob");
        fs::write(&layer, [0x28, 0xb5, 0x2f, 0xfd, 0x44, 0x08]).unwrap();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        assert!(rt.block_on(layer_uses_zstd_decompression(&layer)).unwrap());
    }

    #[test]
    fn layer_decompression_ignores_non_zstd_magic() {
        let tmp = tempdir().unwrap();
        let layer = tmp.path().join("not-zstd-layer.blob");
        fs::write(&layer, [0x1f, 0x8b, 0x08, 0x00, 0x00]).unwrap();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        assert!(!rt.block_on(layer_uses_zstd_decompression(&layer)).unwrap());
    }

    #[test]
    fn layer_decompression_detects_zstd_skippable_frame_magic() {
        let tmp = tempdir().unwrap();
        let layer = tmp.path().join("zstd-skippable.blob");
        fs::write(&layer, [0x50, 0x2a, 0x4d, 0x18, 0x00]).unwrap();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        assert!(rt.block_on(layer_uses_zstd_decompression(&layer)).unwrap());
    }

    #[test]
    fn from_overlaybd_install_root_uses_static_bin_subdir() {
        let tmp = tempdir().unwrap();
        let overlaybd_root = tmp.path().join("overlaybd");
        let tools = OverlaybdTools::from_overlaybd_install_root(&overlaybd_root);

        assert_eq!(
            tools.create_bin,
            overlaybd_root.join("bin/overlaybd-create")
        );
        assert_eq!(tools.apply_bin, overlaybd_root.join("bin/overlaybd-apply"));
        assert_eq!(
            tools.commit_bin,
            overlaybd_root.join("bin/overlaybd-commit")
        );
    }

    fn write_script(path: &Path, contents: &str) {
        fs::write(path, contents).unwrap();
        set_executable(path);
    }

    fn set_executable(path: &Path) {
        #[cfg(unix)]
        {
            let mut perms = fs::metadata(path).unwrap().permissions();
            perms.set_mode(0o755);
            fs::set_permissions(path, perms).unwrap();
        }
    }
}
