//! Project snapshots: preserve working changes without touching the source tree.

use super::{guest::Guest, runtime::WORKSPACE};
use anyhow::{ensure, Context, Result};
use flate2::{read::GzDecoder, write::GzEncoder, Compression};
use shell_util::shell_quote;
use std::collections::{BTreeSet, VecDeque};
use std::fs::{self, File};
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::path::{Component, Path, PathBuf};
use std::process::Command;
use walkdir::WalkDir;

const SKIP_DIRS: &[&str] = &[
    ".git",
    "node_modules",
    ".venv",
    "venv",
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    ".ssh",
    ".aws",
    ".azure",
    ".kube",
];

fn excluded(path: &Path) -> bool {
    let name = path.file_name().unwrap_or_default().to_string_lossy();
    path.components()
        .any(|c| SKIP_DIRS.iter().any(|s| c.as_os_str() == *s))
        || (name == ".env" || name.starts_with(".env."))
            && ![".env.example", ".env.sample"].contains(&name.as_ref())
        || ["id_rsa", "id_ed25519"].contains(&name.as_ref())
        || [Some("pem"), Some("key")].contains(&path.extension().and_then(|s| s.to_str()))
}

fn files(project: &Path) -> Result<BTreeSet<PathBuf>> {
    let probe = Command::new("git")
        .arg("-C")
        .arg(project)
        .args(["rev-parse", "--is-inside-work-tree"])
        .output()
        .context("run Git; install Git first")?;
    let paths: BTreeSet<PathBuf> = if probe.status.success() {
        let result = Command::new("git")
            .arg("-C")
            .arg(project)
            .args([
                "ls-files",
                "--cached",
                "--others",
                "--exclude-standard",
                "-z",
                "--",
                ".",
            ])
            .output()?;
        ensure!(
            result.status.success(),
            "Cannot inspect Git ignores: {}",
            String::from_utf8_lossy(&result.stderr)
        );
        result
            .stdout
            .split(|b| *b == 0)
            .filter(|s| !s.is_empty())
            .map(|s| PathBuf::from(std::ffi::OsStr::from_bytes(s)))
            .collect()
    } else {
        ensure!(
            !project.join(".git").exists(),
            "Cannot inspect Git ignores: {}",
            String::from_utf8_lossy(&probe.stderr)
        );
        WalkDir::new(project)
            .min_depth(1)
            .follow_links(false)
            .into_iter()
            .filter_entry(|e| !excluded(e.path().strip_prefix(project).unwrap_or(e.path())))
            .map(|e| Ok(e?.path().strip_prefix(project)?.to_path_buf()))
            .collect::<Result<_>>()?
    };
    Ok(paths.into_iter().filter(|p| !excluded(p)).collect())
}

fn safe_path(path: &Path) -> bool {
    !path.as_os_str().is_empty()
        && path.components().all(|c| match c {
            Component::Normal(name) => name != ".git",
            Component::CurDir => true,
            _ => false,
        })
}

fn safe_link(path: &Path, target: &Path) -> bool {
    if target.is_absolute() {
        return false;
    }
    let mut parts: Vec<_> = path
        .parent()
        .unwrap_or(Path::new(""))
        .components()
        .collect();
    for part in target.components() {
        match part {
            Component::Normal(name) if name != ".git" => parts.push(part),
            Component::CurDir => (),
            Component::ParentDir if !parts.is_empty() => {
                parts.pop();
            }
            _ => return false,
        }
    }
    true
}

pub(super) fn pack(project: &Path, archive: &Path) -> Result<usize> {
    let output = GzEncoder::new(File::create(archive)?, Compression::default());
    let mut bundle = tar::Builder::new(output);
    bundle.follow_symlinks(false);
    let mut count = 0;
    for path in files(project)? {
        ensure!(safe_path(&path), "Invalid project path: {}", path.display());
        let source = project.join(&path);
        let metadata = match source.symlink_metadata() {
            Ok(value) => value,
            Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e.into()),
        };
        if metadata.file_type().is_symlink() {
            ensure!(
                safe_link(&path, &fs::read_link(&source)?),
                "Use a relative symlink within the project: {}",
                path.display()
            );
            if let Ok(target) = source.canonicalize() {
                ensure!(
                    target.starts_with(project),
                    "Symlink leaves project: {}",
                    path.display()
                );
            }
        } else {
            ensure!(
                metadata.is_file() || metadata.is_dir(),
                "Unsupported project entry: {}",
                path.display()
            );
        }
        bundle.append_path_with_name(source, path)?;
        count += 1;
    }
    bundle.into_inner()?.finish()?;
    Ok(count)
}

fn extract(archive: &Path, destination: &Path) -> Result<()> {
    let mut bundle = tar::Archive::new(GzDecoder::new(File::open(archive)?));
    let mut links = Vec::new();
    for entry in bundle.entries()? {
        let mut entry = entry?;
        let path = entry.path()?.into_owned();
        let kind = entry.header().entry_type();
        ensure!(safe_path(&path), "Unsafe archive path: {}", path.display());
        ensure!(
            kind.is_file() || kind.is_dir() || kind.is_symlink(),
            "Unsupported archive entry: {}",
            path.display()
        );
        if kind.is_symlink() {
            let target = entry.link_name()?.context("missing symlink target")?;
            ensure!(
                safe_link(&path, &target),
                "Unsafe symlink: {}",
                path.display()
            );
            links.push(path.clone());
        }
        // Never write through an earlier archive entry's symlink, even an
        // internal one. Saves are extracted only into a new, private directory.
        let mut parent = destination.to_path_buf();
        for part in path.parent().unwrap_or(Path::new("")).components() {
            parent.push(part.as_os_str());
            ensure!(
                !parent.is_symlink(),
                "Archive traverses a symlink: {}",
                path.display()
            );
        }
        ensure!(
            !destination.join(&path).is_symlink(),
            "Archive overwrites a symlink: {}",
            path.display()
        );
        ensure!(
            entry.unpack_in(destination)?,
            "Archive entry escaped destination"
        );
    }
    // Validate the completed tree too. A later symlink may change how an
    // earlier link's '..' resolves, even if both targets look safe separately.
    for link in links {
        validate_link_chain(destination, &link)?;
    }
    Ok(())
}

fn validate_link_chain(root: &Path, path: &Path) -> Result<()> {
    let mut pending: VecDeque<_> = path
        .components()
        .map(|c| c.as_os_str().to_owned())
        .collect();
    let mut resolved = root.to_owned();
    let mut followed = 0;
    while let Some(part) = pending.pop_front() {
        if part == "." {
            continue;
        }
        if part == ".." {
            resolved.pop();
        } else {
            resolved.push(part);
        }
        ensure!(
            resolved.starts_with(root),
            "Unsafe symlink chain: {}",
            path.display()
        );
        if resolved.is_symlink() {
            followed += 1;
            ensure!(
                followed <= 40,
                "Symlink cycle or excessive links: {}",
                path.display()
            );
            let target = fs::read_link(&resolved)?;
            ensure!(!target.is_absolute(), "Unsafe symlink: {}", path.display());
            resolved.pop();
            for part in target.components().rev() {
                pending.push_front(part.as_os_str().to_owned());
            }
        }
    }
    Ok(())
}

pub(super) async fn import(guest: &Guest<'_>, archive: &Path) -> Result<String> {
    guest
        .upload(archive, "/tmp/agentenv-project.tar.gz", true)
        .await?;
    let excludes = SKIP_DIRS
        .iter()
        .filter(|n| **n != ".git")
        .map(|n| format!("{n}/\n"))
        .collect::<String>();
    guest
        .exec(&format!(
            "mkdir -p {WORKSPACE} && tar -xzf /tmp/agentenv-project.tar.gz -C {WORKSPACE} && \
        cd {WORKSPACE} && git init -q && printf %s {} > .git/info/exclude && git add -f . && \
        git -c user.name=AgentENV -c user.email=agentenv@localhost -c core.hooksPath=/dev/null \
        commit -qm 'Initial project' --allow-empty",
            shell_quote(&excludes)
        ))
        .await?;
    Ok(guest
        .text(&format!("git -C {WORKSPACE} rev-parse HEAD"))
        .await?
        .trim()
        .to_owned())
}

fn export_file_list(bytes: &[u8]) -> Result<Vec<u8>> {
    let mut list = Vec::new();
    for name in bytes
        .split(|b| *b == 0)
        .filter(|name| !name.is_empty())
        .collect::<BTreeSet<_>>()
    {
        let path = Path::new(std::ffi::OsStr::from_bytes(name));
        ensure!(safe_path(path), "Unsafe export path: {}", path.display());
        list.extend_from_slice(name);
        list.push(0);
    }
    Ok(list)
}

fn archive_args<'a>(workspace: &'a str, list: &'a str, archive: &'a str) -> Vec<&'a str> {
    // NUL-delimited verbatim names preserve newlines and leading dashes. Never
    // follow symlinks; store hard-linked files separately for safe extraction.
    vec![
        "--create",
        "--gzip",
        "--no-recursion",
        "--hard-dereference",
        "--directory",
        workspace,
        "--file",
        archive,
        "--null",
        "--verbatim-files-from",
        "--files-from",
        list,
    ]
}

pub(super) async fn save(guest: &Guest<'_>, baseline: &str, session: &Path) -> Result<PathBuf> {
    let patch = guest
        .exec(&format!(
            "cd {WORKSPACE} && git add -A && git diff --cached --no-ext-diff --binary {}",
            shell_quote(baseline)
        ))
        .await?;
    let staging = tempfile::Builder::new()
        .prefix(".saving-")
        .tempdir_in(session)?;
    let archive = staging.path().join("project.tar.gz");
    let names = guest
        .command(
            "git",
            &[
                "-C",
                WORKSPACE,
                "ls-files",
                "--cached",
                "--others",
                "--exclude-standard",
                "-z",
            ],
        )
        .await?;
    let list = export_file_list(&names)?;
    let temporary = format!("/tmp/aenv-export-{}", uuid::Uuid::new_v4().simple());
    let remote_list = format!("{temporary}.files");
    let remote_archive = format!("{temporary}.tar.gz");
    let export = async {
        guest.write(&remote_list, &list).await?;
        let mut args = vec!["-u", "TAR_OPTIONS", "tar"];
        args.extend(archive_args(WORKSPACE, &remote_list, &remote_archive));
        guest.command("env", &args).await?;
        guest.download(&remote_archive, &archive).await
    }
    .await;
    let _ = guest
        .command("rm", &["-f", "--", &remote_list, &remote_archive])
        .await;
    export?;
    let project = staging.path().join("project");
    fs::create_dir(&project)?;
    extract(&archive, &project)?;
    fs::remove_file(archive)?;
    fs::write(staging.path().join("changes.patch"), patch)?;
    let saves = session.join("saves");
    fs::create_dir_all(&saves)?;
    let mut number = 1;
    let target = loop {
        let target = saves.join(format!("{number:04}"));
        if !target.exists() {
            break target;
        }
        number += 1;
    };
    fs::rename(staging.path(), &target)?;
    super::runtime::write_private(
        &session.join("latest.txt"),
        format!("{}\n", target.display()).as_bytes(),
    )?;
    println!("Saved project: {}", target.join("project").display());
    println!("Changes: {}", target.join("changes.patch").display());
    Ok(target)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn system_tar_preserves_literal_names_symlinks_and_hard_linked_files() -> Result<()> {
        use std::os::unix::ffi::OsStringExt;
        let root = tempfile::tempdir()?;
        let project = root.path().join("source");
        fs::create_dir(&project)?;
        let names = [
            std::ffi::OsString::from("space name"),
            "line\nbreak".into(),
            "--checkpoint=1".into(),
            std::ffi::OsString::from_vec(vec![b'n', 0xff]),
        ];
        let mut raw = Vec::new();
        for name in &names {
            fs::write(project.join(name), b"\0binary\xff")?;
            raw.extend_from_slice(name.as_os_str().as_bytes());
            raw.push(0);
        }
        fs::hard_link(project.join(&names[0]), project.join("hard-link"))?;
        std::os::unix::fs::symlink(&names[0], project.join("symlink"))?;
        raw.extend_from_slice(b"hard-link\0symlink\0symlink\0");
        let list = root.path().join("files");
        fs::write(&list, export_file_list(&raw)?)?;
        let archive = root.path().join("files.tar.gz");
        ensure!(Command::new("tar")
            .env_remove("TAR_OPTIONS")
            .args(archive_args(
                project.to_str().unwrap(),
                list.to_str().unwrap(),
                archive.to_str().unwrap()
            ))
            .status()?
            .success());
        let output = root.path().join("output");
        fs::create_dir(&output)?;
        extract(&archive, &output)?;
        for name in &names {
            assert_eq!(fs::read(output.join(name))?, b"\0binary\xff");
        }
        assert_eq!(fs::read(output.join("hard-link"))?, b"\0binary\xff");
        assert_eq!(
            fs::read_link(output.join("symlink"))?,
            PathBuf::from(&names[0])
        );
        assert_eq!(fs::read_dir(&output)?.count(), names.len() + 2);
        // An empty project still produces a valid empty archive.
        fs::write(&list, b"")?;
        ensure!(Command::new("tar")
            .env_remove("TAR_OPTIONS")
            .args(archive_args(
                project.to_str().unwrap(),
                list.to_str().unwrap(),
                archive.to_str().unwrap()
            ))
            .status()?
            .success());
        let empty = root.path().join("empty");
        fs::create_dir(&empty)?;
        extract(&archive, &empty)?;
        assert_eq!(fs::read_dir(empty)?.count(), 0);
        Ok(())
    }

    #[test]
    fn export_list_rejects_metadata_and_traversal() {
        for path in [b"../outside\0".as_slice(), b"/absolute\0", b".git/config\0"] {
            assert!(export_file_list(path).is_err());
        }
        assert_eq!(export_file_list(b"b\0a\0b\0").unwrap(), b"a\0b\0");
    }

    #[test]
    fn snapshots_respect_git_ignores_and_keep_working_changes() -> Result<()> {
        let root = tempfile::tempdir()?;
        let project = root.path().join("project");
        fs::create_dir(&project)?;
        ensure!(Command::new("git")
            .args(["init", "-q"])
            .arg(&project)
            .status()?
            .success());
        fs::write(project.join(".gitignore"), "ignored\n")?;
        fs::write(project.join("file"), "old")?;
        ensure!(Command::new("git")
            .arg("-C")
            .arg(&project)
            .args(["add", "."])
            .status()?
            .success());
        for (name, text) in [
            ("file", "new"),
            ("added", "added"),
            ("ignored", "skip"),
            (".env", "private"),
            (".env.example", "sample"),
        ] {
            fs::write(project.join(name), text)?;
        }
        std::os::unix::fs::symlink("file", project.join("link"))?;
        let archive = root.path().join("project.tar.gz");
        pack(&project, &archive)?;
        let output = root.path().join("output");
        fs::create_dir(&output)?;
        extract(&archive, &output)?;
        assert_eq!(fs::read_to_string(output.join("file"))?, "new");
        assert_eq!(fs::read_to_string(output.join("link"))?, "new");
        assert!(output.join("added").exists());
        assert!(output.join(".env.example").exists());
        assert!(!output.join(".env").exists());
        assert!(!output.join("ignored").exists());
        assert!(!output.join(".git").exists());
        Ok(())
    }

    #[test]
    fn rejects_traversal_external_links_and_writing_through_links() -> Result<()> {
        for path in ["../outside", "/absolute", ".git/config", "a/../../escape"] {
            assert!(!safe_path(Path::new(path)));
        }
        assert!(!safe_link(Path::new("link"), Path::new("../outside")));
        assert!(!safe_link(Path::new("a/link"), Path::new("/absolute")));
        assert!(safe_link(Path::new("a/link"), Path::new("../file")));
        let root = tempfile::tempdir()?;
        let archive = root.path().join("unsafe.tar.gz");
        let mut bundle = tar::Builder::new(GzEncoder::new(
            File::create(&archive)?,
            Compression::default(),
        ));
        let mut header = tar::Header::new_gnu();
        header.set_entry_type(tar::EntryType::Symlink);
        header.set_size(0);
        header.set_mode(0o777);
        bundle.append_link(&mut header, "link", "directory")?;
        header.set_entry_type(tar::EntryType::Regular);
        header.set_size(1);
        bundle.append_data(&mut header, "link/file", &b"x"[..])?;
        bundle.into_inner()?.finish()?;
        let output = root.path().join("output");
        fs::create_dir(&output)?;
        assert!(extract(&archive, &output).is_err());
        Ok(())
    }

    #[test]
    fn symlink_chains_cannot_escape_after_later_links_are_created() -> Result<()> {
        let root = tempfile::tempdir()?;
        fs::create_dir(root.path().join("dir"))?;
        std::os::unix::fs::symlink("..", root.path().join("dir/back"))?;
        std::os::unix::fs::symlink("dir/back/../outside", root.path().join("escape"))?;
        assert!(safe_link(
            Path::new("escape"),
            Path::new("dir/back/../outside")
        ));
        assert!(validate_link_chain(root.path(), Path::new("escape")).is_err());
        std::os::unix::fs::symlink("dir/missing", root.path().join("dangling"))?;
        validate_link_chain(root.path(), Path::new("dangling"))?;
        Ok(())
    }
}
