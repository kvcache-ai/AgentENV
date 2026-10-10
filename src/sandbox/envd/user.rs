use std::collections::HashMap;

use anyhow::{bail, ensure, Context, Result};
use envd::http_client::apis::files_api;

use super::EnvdInstance;
use crate::sandbox::{Executor, ProcessOpts};

const BUSYBOX: &str = "/agentenv/bin/busybox";
/// Locked passwd append. It ships with the server rather than the tools drive
/// so it also applies to sandboxes on older, immutable tools releases.
const ADD_PASSWD_ENTRY: &str = include_str!("add_passwd_entry.sh");

pub(super) fn needs_resolution(user: &str) -> bool {
    user.contains(':') || is_numeric(user)
}

fn is_numeric(value: &str) -> bool {
    !value.is_empty() && value.bytes().all(|c| c.is_ascii_digit())
}

impl EnvdInstance {
    pub(super) async fn resolve_default_user(&self, user: &str) -> Result<String> {
        let passwd = self.read_account_file("/etc/passwd").await?;
        let groups = if user
            .split_once(':')
            .is_some_and(|(_, group)| !is_numeric(group))
        {
            self.read_account_file("/etc/group").await?
        } else {
            Vec::new()
        };
        let resolved = resolve_user(user, &passwd, &groups)?;
        if let Some(entry) = resolved.passwd_entry {
            // envd requires a name even when Docker permits a UID without an
            // account. Add an identity without changing existing accounts.
            let output = Executor::new(self.clone())
                .with_root_user()
                .run_command_with_opts(
                    BUSYBOX,
                    &[
                        "sh",
                        "-c",
                        ADD_PASSWD_ENTRY,
                        "sh",
                        BUSYBOX,
                        "/etc/passwd",
                        &entry,
                    ],
                    &ProcessOpts::default()
                        .with_cwd("/")
                        .with_timeout(std::time::Duration::from_secs(10)),
                )
                .await?;
            if output.exit_code != 0 {
                bail!(
                    "failed to prepare Dockerfile USER {user}: {}",
                    output.stderr
                );
            }
        }
        Ok(resolved.name)
    }

    async fn read_account_file(&self, path: &str) -> Result<Vec<u8>> {
        match files_api::files_get(&self.config, Some(path), Some("root"), None, None).await {
            Ok(mut response) => {
                let mut bytes = Vec::new();
                while let Some(chunk) = response.chunk().await? {
                    ensure!(
                        bytes.len() + chunk.len() <= 1024 * 1024,
                        "guest {path} exceeds the 1 MiB account-file limit"
                    );
                    bytes.extend_from_slice(&chunk);
                }
                Ok(bytes)
            }
            Err(envd::http_client::apis::Error::ResponseError(response))
                if response.status == envd::reqwest::StatusCode::NOT_FOUND =>
            {
                Ok(Vec::new())
            }
            Err(error) => {
                Err(error).with_context(|| format!("read guest {path} to resolve Dockerfile USER"))
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
struct ResolvedUser {
    name: String,
    passwd_entry: Option<String>,
}

fn parse_id(value: &[u8]) -> Result<u32> {
    let value = std::str::from_utf8(value).context("invalid Dockerfile user/group ID")?;
    let id = value
        .parse::<u32>()
        .context("invalid Dockerfile user/group ID")?;
    if id == u32::MAX {
        bail!("invalid Dockerfile user/group ID: {value}");
    }
    Ok(id)
}

fn resolve_user(user: &str, passwd: &[u8], groups: &[u8]) -> Result<ResolvedUser> {
    let (account, group) = user
        .split_once(':')
        .map_or((user, None), |(u, g)| (u, Some(g)));
    let numeric_uid = is_numeric(account)
        .then(|| parse_id(account.as_bytes()))
        .transpose()?;
    // Parse delimiters and IDs as bytes; unrelated names, comments, and paths
    // need not be UTF-8. Only convert the selected account's name/home below.
    let accounts: Vec<Vec<&[u8]>> = passwd
        .split(|&byte| byte == b'\n')
        .enumerate()
        .filter(|(_, line)| !line.is_empty() && !line.starts_with(b"#"))
        .map(|(index, line)| {
            let line_number = index + 1;
            let fields: Vec<_> = line.split(|&byte| byte == b':').collect();
            ensure!(
                fields.len() == 7 && !fields[0].is_empty(),
                "malformed /etc/passwd record at line {line_number}: expected a name and seven fields"
            );
            parse_id(fields[2])
                .with_context(|| format!("invalid /etc/passwd UID at line {line_number}"))?;
            parse_id(fields[3])
                .with_context(|| format!("invalid /etc/passwd GID at line {line_number}"))?;
            Ok(fields)
        })
        .collect::<Result<_>>()?;
    let existing = accounts.iter().find(|fields| {
        numeric_uid.map_or(fields[0] == account.as_bytes(), |uid| {
            parse_id(fields[2]).ok() == Some(uid)
        })
    });
    let uid = match numeric_uid {
        Some(uid) => uid,
        None => parse_id(
            existing.context("Dockerfile USER names an account absent from /etc/passwd")?[2],
        )?,
    };
    let gid = match group {
        Some(group) if is_numeric(group) => parse_id(group.as_bytes())?,
        Some(group) => {
            let entry = groups
                .split(|&byte| byte == b'\n')
                .map(|line| line.split(|&byte| byte == b':').collect::<Vec<_>>())
                .find(|fields| fields[0] == group.as_bytes())
                .context("Dockerfile USER names a group absent from /etc/group")?;
            ensure!(entry.len() == 4, "malformed /etc/group record for {group}");
            parse_id(entry[2])?
        }
        None => existing
            .map(|fields| parse_id(fields[3]))
            .transpose()?
            .unwrap_or(0),
    };
    if group.is_none() {
        if let Some(fields) = existing {
            if parse_id(fields[3])? == gid {
                return Ok(ResolvedUser {
                    name: std::str::from_utf8(fields[0])
                        .context("selected /etc/passwd account name is not UTF-8")?
                        .to_owned(),
                    passwd_entry: None,
                });
            }
        }
    }
    // A separate name preserves explicit UID:GID pairs without rewriting an
    // existing user's primary group. Reuse it on subsequent snapshot restores.
    let home = std::str::from_utf8(existing.map_or(b"/".as_slice(), |fields| fields[5]))
        .context("selected /etc/passwd home is not UTF-8")?;
    let mut names = HashMap::new();
    for fields in &accounts {
        names.entry(fields[0]).or_insert(fields);
    }
    let base_name = format!("aenv-{uid}-{gid}");
    for suffix in 0..=accounts.len() {
        let name = if suffix == 0 {
            base_name.clone()
        } else {
            format!("{base_name}-{suffix}")
        };
        if let Some(fields) = names.get(name.as_bytes()) {
            if fields.len() == 7
                && fields[1] == b"x"
                && parse_id(fields[2]).ok() == Some(uid)
                && parse_id(fields[3]).ok() == Some(gid)
                && fields[4].is_empty()
                && fields[5] == home.as_bytes()
                && fields[6] == b"/bin/sh"
            {
                return Ok(ResolvedUser {
                    name,
                    passwd_entry: None,
                });
            }
            continue;
        }
        let passwd_entry = Some(format!("{name}:x:{uid}:{gid}::{home}:/bin/sh"));
        return Ok(ResolvedUser { name, passwd_entry });
    }
    unreachable!("a free account name exists after inspecting every entry")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use std::process::{Command, Output};
    use std::time::{Duration, Instant};

    /// Runs the guest script on the host; `env` stands in for BusyBox applets.
    fn add_passwd_entry(passwd: &Path, entry: &str) -> Output {
        Command::new("sh")
            .args(["-c", ADD_PASSWD_ENTRY, "sh", "env"])
            .arg(passwd)
            .arg(entry)
            .output()
            .expect("run add_passwd_entry.sh")
    }

    fn assert_added(output: &Output) {
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    fn passwd_append_is_idempotent_and_releases_the_lock() {
        let dir = tempfile::tempdir().unwrap();
        let passwd = dir.path().join("passwd");
        // A missing trailing newline must not merge records.
        std::fs::write(&passwd, "root:x:0:0::/root:/bin/sh").unwrap();
        let entry = "aenv-1-2:x:1:2::/:/bin/sh";

        assert_added(&add_passwd_entry(&passwd, entry));
        assert_added(&add_passwd_entry(&passwd, entry));

        let contents = std::fs::read_to_string(&passwd).unwrap();
        assert_eq!(contents.lines().filter(|line| *line == entry).count(), 1);
        assert!(contents
            .lines()
            .any(|line| line == "root:x:0:0::/root:/bin/sh"));
        let names: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(names, ["passwd"], "lock or temporary files left behind");
    }

    #[test]
    fn passwd_append_reclaims_a_lock_whose_owner_exited() {
        let dir = tempfile::tempdir().unwrap();
        let passwd = dir.path().join("passwd");
        std::fs::write(&passwd, "root:x:0:0::/root:/bin/sh\n").unwrap();
        let mut exited = Command::new("true").spawn().unwrap();
        exited.wait().unwrap();
        std::fs::write(dir.path().join("passwd.lock"), exited.id().to_string()).unwrap();

        assert_added(&add_passwd_entry(&passwd, "aenv-1-0:x:1:0::/:/bin/sh"));
        assert!(!dir.path().join("passwd.lock").exists());
    }

    #[test]
    fn passwd_append_waits_for_a_live_lock_owner() {
        let dir = tempfile::tempdir().unwrap();
        let passwd = dir.path().join("passwd");
        let lock = dir.path().join("passwd.lock");
        std::fs::write(&passwd, "root:x:0:0::/root:/bin/sh\n").unwrap();
        let mut owner = Command::new("sleep").arg("30").spawn().unwrap();
        std::fs::write(&lock, owner.id().to_string()).unwrap();
        let release = {
            let lock = lock.clone();
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(300));
                std::fs::remove_file(lock).unwrap();
            })
        };

        let start = Instant::now();
        let output = add_passwd_entry(&passwd, "aenv-1-0:x:1:0::/:/bin/sh");
        release.join().unwrap();
        owner.kill().unwrap();
        owner.wait().unwrap();

        assert_added(&output);
        assert!(start.elapsed() >= Duration::from_millis(300));
        assert!(std::fs::read_to_string(&passwd)
            .unwrap()
            .contains("aenv-1-0:x:1:0::/:/bin/sh"));
    }

    #[test]
    fn passwd_append_survives_a_locked_concurrent_rewrite() {
        let dir = tempfile::tempdir().unwrap();
        let passwd = dir.path().join("passwd");
        std::fs::write(&passwd, "root:x:0:0::/root:/bin/sh\n").unwrap();
        // Rewrite like shadow's usermod: lock, copy, pause, rename, unlock.
        let mut rewriter = Command::new("sh")
            .args([
                "-c",
                r#"p=$1; i=0
                while [ "$i" -lt 100 ]; do
                    echo $$ > "$p.rw"
                    if ln "$p.rw" "$p.lock" 2>/dev/null; then
                        cp "$p" "$p.new"; sleep 0.005; mv "$p.new" "$p"
                        rm -f "$p.lock"; i=$((i + 1))
                    fi
                    rm -f "$p.rw"
                done"#,
                "sh",
            ])
            .arg(&passwd)
            .spawn()
            .unwrap();

        let entries: Vec<_> = (1..=20)
            .map(|uid| format!("aenv-{uid}-0:x:{uid}:0::/:/bin/sh"))
            .collect();
        for entry in &entries {
            assert_added(&add_passwd_entry(&passwd, entry));
        }
        assert!(rewriter.wait().unwrap().success());

        let contents = std::fs::read_to_string(&passwd).unwrap();
        for entry in &entries {
            assert!(contents.lines().any(|line| line == entry), "lost {entry}");
        }
    }

    const PASSWD: &str =
        "root:x:0:0:root:/root:/bin/sh\nnonroot:x:65532:65532::/home/nonroot:/sbin/nologin\n";

    #[test]
    fn numeric_users_resolve_to_existing_guest_accounts() {
        for (user, name) in [("0", "root"), ("00", "root"), ("65532", "nonroot")] {
            assert_eq!(
                resolve_user(user, PASSWD.as_bytes(), b"").unwrap(),
                ResolvedUser {
                    name: name.into(),
                    passwd_entry: None
                }
            );
        }
    }

    #[test]
    fn missing_numeric_user_keeps_requested_uid_and_gid() {
        assert_eq!(
            resolve_user("1234", PASSWD.as_bytes(), b"")
                .unwrap()
                .passwd_entry
                .as_deref(),
            Some("aenv-1234-0:x:1234:0::/:/bin/sh")
        );
        let resolved = resolve_user("1234:5678", PASSWD.as_bytes(), b"").unwrap();
        let updated = format!("{PASSWD}{}\n", resolved.passwd_entry.unwrap());
        assert_eq!(
            resolve_user("1234:5678", updated.as_bytes(), b"")
                .unwrap()
                .passwd_entry,
            None
        );
    }

    #[test]
    fn explicit_group_preserves_existing_account_and_home() {
        let resolved =
            resolve_user("nonroot:staff", PASSWD.as_bytes(), b"staff:x:1000:\n").unwrap();
        assert_eq!(
            resolved.passwd_entry.as_deref(),
            Some("aenv-65532-1000:x:65532:1000::/home/nonroot:/bin/sh")
        );
    }

    #[test]
    fn conflicting_generated_name_is_not_reused() {
        let passwd = format!(
            "{PASSWD}other:x:1234:0::/home/other:/bin/sh\naenv-1234-0:x:1234:0::/other:/bin/sh\n"
        );
        assert_eq!(
            resolve_user("1234:0", passwd.as_bytes(), b"").unwrap().name,
            "aenv-1234-0-1"
        );
    }

    #[test]
    fn explicit_group_does_not_reuse_existing_account() {
        assert_eq!(
            resolve_user("nonroot:65532", PASSWD.as_bytes(), b"")
                .unwrap()
                .name,
            "aenv-65532-65532"
        );
    }

    #[test]
    fn non_utf8_account_fields_do_not_block_valid_identities() {
        let passwd = b"root:x:0:0:r\xffot:/root:/bin/sh\n\xffuser:x:1000:1000::/\xffhome:/bin/sh\n";
        assert_eq!(resolve_user("0", passwd, b"").unwrap().name, "root");
        let groups = b"\xffgroup:x:1000:\xffuser\nstaff:x:1001:\xffuser\n";
        assert_eq!(
            resolve_user("0:staff", passwd, groups)
                .unwrap()
                .passwd_entry
                .as_deref(),
            Some("aenv-0-1001:x:0:1001::/root:/bin/sh")
        );
    }

    #[test]
    fn malformed_passwd_records_fail_with_line_context() {
        for record in [
            "nonroot:x:65532:65532::/home/nonroot", // Missing shell field.
            "nonroot:x:bad:65532::/home/nonroot:/bin/sh",
            "nonroot:x:65532:bad::/home/nonroot:/bin/sh",
            ":x:65532:65532::/home/nonroot:/bin/sh",
        ] {
            let passwd = format!("root:x:0:0::/root:/bin/sh\n{record}\n");
            for user in ["65532", "nonroot:0"] {
                let error = resolve_user(user, passwd.as_bytes(), b"").unwrap_err();
                assert!(error.to_string().contains("/etc/passwd"), "{error:#}");
                assert!(error.to_string().contains("line 2"), "{error:#}");
            }
        }
    }

    #[test]
    fn blank_lines_and_comments_do_not_hide_existing_accounts() {
        let passwd = format!("\n# Accounts\n{PASSWD}\n");
        assert_eq!(
            resolve_user("0", passwd.as_bytes(), b"").unwrap().name,
            "root"
        );
    }

    #[test]
    fn invalid_ids_and_missing_names_fail() {
        for user in [
            "4294967295",
            "4294967296",
            "0:",
            "0:no-such-group",
            "missing:0",
            ":0",
            "0:0:0",
        ] {
            assert!(
                resolve_user(user, PASSWD.as_bytes(), b"").is_err(),
                "{user}"
            );
        }
    }
}
