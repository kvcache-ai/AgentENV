use anyhow::{bail, Context, Result};
use envd::http_client::apis::files_api;
use shell_util::shell_quote;

use super::EnvdInstance;
use crate::sandbox::{Executor, ProcessOpts};

pub(super) fn needs_resolution(user: &str) -> bool {
    user.contains(':') || is_numeric(user)
}

fn is_numeric(value: &str) -> bool {
    !value.is_empty() && value.bytes().all(|c| c.is_ascii_digit())
}

impl EnvdInstance {
    pub(super) async fn resolve_default_user(&self, user: &str) -> Result<String> {
        let passwd = self.read_account_file("/etc/passwd").await?;
        let groups = if user.contains(':') {
            self.read_account_file("/etc/group").await?
        } else {
            String::new()
        };
        let resolved = resolve_user(user, &passwd, &groups)?;
        if let Some(entry) = resolved.passwd_entry {
            // envd requires a name even when Docker permits a UID without an
            // account. Add an identity without changing existing accounts.
            let script = format!("printf '\\n%s\\n' {} >> /etc/passwd", shell_quote(&entry));
            let output = Executor::new(self.clone())
                .run_command_with_opts(
                    "/bin/sh",
                    &["-c", &script],
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

    async fn read_account_file(&self, path: &str) -> Result<String> {
        match files_api::files_get(&self.config, Some(path), Some("root"), None, None).await {
            Ok(response) => Ok(response.text().await?),
            Err(envd::http_client::apis::Error::ResponseError(response))
                if response.status == envd::reqwest::StatusCode::NOT_FOUND =>
            {
                Ok(String::new())
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

fn parse_id(value: &str) -> Result<u32> {
    let id = value
        .parse::<u32>()
        .context("invalid Dockerfile user/group ID")?;
    if id == u32::MAX {
        bail!("invalid Dockerfile user/group ID: {value}");
    }
    Ok(id)
}

fn resolve_user(user: &str, passwd: &str, groups: &str) -> Result<ResolvedUser> {
    let (account, group) = user
        .split_once(':')
        .map_or((user, None), |(u, g)| (u, Some(g)));
    let numeric_uid = is_numeric(account).then(|| parse_id(account)).transpose()?;
    let accounts: Vec<Vec<&str>> = passwd
        .lines()
        .map(|line| line.split(':').collect())
        .filter(|fields: &Vec<&str>| fields.len() == 7)
        .collect();
    let existing = accounts.iter().find(|fields| {
        numeric_uid.map_or(fields[0] == account, |uid| {
            fields[2].parse::<u32>() == Ok(uid)
        })
    });
    let uid = match numeric_uid {
        Some(uid) => uid,
        None => parse_id(
            existing.context("Dockerfile USER names an account absent from /etc/passwd")?[2],
        )?,
    };
    let gid = match group {
        Some(group) if is_numeric(group) => parse_id(group)?,
        Some(group) => {
            let entry = groups
                .lines()
                .map(|line| line.split(':').collect::<Vec<_>>())
                .find(|fields| fields.len() == 4 && fields[0] == group)
                .context("Dockerfile USER names a group absent from /etc/group")?;
            parse_id(entry[2])?
        }
        None => existing
            .map(|fields| parse_id(fields[3]))
            .transpose()?
            .unwrap_or(0),
    };
    if let Some(fields) = existing {
        if parse_id(fields[3])? == gid {
            return Ok(ResolvedUser {
                name: fields[0].to_owned(),
                passwd_entry: None,
            });
        }
    }
    // A separate name preserves explicit UID:GID pairs without rewriting an
    // existing user's primary group. Reuse it on subsequent snapshot restores.
    let home = existing.map_or("/", |fields| fields[5]);
    let base_name = format!("aenv-{uid}-{gid}");
    for suffix in 0..=accounts.len() {
        let name = if suffix == 0 {
            base_name.clone()
        } else {
            format!("{base_name}-{suffix}")
        };
        if let Some(fields) = accounts.iter().find(|fields| fields[0] == name) {
            if fields[2].parse::<u32>() == Ok(uid) && fields[3].parse::<u32>() == Ok(gid) {
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

    const PASSWD: &str =
        "root:x:0:0:root:/root:/bin/sh\nnonroot:x:65532:65532::/home/nonroot:/sbin/nologin\n";

    #[test]
    fn numeric_users_resolve_to_existing_guest_accounts() {
        for (user, name) in [
            ("0", "root"),
            ("00", "root"),
            ("0:0", "root"),
            ("65532", "nonroot"),
            ("65532:65532", "nonroot"),
        ] {
            assert_eq!(
                resolve_user(user, PASSWD, "").unwrap(),
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
            resolve_user("1234", PASSWD, "")
                .unwrap()
                .passwd_entry
                .as_deref(),
            Some("aenv-1234-0:x:1234:0::/:/bin/sh")
        );
        let resolved = resolve_user("1234:5678", PASSWD, "").unwrap();
        let updated = format!("{PASSWD}{}\n", resolved.passwd_entry.unwrap());
        assert_eq!(
            resolve_user("1234:5678", &updated, "")
                .unwrap()
                .passwd_entry,
            None
        );
    }

    #[test]
    fn explicit_group_preserves_existing_account_and_home() {
        let resolved = resolve_user("nonroot:staff", PASSWD, "staff:x:1000:\n").unwrap();
        assert_eq!(
            resolved.passwd_entry.as_deref(),
            Some("aenv-65532-1000:x:65532:1000::/home/nonroot:/bin/sh")
        );
    }

    #[test]
    fn conflicting_generated_name_is_not_reused() {
        let passwd = format!("{PASSWD}aenv-1234-0:x:999:999::/:/bin/sh\n");
        assert_eq!(
            resolve_user("1234", &passwd, "").unwrap().name,
            "aenv-1234-0-1"
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
            assert!(resolve_user(user, PASSWD, "").is_err(), "{user}");
        }
    }
}
