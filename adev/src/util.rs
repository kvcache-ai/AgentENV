use anyhow::{bail, Context, Result};
use std::path::Path;
use std::process::Command;

/// Run a command, printing it first, and return an error if it fails.
pub fn cmd(program: &str, args: &[&str]) -> Result<()> {
    info(&format!("$ {} {}", program, args.join(" ")));
    let status = Command::new(program)
        .args(args)
        .status()
        .context(format!("failed to run {program}"))?;
    if !status.success() {
        bail!("{} exited with {}", program, status);
    }
    Ok(())
}

/// Run a command in a specific directory, printing it first, and return an error if it fails.
pub fn cmd_in_dir(program: &str, args: &[&str], dir: &std::path::Path) -> Result<()> {
    info(&format!("$ {} {}", program, args.join(" ")));
    let status = Command::new(program)
        .args(args)
        .current_dir(dir)
        .status()
        .context(format!("failed to run {program}"))?;
    if !status.success() {
        bail!("{} exited with {}", program, status);
    }
    Ok(())
}

/// Download a file from a URL to a destination path with a progress bar.
pub fn download_file(url: &str, dest: &Path) -> Result<()> {
    use indicatif::{ProgressBar, ProgressStyle};
    use std::io::Write;

    if dest.exists() && dest.metadata().map(|m| m.len() > 0).unwrap_or(false) {
        info(&format!("Already present: {}", dest.display()));
        return Ok(());
    }

    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent)?;
    }

    info(&format!("Downloading {} -> {}", url, dest.display()));

    let response = reqwest::blocking::get(url).context(format!("GET {url}"))?;
    let status = response.status();
    if !status.is_success() {
        bail!("HTTP {} for {}", status, url);
    }

    let total_size = response.content_length().unwrap_or(0);
    let pb = if total_size > 0 {
        let pb = ProgressBar::new(total_size);
        pb.set_style(
            ProgressStyle::default_bar()
                .template("{bar:40.cyan/blue} {bytes}/{total_bytes} ({eta})")?
                .progress_chars("=>-"),
        );
        Some(pb)
    } else {
        None
    };

    let tmp_path = dest.with_extension("tmp");
    let mut file = std::fs::File::create(&tmp_path)?;
    let mut reader = response;
    let mut downloaded: u64 = 0;
    let mut buf = [0u8; 8192];
    loop {
        use std::io::Read;
        let n = reader.read(&mut buf).context("reading response body")?;
        if n == 0 {
            break;
        }
        file.write_all(&buf[..n])?;
        downloaded += n as u64;
        if let Some(ref pb) = pb {
            pb.set_position(downloaded);
        }
    }
    file.flush()?;
    drop(file);

    if let Some(pb) = pb {
        pb.finish_and_clear();
    }

    std::fs::rename(&tmp_path, dest).context("moving downloaded file to destination")?;
    Ok(())
}

/// Detect the system architecture, normalizing to x86_64 or aarch64.
pub fn detect_arch() -> Result<String> {
    match std::env::consts::ARCH {
        "x86_64" => Ok("x86_64".into()),
        "aarch64" => Ok("aarch64".into()),
        other => bail!("unsupported architecture: {other}"),
    }
}

/// Append content to a file (creates if missing). Warns on failure.
pub fn append_to_file(path: &str, content: &str) {
    if let Err(e) = std::fs::OpenOptions::new()
        .append(true)
        .create(true)
        .open(path)
        .and_then(|mut f| {
            use std::io::Write;
            write!(f, "{content}")
        })
    {
        warn(&format!("failed to append to {path}: {e}"));
    }
}

// Colored log helpers

pub fn info(msg: &str) {
    eprintln!("\x1b[32m[adev]\x1b[0m {msg}");
}

pub fn warn(msg: &str) {
    eprintln!("\x1b[33m[adev] WARNING:\x1b[0m {msg}");
}

#[allow(dead_code)]
pub fn error(msg: &str) {
    eprintln!("\x1b[31m[adev] ERROR:\x1b[0m {msg}");
}
