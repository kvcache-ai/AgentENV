#![allow(dead_code)] // TODO(e2b-stack): removed when the COPY executor lands

//! Host-side planning for template `COPY` steps.
//!
//! The E2B SDK uploads one tar archive per `COPY` instruction whose entry
//! paths are relative to the build context (the glob in `src` is already
//! resolved by the SDK). This module rewrites that archive so every entry
//! carries its final absolute guest path per Docker `COPY` semantics; the
//! build sandbox then only needs a single `tar -xpf archive -C /`.
//!
//! The rewrite runs in two passes so an archive is never held in memory: the
//! first pass indexes entry paths to compute the mapping, the second streams
//! each entry's bytes straight into the rewritten archive. Both passes read
//! from a single open file handle so the source cannot be replaced or unlinked
//! between them.
//!
//! Ownership is written into the rewritten headers rather than applied with a
//! post-extract `chown`, so a copy can only ever change the files it creates.
//! For the same reason the destination root itself never gets an archive
//! entry: `tar -xp` restores mode and ownership onto directory members that
//! already exist.

use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::Path;

use anyhow::{bail, Context, Result};

/// Upper bound on entries in one build-context archive. The indexing pass
/// keeps one normalized path per entry, so this bounds that allocation
/// independently of the byte budget: an archive of a million empty files is
/// tiny but path-heavy. Real build contexts are orders of magnitude smaller.
const MAX_ARCHIVE_ENTRIES: usize = 200_000;

/// Numeric ownership applied to every entry of one copy.
#[derive(Clone, Copy, Debug)]
pub(crate) struct CopyOwnership {
    pub(crate) uid: u64,
    pub(crate) gid: u64,
}

/// Inputs for rewriting one `COPY` step's archive.
pub(crate) struct CopyRequest<'a> {
    pub(crate) source_tar: &'a Path,
    pub(crate) src: &'a str,
    pub(crate) dest: &'a str,
    pub(crate) workdir: &'a str,
    pub(crate) mode: Option<u32>,
    /// Ownership requested by `--chown`, already resolved to numeric ids
    /// inside the build sandbox. `None` keeps Docker's root:root default.
    pub(crate) ownership: Option<CopyOwnership>,
    /// Budget for the decompressed archive, bounding both the rewritten file
    /// on the host and what a single upload can expand to.
    pub(crate) max_total_bytes: u64,
}

/// Summary of a rewritten copy archive.
#[derive(Debug)]
pub(crate) struct CopyPlan {
    /// Number of file/dir/symlink entries written to the rewritten archive.
    pub(crate) entry_count: usize,
    /// Total file bytes written to the rewritten archive.
    pub(crate) total_bytes: u64,
    /// Resolved absolute guest path of the copy destination root.
    pub(crate) dest_root: String,
    /// Whether a directory entry for `dest_root` itself was dropped. When set,
    /// the guest has to create that directory (with the requested ownership
    /// and mode) before extraction, but only if it does not already exist.
    pub(crate) skipped_dest_root: bool,
    /// Whether the copy treats `dest_root` as a directory. Archives without a
    /// directory member for the root (file-only uploads) still need the guest
    /// to create a missing destination with the requested metadata.
    pub(crate) dest_is_dir: bool,
}

/// One archive entry as seen by the indexing pass.
struct EntryIndex {
    /// Normalized context-relative path ("dir/file.txt").
    path: String,
    is_dir: bool,
}

fn is_glob_pattern(src: &str) -> bool {
    src.contains(['*', '?', '['])
}

/// One member of a `[...]` character class.
enum ClassItem {
    Char(char),
    Range(char, char),
}

/// One matchable unit inside a single path segment of a glob pattern.
enum GlobToken {
    Star,
    Any,
    Literal(char),
    Class {
        negated: bool,
        items: Vec<ClassItem>,
    },
}

/// Splits one pattern segment into tokens. An unterminated `[` is a literal.
fn tokenize_segment(pattern: &[char]) -> Vec<GlobToken> {
    let mut tokens = Vec::new();
    let mut i = 0;
    while i < pattern.len() {
        match pattern[i] {
            '*' => {
                tokens.push(GlobToken::Star);
                i += 1;
            }
            '?' => {
                tokens.push(GlobToken::Any);
                i += 1;
            }
            '[' => match pattern[i + 1..].iter().position(|&c| c == ']') {
                None => {
                    tokens.push(GlobToken::Literal('['));
                    i += 1;
                }
                Some(end) => {
                    let class = &pattern[i + 1..i + 1 + end];
                    let (negated, class) = match class.first() {
                        Some('!') | Some('^') => (true, &class[1..]),
                        _ => (false, class),
                    };
                    let mut items = Vec::new();
                    let mut j = 0;
                    while j < class.len() {
                        if j + 2 < class.len() && class[j + 1] == '-' {
                            items.push(ClassItem::Range(class[j], class[j + 2]));
                            j += 3;
                        } else {
                            items.push(ClassItem::Char(class[j]));
                            j += 1;
                        }
                    }
                    tokens.push(GlobToken::Class { negated, items });
                    i += end + 2;
                }
            },
            c => {
                tokens.push(GlobToken::Literal(c));
                i += 1;
            }
        }
    }
    tokens
}

/// Whether a single-character token accepts `c`.
fn token_matches(token: &GlobToken, c: char) -> bool {
    match token {
        GlobToken::Star => false,
        GlobToken::Any => true,
        GlobToken::Literal(expected) => *expected == c,
        GlobToken::Class { negated, items } => {
            let hit = items.iter().any(|item| match item {
                ClassItem::Char(ch) => *ch == c,
                ClassItem::Range(low, high) => *low <= c && c <= *high,
            });
            hit != *negated
        }
    }
}

/// Matches one segment with a single backtrack point per `*`, which keeps the
/// worst case quadratic instead of the exponential blowup a naive recursive
/// matcher has on patterns such as `*a*a*a*a*b`.
fn match_segment(tokens: &[GlobToken], value: &[char]) -> bool {
    let mut token_idx = 0usize;
    let mut value_idx = 0usize;
    let mut last_star: Option<usize> = None;
    let mut last_star_value = 0usize;

    while value_idx < value.len() {
        if token_idx < tokens.len() {
            if matches!(tokens[token_idx], GlobToken::Star) {
                last_star = Some(token_idx);
                last_star_value = value_idx;
                token_idx += 1;
                continue;
            }
            if token_matches(&tokens[token_idx], value[value_idx]) {
                token_idx += 1;
                value_idx += 1;
                continue;
            }
        }
        // Mismatch: let the most recent `*` swallow one more character.
        let Some(star_idx) = last_star else {
            return false;
        };
        token_idx = star_idx + 1;
        last_star_value += 1;
        value_idx = last_star_value;
    }

    tokens[token_idx..]
        .iter()
        .all(|token| matches!(token, GlobToken::Star))
}

/// Minimal fnmatch-style matcher covering `*`, `?` and `[...]` (no `**`),
/// mirroring the Python `glob` patterns the SDK resolves client-side.
///
/// Matching is segment-wise like Go's `path/filepath.Match`, which is what
/// Docker uses for `COPY` sources: none of the wildcards ever match `/`, so a
/// pattern and a value with different segment counts never match.
fn glob_match(pattern: &str, value: &str) -> bool {
    let mut pattern_segments = pattern.split('/');
    let mut value_segments = value.split('/');
    loop {
        match (pattern_segments.next(), value_segments.next()) {
            (None, None) => return true,
            (Some(pattern_segment), Some(value_segment)) => {
                let pattern_segment: Vec<char> = pattern_segment.chars().collect();
                let value_segment: Vec<char> = value_segment.chars().collect();
                if !match_segment(&tokenize_segment(&pattern_segment), &value_segment) {
                    return false;
                }
            }
            _ => return false,
        }
    }
}

/// Normalizes a context-relative source pattern ("./a/b/" -> "a/b").
fn normalize_src(src: &str) -> String {
    let mut src = src.trim();
    while let Some(stripped) = src.strip_prefix("./") {
        src = stripped;
    }
    src.trim_end_matches('/').to_string()
}

/// Joins `path` onto `base` and lexically normalizes the result into an
/// absolute guest path. Absolute `path` values replace `base` entirely, which
/// is how Docker resolves both `WORKDIR` and `COPY` destinations.
pub(crate) fn resolve_guest_path(base: &str, path: &str) -> Result<String> {
    let joined = if path.starts_with('/') {
        path.to_string()
    } else {
        let base = if base.trim().is_empty() { "/" } else { base };
        if !base.starts_with('/') {
            bail!("cannot resolve '{path}' against non-absolute base '{base}'");
        }
        format!("{}/{}", base.trim_end_matches('/'), path)
    };

    let mut parts: Vec<&str> = Vec::new();
    for part in joined.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                if parts.pop().is_none() {
                    bail!("path '{path}' escapes the filesystem root");
                }
            }
            part => parts.push(part),
        }
    }
    Ok(format!("/{}", parts.join("/")))
}

fn join_abs(base: &str, rel: &str) -> String {
    if rel.is_empty() {
        base.to_string()
    } else if base == "/" {
        format!("/{rel}")
    } else {
        format!("{base}/{rel}")
    }
}

fn base_name(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

/// Normalizes one archive entry path and rejects escapes.
fn normalize_entry_path(raw: &Path) -> Result<String> {
    let mut parts: Vec<String> = Vec::new();
    for component in raw.components() {
        match component {
            std::path::Component::Normal(part) => {
                // Lossy conversion would collapse distinct non-UTF-8 names
                // onto one replacement-character name, silently overwriting.
                let Some(part) = part.to_str() else {
                    bail!(
                        "non-UTF-8 path component in archive entry '{}'",
                        raw.display()
                    );
                };
                parts.push(part.to_string());
            }
            std::path::Component::CurDir => {}
            other => bail!(
                "unsupported path component {:?} in archive entry '{}'",
                other,
                raw.display()
            ),
        }
    }
    if parts.is_empty() {
        bail!("empty path in archive entry");
    }
    Ok(parts.join("/"))
}

/// The uploaded archive, opened once and read twice.
///
/// Holding the handle across both passes means the two passes provably see the
/// same inode: a concurrent unlink or replacement of the path cannot make the
/// second pass read a different archive than the one the mapping was built
/// from.
struct SourceArchive {
    file: File,
    gzip: bool,
    /// Hard cap on the bytes any one pass may pull out of the reader.
    budget: u64,
}

impl SourceArchive {
    fn open(source_tar: &Path, max_total_bytes: u64) -> Result<Self> {
        let mut file = File::open(source_tar)
            .with_context(|| format!("open build context archive '{}'", source_tar.display()))?;
        let mut magic = [0u8; 2];
        let gzip = match file.read(&mut magic) {
            Ok(2) => magic == [0x1f, 0x8b],
            _ => false,
        };
        // Every indexed entry charges at least its own 512-byte header against
        // the payload budget, so the configured limit already bounds how many
        // entries an archive can hold.
        let max_entries = (max_total_bytes / 512 + 1).min(MAX_ARCHIVE_ENTRIES as u64);
        Ok(Self {
            file,
            gzip,
            // The tar crate buffers GNU long-name and PAX records whole before
            // any per-entry budget check can run, so the reader itself has to
            // be capped. The slack allows 1 KiB of framing for every entry the
            // budget can hold plus the end-of-archive terminator, which keeps
            // long-name-heavy archives acceptable while keeping the raw stream
            // proportional to the caller's payload budget at any setting.
            budget: max_total_bytes
                .saturating_add(max_entries.saturating_mul(1024))
                .saturating_add(1024),
        })
    }

    /// Starts one pass over the archive from offset 0.
    fn pass(&self) -> Result<tar::Archive<Box<dyn Read>>> {
        let mut file = self
            .file
            .try_clone()
            .context("reopen build context archive")?;
        file.seek(SeekFrom::Start(0))
            .context("rewind build context archive")?;

        let reader: Box<dyn Read> = if self.gzip {
            Box::new(flate2::read::GzDecoder::new(BufReader::new(file)).take(self.budget))
        } else {
            Box::new(BufReader::new(file).take(self.budget))
        };
        Ok(tar::Archive::new(reader))
    }
}

/// Context for a per-entry read failure.
///
/// The reader is capped, so an entry that declares more bytes than the budget
/// allows surfaces here as a truncated-archive error rather than an unbounded
/// allocation; naming the limit keeps that case diagnosable.
fn entry_read_context(max_total_bytes: u64) -> String {
    format!(
        "read build context archive entry; the archive must stay within the configured \
         limit of {max_total_bytes} bytes"
    )
}

fn check_entry_type(entry_type: tar::EntryType) -> Result<()> {
    match entry_type {
        tar::EntryType::Regular
        | tar::EntryType::Directory
        | tar::EntryType::Symlink
        | tar::EntryType::GNUSparse => Ok(()),
        // Metadata-only companion entries (long names, pax headers) are
        // consumed by the tar crate itself and never surface here.
        other => bail!("unsupported entry type {other:?} in build context archive"),
    }
}

/// First pass: index entry paths and enforce the archive budgets without
/// reading any file contents.
fn read_entry_index(source: &SourceArchive, max_total_bytes: u64) -> Result<Vec<EntryIndex>> {
    let mut archive = source.pass()?;
    let mut index = Vec::new();
    let mut total_bytes = 0u64;

    for entry in archive
        .entries()
        .context("read build context archive entries")?
    {
        let entry = entry.with_context(|| entry_read_context(max_total_bytes))?;
        let entry_type = entry.header().entry_type();
        check_entry_type(entry_type)?;

        // Count the entry's own header block and trailing padding: an archive
        // of many empty files still costs real bytes to stream.
        let padded_size = entry
            .size()
            .checked_next_multiple_of(512)
            .unwrap_or(u64::MAX);
        total_bytes = total_bytes.saturating_add(512).saturating_add(padded_size);
        if total_bytes > max_total_bytes {
            bail!(
                "build context archive expands beyond the configured limit of \
                 {max_total_bytes} bytes"
            );
        }
        if index.len() >= MAX_ARCHIVE_ENTRIES {
            bail!("build context archive holds more than {MAX_ARCHIVE_ENTRIES} entries");
        }

        index.push(EntryIndex {
            path: normalize_entry_path(&entry.path().context("entry path")?)?,
            is_dir: entry_type == tar::EntryType::Directory,
        });
    }

    if index.is_empty() {
        bail!("build context archive contains no files");
    }
    Ok(index)
}

/// Final guest paths for every indexed entry, plus what the guest still has to
/// do for the destination root itself.
struct MappedEntries {
    /// Positionally aligned with the entry index; `None` marks an entry the
    /// rewrite drops.
    targets: Vec<Option<String>>,
    /// Resolved absolute destination root.
    dest_root: String,
    /// Whether a directory entry for `dest_root` itself was dropped.
    skipped_dest_root: bool,
    /// Whether the copy treats `dest_root` as a directory.
    dest_is_dir: bool,
}

/// Computes the final absolute guest path for every indexed entry.
fn map_entries(
    index: &[EntryIndex],
    src: &str,
    dest_raw: &str,
    workdir: &str,
) -> Result<MappedEntries> {
    let src = normalize_src(src);
    let dest_is_dir_hint = dest_raw.ends_with('/')
        || dest_raw.ends_with("/.")
        || dest_raw == "."
        || dest_raw.is_empty();
    let dest = resolve_guest_path(workdir, if dest_raw.is_empty() { "." } else { dest_raw })?;

    let copy_whole_context = src.is_empty() || src == ".";
    // Docker gives a wildcard that resolves to exactly one regular file the
    // same destination semantics as a literal single-file source. A matched
    // directory always contributes its own member, so `!is_dir` is what keeps
    // directory sources on the recursive path.
    let single_file_src = !copy_whole_context
        && index.len() == 1
        && !index[0].is_dir
        && (index[0].path == src || glob_match(&src, &index[0].path));

    let mapped: Vec<String> = if single_file_src {
        vec![if dest_is_dir_hint {
            // The base name has to come from the resolved entry: the source
            // may be a pattern, which is never a valid path component.
            join_abs(&dest, base_name(&index[0].path))
        } else {
            dest.clone()
        }]
    } else if copy_whole_context || !is_glob_pattern(&src) {
        // Directory source: Docker copies the directory *contents* into dest.
        let mut mapped = Vec::with_capacity(index.len());
        for entry in index {
            let rel = if copy_whole_context {
                entry.path.as_str()
            } else if entry.path == src {
                ""
            } else if let Some(rel) = entry.path.strip_prefix(&format!("{src}/")) {
                rel
            } else {
                bail!(
                    "archive entry '{}' does not belong to COPY source '{}'",
                    entry.path,
                    src
                );
            };
            mapped.push(join_abs(&dest, rel));
        }
        mapped
    } else {
        // Glob source: every matched top-level item lands inside dest. Matched
        // files keep their base name; matched directories contribute their
        // contents (Docker treats each matched directory like a directory
        // source).
        let mut mapped = Vec::with_capacity(index.len());
        for entry in index {
            let mut components = entry.path.split('/');
            let mut prefix = String::new();
            let mut matched_root: Option<String> = None;
            for component in components.by_ref() {
                if prefix.is_empty() {
                    prefix.push_str(component);
                } else {
                    prefix.push('/');
                    prefix.push_str(component);
                }
                if glob_match(&src, &prefix) {
                    matched_root = Some(prefix.clone());
                    break;
                }
            }
            let Some(root) = matched_root else {
                bail!(
                    "archive entry '{}' does not match COPY source pattern '{}'",
                    entry.path,
                    src
                );
            };
            let rel = entry
                .path
                .strip_prefix(&root)
                .map(|rest| rest.trim_start_matches('/'))
                .unwrap_or("");
            mapped.push(if rel.is_empty() && !entry.is_dir {
                join_abs(&dest, base_name(&root))
            } else {
                join_abs(&dest, rel)
            });
        }
        mapped
    };

    // A directory source (and every glob-matched directory) maps its own root
    // onto dest. Emitting a header for it would make the guest's `tar -xp`
    // restore mode and ownership onto a pre-existing destination directory,
    // which a copy must never touch; the guest creates it instead when absent.
    let mut skipped_dest_root = false;
    let targets = mapped
        .into_iter()
        .zip(index)
        .map(|(target, entry)| {
            if entry.is_dir && target == dest {
                skipped_dest_root = true;
                None
            } else {
                Some(target)
            }
        })
        .collect();

    Ok(MappedEntries {
        targets,
        dest_root: dest,
        skipped_dest_root,
        // An explicit directory destination ("dest/") is a directory even for
        // a single-file copy, and the guest still has to create it when it is
        // missing: no archive member covers the destination root itself.
        dest_is_dir: !single_file_src || dest_is_dir_hint,
    })
}

/// Rewrites the SDK context archive into `output` with final absolute guest
/// paths, the requested ownership, and the optional mode override applied.
pub(crate) fn plan_copy_archive(request: &CopyRequest<'_>, output: &Path) -> Result<CopyPlan> {
    let source = SourceArchive::open(request.source_tar, request.max_total_bytes)?;
    let index = read_entry_index(&source, request.max_total_bytes)?;
    let mapped = map_entries(&index, request.src, request.dest, request.workdir)?;

    let out_file = File::create(output)
        .with_context(|| format!("create rewritten copy archive '{}'", output.display()))?;
    let mut builder = tar::Builder::new(out_file);
    let mut entry_count = 0usize;
    let mut total_bytes = 0u64;
    let mut seen = 0usize;

    // Second pass: stream each entry's bytes into the rewritten archive.
    let mut archive = source.pass()?;
    for entry in archive
        .entries()
        .context("read build context archive entries")?
    {
        let mut entry = entry.with_context(|| entry_read_context(request.max_total_bytes))?;
        let Some(target) = mapped.targets.get(seen) else {
            bail!("build context archive changed while it was being rewritten");
        };
        seen += 1;
        let Some(target) = target else {
            continue;
        };

        let relative = target.trim_start_matches('/');
        if relative.is_empty() {
            // The destination root itself ("/"); parents always exist.
            continue;
        }

        let entry_type = entry.header().entry_type();
        check_entry_type(entry_type)?;
        let link_name = entry
            .link_name()
            .context("entry link name")?
            .map(|link| link.into_owned());
        let mut header = entry.header().clone();
        let (uid, gid) = request
            .ownership
            .map_or((0, 0), |owner| (owner.uid, owner.gid));
        header.set_uid(uid);
        header.set_gid(gid);
        // Clear the name fields so the numeric ids above are authoritative.
        // GNU tar prefers uname/gname when they resolve in the target image,
        // so leaving the uploader's account names in place could hand files
        // to an unrelated guest account.
        header
            .set_username("")
            .and_then(|()| header.set_groupname(""))
            .with_context(|| format!("clear ownership names on entry '{target}'"))?;
        if let Some(mode) = request.mode {
            header.set_mode(mode);
        }

        match entry_type {
            tar::EntryType::Directory => {
                header.set_size(0);
                builder
                    .append_data(&mut header, format!("{relative}/"), std::io::empty())
                    .with_context(|| format!("write directory entry '{target}'"))?;
            }
            tar::EntryType::Symlink => {
                let link = link_name.context("symlink entry is missing its target")?;
                header.set_size(0);
                builder
                    .append_link(&mut header, relative, &link)
                    .with_context(|| format!("write symlink entry '{target}'"))?;
            }
            _ => {
                let size = entry.size();
                header.set_size(size);
                // A GNU sparse entry is read back expanded, so the rewritten
                // entry is a plain regular file.
                header.set_entry_type(tar::EntryType::Regular);
                builder
                    .append_data(&mut header, relative, &mut entry)
                    .with_context(|| format!("write file entry '{target}'"))?;
                total_bytes += size;
            }
        }
        entry_count += 1;
    }

    if seen != mapped.targets.len() {
        bail!("build context archive changed while it was being rewritten");
    }

    let mut out_file = builder.into_inner().context("finish rewritten archive")?;
    out_file.flush().context("flush rewritten archive")?;

    Ok(CopyPlan {
        entry_count,
        total_bytes,
        dest_root: mapped.dest_root,
        skipped_dest_root: mapped.skipped_dest_root,
        dest_is_dir: mapped.dest_is_dir,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use tempfile::TempDir;

    const NO_LIMIT: u64 = u64::MAX;

    fn request<'a>(
        source_tar: &'a Path,
        src: &'a str,
        dest: &'a str,
        workdir: &'a str,
    ) -> CopyRequest<'a> {
        CopyRequest {
            source_tar,
            src,
            dest,
            workdir,
            mode: None,
            ownership: None,
            max_total_bytes: NO_LIMIT,
        }
    }

    fn build_source_tar(dir: &Path, entries: &[(&str, Option<&str>)]) -> std::path::PathBuf {
        // (path, Some(contents)) = file, (path, None) = directory
        let tar_path = dir.join("source.tar");
        let file = File::create(&tar_path).expect("create tar");
        let mut builder = tar::Builder::new(file);
        for (path, contents) in entries {
            let mut header = tar::Header::new_gnu();
            header.set_uid(501);
            header.set_gid(20);
            match contents {
                Some(data) => {
                    header.set_entry_type(tar::EntryType::Regular);
                    header.set_mode(0o644);
                    header.set_size(data.len() as u64);
                    builder
                        .append_data(&mut header, path, data.as_bytes())
                        .expect("append file");
                }
                None => {
                    header.set_entry_type(tar::EntryType::Directory);
                    header.set_mode(0o755);
                    header.set_size(0);
                    builder
                        .append_data(&mut header, format!("{path}/"), std::io::empty())
                        .expect("append dir");
                }
            }
        }
        builder.finish().expect("finish tar");
        tar_path
    }

    struct Rewritten {
        kind: tar::EntryType,
        uid: u64,
        gid: u64,
        mode: u32,
        contents: String,
    }

    fn rewritten_entries(path: &Path) -> BTreeMap<String, Rewritten> {
        let mut archive = tar::Archive::new(File::open(path).expect("open rewritten"));
        let mut out = BTreeMap::new();
        for entry in archive.entries().expect("entries") {
            let mut entry = entry.expect("entry");
            let path = entry.path().expect("path").to_string_lossy().into_owned();
            let kind = entry.header().entry_type();
            let uid = entry.header().uid().expect("uid");
            let gid = entry.header().gid().expect("gid");
            let mode = entry.header().mode().expect("mode");
            let mut contents = String::new();
            entry.read_to_string(&mut contents).expect("read");
            out.insert(
                path,
                Rewritten {
                    kind,
                    uid,
                    gid,
                    mode,
                    contents,
                },
            );
        }
        out
    }

    #[test]
    fn single_file_to_absolute_file_dest() {
        let dir = TempDir::new().expect("tempdir");
        let tar = build_source_tar(dir.path(), &[("hello.txt", Some("hello\n"))]);
        let out = dir.path().join("out.tar");

        let plan =
            plan_copy_archive(&request(&tar, "hello.txt", "/hello.txt", "/"), &out).expect("plan");

        assert_eq!(plan.entry_count, 1);
        assert_eq!(plan.total_bytes, 6);
        assert!(
            !plan.dest_is_dir,
            "a single-file dest needs no directory preparation"
        );
        let entries = rewritten_entries(&out);
        let entry = &entries["hello.txt"];
        assert_eq!(entry.kind, tar::EntryType::Regular);
        assert_eq!(entry.uid, 0, "ownership must default to root");
        assert_eq!(entry.gid, 0);
        assert_eq!(entry.contents, "hello\n");
    }

    #[test]
    fn single_file_to_directory_dest() {
        let dir = TempDir::new().expect("tempdir");
        let tar = build_source_tar(dir.path(), &[("requirements.txt", Some("e2b\n"))]);
        let out = dir.path().join("out.tar");

        let plan = plan_copy_archive(&request(&tar, "requirements.txt", "/home/user/", "/"), &out)
            .expect("plan");

        assert_eq!(plan.dest_root, "/home/user");
        assert!(
            plan.dest_is_dir,
            "an explicit directory destination must be prepared by the guest"
        );
        assert!(rewritten_entries(&out).contains_key("home/user/requirements.txt"));
    }

    #[test]
    fn relative_dest_resolves_against_workdir() {
        let dir = TempDir::new().expect("tempdir");
        let tar = build_source_tar(dir.path(), &[("config.py", Some("x = 1\n"))]);
        let out = dir.path().join("out.tar");

        plan_copy_archive(&request(&tar, "config.py", "conf/app.py", "/srv"), &out).expect("plan");

        assert!(rewritten_entries(&out).contains_key("srv/conf/app.py"));
    }

    #[test]
    fn directory_source_copies_contents_into_dest() {
        let dir = TempDir::new().expect("tempdir");
        let tar = build_source_tar(
            dir.path(),
            &[
                ("app", None),
                ("app/main.py", Some("print()\n")),
                ("app/sub", None),
                ("app/sub/util.py", Some("pass\n")),
            ],
        );
        let out = dir.path().join("out.tar");

        let plan =
            plan_copy_archive(&request(&tar, "app", "/opt/service", "/"), &out).expect("plan");

        assert_eq!(plan.dest_root, "/opt/service");
        assert!(
            plan.skipped_dest_root,
            "the destination root must be left to the guest"
        );
        assert!(plan.dest_is_dir);
        let entries = rewritten_entries(&out);
        assert!(
            !entries.contains_key("opt/service/"),
            "a header for the destination root would rewrite its metadata"
        );
        assert!(entries.contains_key("opt/service/main.py"));
        assert!(entries.contains_key("opt/service/sub/"));
        assert!(entries.contains_key("opt/service/sub/util.py"));
    }

    #[test]
    fn whole_context_source_copies_everything() {
        let dir = TempDir::new().expect("tempdir");
        let tar = build_source_tar(
            dir.path(),
            &[
                ("a.txt", Some("a")),
                ("sub", None),
                ("sub/b.txt", Some("b")),
            ],
        );
        let out = dir.path().join("out.tar");

        plan_copy_archive(&request(&tar, ".", "/workspace", "/"), &out).expect("plan");

        let entries = rewritten_entries(&out);
        assert!(entries.contains_key("workspace/a.txt"));
        assert!(entries.contains_key("workspace/sub/b.txt"));
    }

    #[test]
    fn glob_source_places_matches_by_base_name() {
        let dir = TempDir::new().expect("tempdir");
        let tar = build_source_tar(
            dir.path(),
            &[("one.txt", Some("1")), ("two.txt", Some("2"))],
        );
        let out = dir.path().join("out.tar");

        let plan = plan_copy_archive(&request(&tar, "*.txt", "/data/", "/"), &out).expect("plan");

        assert_eq!(plan.entry_count, 2);
        let entries = rewritten_entries(&out);
        assert!(entries.contains_key("data/one.txt"));
        assert!(entries.contains_key("data/two.txt"));
    }

    #[test]
    fn glob_matching_one_file_renames_onto_a_file_dest() {
        let dir = TempDir::new().expect("tempdir");
        let tar = build_source_tar(dir.path(), &[("one.txt", Some("1"))]);
        let out = dir.path().join("out.tar");

        let plan =
            plan_copy_archive(&request(&tar, "*.txt", "/renamed.txt", "/"), &out).expect("plan");

        assert!(
            !plan.dest_is_dir,
            "a wildcard resolving to one file renames like a literal source"
        );
        assert!(rewritten_entries(&out).contains_key("renamed.txt"));
    }

    #[test]
    fn glob_matching_one_file_keeps_its_name_under_a_directory_dest() {
        let dir = TempDir::new().expect("tempdir");
        let tar = build_source_tar(dir.path(), &[("one.txt", Some("1"))]);
        let out = dir.path().join("out.tar");

        plan_copy_archive(&request(&tar, "*.txt", "/data/", "/"), &out).expect("plan");

        // The base name comes from the matched entry, never from the pattern.
        assert!(rewritten_entries(&out).contains_key("data/one.txt"));
    }

    #[test]
    fn glob_matching_directory_copies_its_contents() {
        let dir = TempDir::new().expect("tempdir");
        let tar = build_source_tar(
            dir.path(),
            &[
                ("pkg-a", None),
                ("pkg-a/lib.py", Some("a")),
                ("pkg-b", None),
                ("pkg-b/lib.py", Some("b")),
            ],
        );
        let out = dir.path().join("out.tar");

        let plan =
            plan_copy_archive(&request(&tar, "pkg-*", "/opt/pkgs", "/"), &out).expect("plan");

        // Docker merges contents of every matched directory into dest; the
        // second lib.py overwrites the first at extract time.
        let entries = rewritten_entries(&out);
        assert!(entries.contains_key("opt/pkgs/lib.py"));
        assert_eq!(plan.dest_root, "/opt/pkgs");
        assert!(plan.skipped_dest_root);
        assert!(!entries.contains_key("opt/pkgs/"));
    }

    #[test]
    fn mode_override_applies_to_entries() {
        let dir = TempDir::new().expect("tempdir");
        let tar = build_source_tar(dir.path(), &[("run.sh", Some("#!/bin/sh\n"))]);
        let out = dir.path().join("out.tar");

        let mut req = request(&tar, "run.sh", "/usr/local/bin/run.sh", "/");
        req.mode = Some(0o755);
        plan_copy_archive(&req, &out).expect("plan");

        assert_eq!(rewritten_entries(&out)["usr/local/bin/run.sh"].mode, 0o755);
    }

    #[test]
    fn ownership_is_written_into_entry_headers() {
        let dir = TempDir::new().expect("tempdir");
        let tar = build_source_tar(
            dir.path(),
            &[("app", None), ("app/main.py", Some("print()\n"))],
        );
        let out = dir.path().join("out.tar");

        let mut req = request(&tar, "app", "/opt/service", "/");
        req.ownership = Some(CopyOwnership {
            uid: 1000,
            gid: 2000,
        });
        plan_copy_archive(&req, &out).expect("plan");

        // Every created entry carries the requested ownership, and nothing
        // outside the archive can be affected.
        for entry in rewritten_entries(&out).values() {
            assert_eq!(entry.uid, 1000);
            assert_eq!(entry.gid, 2000);
        }
    }

    #[test]
    fn gzip_archives_are_accepted() {
        let dir = TempDir::new().expect("tempdir");
        let tar = build_source_tar(dir.path(), &[("hello.txt", Some("hi"))]);
        let gz_path = dir.path().join("source.tar.gz");
        let mut encoder = flate2::write::GzEncoder::new(
            File::create(&gz_path).expect("create gz"),
            flate2::Compression::fast(),
        );
        std::io::copy(&mut File::open(&tar).expect("open tar"), &mut encoder).expect("compress");
        encoder.finish().expect("finish gz");
        let out = dir.path().join("out.tar");

        let plan = plan_copy_archive(&request(&gz_path, "hello.txt", "/hello.txt", "/"), &out)
            .expect("plan");
        assert_eq!(plan.entry_count, 1);
    }

    #[test]
    fn rejects_archives_over_the_decompressed_budget() {
        let dir = TempDir::new().expect("tempdir");
        let tar = build_source_tar(dir.path(), &[("big.txt", Some("0123456789"))]);
        let out = dir.path().join("out.tar");

        let mut req = request(&tar, "big.txt", "/big.txt", "/");
        req.max_total_bytes = 4;
        let err = plan_copy_archive(&req, &out).expect_err("oversized archive must fail");
        assert!(err.to_string().contains("expands beyond"));
    }

    #[test]
    fn rejects_truncated_long_name_records() {
        let dir = TempDir::new().expect("tempdir");
        let inner = build_source_tar(dir.path(), &[("small.txt", Some("s"))]);

        // A GNU long-name record declaring far more bytes than it carries.
        // The tar crate buffers such a record whole; the capped reader bounds
        // that allocation and the overdeclared record fails as truncated
        // instead of being served the rest of the stream as name bytes. The
        // cap scales with the configured budget, so the 64 KiB limit below
        // bounds the allocation at KiB rather than MiB scale.
        let mut header = tar::Header::new_gnu();
        let long_link = b"././@LongLink";
        header.as_gnu_mut().expect("gnu header").name[..long_link.len()].copy_from_slice(long_link);
        header.set_entry_type(tar::EntryType::GNULongName);
        header.set_mode(0o644);
        header.set_size(0o77777777777);
        header.set_cksum();

        let mut bytes = Vec::new();
        bytes.extend_from_slice(header.as_bytes());
        bytes.extend_from_slice(&[0u8; 512]);
        bytes.extend_from_slice(&std::fs::read(&inner).expect("read inner tar"));
        let tar_path = dir.path().join("longname.tar");
        std::fs::write(&tar_path, &bytes).expect("write tar");
        let out = dir.path().join("out.tar");

        let mut req = request(&tar_path, "small.txt", "/small.txt", "/");
        req.max_total_bytes = 64 * 1024;
        let err = plan_copy_archive(&req, &out).expect_err("overdeclared long-name must fail");
        assert!(err.to_string().contains("configured limit"));
    }

    #[test]
    fn counts_entry_framing_against_the_budget() {
        let dir = TempDir::new().expect("tempdir");
        // Twenty empty files carry zero payload bytes but 512 bytes of tar
        // framing each, which the index pass must charge to the budget.
        let entries: Vec<(String, Option<&str>)> = (0..20)
            .map(|i| (format!("empty-{i}.txt"), Some("")))
            .collect();
        let entries: Vec<(&str, Option<&str>)> = entries
            .iter()
            .map(|(name, content)| (name.as_str(), *content))
            .collect();
        let tar = build_source_tar(dir.path(), &entries);
        let out = dir.path().join("out.tar");

        let mut req = request(&tar, ".", "/ctx/", "/");
        req.max_total_bytes = 4 * 1024;
        let err = plan_copy_archive(&req, &out).expect_err("framing must exhaust the budget");
        assert!(err.to_string().contains("expands beyond"));
    }

    #[test]
    fn rejects_entries_escaping_the_root() {
        let dir = TempDir::new().expect("tempdir");
        let tar_path = dir.path().join("evil.tar");
        let file = File::create(&tar_path).expect("create tar");
        let mut builder = tar::Builder::new(file);
        // `append_data` refuses to write `..` paths, so craft the header
        // manually the way a hostile client would.
        let mut header = tar::Header::new_gnu();
        let evil_path = b"../../etc/passwd";
        header.as_gnu_mut().expect("gnu header").name[..evil_path.len()].copy_from_slice(evil_path);
        header.set_entry_type(tar::EntryType::Regular);
        header.set_mode(0o644);
        header.set_size(4);
        header.set_cksum();
        builder
            .append(&header, "pwn\n".as_bytes())
            .expect("append raw entry");
        builder.finish().expect("finish");
        let out = dir.path().join("out.tar");

        let err = plan_copy_archive(&request(&tar_path, "passwd", "/tmp/x", "/"), &out)
            .expect_err("path escape must fail");
        assert!(err.to_string().contains("unsupported path component"));
    }

    #[test]
    fn rejects_non_utf8_entry_names() {
        let dir = TempDir::new().expect("tempdir");
        let tar_path = dir.path().join("latin1.tar");
        let file = File::create(&tar_path).expect("create tar");
        let mut builder = tar::Builder::new(file);
        let mut header = tar::Header::new_gnu();
        // A latin-1 name; lossy conversion would silently rename it and
        // collapse distinct names onto one replacement-character path.
        let raw_name = b"caf\xe9.txt";
        header.as_gnu_mut().expect("gnu header").name[..raw_name.len()].copy_from_slice(raw_name);
        header.set_entry_type(tar::EntryType::Regular);
        header.set_mode(0o644);
        header.set_size(1);
        header.set_cksum();
        builder
            .append(&header, "x".as_bytes())
            .expect("append raw entry");
        builder.finish().expect("finish");
        let out = dir.path().join("out.tar");

        let err = plan_copy_archive(&request(&tar_path, ".", "/ctx/", "/"), &out)
            .expect_err("non-UTF-8 entry name must fail");
        assert!(err.to_string().contains("non-UTF-8 path component"));
    }

    #[test]
    fn rejects_dest_escaping_the_root() {
        let dir = TempDir::new().expect("tempdir");
        let tar = build_source_tar(dir.path(), &[("a.txt", Some("a"))]);
        let out = dir.path().join("out.tar");

        let err = plan_copy_archive(&request(&tar, "a.txt", "../../x", "/"), &out)
            .expect_err("dest escape must fail");
        assert!(err.to_string().contains("escapes the filesystem root"));
    }

    #[test]
    fn rejects_empty_archive() {
        let dir = TempDir::new().expect("tempdir");
        let tar_path = dir.path().join("empty.tar");
        let file = File::create(&tar_path).expect("create tar");
        tar::Builder::new(file).finish().expect("finish");
        let out = dir.path().join("out.tar");

        let err = plan_copy_archive(&request(&tar_path, "x", "/x", "/"), &out)
            .expect_err("empty archive must fail");
        assert!(err.to_string().contains("no files"));
    }

    #[test]
    fn guest_path_resolution_follows_docker_semantics() {
        assert_eq!(
            resolve_guest_path("/srv", "app").expect("relative"),
            "/srv/app"
        );
        assert_eq!(
            resolve_guest_path("/srv/app", "/opt").expect("absolute"),
            "/opt"
        );
        assert_eq!(
            resolve_guest_path("/srv/app", "../lib").expect("parent"),
            "/srv/lib"
        );
        assert_eq!(resolve_guest_path("", "opt").expect("empty base"), "/opt");
        assert!(resolve_guest_path("relative", "app").is_err());
        assert!(resolve_guest_path("/", "../escape").is_err());
    }

    #[test]
    fn glob_match_basics() {
        assert!(glob_match("*.txt", "a.txt"));
        assert!(!glob_match("*.txt", "a.txt.bak"));
        assert!(glob_match("data?", "data1"));
        assert!(glob_match("[ab]*", "b12"));
        assert!(!glob_match("[!ab]*", "b12"));
        assert!(glob_match("pkg-*", "pkg-a"));
    }

    #[test]
    fn glob_wildcards_never_cross_a_separator() {
        assert!(!glob_match("*.txt", "sub/a.txt"));
        assert!(!glob_match("src?nested", "src/nested"));
        assert!(!glob_match("[sa]rc", "src/nested"));
        assert!(glob_match("src/*.rs", "src/main.rs"));
        assert!(!glob_match("src/*.rs", "src/nested/main.rs"));
        assert!(!glob_match("src/*", "src"));
    }

    #[test]
    fn glob_match_stays_polynomial_on_pathological_patterns() {
        // The previous recursive matcher took minutes on this input.
        assert!(!glob_match("*a*a*a*a*a*a*a*a*b", &"a".repeat(64)));
        assert!(glob_match(
            "*a*a*a*a*a*a*a*a*b",
            &format!("{}b", "a".repeat(64))
        ));
    }
}
