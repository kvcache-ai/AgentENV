use anyhow::Result;
use clap::Args;
use std::path::PathBuf;

use crate::config;
use crate::ensure_tool;
use crate::util;

#[derive(Args)]
pub struct CoverageArgs {
    /// Include ignored tests
    #[arg(long, default_value_t = false)]
    pub include_ignored: bool,

    /// Output directory for coverage artifacts
    #[arg(long)]
    pub output_dir: Option<String>,

    /// Regex for filenames to ignore
    #[arg(long)]
    pub ignore_filename_regex: Option<String>,
}

pub fn run(args: CoverageArgs) -> Result<()> {
    ensure_tool::ensure_cargo_tool("llvm-cov", "cargo-llvm-cov")?;
    ensure_tool::ensure_rustup_component("llvm-tools")?;

    if std::env::consts::OS != "linux" {
        anyhow::bail!("Coverage requires Linux");
    }

    let project_root = config::project_root()?;
    let output_dir = args
        .output_dir
        .map(PathBuf::from)
        .unwrap_or_else(|| project_root.join("target/coverage"));
    let summary_path = output_dir.join("summary.txt");
    let lcov_path = output_dir.join("lcov.info");
    let html_dir = output_dir.join("html");
    let badge_json_path = output_dir.join("badge.json");
    let badge_svg_path = output_dir.join("badge.svg");
    let metrics_json_path = output_dir.join("coverage.json");

    let ignore_regex = args.ignore_filename_regex.unwrap_or_else(|| {
        "(^/rustc/|.*/.cargo/registry/|.*/target/|.*/thirdparty/|.*/generated/)".into()
    });

    let include_ignored = args.include_ignored;

    let arch = util::detect_arch().unwrap_or_else(|_| "x86_64".into());
    let env_var = format!(
        "CARGO_TARGET_{}_UNKNOWN_LINUX_GNU_RUNNER",
        arch.to_uppercase()
    );
    if std::env::var_os(&env_var).is_none() {
        std::env::set_var(
            env_var,
            project_root.join("scripts/run-with-capabilities.sh"),
        );
    }

    std::fs::create_dir_all(&output_dir)?;

    util::info("Cleaning previous coverage artifacts");
    util::cmd("cargo", &["llvm-cov", "clean", "--workspace"])?;

    let mut cov_args: Vec<&str> = vec![
        "llvm-cov",
        "--workspace",
        "--exclude",
        "agentenv_http_server",
        "--exclude",
        "firecracker_client",
        "--exclude",
        "envd",
        "--exclude",
        "http_client",
        "--ignore-filename-regex",
        &ignore_regex,
    ];

    if include_ignored {
        cov_args.extend_from_slice(&["--", "--include-ignored"]);
    }

    util::info("Running workspace-wide coverage (all crates, excluding thirdparty/generated)");

    // Write test selection to step summary
    if let Ok(summary_file) = std::env::var("GITHUB_STEP_SUMMARY") {
        util::append_to_file(
            &summary_file,
            &format!(
                "## Coverage Test Selection\n\n\
                 - Scope: `workspace (all crates)`\n\
                 - Include ignored tests: `{}`\n",
                if include_ignored { "1" } else { "0" }
            ),
        );
    }

    util::cmd("cargo", &cov_args)?;

    util::info(&format!("Writing summary to {}", summary_path.display()));
    let summary_output = std::process::Command::new("cargo")
        .args([
            "llvm-cov",
            "report",
            "--ignore-filename-regex",
            &ignore_regex,
        ])
        .output()?;
    std::fs::write(&summary_path, &summary_output.stdout)?;

    util::info(&format!("Writing LCOV report to {}", lcov_path.display()));
    util::cmd(
        "cargo",
        &[
            "llvm-cov",
            "report",
            "--lcov",
            "--output-path",
            &lcov_path.to_string_lossy(),
            "--ignore-filename-regex",
            &ignore_regex,
        ],
    )?;

    util::info(&format!(
        "Writing HTML coverage report to {}",
        html_dir.display()
    ));
    util::cmd(
        "cargo",
        &[
            "llvm-cov",
            "report",
            "--html",
            "--output-dir",
            &html_dir.to_string_lossy(),
            "--ignore-filename-regex",
            &ignore_regex,
        ],
    )?;

    // Parse TOTAL line from summary
    let summary_content = std::fs::read_to_string(&summary_path).unwrap_or_default();
    let total_line = extract_total_line(&summary_content);
    let parsed = total_line.as_ref().and_then(|t| parse_total_fields(t));

    if let Some(ref p) = parsed {
        let formatted = format_total(p);
        util::info(&format!("Coverage total: {formatted}"));

        write_badge_json(&badge_json_path, &p.lines_cov)?;
        write_badge_svg(&badge_svg_path, &p.lines_cov)?;
        write_metrics_json(&metrics_json_path, p)?;
        util::info(&format!(
            "Wrote coverage badge JSON to {}",
            badge_json_path.display()
        ));
        util::info(&format!(
            "Wrote coverage badge SVG to {}",
            badge_svg_path.display()
        ));
        util::info(&format!(
            "Wrote coverage metrics JSON to {}",
            metrics_json_path.display()
        ));
    }

    // Write GitHub step summary
    if let Ok(summary_file) = std::env::var("GITHUB_STEP_SUMMARY") {
        let mut content = format!(
            "## Coverage (report-only)\n\n\
             - Scope: `workspace (all crates)`\n\
             - Include ignored tests: `{}`\n",
            if include_ignored { "1" } else { "0" }
        );
        if let Some(ref p) = parsed {
            content.push_str(&format!("- Total: `{}`\n", format_total(p)));
        }
        content.push_str(&format!("- Summary: `{}`\n", summary_path.display()));
        content.push_str(&format!("- Badge SVG: `{}`\n", badge_svg_path.display()));
        content.push_str(&format!("- LCOV: `{}`\n", lcov_path.display()));
        content.push_str(&format!("- HTML: `{}`\n\n", html_dir.display()));
        content.push_str("### Coverage Table\n\n");
        content.push_str(&emit_markdown_table(&summary_content));
        util::append_to_file(&summary_file, &content);
    }

    Ok(())
}

struct CoverageFields {
    regions_total: String,
    regions_missed: String,
    regions_cov: String,
    funcs_total: String,
    funcs_missed: String,
    funcs_cov: String,
    lines_total: String,
    lines_missed: String,
    lines_cov: String,
    branches_total: String,
    branches_missed: String,
    branches_cov: String,
}

fn strip_ansi(s: &str) -> String {
    use std::sync::LazyLock;
    static RE: LazyLock<regex::Regex> =
        LazyLock::new(|| regex::Regex::new(r"\x1B\[[0-9;]*[A-Za-z]").unwrap());
    RE.replace_all(s, "").to_string()
}

fn extract_total_line(content: &str) -> Option<String> {
    let mut last = None;
    for line in content.lines() {
        let clean = strip_ansi(line).trim().to_string();
        if clean.starts_with("TOTAL") {
            last = Some(clean);
        }
    }
    last
}

fn parse_total_fields(total_line: &str) -> Option<CoverageFields> {
    let parts: Vec<&str> = total_line.split_whitespace().collect();
    if parts.len() < 13 || parts[0] != "TOTAL" {
        return None;
    }
    Some(CoverageFields {
        regions_total: parts[1].into(),
        regions_missed: parts[2].into(),
        regions_cov: parts[3].into(),
        funcs_total: parts[4].into(),
        funcs_missed: parts[5].into(),
        funcs_cov: parts[6].into(),
        lines_total: parts[7].into(),
        lines_missed: parts[8].into(),
        lines_cov: parts[9].into(),
        branches_total: parts[10].into(),
        branches_missed: parts[11].into(),
        branches_cov: parts[12].into(),
    })
}

fn format_total(p: &CoverageFields) -> String {
    let regions_covered = sub_or_zero(&p.regions_total, &p.regions_missed);
    let funcs_covered = sub_or_zero(&p.funcs_total, &p.funcs_missed);
    let lines_covered = sub_or_zero(&p.lines_total, &p.lines_missed);

    if p.branches_cov == "-" {
        format!(
            "regions {}/{} ({}) | functions {}/{} ({}) | lines {}/{} ({}) | branches N/A",
            regions_covered,
            p.regions_total,
            p.regions_cov,
            funcs_covered,
            p.funcs_total,
            p.funcs_cov,
            lines_covered,
            p.lines_total,
            p.lines_cov
        )
    } else {
        let branches_covered = sub_or_zero(&p.branches_total, &p.branches_missed);
        format!(
            "regions {}/{} ({}) | functions {}/{} ({}) | lines {}/{} ({}) | branches {}/{} ({})",
            regions_covered,
            p.regions_total,
            p.regions_cov,
            funcs_covered,
            p.funcs_total,
            p.funcs_cov,
            lines_covered,
            p.lines_total,
            p.lines_cov,
            branches_covered,
            p.branches_total,
            p.branches_cov
        )
    }
}

fn sub_or_zero(total: &str, missed: &str) -> u64 {
    let t: u64 = total.parse().unwrap_or(0);
    let m: u64 = missed.parse().unwrap_or(0);
    t.saturating_sub(m)
}

fn badge_color(percent_str: &str) -> &'static str {
    let p: f64 = percent_str.trim_end_matches('%').parse().unwrap_or(0.0);
    if p >= 90.0 {
        "brightgreen"
    } else if p >= 80.0 {
        "green"
    } else if p >= 70.0 {
        "yellowgreen"
    } else if p >= 60.0 {
        "yellow"
    } else if p >= 50.0 {
        "orange"
    } else {
        "red"
    }
}

fn badge_fill(color: &str) -> &'static str {
    match color {
        "brightgreen" => "#4c1",
        "green" => "#97ca00",
        "yellowgreen" => "#a4a61d",
        "yellow" => "#dfb317",
        "orange" => "#fe7d37",
        _ => "#e05d44",
    }
}

fn write_badge_json(path: &std::path::Path, lines_cov: &str) -> Result<()> {
    let color = badge_color(lines_cov);
    let json = serde_json::json!({
        "label": "Coverage",
        "message": lines_cov,
        "schemaVersion": 1,
        "color": color,
    });
    std::fs::write(path, serde_json::to_string(&json)?)?;
    Ok(())
}

fn write_badge_svg(path: &std::path::Path, lines_cov: &str) -> Result<()> {
    let color = badge_color(lines_cov);
    let fill = badge_fill(color);
    let label = "Coverage";
    let label_width = 67;
    let value_width = std::cmp::max(lines_cov.len() * 7 + 16, 44);
    let total_width = label_width + value_width;
    let label_text_x = label_width / 2;
    let value_text_x = label_width + (value_width / 2);

    let svg = format!(
        r##"<svg xmlns="http://www.w3.org/2000/svg" width="{total_width}" height="20" role="img" aria-label="{label}: {lines_cov}">
  <linearGradient id="s" x2="0" y2="100%">
    <stop offset="0" stop-color="#fff" stop-opacity=".7"/>
    <stop offset=".1" stop-color="#aaa" stop-opacity=".1"/>
    <stop offset=".9" stop-opacity=".3"/>
    <stop offset="1" stop-opacity=".5"/>
  </linearGradient>
  <mask id="m">
    <rect width="{total_width}" height="20" rx="3" fill="#fff"/>
  </mask>
  <g mask="url(#m)">
    <rect width="{label_width}" height="20" fill="#555"/>
    <rect x="{label_width}" width="{value_width}" height="20" fill="{fill}"/>
    <rect width="{total_width}" height="20" fill="url(#s)"/>
  </g>
  <g fill="#fff" text-anchor="middle" font-family="DejaVu Sans,Verdana,Geneva,sans-serif" font-size="11" text-rendering="geometricPrecision">
    <text x="{label_text_x}" y="15" fill="#010101" fill-opacity=".3">{label}</text>
    <text x="{label_text_x}" y="14">{label}</text>
    <text x="{value_text_x}" y="15" fill="#010101" fill-opacity=".3">{lines_cov}</text>
    <text x="{value_text_x}" y="14">{lines_cov}</text>
  </g>
</svg>"##
    );
    std::fs::write(path, svg)?;
    Ok(())
}

fn write_metrics_json(path: &std::path::Path, p: &CoverageFields) -> Result<()> {
    let generated_at = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();

    let run_url = match (
        std::env::var("GITHUB_SERVER_URL"),
        std::env::var("GITHUB_REPOSITORY"),
        std::env::var("GITHUB_RUN_ID"),
    ) {
        (Ok(server), Ok(repo), Ok(run_id)) => {
            Some(format!("{server}/{repo}/actions/runs/{run_id}"))
        }
        _ => None,
    };

    let json = serde_json::json!({
        "generated_at": generated_at,
        "package": "workspace",
        "run_url": run_url,
        "regions": {
            "total": json_num(&p.regions_total),
            "missed": json_num(&p.regions_missed),
            "coverage": p.regions_cov,
        },
        "functions": {
            "total": json_num(&p.funcs_total),
            "missed": json_num(&p.funcs_missed),
            "coverage": p.funcs_cov,
        },
        "lines": {
            "total": json_num(&p.lines_total),
            "missed": json_num(&p.lines_missed),
            "coverage": p.lines_cov,
        },
        "branches": {
            "total": json_num(&p.branches_total),
            "missed": json_num(&p.branches_missed),
            "coverage": p.branches_cov,
        },
    });
    std::fs::write(path, serde_json::to_string_pretty(&json)?)?;
    Ok(())
}

fn json_num(s: &str) -> serde_json::Value {
    s.parse::<u64>()
        .map(serde_json::Value::from)
        .unwrap_or(serde_json::Value::Null)
}

fn emit_markdown_table(summary: &str) -> String {
    let mut table = String::new();
    table.push_str("| File | Regions | Missed Regions | Region Cov | Functions | Missed Functions | Func Cov | Lines | Missed Lines | Line Cov | Branches | Missed Branches | Branch Cov |\n");
    table.push_str("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n");

    for line in summary.lines() {
        let clean = strip_ansi(line).trim().to_string();
        if clean.is_empty() {
            continue;
        }
        let parts: Vec<&str> = clean.split_whitespace().collect();
        if parts.len() >= 13 && parts[0] != "Filename" && !parts[0].starts_with('-') {
            table.push_str(&format!(
                "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |\n",
                parts[0],
                parts[1],
                parts[2],
                parts[3],
                parts[4],
                parts[5],
                parts[6],
                parts[7],
                parts[8],
                parts[9],
                parts[10],
                parts[11],
                parts[12]
            ));
        }
    }
    table
}
