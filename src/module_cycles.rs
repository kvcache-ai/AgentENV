//! The crate's top-level modules must not depend on each other in a cycle.
//! The module graph is read from `lib.rs` and the `crate::` paths in each
//! module's files, so adding, renaming or removing a module needs no change
//! here.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

/// For each top-level module, the modules it uses and the first file that
/// uses each of them.
type ModuleGraph = BTreeMap<String, BTreeMap<String, String>>;

/// The top-level modules declared in `lib_rs`, other than this one.
fn declared_modules(lib_rs: &str) -> Vec<String> {
    lib_rs
        .lines()
        .filter_map(|line| {
            let line = line.trim();
            let line = match line.strip_prefix("pub") {
                Some(rest) if rest.starts_with('(') => rest.split_once(')')?.1,
                Some(rest) => rest,
                None => line,
            };
            line.trim_start().strip_prefix("mod ")?.strip_suffix(';')
        })
        .filter(|module| *module != "module_cycles")
        .map(str::to_owned)
        .collect()
}

/// The Rust files of a top-level module, without the separate generated
/// crates that live under `src/`.
fn module_files(src: &Path, module: &str) -> Vec<PathBuf> {
    fn walk(dir: &Path, files: &mut Vec<PathBuf>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                if path.file_name().is_some_and(|name| name != "generated") {
                    walk(&path, files);
                }
            } else if path.extension().is_some_and(|ext| ext == "rs") {
                files.push(path);
            }
        }
    }

    let mut files = Vec::new();
    let file = src.join(format!("{module}.rs"));
    if file.is_file() {
        files.push(file);
    }
    let dir = src.join(module);
    if dir.is_dir() {
        walk(&dir, &mut files);
    }
    files.sort();
    files
}

fn leading_ident(text: &str) -> &str {
    let end = text
        .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .unwrap_or(text.len());
    &text[..end]
}

/// The first segment of every `crate::` path in `source`, including each
/// item of a grouped `use crate::{a::X, b};` import. Doc links count too.
fn crate_references(source: &str) -> BTreeSet<&str> {
    let mut found = BTreeSet::new();
    let mut rest = source;
    while let Some(at) = rest.find("crate::") {
        rest = &rest[at + "crate::".len()..];
        let Some(group) = rest.strip_prefix('{') else {
            found.insert(leading_ident(rest));
            continue;
        };
        let mut depth = 0;
        let mut item_start = true;
        for (offset, c) in group.char_indices() {
            match c {
                '{' => depth += 1,
                '}' if depth == 0 => break,
                '}' => depth -= 1,
                ',' if depth == 0 => item_start = true,
                c if item_start && depth == 0 && (c.is_ascii_alphabetic() || c == '_') => {
                    found.insert(leading_ident(&group[offset..]));
                    item_start = false;
                }
                _ => {}
            }
        }
    }
    found
}

fn module_graph(src: &Path) -> ModuleGraph {
    let modules = declared_modules(&std::fs::read_to_string(src.join("lib.rs")).unwrap());
    let mut graph = ModuleGraph::new();
    for module in &modules {
        let uses = graph.entry(module.clone()).or_default();
        for file in module_files(src, module) {
            let source = std::fs::read_to_string(&file).unwrap();
            let file = file.strip_prefix(src).unwrap_or(&file);
            for used in crate_references(&source) {
                if used != module && modules.iter().any(|declared| declared == used) {
                    uses.entry(used.to_owned())
                        .or_insert_with(|| format!("src/{}", file.display()));
                }
            }
        }
    }
    graph
}

/// A cycle in `graph`, as the modules along it with the first one repeated
/// at the end.
fn find_cycle(graph: &ModuleGraph) -> Option<Vec<&str>> {
    fn visit<'a>(
        module: &'a str,
        graph: &'a ModuleGraph,
        done: &mut BTreeSet<&'a str>,
        path: &mut Vec<&'a str>,
    ) -> Option<Vec<&'a str>> {
        path.push(module);
        for used in graph.get(module).into_iter().flat_map(|uses| uses.keys()) {
            if let Some(start) = path.iter().position(|on_path| on_path == used) {
                let mut cycle = path[start..].to_vec();
                cycle.push(used);
                return Some(cycle);
            }
            if !done.contains(used.as_str()) {
                if let Some(cycle) = visit(used, graph, done, path) {
                    return Some(cycle);
                }
            }
        }
        path.pop();
        done.insert(module);
        None
    }

    let mut done = BTreeSet::new();
    for module in graph.keys() {
        if !done.contains(module.as_str()) {
            if let Some(cycle) = visit(module, graph, &mut done, &mut Vec::new()) {
                return Some(cycle);
            }
        }
    }
    None
}

#[test]
fn top_level_modules_have_no_dependency_cycle() {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let graph = module_graph(&src);
    if let Some(cycle) = find_cycle(&graph) {
        let edges: Vec<_> = cycle
            .windows(2)
            .map(|edge| format!("{} -> {} ({})", edge[0], edge[1], graph[edge[0]][edge[1]]))
            .collect();
        panic!("module dependency cycle; move the shared items down:\n{edges:#?}");
    }
}

#[test]
fn declared_modules_accept_any_visibility() {
    let lib_rs = "pub mod api;\nmod deps;\npub(crate) mod p2p;\npub(in crate) mod types;\n\
                  #[cfg(test)]\nmod module_cycles;\npub use api::Server;";
    assert_eq!(
        declared_modules(lib_rs),
        ["api", "deps", "p2p", "types"].map(str::to_owned)
    );
}

#[test]
fn crate_references_include_grouped_imports() {
    let source = "use crate::cfg::AppConfig;\n\
                  use crate::{\n    image::{cache, ImageResolver},\n    sandbox,\n};\n\
                  /// See [`X`][crate::orchestrator::X].";
    assert_eq!(
        crate_references(source),
        BTreeSet::from(["cfg", "image", "orchestrator", "sandbox"])
    );
}

#[test]
fn find_cycle_reports_the_modules_along_it() {
    let graph: ModuleGraph = [
        ("a", vec!["b"]),
        ("b", vec!["c"]),
        ("c", vec!["a", "d"]),
        ("d", vec![]),
    ]
    .into_iter()
    .map(|(module, uses)| {
        let uses = uses
            .into_iter()
            .map(|used| (used.to_owned(), String::new()))
            .collect();
        (module.to_owned(), uses)
    })
    .collect();
    assert_eq!(find_cycle(&graph), Some(vec!["a", "b", "c", "a"]));

    let mut acyclic = graph;
    acyclic.get_mut("c").unwrap().remove("a");
    assert_eq!(find_cycle(&acyclic), None);
}
