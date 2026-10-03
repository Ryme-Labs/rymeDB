use std::path::{Path, PathBuf};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let command = args.get(1).map(|s| s.as_str()).unwrap_or("help");
    match command {
        "check-no-comments" => {
            if let Err(e) = check_no_comments() {
                eprintln!("check-no-comments failed: {e}");
                std::process::exit(1);
            }
            println!("check-no-comments ok");
        }
        _ => {
            println!("usage: xtask check-no-comments");
        }
    }
}

fn check_no_comments() -> Result<(), String> {
    let root = workspace_root();
    let mut violations: Vec<String> = Vec::new();
    let mut files: Vec<PathBuf> = Vec::new();
    collect_files(&root.join("crates"), &mut files);
    collect_files(&root.join("xtask"), &mut files);
    for path in files {
        let Some(kind) = file_kind(&path) else {
            continue;
        };
        let raw =
            std::fs::read_to_string(&path).map_err(|e| format!("read {}: {e}", path.display()))?;
        for (index, line) in raw.lines().enumerate() {
            if violates(&kind, line) {
                violations.push(format!("{}:{}: {}", path.display(), index + 1, line.trim()));
            }
        }
    }
    if violations.is_empty() {
        Ok(())
    } else {
        for violation in violations.iter().take(50) {
            eprintln!("{violation}");
        }
        Err(format!("{} violations", violations.len()))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FileKind {
    Rust,
    Sql,
    Shell,
}

fn file_kind(path: &Path) -> Option<FileKind> {
    match path.extension().and_then(|e| e.to_str()) {
        Some("rs") => Some(FileKind::Rust),
        Some("sql") => Some(FileKind::Sql),
        Some("sh") => Some(FileKind::Shell),
        _ => None,
    }
}

fn violates(kind: &FileKind, line: &str) -> bool {
    let trimmed = line.trim_start();
    match kind {
        FileKind::Rust => {
            trimmed.starts_with("//") || trimmed.starts_with("/*") || trimmed.starts_with("*")
        }
        FileKind::Sql => trimmed.starts_with("--"),
        FileKind::Shell => {
            trimmed.starts_with("#") && trimmed != "#!/usr/bin/env bash" && trimmed != "#!/bin/bash"
        }
    }
}

fn workspace_root() -> PathBuf {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest.parent().and_then(|p| p.parent()).map(|p| p.to_path_buf()).unwrap_or(manifest)
}

fn collect_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if path.file_name().and_then(|n| n.to_str()) == Some("target") {
                continue;
            }
            collect_files(&path, out);
        } else if path.is_file() {
            out.push(path);
        }
    }
}
