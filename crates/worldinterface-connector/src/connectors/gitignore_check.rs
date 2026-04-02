use std::path::{Path, PathBuf};

use ignore::gitignore::GitignoreBuilder;

/// Check whether a path is ignored by .gitignore rules.
pub fn is_gitignored(path: &Path) -> bool {
    let abs_path = match path.canonicalize() {
        Ok(p) => p,
        Err(_) => {
            if let Some(parent) = path.parent() {
                match parent.canonicalize() {
                    Ok(parent_abs) => parent_abs.join(path.file_name().unwrap_or_default()),
                    Err(_) => return false,
                }
            } else {
                return false;
            }
        }
    };

    let git_root = match find_git_root(&abs_path) {
        Some(root) => root,
        None => return false,
    };

    let mut builder = GitignoreBuilder::new(&git_root);
    load_gitignore_chain(&mut builder, &git_root, abs_path.parent().unwrap_or(&git_root));

    match builder.build() {
        Ok(gitignore) => {
            let is_dir = abs_path.is_dir();
            matches!(
                gitignore.matched_path_or_any_parents(&abs_path, is_dir),
                ignore::Match::Ignore(_)
            )
        }
        Err(_) => false,
    }
}

fn find_git_root(start: &Path) -> Option<PathBuf> {
    let mut current =
        if start.is_file() { start.parent()?.to_path_buf() } else { start.to_path_buf() };

    loop {
        if current.join(".git").exists() {
            return Some(current);
        }
        if !current.pop() {
            return None;
        }
    }
}

fn load_gitignore_chain(builder: &mut GitignoreBuilder, git_root: &Path, target_dir: &Path) {
    let root_ignore = git_root.join(".gitignore");
    if root_ignore.exists() {
        let _ = builder.add(root_ignore);
    }

    if let Ok(relative) = target_dir.strip_prefix(git_root) {
        let mut accumulated = git_root.to_path_buf();
        for component in relative.components() {
            accumulated.push(component);
            let nested_ignore = accumulated.join(".gitignore");
            if nested_ignore.exists() {
                let _ = builder.add(nested_ignore);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn init_git_repo(dir: &Path) {
        std::fs::create_dir(dir.join(".git")).unwrap();
    }

    #[test]
    fn gitignore_respects_simple_pattern() {
        let dir = tempfile::tempdir().unwrap();
        init_git_repo(dir.path());
        std::fs::write(dir.path().join(".gitignore"), "*.log\n").unwrap();
        let file = dir.path().join("debug.log");
        std::fs::write(&file, "x").unwrap();

        assert!(is_gitignored(&file));
    }

    #[test]
    fn gitignore_allows_unignored_files() {
        let dir = tempfile::tempdir().unwrap();
        init_git_repo(dir.path());
        std::fs::write(dir.path().join(".gitignore"), "*.log\n").unwrap();
        let file = dir.path().join("main.rs");
        std::fs::write(&file, "fn main() {}\n").unwrap();

        assert!(!is_gitignored(&file));
    }

    #[test]
    fn gitignore_respects_negation() {
        let dir = tempfile::tempdir().unwrap();
        init_git_repo(dir.path());
        std::fs::write(dir.path().join(".gitignore"), "*.log\n!important.log\n").unwrap();
        let file = dir.path().join("important.log");
        std::fs::write(&file, "x").unwrap();

        assert!(!is_gitignored(&file));
    }

    #[test]
    fn gitignore_respects_directory_pattern() {
        let dir = tempfile::tempdir().unwrap();
        init_git_repo(dir.path());
        std::fs::write(dir.path().join(".gitignore"), "target/\n").unwrap();
        std::fs::create_dir_all(dir.path().join("target")).unwrap();
        let file = dir.path().join("target").join("app");
        std::fs::write(&file, "x").unwrap();

        assert!(is_gitignored(&file));
    }

    #[test]
    fn gitignore_no_git_repo_allows_all() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("debug.log");
        std::fs::write(&file, "x").unwrap();

        assert!(!is_gitignored(&file));
    }

    #[test]
    fn gitignore_nested_gitignore() {
        let dir = tempfile::tempdir().unwrap();
        init_git_repo(dir.path());
        let nested = dir.path().join("src");
        std::fs::create_dir_all(&nested).unwrap();
        std::fs::write(nested.join(".gitignore"), "generated.rs\n").unwrap();
        let file = nested.join("generated.rs");
        std::fs::write(&file, "x").unwrap();

        assert!(is_gitignored(&file));
    }

    #[test]
    fn gitignore_nonexistent_path_checks_parent() {
        let dir = tempfile::tempdir().unwrap();
        init_git_repo(dir.path());
        std::fs::write(dir.path().join(".gitignore"), "ignored/\n").unwrap();
        std::fs::create_dir_all(dir.path().join("ignored")).unwrap();
        let file = dir.path().join("ignored").join("new.txt");

        assert!(is_gitignored(&file));
    }
}
