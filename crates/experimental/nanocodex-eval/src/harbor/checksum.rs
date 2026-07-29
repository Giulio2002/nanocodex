use std::{
    collections::HashSet,
    fs::{self, File},
    io::{BufReader, Read as _},
    path::{Path, PathBuf},
};

use ignore::gitignore::{Gitignore, GitignoreBuilder};
use sha2::{Digest, Sha256};

use super::HarborError;

const PACKAGER_FILES: [&str; 3] = ["task.toml", "instruction.md", "README.md"];
const PACKAGER_DIRECTORIES: [&str; 4] = ["environment", "tests", "solution", "steps"];
const PACKAGER_DEFAULT_IGNORES: [&str; 6] =
    ["__pycache__/", "*.pyc", ".DS_Store", "*.swp", "*.swo", "*~"];

/// Matches `dirhash(directory, "sha256")`, which Harbor currently records as
/// `TrialResult.task_checksum`.
pub(crate) fn directory_hash(root: &Path) -> Result<String, HarborError> {
    let mut ancestors = HashSet::new();
    hash_directory(root, &mut ancestors)?.ok_or_else(|| HarborError::EmptyTask(root.to_path_buf()))
}

/// Matches Harbor `Packager.compute_content_hash`, whose publisher stores the
/// result in `TaskLock.digest` and sends it as `task_content_hash`.
pub(crate) fn packager_content_hash(root: &Path) -> Result<String, HarborError> {
    let root = fs::canonicalize(root)?;
    let files = collect_packager_files(&root)?;
    let mut outer = Sha256::new();
    for file in files {
        let relative = packager_relative_path(&root, &file)?;
        let file_hash = file_hex_digest(&file)?;
        outer.update(relative.as_bytes());
        outer.update(b"\0");
        outer.update(file_hash.as_bytes());
        outer.update(b"\n");
    }
    Ok(hex::encode(outer.finalize()))
}

fn collect_packager_files(root: &Path) -> Result<Vec<PathBuf>, HarborError> {
    let mut files = Vec::new();
    for name in PACKAGER_FILES {
        let file = root.join(name);
        if file.try_exists()? {
            files.push(file);
        }
    }
    for name in PACKAGER_DIRECTORIES {
        let directory = root.join(name);
        if directory.try_exists()? {
            collect_files_recursive(&directory, &mut files)?;
        }
    }

    let ignores = packager_ignores(root)?;
    files.retain(|file| !ignores.matched_path_or_any_parents(file, false).is_ignore());
    let mut normalized = files
        .into_iter()
        .map(|file| Ok((packager_relative_path(root, &file)?, file)))
        .collect::<Result<Vec<_>, HarborError>>()?;
    normalized.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    Ok(normalized.into_iter().map(|(_, file)| file).collect())
}

fn collect_files_recursive(directory: &Path, files: &mut Vec<PathBuf>) -> Result<(), HarborError> {
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let path = entry.path();
        let metadata = fs::metadata(&path)?;
        if metadata.is_dir() {
            collect_files_recursive(&path, files)?;
        } else if metadata.is_file() {
            files.push(path);
        }
    }
    Ok(())
}

fn packager_ignores(root: &Path) -> Result<Gitignore, HarborError> {
    let mut builder = GitignoreBuilder::new(root);
    let gitignore = root.join(".gitignore");
    if gitignore.try_exists()? {
        if let Some(error) = builder.add(gitignore) {
            return Err(error.into());
        }
    } else {
        for pattern in PACKAGER_DEFAULT_IGNORES {
            builder.add_line(None, pattern)?;
        }
    }
    Ok(builder.build()?)
}

fn packager_relative_path(root: &Path, file: &Path) -> Result<String, HarborError> {
    file.strip_prefix(root)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?
        .to_str()
        .map(|path| path.replace(std::path::MAIN_SEPARATOR, "/"))
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Harbor package path is not UTF-8: {}", file.display()),
            )
        })
        .map_err(Into::into)
}

fn file_hex_digest(path: &Path) -> Result<String, HarborError> {
    let mut file = BufReader::new(File::open(path)?);
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 1024 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(hex::encode(digest.finalize()))
}

fn hash_directory(
    directory: &Path,
    ancestors: &mut HashSet<PathBuf>,
) -> Result<Option<String>, HarborError> {
    let canonical = fs::canonicalize(directory)?;
    if !ancestors.insert(canonical) {
        return Err(HarborError::CyclicTaskDirectory(directory.to_path_buf()));
    }

    let result = (|| {
        let mut descriptors = Vec::new();
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            let path = entry.path();
            let metadata = fs::metadata(&path)?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if metadata.is_dir() {
                if let Some(hash) = hash_directory(&path, ancestors)? {
                    descriptors.push(format!("dirhash:{hash}\0name:{name}"));
                }
            } else if metadata.is_file() {
                let hash = hex_digest(&fs::read(path)?);
                descriptors.push(format!("data:{hash}\0name:{name}"));
            }
        }
        if descriptors.is_empty() {
            return Ok(None);
        }
        descriptors.sort();
        Ok(Some(hex_digest(descriptors.join("\0\0").as_bytes())))
    })();

    ancestors.remove(&fs::canonicalize(directory)?);
    result
}

fn hex_digest(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, fs, path::Path};

    use serde::Deserialize;
    use tempfile::tempdir;

    use super::{collect_packager_files, directory_hash, packager_content_hash};

    #[derive(Deserialize)]
    struct PackagerGolden {
        harbor_version: String,
        content_hash: String,
        files: Vec<String>,
        #[serde(default)]
        inputs: BTreeMap<String, String>,
    }

    #[test]
    fn matches_harbor_directory_hash_for_the_fixture() {
        let task = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../tasks/write-greeting");

        assert_eq!(
            directory_hash(&task).unwrap(),
            "eaa13434b21464b5a55c6a61b660c89fee3364084233f48311d29c143722390f"
        );
    }

    #[test]
    fn matches_harbor_packager_cross_language_golden() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../tasks/write-greeting");
        let root = fs::canonicalize(root).unwrap();
        let golden: PackagerGolden = serde_json::from_str(include_str!(
            "fixtures/packager-write-greeting-v0.18.1.json"
        ))
        .unwrap();

        assert_eq!(golden.harbor_version, "0.18.1.dev202607150126");
        assert_eq!(packager_content_hash(&root).unwrap(), golden.content_hash);
        assert_eq!(
            collect_packager_files(&root)
                .unwrap()
                .iter()
                .map(|file| {
                    file.strip_prefix(&root)
                        .unwrap()
                        .to_string_lossy()
                        .replace(std::path::MAIN_SEPARATOR, "/")
                })
                .collect::<Vec<_>>(),
            golden.files
        );
        assert_ne!(
            packager_content_hash(&root).unwrap(),
            directory_hash(&root).unwrap()
        );
    }

    #[test]
    fn matches_harbor_packager_gitignore_cross_language_golden() {
        let golden: PackagerGolden =
            serde_json::from_str(include_str!("fixtures/packager-ignore-v0.18.1.json")).unwrap();
        let root = tempdir().unwrap();
        for (relative, contents) in &golden.inputs {
            let path = root.path().join(relative);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, contents).unwrap();
        }

        assert_eq!(golden.harbor_version, "0.18.1.dev202607150126");
        assert_eq!(
            packager_content_hash(root.path()).unwrap(),
            golden.content_hash
        );
        assert_eq!(
            relative_files(root.path(), &collect_packager_files(root.path()).unwrap()),
            golden.files
        );
    }

    #[test]
    fn mirrors_packager_default_and_custom_ignore_selection() {
        let root = tempdir().unwrap();
        fs::create_dir_all(root.path().join("environment/__pycache__")).unwrap();
        fs::create_dir(root.path().join("tests")).unwrap();
        fs::write(root.path().join("task.toml"), "task\n").unwrap();
        fs::write(root.path().join("instruction.md"), "instruction\n").unwrap();
        fs::write(root.path().join("environment/keep.txt"), "keep\n").unwrap();
        fs::write(root.path().join("environment/drop.pyc"), "default\n").unwrap();
        fs::write(
            root.path().join("environment/__pycache__/drop.txt"),
            "default parent\n",
        )
        .unwrap();
        fs::write(root.path().join("tests/test.sh"), "test\n").unwrap();

        let default_files = collect_packager_files(root.path()).unwrap();
        assert_eq!(
            relative_files(root.path(), &default_files),
            [
                "environment/keep.txt",
                "instruction.md",
                "task.toml",
                "tests/test.sh",
            ]
        );

        fs::write(root.path().join(".gitignore"), "*.txt\n").unwrap();
        let custom_files = collect_packager_files(root.path()).unwrap();
        assert_eq!(
            relative_files(root.path(), &custom_files),
            [
                "environment/drop.pyc",
                "instruction.md",
                "task.toml",
                "tests/test.sh",
            ]
        );
    }

    fn relative_files(root: &Path, files: &[std::path::PathBuf]) -> Vec<String> {
        files
            .iter()
            .map(|file| {
                file.strip_prefix(root)
                    .unwrap()
                    .to_string_lossy()
                    .replace(std::path::MAIN_SEPARATOR, "/")
            })
            .collect()
    }
}
