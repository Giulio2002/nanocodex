use std::{
    fs, io,
    path::{Path, PathBuf},
};

use clap::Args;
use eyre::{Result, WrapErr, ensure};
use ignore::WalkBuilder;
use serde_json::Value;

const DISPOSABLE_VM_DISKS: [&str; 3] =
    ["rootfs.ext4", "verifier-rootfs.ext4", "verifier/cache.ext4"];

/// Remove disposable VM disks from completed retained trials.
#[derive(Args)]
pub(crate) struct Cleanup {
    /// Retained trial, job, or parent directory to clean recursively.
    #[arg(value_name = "DIRECTORY")]
    directory: PathBuf,

    /// Report what would be removed without changing the retained run.
    #[arg(long)]
    dry_run: bool,
}

impl Cleanup {
    pub(crate) fn run(self) -> Result<()> {
        ensure!(
            self.directory.is_dir(),
            "cleanup directory does not exist: {}",
            self.directory.display()
        );
        let report = cleanup_completed_vm_disks(&self.directory, self.dry_run)?;
        let action = if self.dry_run {
            "Would remove"
        } else {
            "Removed"
        };
        println!(
            "{action} {} disposable VM disk{} ({}) from {} completed trial{}",
            report.files,
            if report.files == 1 { "" } else { "s" },
            format_bytes(report.allocated_bytes),
            report.trials,
            if report.trials == 1 { "" } else { "s" },
        );
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct CleanupReport {
    trials: usize,
    files: usize,
    allocated_bytes: u64,
}

fn cleanup_completed_vm_disks(root: &Path, dry_run: bool) -> Result<CleanupReport> {
    let mut report = CleanupReport::default();
    for entry in WalkBuilder::new(root).hidden(false).build() {
        let entry = entry.wrap_err("walking retained run")?;
        if entry
            .file_type()
            .is_none_or(|file_type| !file_type.is_file())
            || entry.file_name() != "result.json"
            || !is_completed_trial_result(entry.path())?
        {
            continue;
        }
        let trial = entry
            .path()
            .parent()
            .ok_or_else(|| io::Error::other("trial result has no parent directory"))?;
        let mut removed_from_trial = false;
        for relative in DISPOSABLE_VM_DISKS {
            let disk = trial.join(relative);
            let Ok(metadata) = fs::symlink_metadata(&disk) else {
                continue;
            };
            if !metadata.file_type().is_file() {
                continue;
            }
            report.files += 1;
            report.allocated_bytes = report
                .allocated_bytes
                .saturating_add(allocated_bytes(&metadata));
            removed_from_trial = true;
            if !dry_run {
                fs::remove_file(&disk)
                    .wrap_err_with(|| format!("removing disposable disk {}", disk.display()))?;
            }
        }
        report.trials += usize::from(removed_from_trial);
    }
    Ok(report)
}

fn is_completed_trial_result(path: &Path) -> Result<bool> {
    let bytes =
        fs::read(path).wrap_err_with(|| format!("reading retained result {}", path.display()))?;
    let Ok(result) = serde_json::from_slice::<Value>(&bytes) else {
        return Ok(false);
    };
    Ok(result.get("task_name").is_some_and(Value::is_string)
        && result.get("trial_name").is_some_and(Value::is_string)
        && result.get("finished_at").is_some_and(Value::is_string))
}

#[cfg(unix)]
fn allocated_bytes(metadata: &fs::Metadata) -> u64 {
    use std::os::unix::fs::MetadataExt;

    metadata.blocks().saturating_mul(512)
}

#[cfg(not(unix))]
fn allocated_bytes(metadata: &fs::Metadata) -> u64 {
    metadata.len()
}

fn format_bytes(bytes: u64) -> String {
    const GIB: u64 = 1024 * 1024 * 1024;
    const MIB: u64 = 1024 * 1024;
    if bytes >= GIB {
        format_decimal_bytes(bytes, GIB, "GiB")
    } else {
        format_decimal_bytes(bytes, MIB, "MiB")
    }
}

fn format_decimal_bytes(bytes: u64, unit: u64, suffix: &str) -> String {
    let whole = bytes / unit;
    let tenths = bytes % unit * 10 / unit;
    format!("{whole}.{tenths} {suffix}")
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::cleanup_completed_vm_disks;

    #[test]
    fn removes_only_disks_from_completed_trials() {
        let root = tempfile::tempdir().unwrap();
        let completed = root.path().join("job/completed");
        let active = root.path().join("job/active");
        fs::create_dir_all(completed.join("verifier")).unwrap();
        fs::create_dir_all(active.join("verifier")).unwrap();
        fs::write(
            completed.join("result.json"),
            r#"{"task_name":"task","trial_name":"trial","finished_at":"now"}"#,
        )
        .unwrap();
        for trial in [&completed, &active] {
            fs::write(trial.join("rootfs.ext4"), b"root").unwrap();
            fs::write(trial.join("verifier/cache.ext4"), b"cache").unwrap();
        }

        let report = cleanup_completed_vm_disks(root.path(), false).unwrap();

        assert_eq!(report.trials, 1);
        assert_eq!(report.files, 2);
        assert!(!completed.join("rootfs.ext4").exists());
        assert!(!completed.join("verifier/cache.ext4").exists());
        assert!(active.join("rootfs.ext4").exists());
        assert!(active.join("verifier/cache.ext4").exists());
    }

    #[test]
    fn dry_run_preserves_completed_disks() {
        let root = tempfile::tempdir().unwrap();
        fs::write(
            root.path().join("result.json"),
            r#"{"task_name":"task","trial_name":"trial","finished_at":"now"}"#,
        )
        .unwrap();
        fs::write(root.path().join("rootfs.ext4"), b"root").unwrap();

        let report = cleanup_completed_vm_disks(root.path(), true).unwrap();

        assert_eq!(report.files, 1);
        assert!(root.path().join("rootfs.ext4").exists());
    }
}
