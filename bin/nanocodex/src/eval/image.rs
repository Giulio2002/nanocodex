use std::{io, path::Path};

use nanocodex_eval::Task;
use nanocodex_vm::image::{CachePolicy, ImageError, PreparedRootDisk, VmImageBuilder};

const BYTES_PER_MIB: u64 = 1024 * 1024;

pub(super) async fn prepare_task_image(
    builder: &VmImageBuilder,
    task: &Task,
    cache: &Path,
    policy: CachePolicy,
) -> Result<PreparedRootDisk, ImageError> {
    let context = tempfile::tempdir()?;
    task.materialize_environment(context.path())
        .map_err(io::Error::other)?;
    builder
        .prepare(
            context.path(),
            task.resources().storage_mb.saturating_mul(BYTES_PER_MIB),
            cache,
            policy,
        )
        .await
}

pub(super) async fn prepare_verifier_image(
    builder: &VmImageBuilder,
    task: &Task,
    cache: &Path,
    policy: CachePolicy,
) -> Result<PreparedRootDisk, ImageError> {
    let context = tempfile::tempdir()?;
    task.materialize_verifier_files(context.path())
        .map_err(io::Error::other)?;
    builder
        .prepare(
            context.path(),
            task.resources().storage_mb.saturating_mul(BYTES_PER_MIB),
            cache,
            policy,
        )
        .await
}
