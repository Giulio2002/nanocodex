use std::{
    env, fs, io,
    io::Read as _,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
};

use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::process::Command;

const GVPROXY_VERSION: &str = "v0.8.9";

#[derive(Debug, Error)]
pub(crate) enum GvproxyError {
    #[error("NANOCODEX_EVAL_GVPROXY does not name a file: {0}")]
    InvalidOverride(PathBuf),

    #[error("gvproxy is not published for {os}/{architecture}")]
    UnsupportedPlatform {
        os: &'static str,
        architecture: &'static str,
    },

    #[error("failed to download gvproxy: curl exited with {0}")]
    Download(std::process::ExitStatus),

    #[error("downloaded gvproxy digest was {actual}, expected {expected}")]
    Digest {
        expected: &'static str,
        actual: String,
    },

    #[error(transparent)]
    Io(#[from] io::Error),
}

pub(crate) async fn prepare_gvproxy(cache: &Path) -> Result<PathBuf, GvproxyError> {
    let override_path = env::var_os("NANOCODEX_EVAL_GVPROXY")
        .or_else(|| env::var_os("NANOEVAL_GVPROXY"))
        .map(PathBuf::from);
    if let Some(path) = override_path {
        return path
            .is_file()
            .then_some(path.clone())
            .ok_or(GvproxyError::InvalidOverride(path));
    }
    if let Some(path) = find_on_path("gvproxy") {
        return Ok(path);
    }
    let artifact = gvproxy_artifact()?;
    let directory = cache.join("gvproxy").join(GVPROXY_VERSION);
    let binary = directory.join("gvproxy");
    if binary.is_file() && file_digest(&binary)? == artifact.digest {
        return Ok(binary);
    }
    fs::create_dir_all(&directory)?;
    let temporary = directory.join(format!("gvproxy.{}.tmp", std::process::id()));
    let status = Command::new("/usr/bin/curl")
        .arg("--fail")
        .arg("--location")
        .arg("--silent")
        .arg("--show-error")
        .arg("--output")
        .arg(&temporary)
        .arg(artifact.url)
        .status()
        .await?;
    if !status.success() {
        return Err(GvproxyError::Download(status));
    }
    let actual = file_digest(&temporary)?;
    if actual != artifact.digest {
        let _ = fs::remove_file(&temporary);
        return Err(GvproxyError::Digest {
            expected: artifact.digest,
            actual,
        });
    }
    fs::set_permissions(&temporary, fs::Permissions::from_mode(0o755))?;
    fs::rename(temporary, &binary)?;
    Ok(binary)
}

struct GvproxyArtifact {
    url: &'static str,
    digest: &'static str,
}

fn gvproxy_artifact() -> Result<GvproxyArtifact, GvproxyError> {
    let artifact = match (env::consts::OS, env::consts::ARCH) {
        ("macos", "aarch64" | "x86_64") => GvproxyArtifact {
            url: "https://github.com/containers/gvisor-tap-vsock/releases/download/v0.8.9/gvproxy-darwin",
            digest: "c6f7b4bc7f21bf810b5cf54e04d979b014c5d96472a03a9e97fe62a00940067c",
        },
        ("linux", "aarch64") => GvproxyArtifact {
            url: "https://github.com/containers/gvisor-tap-vsock/releases/download/v0.8.9/gvproxy-linux-arm64",
            digest: "6ecca02839254c9a0cc184bba7aac63755a22d7ed10d455b852528a99d7f7d4b",
        },
        ("linux", "x86_64") => GvproxyArtifact {
            url: "https://github.com/containers/gvisor-tap-vsock/releases/download/v0.8.9/gvproxy-linux-amd64",
            digest: "3011c5629c9138d2050fb23c510e09ae53e30ec52e6a9ab85632bc1550e8ef63",
        },
        (os, architecture) => {
            return Err(GvproxyError::UnsupportedPlatform { os, architecture });
        }
    };
    Ok(artifact)
}

fn find_on_path(name: &str) -> Option<PathBuf> {
    env::var_os("PATH")
        .into_iter()
        .flat_map(|path| env::split_paths(&path).collect::<Vec<_>>())
        .map(|directory| directory.join(name))
        .find(|path| path.is_file())
}

fn file_digest(path: &Path) -> io::Result<String> {
    let mut file = fs::File::open(path)?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(hex::encode(digest.finalize()))
}
