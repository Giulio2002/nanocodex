use std::{
    env, fs, io,
    io::Read as _,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
};

use nanocodex_vm::Gvproxy as GvproxyProcess;
use sha2::{Digest, Sha256};
use tempfile::TempDir;
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
    Process(#[from] nanocodex_vm::GvproxyError),

    #[error(transparent)]
    Io(#[from] io::Error),
}

/// One userspace network stack dedicated to one VM attempt.
pub(crate) struct Gvproxy {
    process: GvproxyProcess,
    _directory: TempDir,
}

impl Gvproxy {
    pub(crate) fn spawn(binary: &Path, log: &Path) -> Result<Self, GvproxyError> {
        Self::spawn_with(binary, log, GvproxyProcess::spawn_isolated)
    }

    pub(crate) fn spawn_inherited(binary: &Path, log: &Path) -> Result<Self, GvproxyError> {
        Self::spawn_with(binary, log, GvproxyProcess::spawn)
    }

    fn spawn_with(
        binary: &Path,
        log: &Path,
        spawn: impl FnOnce(&Path, &Path, &Path) -> Result<GvproxyProcess, nanocodex_vm::GvproxyError>,
    ) -> Result<Self, GvproxyError> {
        let directory = tempfile::Builder::new()
            .prefix("nanocodex-eval-gvproxy-")
            .tempdir()?;
        let process = spawn(binary, directory.path(), log)?;
        Ok(Self {
            process,
            _directory: directory,
        })
    }

    pub(crate) fn socket(&self) -> &Path {
        self.process.network_socket()
    }
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

#[cfg(test)]
mod tests {
    use std::{
        fs,
        os::unix::fs::PermissionsExt as _,
        path::{Path, PathBuf},
        process::Stdio,
        time::{Duration, Instant},
    };

    use nix::{
        sys::signal::{Signal, killpg},
        unistd::Pid,
    };

    use super::Gvproxy;

    const HELPER_DIRECTORY: &str = "NANOCODEX_GVPROXY_SIGNAL_HELPER_DIRECTORY";
    const HELPER_ISOLATED: &str = "NANOCODEX_GVPROXY_SIGNAL_HELPER_ISOLATED";

    #[tokio::test]
    async fn terminal_interrupt_reaches_preparation_but_not_attempt_gvproxy() {
        let inherited = run_terminal_interrupt_case(false).await;
        assert!(inherited.path().join("interrupted").is_file());
        assert!(!inherited.path().join("completed").exists());

        let isolated = run_terminal_interrupt_case(true).await;
        assert!(!isolated.path().join("interrupted").exists());
        assert!(isolated.path().join("completed").is_file());
    }

    async fn run_terminal_interrupt_case(isolated: bool) -> tempfile::TempDir {
        let directory = tempfile::tempdir().unwrap();
        let mut command = tokio::process::Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "eval::vm_network::tests::terminal_interrupt_helper",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(HELPER_DIRECTORY, directory.path())
            .env(HELPER_ISOLATED, if isolated { "1" } else { "0" })
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .process_group(0);
        let child = command.spawn().unwrap();
        let process_group = Pid::from_raw(i32::try_from(child.id().unwrap()).unwrap());
        wait_for_path(&directory.path().join("owner-ready")).await;
        killpg(process_group, Signal::SIGINT).unwrap();
        let output = tokio::time::timeout(Duration::from_secs(5), child.wait_with_output())
            .await
            .expect("signal helper did not exit")
            .unwrap();
        assert!(
            output.status.success(),
            "signal helper failed:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        directory
    }

    async fn wait_for_path(path: &Path) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !path.exists() {
            assert!(
                Instant::now() < deadline,
                "{} was not created",
                path.display()
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[tokio::test]
    async fn terminal_interrupt_helper() {
        let Some(directory) = std::env::var_os(HELPER_DIRECTORY).map(PathBuf::from) else {
            return;
        };
        let binary = directory.join("fake-gvproxy");
        fs::write(
            &binary,
            "#!/bin/sh\n\
             directory=$(CDPATH= cd -- \"$(dirname -- \"$0\")\" && pwd)\n\
             while [ \"$#\" -gt 0 ]; do\n\
               case \"$1\" in\n\
                 --listen-vfkit) network=${2#unixgram:}; shift 2 ;;\n\
                 --services) services=${2#unix://}; shift 2 ;;\n\
                 *) shift ;;\n\
               esac\n\
             done\n\
             trap 'printf interrupted > \"$directory/interrupted\"; exit 130' INT\n\
             : > \"$network\"\n\
             : > \"$services\"\n\
             sleep 1\n\
             printf completed > \"$directory/completed\"\n\
             exec /bin/sleep 30\n",
        )
        .unwrap();
        fs::set_permissions(&binary, fs::Permissions::from_mode(0o700)).unwrap();

        let interrupt = tokio::spawn(tokio::signal::ctrl_c());
        tokio::task::yield_now().await;
        let log = directory.join("gvproxy.log");
        let proxy = if std::env::var(HELPER_ISOLATED).unwrap() == "1" {
            Gvproxy::spawn(&binary, &log)
        } else {
            Gvproxy::spawn_inherited(&binary, &log)
        }
        .unwrap();
        fs::write(directory.join("owner-ready"), []).unwrap();
        interrupt.await.unwrap().unwrap();
        tokio::time::sleep(Duration::from_millis(1_250)).await;
        drop(proxy);
    }
}
