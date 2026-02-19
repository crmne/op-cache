use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
use std::process::Command;
use std::time::{Duration, Instant};

use crate::config::Config;
use crate::daemon;
use crate::error::Error;

const SPAWN_TIMEOUT: Duration = Duration::from_secs(5);
const POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Spawn the daemon if not already running
pub fn ensure_daemon_running(config: &Config) -> Result<(), Error> {
    config
        .ensure_runtime_dir_secure()
        .map_err(|e| Error::Internal(format!("failed to prepare runtime directory: {}", e)))?;

    if daemon::is_running(config) {
        return Ok(());
    }

    spawn_daemon()?;
    wait_for_daemon(config)
}

fn spawn_daemon() -> Result<(), Error> {
    let exe_path = std::env::current_exe()
        .map_err(|e| Error::SpawnFailed(format!("failed to get executable path: {}", e)))?;

    // Spawn the daemon process - it will fork and daemonize itself
    Command::new(exe_path)
        .arg("daemon")
        .spawn()
        .map_err(|e| Error::SpawnFailed(format!("failed to spawn daemon: {}", e)))?;

    Ok(())
}

fn wait_for_daemon(config: &Config) -> Result<(), Error> {
    let start = Instant::now();

    while start.elapsed() < SPAWN_TIMEOUT {
        if socket_is_secure(config) {
            if std::os::unix::net::UnixStream::connect(&config.socket_path).is_ok() {
                return Ok(());
            }
        }
        std::thread::sleep(POLL_INTERVAL);
    }

    Err(Error::DaemonStartTimeout)
}

fn socket_is_secure(config: &Config) -> bool {
    let metadata = match std::fs::symlink_metadata(&config.socket_path) {
        Ok(metadata) => metadata,
        Err(_) => return false,
    };

    if !metadata.file_type().is_socket() {
        return false;
    }

    let expected_uid = unsafe { libc::geteuid() };
    let mode = metadata.permissions().mode() & 0o777;
    metadata.uid() == expected_uid && (mode & 0o077) == 0
}
