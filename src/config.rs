use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::Path;
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    #[serde(default = "default_socket_path")]
    pub socket_path: PathBuf,

    #[serde(default = "default_ttl_seconds")]
    pub ttl_seconds: u64,

    #[serde(default = "default_max_entries")]
    pub max_entries: u64,

    #[serde(default = "default_op_path")]
    pub op_path: String,

    #[serde(default = "default_op_timeout_seconds")]
    pub op_timeout_seconds: u64,
}

fn default_socket_path() -> PathBuf {
    default_runtime_dir().join("op-cache.sock")
}

fn default_runtime_dir() -> PathBuf {
    match std::env::var("XDG_RUNTIME_DIR") {
        Ok(dir) if !dir.trim().is_empty() => PathBuf::from(dir).join("op-cache"),
        _ => PathBuf::from(format!("/tmp/op-cache-{}", unsafe { libc::geteuid() })),
    }
}

fn default_ttl_seconds() -> u64 {
    86400 // 24 hours
}

fn default_max_entries() -> u64 {
    1000
}

fn default_op_path() -> String {
    "op".to_string()
}

fn default_op_timeout_seconds() -> u64 {
    30
}

impl Default for Config {
    fn default() -> Self {
        Self {
            socket_path: default_socket_path(),
            ttl_seconds: default_ttl_seconds(),
            max_entries: default_max_entries(),
            op_path: default_op_path(),
            op_timeout_seconds: default_op_timeout_seconds(),
        }
    }
}

impl Config {
    pub fn load() -> Result<Self> {
        let config_path = Self::config_path()?;

        if config_path.exists() {
            let contents = std::fs::read_to_string(&config_path)?;
            let config: Config = serde_yaml::from_str(&contents)?;
            Ok(config)
        } else {
            Ok(Config::default())
        }
    }

    pub fn config_path() -> Result<PathBuf> {
        if let Some(proj_dirs) = directories::ProjectDirs::from("", "", "op-cache") {
            Ok(proj_dirs.config_dir().join("config.yaml"))
        } else {
            Ok(PathBuf::from("~/.config/op-cache/config.yaml"))
        }
    }

    pub fn pid_path(&self) -> PathBuf {
        self.socket_path.with_extension("pid")
    }

    pub fn log_path(&self) -> PathBuf {
        self.socket_path.with_extension("log")
    }

    pub fn runtime_dir(&self) -> PathBuf {
        self.socket_path
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(default_runtime_dir)
    }

    pub fn ensure_runtime_dir_secure(&self) -> Result<()> {
        let dir = self.runtime_dir();
        std::fs::create_dir_all(&dir)?;

        let metadata = std::fs::symlink_metadata(&dir)?;
        if !metadata.is_dir() {
            anyhow::bail!("runtime path is not a directory: {:?}", dir);
        }

        let expected_uid = unsafe { libc::geteuid() };
        if metadata.uid() != expected_uid {
            anyhow::bail!(
                "runtime directory must be owned by uid {}: {:?}",
                expected_uid,
                dir
            );
        }

        let mode = metadata.permissions().mode() & 0o777;
        if mode != 0o700 {
            std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700))?;
        }

        Ok(())
    }
}
