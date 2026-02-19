use std::time::Duration;
use std::{io, os::fd::AsRawFd};

use tokio::net::UnixStream;
use tokio::process::Command;

use crate::config::Config;
use crate::error::Error;
use crate::protocol::{read_message, write_message, CacheStats, Request, Response};
use crate::spawn::ensure_daemon_running;

pub struct Client {
    config: Config,
}

impl Client {
    pub fn new(config: Config) -> Self {
        Self { config }
    }

    async fn connect(&self) -> Result<UnixStream, Error> {
        let stream = UnixStream::connect(&self.config.socket_path)
            .await
            .map_err(Error::ConnectionFailed)?;
        validate_peer_uid(&stream)?;
        Ok(stream)
    }

    async fn send_request(&self, request: Request) -> Result<Response, Error> {
        let mut stream = self.connect().await?;
        write_message(&mut stream, &request).await?;
        let response: Response = read_message(&mut stream).await?;
        Ok(response)
    }

    /// Hash the reference for use as cache key
    fn cache_key(reference: &str) -> String {
        let hash = blake3::hash(reference.as_bytes());
        hash.to_hex().to_string()
    }

    /// Read a secret, using cache if available
    pub async fn read(&self, reference: &str) -> Result<String, Error> {
        // Validate reference format
        if !reference.starts_with("op://") {
            return Err(Error::InvalidReference(format!(
                "reference must start with 'op://': {}",
                reference
            )));
        }

        // Ensure daemon is running
        ensure_daemon_running(&self.config)?;

        let key = Self::cache_key(reference);

        // Check cache first
        let response = self.send_request(Request::Get { key: key.clone() }).await?;

        match response {
            Response::Hit { value } => Ok(value),
            Response::Miss => {
                // Cache miss - execute op read ourselves
                let value = self.execute_op_read(reference).await?;

                // Store in cache
                let _ = self
                    .send_request(Request::Set {
                        key,
                        value: value.clone(),
                    })
                    .await;

                Ok(value)
            }
            _ => Err(Error::Protocol("unexpected response".to_string())),
        }
    }

    /// Execute op read command
    async fn execute_op_read(&self, reference: &str) -> Result<String, Error> {
        let timeout = Duration::from_secs(self.config.op_timeout_seconds);
        let output = tokio::time::timeout(
            timeout,
            Command::new(&self.config.op_path)
                .arg("read")
                .arg(reference)
                .output(),
        )
        .await
        .map_err(|_| {
            Error::OpFailed(format!(
                "op read timed out after {}s for {}",
                self.config.op_timeout_seconds, reference
            ))
        })?
        .map_err(|e| Error::OpFailed(format!("failed to execute op: {}", e)))?;

        if output.status.success() {
            let value = String::from_utf8_lossy(&output.stdout)
                .trim_end()
                .to_string();
            Ok(value)
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr);
            Err(Error::OpFailed(stderr.trim().to_string()))
        }
    }

    /// Get cache statistics
    pub async fn stats(&self) -> Result<CacheStats, Error> {
        ensure_daemon_running(&self.config)?;

        let response = self.send_request(Request::GetStats).await?;
        match response {
            Response::Stats(stats) => Ok(stats),
            _ => Err(Error::Protocol("unexpected response".to_string())),
        }
    }

    /// Clear the cache
    pub async fn clear(&self) -> Result<(), Error> {
        ensure_daemon_running(&self.config)?;

        let response = self.send_request(Request::ClearCache).await?;
        match response {
            Response::CacheCleared => Ok(()),
            _ => Err(Error::Protocol("unexpected response".to_string())),
        }
    }

    /// Stop the daemon
    pub async fn stop(&self) -> Result<(), Error> {
        let response = self.send_request(Request::Shutdown).await?;
        match response {
            Response::ShuttingDown => Ok(()),
            _ => Err(Error::Protocol("unexpected response".to_string())),
        }
    }
}

fn validate_peer_uid(stream: &UnixStream) -> Result<(), Error> {
    let mut creds = libc::ucred {
        pid: 0,
        uid: 0,
        gid: 0,
    };
    let mut len = std::mem::size_of::<libc::ucred>() as libc::socklen_t;
    let rc = unsafe {
        libc::getsockopt(
            stream.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_PEERCRED,
            &mut creds as *mut _ as *mut libc::c_void,
            &mut len,
        )
    };
    if rc != 0 {
        return Err(Error::ConnectionFailed(io::Error::last_os_error()));
    }

    let expected_uid = unsafe { libc::geteuid() };
    if creds.uid != expected_uid {
        return Err(Error::Internal(format!(
            "daemon peer uid mismatch: expected {}, got {}",
            expected_uid, creds.uid
        )));
    }

    Ok(())
}
