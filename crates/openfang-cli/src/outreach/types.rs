use serde::{Deserialize, Serialize};
use serde_json::Value;
#[cfg(unix)]
use std::fs::File;
use std::path::PathBuf;

#[derive(Debug, Serialize)]
pub(super) struct OutreachCapture {
    pub status: String,
    pub platform_key: String,
    pub title: String,
    pub url: String,
    pub text: String,
    pub html: String,
    pub visible_actions: Vec<Value>,
    pub important_sections: Vec<Value>,
    pub reason: String,
    pub login_status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct DispatchOutput {
    pub status: String,
    pub adapter: String,
    pub summary: String,
    pub note: String,
    pub destination: String,
    pub external_url: String,
    pub cost_snapshot: Option<Value>,
}

pub(super) struct ProfileLock {
    pub path: PathBuf,
    #[cfg(unix)]
    pub file: File,
}

impl Drop for ProfileLock {
    fn drop(&mut self) {
        #[cfg(unix)]
        unsafe {
            use std::os::fd::AsRawFd;
            let _ = libc::flock(self.file.as_raw_fd(), libc::LOCK_UN);
        }
        let _ = std::fs::remove_file(&self.path);
    }
}
