use crate::{BlockResult, BlockUtilsError};
use std::path::Path;
use std::process::Command;

/// Retrieve the error logs from the nvme device
pub fn get_error_log(dev: &Path) -> BlockResult<String> {
    let out = Command::new("nvme")
        .args(&["error-log", &dev.to_string_lossy(), "-o", "json"])
        .output()?;
    if !out.status.success() {
        return Err(BlockUtilsError::new(
            String::from_utf8_lossy(&out.stderr).into_owned(),
        ));
    }
    let stdout = String::from_utf8_lossy(&out.stdout);
    let deserialized: String = serde_json::from_str(&stdout)?;
    Ok(deserialized)
}

/// Retrieve the firmware logs from the nvme device
pub fn get_firmware_log(dev: &Path) -> BlockResult<String> {
    let out = Command::new("nvme")
        .args(&["fw-log", &dev.to_string_lossy(), "-o", "json"])
        .output()?;
    if !out.status.success() {
        return Err(BlockUtilsError::new(
            String::from_utf8_lossy(&out.stderr).into_owned(),
        ));
    }
    let stdout = String::from_utf8_lossy(&out.stdout);
    let deserialized: String = serde_json::from_str(&stdout)?;
    Ok(deserialized)
}

/// Retrieve the smart logs from the nvme device
pub fn get_smart_log(dev: &Path) -> BlockResult<String> {
    let out = Command::new("nvme")
        .args(&["smart-log", &dev.to_string_lossy(), "-o", "json"])
        .output()?;
    if !out.status.success() {
        return Err(BlockUtilsError::new(
            String::from_utf8_lossy(&out.stderr).into_owned(),
        ));
    }
    let stdout = String::from_utf8_lossy(&out.stdout);
    let deserialized: String = serde_json::from_str(&stdout)?;
    Ok(deserialized)
}

// Format an nvme block device
pub fn format(dev: &Path) -> BlockResult<()> {
    let out = Command::new("nvme")
        .args(&["format", &dev.to_string_lossy()])
        .output()?;
    if !out.status.success() {
        return Err(BlockUtilsError::new(
            String::from_utf8_lossy(&out.stderr).into_owned(),
        ));
    }
    Ok(())
}

pub fn list_nvme_namespaces(dev: &Path) -> BlockResult<Vec<String>> {
    let out = Command::new("nvme")
        .args(&["list-ns", &dev.to_string_lossy(), "-o", "json"])
        .output()?;
    if !out.status.success() {
        return Err(BlockUtilsError::new(
            String::from_utf8_lossy(&out.stderr).into_owned(),
        ));
    }
    let stdout = String::from_utf8_lossy(&out.stdout);
    let deserialized: Vec<String> = serde_json::from_str(&stdout)?;
    Ok(deserialized)
}

/// List the nvme controllers on the host
pub fn list_nvme_controllers() -> BlockResult<Vec<String>> {
    let out = Command::new("nvme-list").args(&["-o", "json"]).output()?;
    if !out.status.success() {
        return Err(BlockUtilsError::new(
            String::from_utf8_lossy(&out.stderr).into_owned(),
        ));
    }
    let stdout = String::from_utf8_lossy(&out.stdout);
    let deserialized: Vec<String> = serde_json::from_str(&stdout)?;
    Ok(deserialized)
}

/// List the nvme devices on the host
pub fn list_nvme_devices() -> BlockResult<Vec<String>> {
    let out = Command::new("nvme-list").args(&["-o", "json"]).output()?;
    if !out.status.success() {
        return Err(BlockUtilsError::new(
            String::from_utf8_lossy(&out.stderr).into_owned(),
        ));
    }
    let stdout = String::from_utf8_lossy(&out.stdout);
    let deserialized: Vec<String> = serde_json::from_str(&stdout)?;
    Ok(deserialized)
}
