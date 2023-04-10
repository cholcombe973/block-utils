use crate::{BlockResult, BlockUtilsError};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::process::Command;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct NvmeDevice {
    pub name_space: u64,
    pub device_path: String,
    pub index: Option<u64>,
    pub model_number: String,
    pub product_name: Option<String>,
    pub firmware: Option<String>,
    pub serial_number: String,
    pub used_bytes: u64,
    #[serde(rename = "MaximumLBA")]
    pub maximum_lba: u64,
    pub physical_size: u64,
    pub sector_size: u32,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct NvmeDeviceContainer {
    devices: Vec<NvmeDevice>,
}

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
pub fn list_nvme_devices() -> BlockResult<Vec<NvmeDevice>> {
    let out = Command::new("nvme")
        .args(&["list", "-o", "json"])
        .output()?;
    if !out.status.success() {
        return Err(BlockUtilsError::new(
            String::from_utf8_lossy(&out.stderr).into_owned(),
        ));
    }
    let stdout = String::from_utf8_lossy(&out.stdout);
    let deserialized: NvmeDeviceContainer = serde_json::from_str(&stdout)?;
    Ok(deserialized.devices)
}
