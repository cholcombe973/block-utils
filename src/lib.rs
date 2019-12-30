#[macro_use]
extern crate nom;

pub mod nvme;

use fstab::{FsEntry, FsTab};
use log::{debug, log, warn};
use nom::is_digit;
use regex::Regex;
use uuid::Uuid;

use std::error::Error as err;
use std::ffi::OsStr;
use std::fmt;
use std::fs::{self, read_dir, File};
use std::io::{BufRead, BufReader, Write};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output};
use std::str::{from_utf8, FromStr};

pub type BlockResult<T> = Result<T, BlockUtilsError>;

#[cfg(test)]
mod tests {
    use nix::unistd::{close, ftruncate};
    use tempdir::TempDir;

    use std::fs::File;
    use std::os::unix::io::IntoRawFd;

    #[test]
    fn test_create_xfs() {
        let tmp_dir = TempDir::new("block_utils").unwrap();
        let file_path = tmp_dir.path().join("xfs_device");
        let f = File::create(&file_path).expect("Failed to create file");
        let fd = f.into_raw_fd();
        // Create a sparse file of 100MB in size to test xfs creation
        ftruncate(fd, 104_857_600).unwrap();
        let xfs_options = super::Filesystem::Xfs {
            stripe_size: None,
            stripe_width: None,
            block_size: None,
            inode_size: Some(512),
            force: false,
            agcount: Some(32),
        };
        let result = super::format_block_device(&file_path, &xfs_options);
        println!("Result: {:?}", result);
        close(fd).expect("Failed to close file descriptor");
    }

    #[test]
    fn test_create_ext4() {
        let tmp_dir = TempDir::new("block_utils").unwrap();
        let file_path = tmp_dir.path().join("ext4_device");
        let f = File::create(&file_path).expect("Failed to create file");
        let fd = f.into_raw_fd();
        // Create a sparse file of 100MB in size to test ext creation
        ftruncate(fd, 104_857_600).unwrap();
        let xfs_options = super::Filesystem::Ext4 {
            inode_size: 512,
            stride: Some(2),
            stripe_width: None,
            reserved_blocks_percentage: 10,
        };
        let result = super::format_block_device(&file_path, &xfs_options);
        println!("Result: {:?}", result);
        close(fd).expect("Failed to close file descriptor");
    }
}

#[derive(Debug)]
pub enum BlockUtilsError {
    Error(String),
    IoError(::std::io::Error),
    ParseBoolError(::std::str::ParseBoolError),
    ParseIntError(::std::num::ParseIntError),
    SerdeError(serde_json::Error),
    UdevError(::udev::Error),
}

impl fmt::Display for BlockUtilsError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(self.description())
    }
}

impl err for BlockUtilsError {
    fn description(&self) -> &str {
        match *self {
            BlockUtilsError::Error(ref e) => &e,
            BlockUtilsError::IoError(ref e) => e.description(),
            BlockUtilsError::ParseBoolError(ref e) => e.description(),
            BlockUtilsError::ParseIntError(ref e) => e.description(),
            BlockUtilsError::SerdeError(ref e) => e.description(),
            BlockUtilsError::UdevError(ref e) => e.description(),
        }
    }
    fn source(&self) -> Option<&(dyn err + 'static)> {
        match *self {
            BlockUtilsError::Error(_) => None,
            BlockUtilsError::IoError(ref e) => e.source(),
            BlockUtilsError::ParseBoolError(ref e) => e.source(),
            BlockUtilsError::ParseIntError(ref e) => e.source(),
            BlockUtilsError::SerdeError(ref e) => e.source(),
            BlockUtilsError::UdevError(ref e) => e.source(),
        }
    }
}

impl BlockUtilsError {
    /// Create a new GlusterError with a String message
    fn new(err: String) -> BlockUtilsError {
        BlockUtilsError::Error(err)
    }
}

impl From<::std::io::Error> for BlockUtilsError {
    fn from(err: ::std::io::Error) -> BlockUtilsError {
        BlockUtilsError::IoError(err)
    }
}

impl From<::std::str::ParseBoolError> for BlockUtilsError {
    fn from(err: ::std::str::ParseBoolError) -> BlockUtilsError {
        BlockUtilsError::ParseBoolError(err)
    }
}

impl From<::std::num::ParseIntError> for BlockUtilsError {
    fn from(err: ::std::num::ParseIntError) -> BlockUtilsError {
        BlockUtilsError::ParseIntError(err)
    }
}

impl From<::udev::Error> for BlockUtilsError {
    fn from(err: ::udev::Error) -> BlockUtilsError {
        BlockUtilsError::UdevError(err)
    }
}

impl From<::serde_json::Error> for BlockUtilsError {
    fn from(err: ::serde_json::Error) -> BlockUtilsError {
        BlockUtilsError::SerdeError(err)
    }
}

// Formats a block device at Path p with XFS
/// This is used for formatting btrfs filesystems and setting the metadata profile
#[derive(Clone, Debug)]
pub enum MetadataProfile {
    Raid0,
    Raid1,
    Raid5,
    Raid6,
    Raid10,
    Single,
    Dup,
}

/// What raid card if any the system is using to serve disks
#[derive(Clone, Debug)]
pub enum Vendor {
    None,
    Cisco,
    Hp,
    Lsi,
    Qemu,
    Vbox, // Virtual Box
}

impl FromStr for Vendor {
    type Err = BlockUtilsError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "ATA" => Ok(Vendor::None),
            "CISCO" => Ok(Vendor::Cisco),
            "HP" => Ok(Vendor::Hp),
            "hp" => Ok(Vendor::Hp),
            "HPE" => Ok(Vendor::Hp),
            "LSI" => Ok(Vendor::Lsi),
            "QEMU" => Ok(Vendor::Qemu),
            "VBOX" => Ok(Vendor::Vbox),
            _ => Err(BlockUtilsError::new(format!("Unknown Vendor: {}", s))),
        }
    }
}

// This will be used to make intelligent decisions about setting up the device
/// Device information that is gathered with udev
#[derive(Clone, Debug)]
pub struct Device {
    pub id: Option<Uuid>,
    pub name: String,
    pub media_type: MediaType,
    pub device_type: DeviceType,
    pub capacity: u64,
    pub fs_type: FilesystemType,
    pub serial_number: Option<String>,
}

#[derive(Debug)]
pub struct AsyncInit {
    /// The child process needed for this device initializati
    /// This will be an async spawned Child handle
    pub format_child: Child,
    /// After formatting is complete run these commands to se
    /// ZFS needs this.  These should prob be run in sync mod
    pub post_setup_commands: Vec<(String, Vec<String>)>,
    /// The device we're initializing
    pub device: PathBuf,
}

#[derive(Debug)]
pub enum Scheduler {
    /// Try to balance latency and throughput
    Cfq,
    /// Latency is most important
    Deadline,
    /// Throughput is most important
    Noop,
}

impl fmt::Display for Scheduler {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let s = match *self {
            Scheduler::Cfq => "cfq",
            Scheduler::Deadline => "deadline",
            Scheduler::Noop => "noop",
        };
        write!(f, "{}", s)
    }
}

impl FromStr for Scheduler {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "cfq" => Ok(Scheduler::Cfq),
            "deadline" => Ok(Scheduler::Deadline),
            "noop" => Ok(Scheduler::Noop),
            _ => Err(format!("Unknown scheduler {}", s)),
        }
    }
}

/// What type of media has been detected.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MediaType {
    /// AKA SSD
    SolidState,
    /// Regular rotational device
    Rotational,
    /// Special loopback device
    Loopback,
    // Logical volume device
    LVM,
    // Software raid device
    MdRaid,
    // NVM Express
    NVME,
    // Ramdisk
    Ram,
    Virtual,
    Unknown,
}
/// What type of device has been detected.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeviceType {
    Disk,
    Partition,
    Unknown,
}

impl FromStr for DeviceType {
    type Err = BlockUtilsError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.to_lowercase();
        match s.as_ref() {
            "disk" => Ok(DeviceType::Disk),
            "partition" => Ok(DeviceType::Partition),
            _ => Ok(DeviceType::Unknown),
        }
    }
}

impl DeviceType {
    pub fn to_str(&self) -> &str {
        match *self {
            DeviceType::Disk => "disk",
            DeviceType::Partition => "partition",
            DeviceType::Unknown => "unknown",
        }
    }
    pub fn to_string(&self) -> String {
        match *self {
            DeviceType::Disk => "disk".to_string(),
            DeviceType::Partition => "partition".to_string(),
            DeviceType::Unknown => "unknown".to_string(),
        }
    }
}

/// What type of filesystem
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FilesystemType {
    Btrfs,
    Ext2,
    Ext3,
    Ext4,
    Lvm,
    Xfs,
    Zfs,
    Unknown,
}

impl FromStr for FilesystemType {
    type Err = BlockUtilsError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.to_lowercase();
        match s.as_ref() {
            "btrfs" => Ok(FilesystemType::Btrfs),
            "ext2" => Ok(FilesystemType::Ext2),
            "ext3" => Ok(FilesystemType::Ext3),
            "ext4" => Ok(FilesystemType::Ext4),
            "lvm2_member" => Ok(FilesystemType::Lvm),
            "xfs" => Ok(FilesystemType::Xfs),
            "zfs" => Ok(FilesystemType::Zfs),
            _ => Ok(FilesystemType::Unknown),
        }
    }
}

impl FilesystemType {
    pub fn to_str(&self) -> &str {
        match *self {
            FilesystemType::Btrfs => "btrfs",
            FilesystemType::Ext2 => "ext2",
            FilesystemType::Ext3 => "ext3",
            FilesystemType::Ext4 => "ext4",
            FilesystemType::Lvm => "lvm",
            FilesystemType::Xfs => "xfs",
            FilesystemType::Zfs => "zfs",
            FilesystemType::Unknown => "unknown",
        }
    }
    pub fn to_string(&self) -> String {
        match *self {
            FilesystemType::Btrfs => "btrfs".to_string(),
            FilesystemType::Ext2 => "ext2".to_string(),
            FilesystemType::Ext3 => "ext3".to_string(),
            FilesystemType::Ext4 => "ext4".to_string(),
            FilesystemType::Lvm => "lvm".to_string(),
            FilesystemType::Xfs => "xfs".to_string(),
            FilesystemType::Zfs => "zfs".to_string(),
            FilesystemType::Unknown => "unknown".to_string(),
        }
    }
}

impl MetadataProfile {
    pub fn to_string(&self) -> String {
        match self {
            MetadataProfile::Raid0 => "raid0".to_string(),
            MetadataProfile::Raid1 => "raid1".to_string(),
            MetadataProfile::Raid5 => "raid5".to_string(),
            MetadataProfile::Raid6 => "raid6".to_string(),
            MetadataProfile::Raid10 => "raid10".to_string(),
            MetadataProfile::Single => "single".to_string(),
            MetadataProfile::Dup => "dup".to_string(),
        }
    }
}

/// This allows you to tweak some settings when you're formatting the filesystem
#[derive(Debug)]
pub enum Filesystem {
    Btrfs {
        leaf_size: u64,
        metadata_profile: MetadataProfile,
        node_size: u64,
    },
    Ext4 {
        inode_size: u64,
        reserved_blocks_percentage: u8,
        stride: Option<u64>,
        stripe_width: Option<u64>,
    },
    Xfs {
        /// This is optional.  Boost knobs are on by default:
        /// http://xfs.org/index.php/XFS_FAQ#Q:_I_want_to_tune_my_XFS_filesystems_
        /// for_.3Csomething.3E
        block_size: Option<u64>, // Note this MUST be a power of 2
        force: bool,
        inode_size: Option<u64>,
        stripe_size: Option<u64>,  // RAID controllers stripe
        stripe_width: Option<u64>, // IE # of data disks
        agcount: Option<u64>,      // number of allocation  groups
    },
    Zfs {
        /// The default blocksize for volumes is 8 Kbytes. An
        /// power of 2 from 512 bytes to 128 Kbytes is valid.
        block_size: Option<u64>,
        /// Enable compression on the volume. Default is fals
        compression: Option<bool>,
    },
}

impl Filesystem {
    pub fn new(name: &str) -> Filesystem {
        match name.trim() {
            // Defaults.  Can be changed as needed by the caller
            "zfs" => Filesystem::Zfs {
                block_size: None,
                compression: None,
            },
            "xfs" => Filesystem::Xfs {
                stripe_size: None,
                stripe_width: None,
                block_size: None,
                inode_size: Some(512),
                force: false,
                agcount: Some(32),
            },
            "btrfs" => Filesystem::Btrfs {
                metadata_profile: MetadataProfile::Single,
                leaf_size: 32768,
                node_size: 32768,
            },
            "ext4" => Filesystem::Ext4 {
                inode_size: 512,
                reserved_blocks_percentage: 0,
                stride: None,
                stripe_width: None,
            },
            _ => Filesystem::Xfs {
                stripe_size: None,
                stripe_width: None,
                block_size: None,
                inode_size: None,
                force: false,
                agcount: None,
            },
        }
    }
}

fn run_command<S: AsRef<OsStr>>(command: &str, arg_list: &[S]) -> BlockResult<Output> {
    Ok(Command::new(command).args(arg_list).output()?)
}

/// Utility function to mount a device at a mount point
/// NOTE: This assumes the device is formatted at this point.  The mount
/// will fail if the device isn't formatted.
pub fn mount_device(device: &Device, mount_point: &Path) -> BlockResult<i32> {
    let mut arg_list: Vec<String> = Vec::new();
    match device.id {
        Some(id) => {
            arg_list.push("-U".to_string());
            arg_list.push(id.to_hyphenated().to_string());
        }
        None => {
            arg_list.push(format!("/dev/{}", device.name));
        }
    };
    arg_list.push(mount_point.to_string_lossy().into_owned());
    debug!("mount: {:?}", arg_list);

    process_output(&run_command("mount", &arg_list)?)
}

//Utility function to unmount a device at a mount point
pub fn unmount_device(mount_point: &Path) -> BlockResult<i32> {
    let mut arg_list: Vec<String> = Vec::new();
    arg_list.push(mount_point.to_string_lossy().into_owned());

    process_output(&run_command("umount", &arg_list)?)
}

/// Parse mtab and return the device which is mounted at a given directory
pub fn get_mount_device(mount_dir: &Path) -> BlockResult<Option<PathBuf>> {
    let dir = mount_dir.to_string_lossy().into_owned();
    let f = File::open("/etc/mtab")?;
    let reader = BufReader::new(f);

    for line in reader.lines() {
        let l = line?;
        if l.contains(&dir) {
            let parts: Vec<&str> = l.split_whitespace().collect();
            if !parts.is_empty() {
                return Ok(Some(PathBuf::from(parts[0])));
            }
        }
    }
    Ok(None)
}

/// Parse mtab and return all mounted block devices not including LVM
pub fn get_mounted_devices() -> BlockResult<Vec<Device>> {
    let mut results: Vec<Device> = Vec::new();

    let mtab_devices: Vec<FsEntry> = FsTab::new(Path::new("/etc/mtab"))
        .get_entries()?
        .into_iter()
        .filter(|d| d.fs_spec.contains("/dev/"))
        .filter(|d| !d.fs_spec.contains("mapper"))
        .collect();
    for d in mtab_devices {
        results.push(Device {
            id: None,
            name: Path::new(&d.fs_spec)
                .file_name()
                .unwrap_or_else(|| OsStr::new(""))
                .to_string_lossy()
                .into_owned(),
            media_type: MediaType::Unknown,
            device_type: DeviceType::Unknown,
            capacity: 0,
            fs_type: FilesystemType::from_str(&d.vfs_type)?,
            serial_number: None,
        });
    }

    Ok(results)
}

/// Parse mtab and return the mountpoint the device is mounted at.
/// This is the opposite of get_mount_device
pub fn get_mountpoint(device: &Path) -> BlockResult<Option<PathBuf>> {
    let s = device.to_string_lossy().into_owned();
    let f = File::open("/etc/mtab")?;
    let reader = BufReader::new(f);

    for line in reader.lines() {
        let l = line?;
        let parts: Vec<&str> = l.split_whitespace().collect();
        let mut index = -1;
        for (i, p) in parts.iter().enumerate() {
            if p == &s {
                index = i as i64;
            }
        }
        if index >= 0 {
            return Ok(Some(PathBuf::from(parts[1])));
        }
    }
    Ok(None)
}

fn process_output(output: &Output) -> BlockResult<i32> {
    if output.status.success() {
        Ok(0)
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
        Err(BlockUtilsError::new(stderr))
    }
}

pub fn erase_block_device(device: &Path) -> BlockResult<()> {
    let output = Command::new("sgdisk")
        .args(&["--zap", &device.to_string_lossy()])
        .output()?;
    if output.status.success() {
        Ok(())
    } else {
        Err(BlockUtilsError::new(format!(
            "Disk {:?} failed to erase: {}",
            device,
            String::from_utf8_lossy(&output.stderr)
        )))
    }
}

/// Synchronous utility to format a block device with a given filesystem.
/// Note: ZFS creation can be slow because there's potentially several commands that need to
/// be run.  async_format_block_device will be faster if you have many block devices to format
pub fn format_block_device(device: &Path, filesystem: &Filesystem) -> BlockResult<i32> {
    match *filesystem {
        Filesystem::Btrfs {
            ref metadata_profile,
            ref leaf_size,
            ref node_size,
        } => {
            let mut arg_list: Vec<String> = Vec::new();

            arg_list.push("-m".to_string());
            arg_list.push(metadata_profile.clone().to_string());

            arg_list.push("-l".to_string());
            arg_list.push(leaf_size.to_string());

            arg_list.push("-n".to_string());
            arg_list.push(node_size.to_string());

            arg_list.push(device.to_string_lossy().to_string());
            // Check if mkfs.btrfs is installed
            if !Path::new("/sbin/mkfs.btrfs").exists() {
                return Err(BlockUtilsError::new(
                    "Please install btrfs-tools".to_string(),
                ));
            }
            process_output(&run_command("mkfs.btrfs", &arg_list)?)
        }
        Filesystem::Xfs {
            ref inode_size,
            ref force,
            ref block_size,
            ref stripe_size,
            ref stripe_width,
            ref agcount,
        } => {
            let mut arg_list: Vec<String> = Vec::new();

            if let Some(b) = block_size {
                /*
                From XFS man page:
                The default value is 4096 bytes (4 KiB), the minimum  is
                512,  and the maximum is 65536 (64 KiB).  XFS on Linux currently
                only supports pagesize or smaller blocks.
                */
                let b: u64 = if *b < 512 {
                    warn!("xfs block size must be 512 bytes minimum.  Correcting");
                    512
                } else if *b > 65536 {
                    warn!("xfs block size must be 65536 bytes maximum.  Correcting");
                    65536
                } else {
                    *b
                };
                arg_list.push("-b".to_string());
                arg_list.push(format!("size={}", b));
            }

            if (*inode_size).is_some() {
                arg_list.push("-i".to_string());
                arg_list.push(format!("size={}", inode_size.unwrap()));
            }

            if *force {
                arg_list.push("-f".to_string());
            }

            if (*stripe_size).is_some() && (*stripe_width).is_some() {
                arg_list.push("-d".to_string());
                arg_list.push(format!("su={}", stripe_size.unwrap()));
                arg_list.push(format!("sw={}", stripe_width.unwrap()));
                if (*agcount).is_some() {
                    arg_list.push(format!("agcount={}", agcount.unwrap()));
                }
            }
            arg_list.push(device.to_string_lossy().to_string());

            // Check if mkfs.xfs is installed
            if !Path::new("/sbin/mkfs.xfs").exists() {
                return Err(BlockUtilsError::new("Please install xfsprogs".into()));
            }
            process_output(&run_command("/sbin/mkfs.xfs", &arg_list)?)
        }
        Filesystem::Ext4 {
            ref inode_size,
            ref reserved_blocks_percentage,
            ref stride,
            ref stripe_width,
        } => {
            let mut arg_list: Vec<String> = Vec::new();

            if stride.is_some() || stripe_width.is_some() {
                arg_list.push("-E".to_string());
                if let Some(stride) = stride {
                    arg_list.push(format!("stride={}", stride));
                }
                if let Some(stripe_width) = stripe_width {
                    arg_list.push(format!(",stripe_width={}", stripe_width));
                }
            }

            arg_list.push("-I".to_string());
            arg_list.push(inode_size.to_string());

            arg_list.push("-m".to_string());
            arg_list.push(reserved_blocks_percentage.to_string());

            arg_list.push(device.to_string_lossy().to_string());

            process_output(&run_command("mkfs.ext4", &arg_list)?)
        }
        Filesystem::Zfs {
            ref block_size,
            ref compression,
        } => {
            // Check if zfs is installed
            if !Path::new("/sbin/zfs").exists() {
                return Err(BlockUtilsError::new("Please install zfsutils-linux".into()));
            }
            let base_name = device.file_name();
            match base_name {
                Some(name) => {
                    //Mount at /mnt/{dev_name}
                    let arg_list: Vec<String> = vec![
                        "create".to_string(),
                        "-f".to_string(),
                        "-m".to_string(),
                        format!("/mnt/{}", name.to_string_lossy().into_owned()),
                        name.to_string_lossy().into_owned(),
                        device.to_string_lossy().into_owned(),
                    ];
                    // Create the zpool
                    let _ = process_output(&run_command("/sbin/zpool", &arg_list)?)?;
                    if block_size.is_some() {
                        // If zpool creation is successful then we set these
                        let _ = process_output(&run_command(
                            "/sbin/zfs",
                            &[
                                "set".to_string(),
                                format!("recordsize={}", block_size.unwrap()),
                                name.to_string_lossy().into_owned(),
                            ],
                        )?)?;
                    }
                    if compression.is_some() {
                        let _ = process_output(&run_command(
                            "/sbin/zfs",
                            &[
                                "set".to_string(),
                                "compression=on".to_string(),
                                name.to_string_lossy().into_owned(),
                            ],
                        )?)?;
                    }
                    let _ = process_output(&run_command(
                        "/sbin/zfs",
                        &[
                            "set".to_string(),
                            "acltype=posixacl".to_string(),
                            name.to_string_lossy().into_owned(),
                        ],
                    )?)?;
                    let _ = process_output(&run_command(
                        "/sbin/zfs",
                        &[
                            "set".to_string(),
                            "atime=off".to_string(),
                            name.to_string_lossy().into_owned(),
                        ],
                    )?)?;
                    Ok(0)
                }
                None => Err(BlockUtilsError::new(format!(
                    "Unable to determine filename for device: {:?}",
                    device
                ))),
            }
        }
    }
}

pub fn async_format_block_device(
    device: &PathBuf,
    filesystem: &Filesystem,
) -> BlockResult<AsyncInit> {
    match *filesystem {
        Filesystem::Btrfs {
            ref metadata_profile,
            ref leaf_size,
            ref node_size,
        } => {
            let arg_list: Vec<String> = vec![
                "-m".to_string(),
                metadata_profile.clone().to_string(),
                "-l".to_string(),
                leaf_size.to_string(),
                "-n".to_string(),
                node_size.to_string(),
                device.to_string_lossy().to_string(),
            ];
            // Check if mkfs.btrfs is installed
            if !Path::new("/sbin/mkfs.btrfs").exists() {
                return Err(BlockUtilsError::new("Please install btrfs-tools".into()));
            }
            Ok(AsyncInit {
                format_child: Command::new("mkfs.btrfs").args(&arg_list).spawn()?,
                post_setup_commands: vec![],
                device: device.clone(),
            })
        }
        Filesystem::Xfs {
            ref block_size,
            ref inode_size,
            ref stripe_size,
            ref stripe_width,
            ref force,
            ref agcount,
        } => {
            let mut arg_list: Vec<String> = Vec::new();

            if (*inode_size).is_some() {
                arg_list.push("-i".to_string());
                arg_list.push(format!("size={}", inode_size.unwrap()));
            }

            if *force {
                arg_list.push("-f".to_string());
            }

            if let Some(b) = block_size {
                arg_list.push("-b".to_string());
                arg_list.push(b.to_string());
            }

            if (*stripe_size).is_some() && (*stripe_width).is_some() {
                arg_list.push("-d".to_string());
                arg_list.push(format!("su={}", stripe_size.unwrap()));
                arg_list.push(format!("sw={}", stripe_width.unwrap()));
                if (*agcount).is_some() {
                    arg_list.push(format!("agcount={}", agcount.unwrap()));
                }
            }

            arg_list.push(device.to_string_lossy().to_string());

            // Check if mkfs.xfs is installed
            if !Path::new("/sbin/mkfs.xfs").exists() {
                return Err(BlockUtilsError::new("Please install xfsprogs".into()));
            }
            let format_handle = Command::new("/sbin/mkfs.xfs").args(&arg_list).spawn()?;
            Ok(AsyncInit {
                format_child: format_handle,
                post_setup_commands: vec![],
                device: device.clone(),
            })
        }
        Filesystem::Zfs {
            ref block_size,
            ref compression,
        } => {
            // Check if zfs is installed
            if !Path::new("/sbin/zfs").exists() {
                return Err(BlockUtilsError::new("Please install zfsutils-linux".into()));
            }
            let base_name = device.file_name();
            match base_name {
                Some(name) => {
                    //Mount at /mnt/{dev_name}
                    let mut post_setup_commands: Vec<(String, Vec<String>)> = Vec::new();
                    let arg_list: Vec<String> = vec![
                        "create".to_string(),
                        "-f".to_string(),
                        "-m".to_string(),
                        format!("/mnt/{}", name.to_string_lossy().into_owned()),
                        name.to_string_lossy().into_owned(),
                        device.to_string_lossy().into_owned(),
                    ];
                    let zpool_create = Command::new("/sbin/zpool").args(&arg_list).spawn()?;

                    if block_size.is_some() {
                        // If zpool creation is successful then we set these
                        post_setup_commands.push((
                            "/sbin/zfs".to_string(),
                            vec![
                                "set".to_string(),
                                format!("recordsize={}", block_size.unwrap()),
                                name.to_string_lossy().into_owned(),
                            ],
                        ));
                    }
                    if compression.is_some() {
                        post_setup_commands.push((
                            "/sbin/zfs".to_string(),
                            vec![
                                "set".to_string(),
                                "compression=on".to_string(),
                                name.to_string_lossy().into_owned(),
                            ],
                        ));
                    }
                    post_setup_commands.push((
                        "/sbin/zfs".to_string(),
                        vec![
                            "set".to_string(),
                            "acltype=posixacl".to_string(),
                            name.to_string_lossy().into_owned(),
                        ],
                    ));
                    post_setup_commands.push((
                        "/sbin/zfs".to_string(),
                        vec![
                            "set".to_string(),
                            "atime=off".to_string(),
                            name.to_string_lossy().into_owned(),
                        ],
                    ));
                    Ok(AsyncInit {
                        format_child: zpool_create,
                        post_setup_commands,
                        device: device.clone(),
                    })
                }
                None => Err(BlockUtilsError::new(format!(
                    "Unable to determine filename for device: {:?}",
                    device
                ))),
            }
        }
        Filesystem::Ext4 {
            ref inode_size,
            ref reserved_blocks_percentage,
            ref stride,
            ref stripe_width,
        } => {
            let mut arg_list: Vec<String> =
                vec!["-m".to_string(), reserved_blocks_percentage.to_string()];

            arg_list.push("-I".to_string());
            arg_list.push(inode_size.to_string());

            if (*stride).is_some() {
                arg_list.push("-E".to_string());
                arg_list.push(format!("stride={}", stride.unwrap()));
            }
            if (*stripe_width).is_some() {
                arg_list.push("-E".to_string());
                arg_list.push(format!("stripe_width={}", stripe_width.unwrap()));
            }
            arg_list.push(device.to_string_lossy().into_owned());

            Ok(AsyncInit {
                format_child: Command::new("mkfs.ext4").args(&arg_list).spawn()?,
                post_setup_commands: vec![],
                device: device.clone(),
            })
        }
    }
}

#[test]
fn test_get_device_info() {
    print!("{:?}", get_device_info(&PathBuf::from("/dev/sda5")));
    print!("{:?}", get_device_info(&PathBuf::from("/dev/loop0")));
}

fn get_size(device: &udev::Device) -> Option<u64> {
    match device.attribute_value("size") {
        // 512 is the block size
        Some(size_str) => {
            let size = size_str.to_str().unwrap_or("0").parse::<u64>().unwrap_or(0);
            Some(size * 512)
        }
        None => None,
    }
}

fn get_uuid(device: &udev::Device) -> Option<Uuid> {
    match device.property_value("ID_FS_UUID") {
        Some(value) => Uuid::parse_str(&value.to_string_lossy()).ok(),
        None => None,
    }
}

fn get_serial(device: &udev::Device) -> Option<String> {
    match device.property_value("ID_SERIAL") {
        Some(value) => Some(value.to_string_lossy().into_owned()),
        None => None,
    }
}

fn get_fs_type(device: &udev::Device) -> BlockResult<FilesystemType> {
    match device.property_value("ID_FS_TYPE") {
        Some(s) => {
            let value = s.to_string_lossy();
            FilesystemType::from_str(&value)
        }
        None => Ok(FilesystemType::Unknown),
    }
}

fn get_media_type(device: &udev::Device) -> MediaType {
    let device_sysname = device.sysname().to_string_lossy();

    // Test for loopback
    if let Ok(loop_regex) = Regex::new(r"loop\d+") {
        if loop_regex.is_match(&device_sysname) {
            return MediaType::Loopback;
        }
    }

    // Test for ramdisk
    if let Ok(ramdisk_regex) = Regex::new(r"ram\d+") {
        if ramdisk_regex.is_match(&device_sysname) {
            return MediaType::Ram;
        }
    }

    // Test for software raid
    if let Ok(ramdisk_regex) = Regex::new(r"md\d+") {
        if ramdisk_regex.is_match(&device_sysname) {
            return MediaType::MdRaid;
        }
    }

    // Test for nvme
    if device_sysname.contains("nvme") {
        return MediaType::NVME;
    }

    // Test for LVM
    if device.property_value("DM_NAME").is_some() {
        return MediaType::LVM;
    }

    // That should take care of the tricky ones.  Lets try to identify if it's
    // SSD or rotational now
    if let Some(rotation) = device.property_value("ID_ATA_ROTATION_RATE_RPM") {
        if rotation == "0" {
            return MediaType::SolidState;
        } else {
            return MediaType::Rotational;
        }
    }

    // No rotation rate.  Lets see if it's a virtual qemu disk
    if let Some(vendor) = device.property_value("ID_VENDOR") {
        let value = vendor.to_string_lossy();
        match value.as_ref() {
            "QEMU" => return MediaType::Virtual,
            _ => return MediaType::Unknown,
        }
    }

    // I give up
    MediaType::Unknown
}

fn get_device_type(device: &udev::Device) -> BlockResult<DeviceType> {
    match device.devtype() {
        Some(s) => {
            let value = s.to_string_lossy();
            DeviceType::from_str(&value)
        }
        None => Ok(DeviceType::Unknown),
    }
}

/// Checks and returns if a particular directory is a mountpoint
pub fn is_mounted(directory: &Path) -> BlockResult<bool> {
    let parent = directory.parent();

    let dir_metadata = fs::metadata(directory)?;
    let file_type = dir_metadata.file_type();

    if file_type.is_symlink() {
        // A symlink can never be a mount point
        return Ok(false);
    }

    if parent.is_some() {
        let parent_metadata = fs::metadata(parent.unwrap())?;
        if parent_metadata.dev() != dir_metadata.dev() {
            // path/.. on a different device as path
            return Ok(true);
        }
    } else {
        // If the directory doesn't have a parent it's the root filesystem
        return Ok(false);
    }
    Ok(false)
}

/// Scan a system and return all block devices that udev knows about
/// This function will skip udev devices identified as partition.  If
/// it can't discover this it will error on the side of caution and
/// return the device
pub fn get_block_devices() -> BlockResult<Vec<PathBuf>> {
    let mut block_devices: Vec<PathBuf> = Vec::new();
    let context = udev::Context::new()?;
    let mut enumerator = udev::Enumerator::new(&context)?;
    let devices = enumerator.scan_devices()?;

    for device in devices {
        if device.subsystem() == Some(OsStr::new("block")) {
            let partition = match device.devtype() {
                Some(devtype) => devtype == "partition",
                None => false,
            };
            if !partition {
                let mut path = PathBuf::from("/dev");
                path.push(device.sysname());
                block_devices.push(path);
            }
        }
    }

    Ok(block_devices)
}

/// Checks to see if the subsystem this device is using is block
pub fn is_block_device(device_path: &PathBuf) -> BlockResult<bool> {
    let context = udev::Context::new()?;
    let mut enumerator = udev::Enumerator::new(&context)?;
    let devices = enumerator.scan_devices()?;

    let sysname = device_path.file_name().ok_or_else(|| {
        BlockUtilsError::new(format!(
            "Unable to get file_name on device {:?}",
            device_path
        ))
    })?;

    for device in devices {
        if sysname == device.sysname() && device.subsystem() == Some(OsStr::new("block")) {
            return Ok(true);
        }
    }

    Err(BlockUtilsError::new(format!(
        "Unable to find device with name {:?}",
        device_path
    )))
}

/// A raid array enclosure
#[derive(Clone, Debug)]
pub struct Enclosure {
    pub active: Option<String>,
    pub fault: Option<String>,
    pub power_status: Option<String>,
    pub slot: u8,
    pub status: Option<String>,
    pub enclosure_type: Option<String>,
}

impl Default for Enclosure {
    fn default() -> Enclosure {
        Enclosure {
            active: None,
            fault: None,
            power_status: None,
            slot: 0,
            status: None,
            enclosure_type: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum DeviceState {
    Blocked,
    FailFast,
    Lost,
    Running,
    RunningRta,
}

impl fmt::Display for DeviceState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DeviceState::Blocked => write!(f, "blocked"),
            DeviceState::FailFast => write!(f, "fail_fast"),
            DeviceState::Lost => write!(f, "lost"),
            DeviceState::Running => write!(f, "running"),
            DeviceState::RunningRta => write!(f, "running_rta"),
        }
    }
}

impl FromStr for DeviceState {
    type Err = BlockUtilsError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "blocked" => Ok(DeviceState::Blocked),
            "failfast" => Ok(DeviceState::FailFast),
            "lost" => Ok(DeviceState::Lost),
            "running" => Ok(DeviceState::Running),
            "running_rta" => Ok(DeviceState::RunningRta),
            _ => Err(BlockUtilsError::new(format!("Unknown scsi state: {}", s))),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ScsiInfo {
    pub block_device: Option<PathBuf>,
    pub enclosure: Option<Enclosure>,
    pub host: u8,
    pub channel: u8,
    pub id: u8,
    pub lun: u8,
    pub vendor: Vendor,
    pub model: Option<String>,
    pub rev: Option<String>,
    pub state: Option<DeviceState>,
    pub scsi_type: ScsiDeviceType,
    pub scsi_revision: u32,
}

// Taken from https://github.com/hreinecke/lsscsi/blob/master/src/lsscsi.c
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ScsiDeviceType {
    DirectAccess,
    SequentialAccess,
    Printer,
    Processor,
    WriteOnce,
    CdRom,
    Scanner,
    Opticalmemory,
    MediumChanger,
    Communications,
    Unknowna,
    Unknownb,
    StorageArray,
    Enclosure,
    SimplifiedDirectAccess,
    OpticalCardReadWriter,
    BridgeController,
    ObjectBasedStorage,
    AutomationDriveInterface,
    SecurityManager,
    ZonedBlock,
    Reserved15,
    Reserved16,
    Reserved17,
    Reserved18,
    Reserved19,
    Reserved1a,
    Reserved1b,
    Reserved1c,
    Reserved1e,
    WellKnownLu,
    NoDevice,
}

impl FromStr for ScsiDeviceType {
    type Err = BlockUtilsError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "0" => Ok(ScsiDeviceType::DirectAccess),
            "1" => Ok(ScsiDeviceType::SequentialAccess),
            "2" => Ok(ScsiDeviceType::Printer),
            "3" => Ok(ScsiDeviceType::Processor),
            "4" => Ok(ScsiDeviceType::WriteOnce),
            "5" => Ok(ScsiDeviceType::CdRom),
            "6" => Ok(ScsiDeviceType::Scanner),
            "7" => Ok(ScsiDeviceType::Opticalmemory),
            "8" => Ok(ScsiDeviceType::MediumChanger),
            "9" => Ok(ScsiDeviceType::Communications),
            "10" => Ok(ScsiDeviceType::Unknowna),
            "11" => Ok(ScsiDeviceType::Unknownb),
            "12" => Ok(ScsiDeviceType::StorageArray),
            "13" => Ok(ScsiDeviceType::Enclosure),
            "14" => Ok(ScsiDeviceType::SimplifiedDirectAccess),
            "15" => Ok(ScsiDeviceType::OpticalCardReadWriter),
            "16" => Ok(ScsiDeviceType::BridgeController),
            "17" => Ok(ScsiDeviceType::ObjectBasedStorage),
            "18" => Ok(ScsiDeviceType::AutomationDriveInterface),
            "19" => Ok(ScsiDeviceType::SecurityManager),
            "20" => Ok(ScsiDeviceType::ZonedBlock),
            "21" => Ok(ScsiDeviceType::Reserved15),
            "22" => Ok(ScsiDeviceType::Reserved16),
            "23" => Ok(ScsiDeviceType::Reserved17),
            "24" => Ok(ScsiDeviceType::Reserved18),
            "25" => Ok(ScsiDeviceType::Reserved19),
            "26" => Ok(ScsiDeviceType::Reserved1a),
            "27" => Ok(ScsiDeviceType::Reserved1b),
            "28" => Ok(ScsiDeviceType::Reserved1c),
            "29" => Ok(ScsiDeviceType::Reserved1e),
            "30" => Ok(ScsiDeviceType::WellKnownLu),
            "31" => Ok(ScsiDeviceType::NoDevice),
            "Direct-Access" => Ok(ScsiDeviceType::DirectAccess),
            "Enclosure" => Ok(ScsiDeviceType::Enclosure),
            "RAID" => Ok(ScsiDeviceType::StorageArray),
            _ => Err(BlockUtilsError::new(format!("Unknown scheduler {}", s))),
        }
    }
}

impl Default for ScsiInfo {
    fn default() -> ScsiInfo {
        ScsiInfo {
            block_device: None,
            enclosure: None,
            host: 0,
            channel: 0,
            id: 0,
            lun: 0,
            vendor: Vendor::None,
            model: None,
            rev: None,
            state: None,
            scsi_type: ScsiDeviceType::NoDevice,
            scsi_revision: 0,
        }
    }
}

impl PartialEq for ScsiInfo {
    fn eq(&self, other: &ScsiInfo) -> bool {
        self.host == other.host
            && self.channel == other.channel
            && self.id == other.id
            && self.lun == other.lun
    }
}

#[test]
fn test_scsi_parser() {
    let s = fs::read_to_string("tests/proc_scsi").unwrap();
    println!("scsi_host_info {:#?}", scsi_host_info(s.as_bytes()));
}

named!(host<u8>, ws!(preceded!(ws!(tag!("Host: scsi")), take_u8)));

named!(
    model<&str>,
    ws!(map_res!(
        preceded!(ws!(tag!("Model:")), take_until_and_consume!("  ")),
        from_utf8
    ))
);

named!(
    rev<&str>,
    ws!(map_res!(
        preceded!(ws!(tag!("Rev:")), take_until_and_consume!(" ")),
        from_utf8
    ))
);

named!(
    vendor<&str>,
    ws!(map_res!(
        preceded!(ws!(tag!("Vendor:")), take_until_and_consume!(" ")),
        from_utf8
    ))
);

named!(
    scsi_type<&str>,
    ws!(map_res!(
        preceded!(ws!(tag!("Type:")), take_until_and_consume!(" ")),
        from_utf8
    ))
);

named!(
    take_u8<u8>,
    map_res!(map_res!(take_while!(is_digit), from_utf8), u8::from_str)
);

named!(
    take_u32<u32>,
    map_res!(map_res!(take_while!(is_digit), from_utf8), u32::from_str)
);

named!(channel<u8>, ws!(preceded!(ws!(tag!("Channel:")), take_u8)));

named!(scsi_id<u8>, ws!(preceded!(ws!(tag!("Id:")), take_u8)));

named!(scsi_lun<u8>, ws!(preceded!(ws!(tag!("Lun:")), take_u8)));

named!(
    revision<u32>,
    ws!(preceded!(ws!(tag!("ANSI  SCSI revision:")), take_u32))
);

named!(scsi_host_info<&[u8],Vec<ScsiInfo>>,
  many1!(do_parse!(    // the parser takes a byte array as input, and returns a Vec<ScsiInfo>
    opt!(tag!("Attached devices:")) >>
    host: host >>
    channel: channel >>
    id: scsi_id >>
    lun: scsi_lun >>
    vendor: vendor >>
    scsi_model: model >>
    scsi_rev: rev >>
    scsi_type: scsi_type >>
    revision: revision >>

    // the final tuple will be able to use the variables defined previously
    (ScsiInfo{
        block_device: None,
        enclosure: None,
        host,
        channel,
        id,
        lun,
        vendor: Vendor::from_str(vendor).unwrap(),
        model: Some(scsi_model.to_string()),
        rev: Some(scsi_rev.trim().to_string()),
        state: None,
        scsi_type: ScsiDeviceType::from_str(&scsi_type.to_string()).unwrap(),
        scsi_revision: revision,
    })
 ))
);

#[test]
fn test_sort_raid_info() {
    let mut scsi_0 = ScsiInfo::default();
    scsi_0.host = 0;
    scsi_0.channel = 0;
    scsi_0.id = 0;
    scsi_0.lun = 0;
    let mut scsi_1 = ScsiInfo::default();
    scsi_1.host = 2;
    scsi_1.channel = 0;
    scsi_1.id = 0;
    scsi_1.lun = 0;
    let mut scsi_2 = ScsiInfo::default();
    scsi_2.host = 2;
    scsi_2.channel = 1;
    scsi_2.id = 0;
    scsi_2.lun = 0;
    let mut scsi_3 = ScsiInfo::default();
    scsi_3.host = 2;
    scsi_3.channel = 1;
    scsi_3.id = 0;
    scsi_3.lun = 1;

    let scsi_info = vec![scsi_0, scsi_1, scsi_2, scsi_3];
    sort_scsi_info(&scsi_info);
}

/// Examine the ScsiInfo devices and associate a host ScsiInfo device if it
/// exists
pub fn sort_scsi_info(info: &[ScsiInfo]) -> Vec<(ScsiInfo, Option<ScsiInfo>)> {
    let mut sorted: Vec<(ScsiInfo, Option<ScsiInfo>)> = Vec::new();

    for dev in info {
        // Find the position of the host this device belongs to possibly
        let host = info
            .iter()
            .position(|d| d.host == dev.host && d.channel == 0 && d.id == 0 && d.lun == 0);
        match host {
            Some(pos) => {
                let host_dev = info[pos].clone();
                // If the host is itself then don't add it
                if host_dev == *dev {
                    sorted.push((dev.clone(), None));
                } else {
                    sorted.push((dev.clone(), Some(info[pos].clone())));
                }
            }
            None => {
                sorted.push((dev.clone(), None));
            }
        }
    }

    sorted
}

fn get_enclosure_data(p: &Path) -> BlockResult<Enclosure> {
    let mut e = Enclosure::default();
    for entry in read_dir(p)? {
        let entry = entry?;
        if entry.file_name() == OsStr::new("active") {
            e.active = Some(fs::read_to_string(&entry.path())?.trim().to_string());
        } else if entry.file_name() == OsStr::new("fault") {
            e.fault = Some(fs::read_to_string(&entry.path())?.trim().to_string());
        } else if entry.file_name() == OsStr::new("power_status") {
            e.power_status = Some(fs::read_to_string(&entry.path())?.trim().to_string());
        } else if entry.file_name() == OsStr::new("slot") {
            e.slot = u8::from_str(fs::read_to_string(&entry.path())?.trim())?;
        } else if entry.file_name() == OsStr::new("status") {
            e.status = Some(fs::read_to_string(&entry.path())?.trim().to_string());
        } else if entry.file_name() == OsStr::new("type") {
            e.enclosure_type = Some(fs::read_to_string(&entry.path())?.trim().to_string());
        }
    }

    Ok(e)
}

/// Gathers all available scsi information
pub fn get_scsi_info() -> BlockResult<Vec<ScsiInfo>> {
    // Taken from the strace output of lsscsi
    let scsi_path = Path::new("/sys/bus/scsi/devices");
    if scsi_path.exists() {
        let mut scsi_devices: Vec<ScsiInfo> = Vec::new();
        for entry in read_dir(&scsi_path)? {
            let entry = entry?;
            let path = entry.path();
            let name = path.file_name();
            if let Some(name) = name {
                let n = name.to_string_lossy();
                let f = match n.chars().next() {
                    Some(c) => c,
                    None => {
                        warn!("{} doesn't have any characters.  Skipping", n);
                        continue;
                    }
                };
                // Only get the devices that start with a digit
                if f.is_digit(10) {
                    let mut s = ScsiInfo::default();
                    let parts: Vec<&str> = n.split(':').collect();
                    if parts.len() != 4 {
                        warn!("Invalid device name: {}. Should be 0:0:0:0 format", n);
                        continue;
                    }
                    s.host = u8::from_str(parts[0])?;
                    s.channel = u8::from_str(parts[1])?;
                    s.id = u8::from_str(parts[2])?;
                    s.lun = u8::from_str(parts[3])?;
                    for scsi_entries in read_dir(&path)? {
                        let scsi_entry = scsi_entries?;
                        if scsi_entry.file_name() == OsStr::new("block") {
                            let block_path = path.join("block");
                            if block_path.exists() {
                                let mut device_name = read_dir(&block_path)?.take(1);
                                if let Some(name) = device_name.next() {
                                    s.block_device =
                                        Some(Path::new("/dev/").join(name?.file_name()));
                                }
                            }
                        } else if scsi_entry
                            .file_name()
                            .to_string_lossy()
                            .starts_with("enclosure_device")
                        {
                            let enclosure_path = path.join(scsi_entry.file_name());
                            let e = get_enclosure_data(&enclosure_path)?;
                            s.enclosure = Some(e);
                        } else if scsi_entry.file_name() == OsStr::new("model") {
                            s.model =
                                Some(fs::read_to_string(&scsi_entry.path())?.trim().to_string());
                        } else if scsi_entry.file_name() == OsStr::new("rev") {
                            s.rev =
                                Some(fs::read_to_string(&scsi_entry.path())?.trim().to_string());
                        } else if scsi_entry.file_name() == OsStr::new("state") {
                            s.state = Some(DeviceState::from_str(
                                fs::read_to_string(&scsi_entry.path())?.trim(),
                            )?);
                        } else if scsi_entry.file_name() == OsStr::new("type") {
                            s.scsi_type = ScsiDeviceType::from_str(
                                fs::read_to_string(&scsi_entry.path())?.trim(),
                            )?;
                        } else if scsi_entry.file_name() == OsStr::new("vendor") {
                            s.vendor =
                                Vendor::from_str(fs::read_to_string(&scsi_entry.path())?.trim())?;
                        }
                    }
                    scsi_devices.push(s);
                }
            }
        }
        Ok(scsi_devices)
    } else {
        // Fallback behavior still works but gathers much less information
        let buff = fs::read_to_string("/proc/scsi/scsi")?;

        match scsi_host_info(buff.as_bytes()) {
            Ok((_, value)) => Ok(value),
            Err(nom::Err::Incomplete(needed)) => Err(BlockUtilsError::new(format!(
                "Unable to parse /proc/scsi/scsi output: {}.  Needed {:?} more bytes",
                buff, needed
            ))),
            Err(nom::Err::Error(e)) | Err(nom::Err::Failure(e)) => Err(BlockUtilsError::new(
                format!("Unable to parse /proc/scsi/scsi output: {}", buff),
            )),
        }
    }
}

/// returns the device info and possibly partition entry for the device with the path or symlink given
pub fn get_device_from_path(dev_path: &Path) -> BlockResult<(Option<u64>, Option<Device>)> {
    let context = udev::Context::new()?;
    let mut enumerator = udev::Enumerator::new(&context)?;
    let host_devices = enumerator.scan_devices()?;
    for device in host_devices {
        if let Some(dev_type) = device.devtype() {
            if dev_type == "disk" || dev_type == "partition" {
                let name = Path::new("/dev").join(device.sysname());
                let dev_links = OsStr::new("DEVLINKS");
                if dev_path == name {
                    let part_num = match device.property_value("ID_PART_ENTRY_NUMBER") {
                        Some(value) => value.to_string_lossy().parse::<u64>().ok(),
                        None => None,
                    };
                    let id: Option<Uuid> = get_uuid(&device);
                    let serial = get_serial(&device);
                    let media_type = get_media_type(&device);
                    let device_type = get_device_type(&device)?;
                    let capacity = match get_size(&device) {
                        Some(size) => size,
                        None => 0,
                    };
                    let fs_type = get_fs_type(&device)?;

                    let dev = Device {
                        id,
                        name: device.sysname().to_string_lossy().into_owned(),
                        media_type,
                        device_type,
                        capacity,
                        fs_type,
                        serial_number: serial,
                    };
                    return Ok((part_num, Some(dev)));
                }
                if let Some(links) = device.property_value(dev_links) {
                    let path = dev_path.to_string_lossy().to_string();
                    if links.to_string_lossy().contains(&path) {
                        let part_num = match device.property_value("ID_PART_ENTRY_NUMBER") {
                            Some(value) => value.to_string_lossy().parse::<u64>().ok(),
                            None => None,
                        };
                        let id: Option<Uuid> = get_uuid(&device);
                        let serial = get_serial(&device);
                        let media_type = get_media_type(&device);
                        let device_type = get_device_type(&device)?;
                        let capacity = match get_size(&device) {
                            Some(size) => size,
                            None => 0,
                        };
                        let fs_type = get_fs_type(&device)?;

                        let dev = Device {
                            id,
                            name: device.sysname().to_string_lossy().into_owned(),
                            media_type,
                            device_type,
                            capacity,
                            fs_type,
                            serial_number: serial,
                        };
                        return Ok((part_num, Some(dev)));
                    }
                }
            }
        }
    }
    Ok((None, None))
}

/// Returns device info on every device it can find in the devices slice
/// The device info may not be in the same order as the slice so be aware.
/// This function is more efficient because it only call udev list once
pub fn get_all_device_info(devices: &[PathBuf]) -> BlockResult<Vec<Device>> {
    let device_names: Vec<&OsStr> = devices
        .iter()
        .map(|d| d.file_name())
        .filter(|d| d.is_some())
        .map(|d| d.unwrap())
        .collect();
    let mut device_infos: Vec<Device> = Vec::new();

    let context = udev::Context::new()?;
    let mut enumerator = udev::Enumerator::new(&context)?;
    let host_devices = enumerator.scan_devices()?;

    for device in host_devices {
        //let sysname = PathBuf::from(device.sysname());
        //println!("devices.contains({:?})", &sysname);
        if device_names.contains(&device.sysname())
            && device.subsystem() == Some(OsStr::new("block"))
        {
            // Ok we're a block device
            let id: Option<Uuid> = get_uuid(&device);
            let serial = get_serial(&device);
            let media_type = get_media_type(&device);
            let device_type = get_device_type(&device)?;
            let capacity = match get_size(&device) {
                Some(size) => size,
                None => 0,
            };
            let fs_type = get_fs_type(&device)?;

            device_infos.push(Device {
                id,
                name: device.sysname().to_string_lossy().into_owned(),
                media_type,
                device_type,
                capacity,
                fs_type,
                serial_number: serial,
            });
        }
    }
    Ok(device_infos)
}

/// Returns device information that is gathered with udev.
pub fn get_device_info(device_path: &Path) -> BlockResult<Device> {
    let context = udev::Context::new()?;
    let mut enumerator = udev::Enumerator::new(&context)?;
    let devices = enumerator.scan_devices()?;

    let sysname = device_path.file_name().ok_or_else(|| {
        BlockUtilsError::new(format!(
            "Unable to get file_name on device {:?}",
            device_path
        ))
    })?;

    for device in devices {
        if sysname == device.sysname() && device.subsystem() == Some(OsStr::new("block")) {
            // This is going to get complicated
            // Ok we're a block device
            let id: Option<Uuid> = get_uuid(&device);
            let serial = get_serial(&device);
            let media_type = get_media_type(&device);
            let device_type = get_device_type(&device)?;
            let capacity = match get_size(&device) {
                Some(size) => size,
                None => 0,
            };
            let fs_type = get_fs_type(&device)?;

            return Ok(Device {
                id,
                name: sysname.to_string_lossy().to_string(),
                media_type,
                device_type,
                capacity,
                fs_type,
                serial_number: serial,
            });
        }
    }
    Err(BlockUtilsError::new(format!(
        "Unable to find device with name {:?}",
        device_path
    )))
}

pub fn set_elevator(device_path: &PathBuf, elevator: &Scheduler) -> BlockResult<usize> {
    let device_name = match device_path.file_name() {
        Some(name) => name.to_string_lossy().into_owned(),
        None => "".to_string(),
    };
    let mut f = File::open("/etc/rc.local")?;
    let elevator_cmd = format!(
        "echo {scheduler} > /sys/block/{device}/queue/scheduler",
        scheduler = elevator,
        device = device_name
    );

    let mut script = shellscript::parse(&mut f)?;
    let existing_cmd = script
        .commands
        .iter()
        .position(|cmd| cmd.contains(&device_name));
    if let Some(pos) = existing_cmd {
        script.commands.remove(pos);
    }
    script.commands.push(elevator_cmd);
    let mut f = File::create("/etc/rc.local")?;
    let bytes_written = script.write(&mut f)?;
    Ok(bytes_written)
}

pub fn weekly_defrag(mount: &Path, fs_type: &FilesystemType, interval: &str) -> BlockResult<usize> {
    let crontab = Path::new("/var/spool/cron/crontabs/root");
    let defrag_command = match *fs_type {
        FilesystemType::Ext4 => "e4defrag",
        FilesystemType::Btrfs => "btrfs filesystem defragment -r",
        FilesystemType::Xfs => "xfs_fsr",
        _ => "",
    };
    let job = format!(
        "{interval} {cmd} {path}",
        interval = interval,
        cmd = defrag_command,
        path = mount.display()
    );

    //TODO Change over to using the cronparse library.  Has much better parsing however
    //there's currently no way to add new entries yet
    let mut existing_crontab = {
        if crontab.exists() {
            let buff = fs::read_to_string("/var/spool/cron/crontabs/root")?;
            buff.split('\n')
                .map(|s| s.to_string())
                .collect::<Vec<String>>()
        } else {
            Vec::new()
        }
    };
    let mount_str = mount.to_string_lossy().into_owned();
    let existing_job_position = existing_crontab
        .iter()
        .position(|line| line.contains(&mount_str));
    // If we found an existing job we remove the old and insert the new job
    if let Some(pos) = existing_job_position {
        existing_crontab.remove(pos);
    }
    existing_crontab.push(job.clone());

    //Write back out
    let mut f = File::create("/var/spool/cron/crontabs/root")?;
    let written_bytes = f.write(&existing_crontab.join("\n").as_bytes())?;
    Ok(written_bytes)
}
