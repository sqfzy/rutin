use std::ffi::CString;
use std::io::{Error, ErrorKind, Result};
use std::os::unix::io::RawFd;
use std::ptr;

/// AF_XDP socket descriptor
pub const AF_XDP: i32 = 44;

/// XDP socket flags
pub const XDP_FLAGS_UPDATE_IF_NOEXIST: u32 = 1;
pub const XDP_FLAGS_SKB_MODE: u32 = 2;
pub const XDP_FLAGS_DRV_MODE: u32 = 4;

/// XDP socket options
pub const SOL_XDP: i32 = 283;
pub const XDP_MMAP_OFFSETS: i32 = 1;
pub const XDP_RX_RING: i32 = 2;
pub const XDP_TX_RING: i32 = 3;
pub const XDP_UMEM: i32 = 4;
pub const XDP_UMEM_FILL_RING: i32 = 5;
pub const XDP_UMEM_COMPLETION_RING: i32 = 6;

/// XDP device structure for managing AF_XDP sockets
#[derive(Debug)]
pub struct XdpDevice {
    /// Interface name
    pub interface: String,
    /// Socket file descriptor
    pub socket_fd: Option<RawFd>,
    /// Queue ID
    pub queue_id: u32,
    /// Device flags
    pub flags: u32,
}

impl XdpDevice {
    /// Create a new XDP device
    pub fn new(interface: &str, queue_id: u32) -> Self {
        Self {
            interface: interface.to_string(),
            socket_fd: None,
            queue_id,
            flags: XDP_FLAGS_SKB_MODE, // Default to SKB mode for compatibility
        }
    }

    /// Create XDP socket
    pub fn create_socket(&mut self) -> Result<()> {
        let socket_fd = unsafe {
            libc::socket(AF_XDP, libc::SOCK_RAW, 0)
        };

        if socket_fd == -1 {
            return Err(Error::last_os_error());
        }

        self.socket_fd = Some(socket_fd);
        Ok(())
    }

    /// Bind socket to interface
    pub fn bind_to_interface(&self) -> Result<()> {
        let socket_fd = self.socket_fd.ok_or_else(|| {
            Error::new(ErrorKind::InvalidInput, "Socket not created")
        })?;

        // Create sockaddr_xdp structure
        let sockaddr = SockaddrXdp {
            sxdp_family: AF_XDP as u16,
            sxdp_flags: self.flags as u16,
            sxdp_ifindex: self.get_interface_index()?,
            sxdp_queue_id: self.queue_id,
            sxdp_shared_umem_fd: 0,
        };

        let ret = unsafe {
            libc::bind(
                socket_fd,
                &sockaddr as *const SockaddrXdp as *const libc::sockaddr,
                std::mem::size_of::<SockaddrXdp>() as u32,
            )
        };

        if ret == -1 {
            return Err(Error::last_os_error());
        }

        Ok(())
    }

    /// Get interface index by name
    fn get_interface_index(&self) -> Result<u32> {
        let interface_cstr = CString::new(self.interface.clone())
            .map_err(|_| Error::new(ErrorKind::InvalidInput, "Invalid interface name"))?;

        let index = unsafe {
            libc::if_nametoindex(interface_cstr.as_ptr())
        };

        if index == 0 {
            return Err(Error::new(ErrorKind::NotFound, "Interface not found"));
        }

        Ok(index)
    }

    /// Setup UMEM (User Memory) region
    pub fn setup_umem(&self, size: usize) -> Result<*mut u8> {
        let ptr = unsafe {
            libc::mmap(
                ptr::null_mut(),
                size,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                -1,
                0,
            )
        };

        if ptr == libc::MAP_FAILED {
            return Err(Error::last_os_error());
        }

        Ok(ptr as *mut u8)
    }

    /// Check if device is ready
    pub fn is_ready(&self) -> bool {
        self.socket_fd.is_some()
    }

    /// Get interface name
    pub fn get_interface(&self) -> &str {
        &self.interface
    }

    /// Get queue ID
    pub fn get_queue_id(&self) -> u32 {
        self.queue_id
    }

    /// Close the device
    pub fn close(&mut self) -> Result<()> {
        if let Some(fd) = self.socket_fd.take() {
            let ret = unsafe { libc::close(fd) };
            if ret == -1 {
                return Err(Error::last_os_error());
            }
        }
        Ok(())
    }
}

impl Drop for XdpDevice {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

/// XDP socket address structure
#[repr(C)]
#[derive(Debug, Clone, Copy)]
struct SockaddrXdp {
    sxdp_family: u16,
    sxdp_flags: u16,
    sxdp_ifindex: u32,
    sxdp_queue_id: u32,
    sxdp_shared_umem_fd: u32,
}

/// XDP utilities
pub mod utils {
    use super::*;

    /// Check if XDP is supported on the system
    pub fn is_xdp_supported() -> bool {
        // Try to create an AF_XDP socket
        let socket_fd = unsafe {
            libc::socket(AF_XDP, libc::SOCK_RAW, 0)
        };

        if socket_fd == -1 {
            return false;
        }

        unsafe {
            libc::close(socket_fd);
        }

        true
    }

    /// Get available network interfaces
    pub fn get_network_interfaces() -> Result<Vec<String>> {
        let mut interfaces = Vec::new();
        
        // This is a simplified implementation
        // In a real implementation, you'd use getifaddrs or similar
        let common_interfaces = vec!["lo", "eth0", "veth0", "veth1"];
        
        for iface in common_interfaces {
            let interface_cstr = CString::new(iface)
                .map_err(|_| Error::new(ErrorKind::InvalidInput, "Invalid interface name"))?;
            
            let index = unsafe {
                libc::if_nametoindex(interface_cstr.as_ptr())
            };
            
            if index != 0 {
                interfaces.push(iface.to_string());
            }
        }
        
        Ok(interfaces)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_xdp_device_creation() {
        let device = XdpDevice::new("lo", 0);
        assert_eq!(device.get_interface(), "lo");
        assert_eq!(device.get_queue_id(), 0);
        assert!(!device.is_ready());
    }

    #[test]
    fn test_xdp_support_check() {
        // This test might fail in environments without XDP support
        // but should not panic
        let _supported = utils::is_xdp_supported();
    }

    #[test]
    fn test_get_network_interfaces() {
        let result = utils::get_network_interfaces();
        assert!(result.is_ok());
        let interfaces = result.unwrap();
        // Should at least have loopback interface
        assert!(!interfaces.is_empty());
    }
}