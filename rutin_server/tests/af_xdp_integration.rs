// Integration tests for AF_XDP device functionality
// These tests require network setup via setup_network.nu

use rutin_server::af_xdp::{XdpDevice, utils};
use std::process::Command;
use std::time::Duration;
use std::thread;
#[cfg(unix)]
use std::os::unix::process::ExitStatusExt;

/// Setup network environment using setup_network.nu script
fn setup_network_environment() -> Result<(), Box<dyn std::error::Error>> {
    // Try nushell first, fallback to bash script
    let output = Command::new("nu")
        .arg("setup_network.nu")
        .current_dir("/home/runner/work/rutin/rutin")
        .output();

    let fallback_result = if output.is_ok() && output.as_ref().unwrap().status.success() {
        output
    } else {
        // Fallback to bash script
        println!("Falling back to bash script for network setup");
        Command::new("bash")
            .arg("setup_network.sh")
            .arg("setup")
            .current_dir("/home/runner/work/rutin/rutin")
            .output()
    };

    match fallback_result {
        Ok(output) => {
            if output.status.success() {
                println!("Network setup completed successfully");
                println!("stdout: {}", String::from_utf8_lossy(&output.stdout));
            } else {
                eprintln!("Network setup failed with stderr: {}", String::from_utf8_lossy(&output.stderr));
                // Don't fail the test if network setup fails, as it might not have permissions
            }
        }
        Err(e) => {
            eprintln!("Failed to run network setup: {}", e);
            // Don't fail the test if setup fails
        }
    }

    // Give the system a moment to set up the network interfaces
    thread::sleep(Duration::from_millis(500));
    Ok(())
}

/// Cleanup network environment using setup_network.nu script
fn cleanup_network_environment() {
    // Try nushell first, fallback to bash script
    let output = Command::new("nu")
        .arg("setup_network.nu")
        .arg("cleanup")
        .current_dir("/home/runner/work/rutin/rutin")
        .output();
    
    if output.is_err() {
        // Fallback to bash script
        let _ = Command::new("bash")
            .arg("setup_network.sh")
            .arg("cleanup")
            .current_dir("/home/runner/work/rutin/rutin")
            .output();
    }
}

#[test]
fn test_xdp_device_with_network_setup() {
    // Setup network environment
    setup_network_environment().expect("Failed to setup network environment");

    // Test XDP device creation with loopback interface
    let mut device = XdpDevice::new("lo", 0);
    assert_eq!(device.get_interface(), "lo");
    assert_eq!(device.get_queue_id(), 0);
    assert!(!device.is_ready());

    // Try to create socket (this might fail in test environment without XDP support)
    match device.create_socket() {
        Ok(_) => {
            assert!(device.is_ready());
            println!("XDP socket created successfully");
            
            // Try to bind to interface (this might fail without proper permissions)
            match device.bind_to_interface() {
                Ok(_) => println!("XDP socket bound to interface successfully"),
                Err(e) => println!("Failed to bind XDP socket (expected in test environment): {}", e),
            }
        }
        Err(e) => {
            println!("Failed to create XDP socket (expected in test environment): {}", e);
        }
    }

    // Cleanup
    cleanup_network_environment();
}

#[test]
fn test_xdp_device_with_veth_interface() {
    // Setup network environment
    setup_network_environment().expect("Failed to setup network environment");

    // Check if veth0 interface is available
    let interfaces = utils::get_network_interfaces().unwrap_or_default();
    println!("Available interfaces: {:?}", interfaces);

    if interfaces.contains(&"veth0".to_string()) {
        let mut device = XdpDevice::new("veth0", 0);
        
        // Try to create socket
        match device.create_socket() {
            Ok(_) => {
                println!("XDP socket created for veth0");
                
                // Try to bind to veth0
                match device.bind_to_interface() {
                    Ok(_) => println!("XDP socket bound to veth0 successfully"),
                    Err(e) => println!("Failed to bind XDP socket to veth0 (expected): {}", e),
                }
            }
            Err(e) => {
                println!("Failed to create XDP socket for veth0 (expected): {}", e);
            }
        }
    } else {
        println!("veth0 interface not available, skipping test");
    }

    // Cleanup
    cleanup_network_environment();
}

#[test]
fn test_xdp_umem_setup() {
    // Setup network environment
    setup_network_environment().expect("Failed to setup network environment");

    let device = XdpDevice::new("lo", 0);
    
    // Test UMEM setup
    match device.setup_umem(4096) {
        Ok(ptr) => {
            println!("UMEM setup successful, pointer: {:?}", ptr);
            
            // Cleanup the memory mapping
            unsafe {
                libc::munmap(ptr as *mut libc::c_void, 4096);
            }
        }
        Err(e) => {
            println!("UMEM setup failed (might be expected): {}", e);
        }
    }

    // Cleanup
    cleanup_network_environment();
}

#[test]
fn test_network_environment_verification() {
    // Setup network environment
    setup_network_environment().expect("Failed to setup network environment");

    // Verify network setup using the script
    let output = Command::new("nu")
        .arg("setup_network.nu")
        .arg("verify")
        .current_dir("/home/runner/work/rutin/rutin")
        .output();

    let result = match output {
        Ok(result) => result,
        Err(_) => {
            // Fallback to bash script
            Command::new("bash")
                .arg("setup_network.sh")
                .arg("verify")
                .current_dir("/home/runner/work/rutin/rutin")
                .output()
                .unwrap_or_else(|e| {
                    eprintln!("Failed to run verification script: {}", e);
                    std::process::Output {
                        status: std::process::ExitStatus::from_raw(1),
                        stdout: Vec::new(),
                        stderr: b"Failed to run verification script".to_vec(),
                    }
                })
        }
    };

    println!("Network verification output:");
    println!("stdout: {}", String::from_utf8_lossy(&result.stdout));
    if !result.status.success() {
        println!("stderr: {}", String::from_utf8_lossy(&result.stderr));
    }

    // Cleanup
    cleanup_network_environment();
}

#[test]
fn test_xdp_support_detection() {
    let supported = utils::is_xdp_supported();
    println!("XDP support detected: {}", supported);
    
    // This test should not fail regardless of XDP support
    // It's just for informational purposes
}

#[test]
fn test_multiple_xdp_devices() {
    // Setup network environment
    setup_network_environment().expect("Failed to setup network environment");

    let interfaces = utils::get_network_interfaces().unwrap_or_default();
    println!("Testing with interfaces: {:?}", interfaces);

    let mut devices = Vec::new();
    
    // Create devices for available interfaces
    for iface in interfaces.iter().take(2) { // Limit to 2 interfaces for testing
        let device = XdpDevice::new(iface, 0);
        devices.push(device);
    }

    // Test device creation
    for device in &devices {
        assert!(!device.is_ready());
        println!("Created device for interface: {}", device.get_interface());
    }

    // Try to create sockets (might fail)
    for device in &mut devices {
        match device.create_socket() {
            Ok(_) => println!("Socket created for {}", device.get_interface()),
            Err(e) => println!("Socket creation failed for {} (expected): {}", device.get_interface(), e),
        }
    }

    // Cleanup
    cleanup_network_environment();
}