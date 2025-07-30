#!/usr/bin/env nu

# setup_network.nu - Network environment setup script for AF_XDP integration tests

def main [] {
    print "Setting up network environment for AF_XDP integration tests..."
    
    # Check if running as root (required for network namespace operations)
    let current_user = (whoami)
    if $current_user != "root" {
        print "Warning: Network setup typically requires root privileges"
        print "Some operations may fail without proper permissions"
    }
    
    # Setup virtual ethernet pair for testing
    setup_veth_pair
    
    # Setup network namespace for isolated testing
    setup_network_namespace
    
    # Configure IP addresses
    configure_ip_addresses
    
    print "Network environment setup completed successfully"
}

# Create virtual ethernet pair
def setup_veth_pair [] {
    print "Creating virtual ethernet pair (veth0 <-> veth1)..."
    
    # Delete existing veth pair if it exists
    try {
        ip link delete veth0
    } catch {
        # Ignore error if veth0 doesn't exist
    }
    
    # Create new veth pair
    try {
        ip link add veth0 type veth peer name veth1
        ip link set veth0 up
        ip link set veth1 up
        print "Virtual ethernet pair created successfully"
    } catch {
        print "Error: Failed to create virtual ethernet pair"
        print "Make sure you have sufficient privileges and 'ip' command is available"
    }
}

# Setup network namespace for testing
def setup_network_namespace [] {
    print "Setting up network namespace 'xdp_test'..."
    
    # Delete existing namespace if it exists
    try {
        ip netns delete xdp_test
    } catch {
        # Ignore error if namespace doesn't exist
    }
    
    # Create new namespace
    try {
        ip netns add xdp_test
        print "Network namespace 'xdp_test' created successfully"
    } catch {
        print "Warning: Failed to create network namespace"
        print "Tests may still work using the default namespace"
    }
}

# Configure IP addresses for testing
def configure_ip_addresses [] {
    print "Configuring IP addresses..."
    
    try {
        # Configure veth0 in default namespace
        ip addr add 192.168.100.1/24 dev veth0
        
        # Configure veth1 in test namespace
        ip link set veth1 netns xdp_test
        ip netns exec xdp_test ip addr add 192.168.100.2/24 dev veth1
        ip netns exec xdp_test ip link set veth1 up
        ip netns exec xdp_test ip link set lo up
        
        print "IP addresses configured successfully"
        print "  veth0: 192.168.100.1/24 (default namespace)"
        print "  veth1: 192.168.100.2/24 (xdp_test namespace)"
    } catch {
        print "Warning: Failed to configure IP addresses"
        print "Manual network configuration may be required"
    }
}

# Cleanup function
def cleanup [] {
    print "Cleaning up network environment..."
    
    try {
        # Delete network namespace
        ip netns delete xdp_test
    } catch {
        # Ignore error
    }
    
    try {
        # Delete veth pair
        ip link delete veth0
    } catch {
        # Ignore error
    }
    
    print "Network cleanup completed"
}

# Verify network setup
def verify_setup [] {
    print "Verifying network setup..."
    
    # Check if veth0 exists
    let veth0_exists = (ip link show veth0 | length) > 0
    if $veth0_exists {
        print "✓ veth0 interface exists"
    } else {
        print "✗ veth0 interface not found"
    }
    
    # Check if namespace exists
    let ns_exists = (ip netns list | str contains "xdp_test")
    if $ns_exists {
        print "✓ xdp_test namespace exists"
    } else {
        print "✗ xdp_test namespace not found"
    }
    
    # Test connectivity
    try {
        ping -c 1 -W 1 192.168.100.2
        print "✓ Network connectivity verified"
    } catch {
        print "✗ Network connectivity test failed"
    }
}

# Show help
def show_help [] {
    print "Usage: nu setup_network.nu [command]"
    print ""
    print "Commands:"
    print "  (default)  - Setup complete network environment"
    print "  cleanup    - Cleanup network environment" 
    print "  verify     - Verify network setup"
    print "  help       - Show this help message"
    print ""
    print "This script sets up a network environment for AF_XDP integration tests."
    print "It creates virtual ethernet pairs and network namespaces for isolated testing."
}

# Handle command line arguments
if ($env.ARGS | length) > 0 {
    match ($env.ARGS | first) {
        "cleanup" => cleanup
        "verify" => verify_setup
        "help" => show_help
        _ => {
            print $"Unknown command: ($env.ARGS | first)"
            show_help
        }
    }
} else {
    main
}