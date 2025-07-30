#!/bin/bash

# setup_network.sh - Network environment setup script for AF_XDP integration tests
# This is a bash fallback for setup_network.nu

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

print_usage() {
    echo "Usage: $0 [command]"
    echo ""
    echo "Commands:"
    echo "  setup     - Setup complete network environment (default)"
    echo "  cleanup   - Cleanup network environment" 
    echo "  verify    - Verify network setup"
    echo "  help      - Show this help message"
    echo ""
    echo "This script sets up a network environment for AF_XDP integration tests."
    echo "It creates virtual ethernet pairs and network namespaces for isolated testing."
}

setup_veth_pair() {
    echo "Creating virtual ethernet pair (veth0 <-> veth1)..."
    
    # Delete existing veth pair if it exists
    ip link delete veth0 2>/dev/null || true
    
    # Create new veth pair
    if ip link add veth0 type veth peer name veth1 && \
       ip link set veth0 up && \
       ip link set veth1 up; then
        echo "Virtual ethernet pair created successfully"
    else
        echo "Error: Failed to create virtual ethernet pair"
        echo "Make sure you have sufficient privileges and 'ip' command is available"
        return 1
    fi
}

setup_network_namespace() {
    echo "Setting up network namespace 'xdp_test'..."
    
    # Delete existing namespace if it exists
    ip netns delete xdp_test 2>/dev/null || true
    
    # Create new namespace
    if ip netns add xdp_test; then
        echo "Network namespace 'xdp_test' created successfully"
    else
        echo "Warning: Failed to create network namespace"
        echo "Tests may still work using the default namespace"
        return 0
    fi
}

configure_ip_addresses() {
    echo "Configuring IP addresses..."
    
    # Configure veth0 in default namespace
    if ip addr add 192.168.100.1/24 dev veth0 && \
       ip link set veth1 netns xdp_test && \
       ip netns exec xdp_test ip addr add 192.168.100.2/24 dev veth1 && \
       ip netns exec xdp_test ip link set veth1 up && \
       ip netns exec xdp_test ip link set lo up; then
        echo "IP addresses configured successfully"
        echo "  veth0: 192.168.100.1/24 (default namespace)"
        echo "  veth1: 192.168.100.2/24 (xdp_test namespace)"
    else
        echo "Warning: Failed to configure IP addresses"
        echo "Manual network configuration may be required"
        return 0
    fi
}

setup_network() {
    echo "Setting up network environment for AF_XDP integration tests..."
    
    # Check if running as root
    if [ "$EUID" -ne 0 ]; then
        echo "Warning: Network setup typically requires root privileges"
        echo "Some operations may fail without proper permissions"
    fi
    
    # Setup components
    setup_veth_pair
    setup_network_namespace
    configure_ip_addresses
    
    echo "Network environment setup completed successfully"
}

cleanup_network() {
    echo "Cleaning up network environment..."
    
    # Delete network namespace
    ip netns delete xdp_test 2>/dev/null || true
    
    # Delete veth pair
    ip link delete veth0 2>/dev/null || true
    
    echo "Network cleanup completed"
}

verify_setup() {
    echo "Verifying network setup..."
    
    # Check if veth0 exists
    if ip link show veth0 >/dev/null 2>&1; then
        echo "✓ veth0 interface exists"
    else
        echo "✗ veth0 interface not found"
    fi
    
    # Check if namespace exists
    if ip netns list | grep -q "xdp_test"; then
        echo "✓ xdp_test namespace exists"
    else
        echo "✗ xdp_test namespace not found"
    fi
    
    # Test connectivity
    if ping -c 1 -W 1 192.168.100.2 >/dev/null 2>&1; then
        echo "✓ Network connectivity verified"
    else
        echo "✗ Network connectivity test failed"
    fi
}

# Main execution
case "${1:-setup}" in
    "setup")
        setup_network
        ;;
    "cleanup")
        cleanup_network
        ;;
    "verify")
        verify_setup
        ;;
    "help"|"-h"|"--help")
        print_usage
        ;;
    *)
        echo "Unknown command: $1"
        print_usage
        exit 1
        ;;
esac