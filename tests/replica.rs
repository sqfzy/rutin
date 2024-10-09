use std::process::Command;

use rutin::util::test_init;
use tracing::info;

fn check_command_installed(command: &str) -> bool {
    Command::new("which")
        .arg(command)
        .output()
        .map(|output| output.status.success())
        .unwrap_or(false)
}

#[test]
fn replica_redis_test() {
    test_init();

    if !check_command_installed("redis-server") || !check_command_installed("redis-cli") {
        return;
    }

    let redis_port = "6303";
    let rutin_port = "6304";

    let key = (0..10)
        .map(|_| fastrand::alphanumeric())
        .collect::<String>();
    let value = (0..10)
        .map(|_| fastrand::alphanumeric())
        .collect::<String>();

    info!("start redis-server on port {}", redis_port);
    let mut redis_server = Command::new("redis-server")
        .arg("--port")
        .arg(redis_port)
        .arg("--dbfilename")
        .arg("replica_test.rdb")
        .arg("--dir")
        .arg("./tests/rdb")
        .spawn()
        .expect("failed to start redis-server");

    info!("start rutin-server on port {}", rutin_port);
    let mut rutin_server = Command::new("cargo")
        .arg("run")
        .arg("--")
        .arg("--port")
        .arg(rutin_port)
        .arg("--replicaof")
        .arg(format!("127.0.0.1:{redis_port}"))
        .arg("--log-level")
        .arg("debug")
        .spawn()
        .expect("failed to start rutin");

    let get_value = Command::new("redis-cli")
        .arg("-p")
        .arg(rutin_port)
        .arg("get")
        .arg(&key)
        .output()
        .expect("failed to get value");

    assert_ne!(String::from_utf8_lossy(&get_value.stdout).trim(), &value);

    Command::new("redis-cli")
        .arg("-p")
        .arg(redis_port)
        .arg("set")
        .arg(&key)
        .arg(&value)
        .output()
        .expect("failed to set value");

    let get_value = Command::new("redis-cli")
        .arg("-p")
        .arg(rutin_port)
        .arg("get")
        .arg(&key)
        .output()
        .expect("failed to get value");

    assert_eq!(String::from_utf8_lossy(&get_value.stdout).trim(), &value);

    redis_server.kill().expect("Failed to kill redis");
    rutin_server.kill().expect("Failed to kill rutin");
}
