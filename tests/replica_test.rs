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

    let mut redis_server = Command::new("redis-server")
        .arg("--port")
        .arg("6378")
        .arg("--dbfilename")
        .arg("replica_test.rdb")
        .arg("--dir")
        .arg("./tests/rdb")
        .spawn()
        .expect("Failed to start redis-server on port 6378");
    info!("redis-server started on port 6378");

    let mut rutin_server = Command::new("cargo")
        .arg("run")
        .arg("--")
        .arg("--port")
        .arg("6379")
        .arg("--replicaof")
        .arg("127.0.0.1:6378")
        .spawn()
        .expect("Failed to start rutin on port 6379");
    info!("rutin started on port 6379");

    let get_foo = Command::new("redis-cli")
        .arg("get")
        .arg("foo")
        .output()
        .expect("Failed to get foo");
    info!("get foo: {:?}", get_foo);

    assert_eq!(
        String::from_utf8_lossy(&get_foo.stdout).trim(),
        "bar",
        "foo value is not bar"
    );

    let set_foo2 = Command::new("redis-cli")
        .arg("-p")
        .arg("6378")
        .arg("set")
        .arg("foo2")
        .arg("bar2")
        .output()
        .expect("Failed to set foo2 on master");
    info!("set foo2: {:?}", set_foo2);

    assert!(set_foo2.status.success(), "Failed to set foo2 on master");

    let get_foo2 = Command::new("redis-cli")
        .arg("get")
        .arg("foo2")
        .output()
        .expect("Failed to get foo2");
    info!("get foo2: {:?}", get_foo2);

    assert_eq!(
        String::from_utf8_lossy(&get_foo2.stdout).trim(),
        "bar2",
        "foo2 value is not bar2"
    );

    redis_server.kill().expect("Failed to kill redis");
    rutin_server.kill().expect("Failed to kill rutin");

    redis_server.wait().expect("Failed to wait for redis");
    rutin_server.wait().expect("Failed to wait for rutin");
}
