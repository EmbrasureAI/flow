//! The state directory holds replicated row data; files Flow creates there
//! must not be readable by other local users.
#![cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::Command;
use tempfile::TempDir;

fn mode(path: &Path) -> u32 {
    std::fs::metadata(path).unwrap().permissions().mode() & 0o7777
}

fn init(state: &Path) -> std::process::Output {
    let temp = state.parent().unwrap();
    let mut config: toml::Value =
        toml::from_str(include_str!("../../../examples/flow.toml")).unwrap();
    config["state_dir"] = toml::Value::String(state.to_string_lossy().into_owned());
    config["source"]["connection_env"] =
        toml::Value::String("FLOW_STATE_PERMISSIONS_TEST_MISSING_CONNECTION".into());
    let path = temp.join("flow.toml");
    std::fs::write(&path, toml::to_string(&config).unwrap()).unwrap();
    Command::new(env!("CARGO_BIN_EXE_embrasure-flow"))
        .args(["--config", path.to_str().unwrap(), "init"])
        .env_remove("FLOW_STATE_PERMISSIONS_TEST_MISSING_CONNECTION")
        .output()
        .unwrap()
}

#[test]
fn state_dir_and_created_files_are_private_to_the_service_user() {
    let temp = TempDir::new().unwrap();
    let state = temp.path().join("state");
    // Init fails without a source, but still creates state and exports metrics.
    let output = init(&state);
    assert!(!output.status.success());
    assert_eq!(mode(&state), 0o700);
    assert_eq!(mode(&state.join("metrics.prom")), 0o600);

    // A directory left accessible by an earlier release is tightened in place.
    std::fs::set_permissions(&state, std::fs::Permissions::from_mode(0o755)).unwrap();
    let output = init(&state);
    assert!(!output.status.success());
    assert_eq!(mode(&state), 0o700);
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("removed group and other access"),
        "{}",
        String::from_utf8_lossy(&output.stdout)
    );
}
