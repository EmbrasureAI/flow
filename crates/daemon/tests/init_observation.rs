use std::process::Command;
use tempfile::TempDir;

#[test]
fn failed_init_exports_process_metrics_without_hiding_the_primary_error() {
    let temp = TempDir::new().unwrap();
    let state = temp.path().join("state");
    std::fs::create_dir(&state).unwrap();
    std::fs::write(state.join("metrics.prom"), "previous-process\n").unwrap();
    let mut config: toml::Value =
        toml::from_str(include_str!("../../../examples/flow.toml")).unwrap();
    config["state_dir"] = toml::Value::String(state.to_string_lossy().into_owned());
    config["source"]["connection_env"] =
        toml::Value::String("FLOW_INIT_TEST_MISSING_CONNECTION".into());
    let path = temp.path().join("flow.toml");
    std::fs::write(&path, toml::to_string(&config).unwrap()).unwrap();
    let run = || {
        Command::new(env!("CARGO_BIN_EXE_embrasure-flow"))
            .args(["--config", path.to_str().unwrap(), "init"])
            .env_remove("FLOW_INIT_TEST_MISSING_CONNECTION")
            .output()
            .unwrap()
    };
    let first = run();
    assert!(!first.status.success());
    assert!(String::from_utf8_lossy(&first.stderr).contains(
        "source connection environment variable is missing: FLOW_INIT_TEST_MISSING_CONNECTION"
    ));
    let metrics = std::fs::read_to_string(state.join("metrics.prom")).unwrap();
    assert!(
        metrics.contains("flow_bootstrap_runs_total{outcome=\"error\"} 1"),
        "{metrics}"
    );
    assert!(metrics.contains("flow_bootstrap_seconds_count{outcome=\"error\"} 1"));
    assert!(!metrics.contains("previous-process"));
    // Refuse the final atomic rename while leaving the state itself usable.
    std::fs::remove_file(state.join("metrics.prom")).unwrap();
    std::fs::create_dir(state.join("metrics.prom")).unwrap();
    let second = run();
    assert!(!second.status.success());
    assert!(
        String::from_utf8_lossy(&second.stderr)
            .contains("source connection environment variable is missing")
    );
    assert!(
        String::from_utf8_lossy(&second.stdout).contains("failed to flush bootstrap diagnostics")
    );
}
