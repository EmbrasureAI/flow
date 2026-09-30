use std::process::Command;
use tempfile::TempDir;

fn fatal_event(stdout: &[u8]) -> serde_json::Value {
    String::from_utf8_lossy(stdout)
        .lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .find(|event| event["fields"]["event"] == "fatal")
        .expect("fatal event")
}

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
    // An operator must provide the environment; restarting cannot fix it.
    assert_eq!(first.status.code(), Some(78));
    // The fatal error is one JSON event in the log stream, not stderr text.
    let fatal = fatal_event(&first.stdout);
    assert_eq!(
        fatal["fields"]["error"],
        "source connection environment variable is missing: FLOW_INIT_TEST_MISSING_CONNECTION"
    );
    assert_eq!(fatal["fields"]["exit_code"], 78);
    assert_eq!(fatal["fields"]["class"], "config");
    assert_eq!(fatal["level"], "ERROR");
    assert!(
        first.stderr.is_empty(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    let status: serde_json::Value =
        serde_json::from_slice(&std::fs::read(state.join("status.json")).unwrap()).unwrap();
    assert_eq!(status["state"], "stopped");
    assert_eq!(status["last_error"]["exit_code"], 78);
    assert_eq!(
        status["last_error"]["message"],
        "source connection environment variable is missing: FLOW_INIT_TEST_MISSING_CONNECTION"
    );
    let status_command = Command::new(env!("CARGO_BIN_EXE_embrasure-flow"))
        .args(["--config", path.to_str().unwrap(), "status"])
        .output()
        .unwrap();
    // `status` prints the observation and exits 3 while not ready.
    assert_eq!(status_command.status.code(), Some(3));
    assert!(String::from_utf8_lossy(&status_command.stdout).contains("\"last_error\""));
    // A filter that disables the fatal event still reports the error.
    let filtered = Command::new(env!("CARGO_BIN_EXE_embrasure-flow"))
        .args(["--config", path.to_str().unwrap(), "init"])
        .env_remove("FLOW_INIT_TEST_MISSING_CONNECTION")
        .env("RUST_LOG", "flow_events=debug")
        .output()
        .unwrap();
    assert_eq!(filtered.status.code(), Some(78));
    assert!(
        String::from_utf8_lossy(&filtered.stderr)
            .contains("source connection environment variable is missing"),
        "{}",
        String::from_utf8_lossy(&filtered.stderr)
    );
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
        fatal_event(&second.stdout)["fields"]["error"]
            .as_str()
            .unwrap()
            .contains("source connection environment variable is missing")
    );
    assert!(
        String::from_utf8_lossy(&second.stdout).contains("failed to flush bootstrap diagnostics")
    );
}
