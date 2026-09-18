use std::process::{Command, Output};
use tempfile::TempDir;

fn output_text(output: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

#[test]
fn config_parse_errors_keep_locations_without_echoing_secrets() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("flow.toml");
    let secret = "FAKE_CONFIG_SECRET";
    for input in [
        format!("[catalog]\ncredential = \"{secret}\n"),
        include_str!("../../../examples/flow.toml").replace("Int64", secret),
        format!(
            "{secret} = true\n{}",
            include_str!("../../../examples/flow.toml")
        ),
    ] {
        std::fs::write(&path, input).unwrap();
        let output = Command::new(env!("CARGO_BIN_EXE_embrasure-flow"))
            .arg("--config")
            .arg(&path)
            .arg("check")
            .output()
            .unwrap();
        assert!(!output.status.success());
        let text = output_text(&output);
        assert!(
            text.contains("invalid configuration at line") && text.contains("column"),
            "{text}"
        );
        assert!(!text.contains(secret), "{text}");
    }
}

#[test]
fn catalog_environment_is_required_only_when_connecting() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("flow.toml");
    let mut config: toml::Value =
        toml::from_str(include_str!("../../../examples/flow.toml")).unwrap();
    config["state_dir"] =
        toml::Value::String(temp.path().join("state").to_string_lossy().into_owned());
    config["source"]["connection_env"] = "FLOW_TEST_MISSING_SOURCE".into();
    for key in ["token", "credential"] {
        config["catalog"]
            .as_table_mut()
            .unwrap()
            .insert(format!("{key}_env"), "FLOW_TEST_CATALOG_SECRET".into());
        std::fs::write(&path, toml::to_string(&config).unwrap()).unwrap();
        let command = || {
            let mut command = Command::new(env!("CARGO_BIN_EXE_embrasure-flow"));
            command
                .arg("--config")
                .arg(&path)
                .env_remove("FLOW_TEST_CATALOG_SECRET")
                .env_remove("FLOW_TEST_MISSING_SOURCE");
            command
        };
        assert!(command().arg("check").output().unwrap().status.success());
        for value in [None, Some(""), Some("client:FAKE_ENV_SECRET")] {
            let mut cmd = command();
            if let Some(value) = value {
                cmd.env("FLOW_TEST_CATALOG_SECRET", value);
            }
            let output = cmd.arg("init").output().unwrap();
            assert!(!output.status.success());
            let text = output_text(&output);
            assert!(!text.contains("FAKE_ENV_SECRET"), "{text}");
            if value == Some("client:FAKE_ENV_SECRET") {
                assert!(text.contains("FLOW_TEST_MISSING_SOURCE"), "{text}");
            } else {
                assert!(text.contains(&format!("catalog.{key}_env")), "{text}");
            }
        }
        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStringExt;
            let mut bytes = b"FAKE_ENV_SECRET".to_vec();
            bytes.push(0xff);
            let output = command()
                .env(
                    "FLOW_TEST_CATALOG_SECRET",
                    std::ffi::OsString::from_vec(bytes),
                )
                .arg("init")
                .output()
                .unwrap();
            assert!(!output.status.success());
            let text = output_text(&output);
            assert!(text.contains(&format!("catalog.{key}_env")), "{text}");
            assert!(!text.contains("FAKE_ENV_SECRET"), "{text}");
        }
        config["catalog"]
            .as_table_mut()
            .unwrap()
            .remove(&format!("{key}_env"));
    }
}
