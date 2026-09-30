//! Process exit classification for supervisors. A restart can fix a failure
//! exiting with 1 or `UNAVAILABLE`; `CONFIG` and `RESYNC` need an operator, so
//! a supervisor should stop restarting (systemd `RestartPreventExitStatus=`).
use std::{fmt, sync::OnceLock};

/// Unclassified failure; restarting may succeed.
pub(crate) const FAILURE: u8 = 1;
/// `status` only: the observation was read, but the service is not ready.
pub(crate) const NOT_READY: u8 = 3;
/// A source or catalog dependency stayed unavailable through the startup
/// retry window (sysexits `EX_TEMPFAIL`).
pub(crate) const UNAVAILABLE: u8 = 75;
/// Configuration is invalid or disagrees with durable state (`EX_CONFIG`).
pub(crate) const CONFIG: u8 = 78;
/// Source history can no longer be proven complete; only a resync proceeds.
pub(crate) const RESYNC: u8 = 79;

/// A stop that restarting the same configuration and state cannot clear.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Stop {
    Config,
    Resync,
}

/// An error whose Flow-generated message is safe for local status files.
#[derive(Debug)]
pub(crate) struct OperatorAction {
    stop: Stop,
    message: String,
}

impl fmt::Display for OperatorAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for OperatorAction {}

pub(crate) fn config(message: impl Into<String>) -> anyhow::Error {
    OperatorAction {
        stop: Stop::Config,
        message: message.into(),
    }
    .into()
}

pub(crate) fn resync(message: impl Into<String>) -> anyhow::Error {
    OperatorAction {
        stop: Stop::Resync,
        message: message.into(),
    }
    .into()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Class {
    Failure,
    Unavailable,
    Config,
    Resync,
}

impl Class {
    pub(crate) fn code(self) -> u8 {
        match self {
            Self::Failure => FAILURE,
            Self::Unavailable => UNAVAILABLE,
            Self::Config => CONFIG,
            Self::Resync => RESYNC,
        }
    }

    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Failure => "failure",
            Self::Unavailable => "unavailable",
            Self::Config => "config",
            Self::Resync => "resync_required",
        }
    }
}

impl From<Stop> for Class {
    fn from(stop: Stop) -> Self {
        match stop {
            Stop::Config => Self::Config,
            Stop::Resync => Self::Resync,
        }
    }
}

pub(crate) fn operator_action(error: &anyhow::Error) -> bool {
    error.downcast_ref::<OperatorAction>().is_some()
        || error.is::<crate::source::PublicationChanged>()
}

/// The exit class and, for operator stops, a message safe to persist. Other
/// error chains can contain URLs or row values and stay in the log only.
pub(crate) fn classify(error: &anyhow::Error) -> (Class, Option<String>) {
    if let Some(action) = error.downcast_ref::<OperatorAction>() {
        return (action.stop.into(), Some(action.message.clone()));
    }
    if let Some(changed) = error.downcast_ref::<crate::source::PublicationChanged>() {
        return (Class::Resync, Some(changed.to_string()));
    }
    if crate::retry::startup_transient(error) {
        return (Class::Unavailable, None);
    }
    (Class::Failure, None)
}

/// Capture reports its terminal error as text across a channel. The first
/// operator stop it saw keeps its class and safe message for the process exit.
static CAPTURE_STOP: OnceLock<(Stop, String)> = OnceLock::new();

pub(crate) fn remember_capture_stop(error: &anyhow::Error) {
    if let Some(stop) = operator_stop(error) {
        let _ = CAPTURE_STOP.set(stop);
    }
}

fn operator_stop(error: &anyhow::Error) -> Option<(Stop, String)> {
    let (class, message) = classify(error);
    let stop = match class {
        Class::Config => Stop::Config,
        Class::Resync => Stop::Resync,
        Class::Failure | Class::Unavailable => return None,
    };
    Some((stop, message?))
}

/// The coordinator's error for a stopped capture actor, keeping its class.
pub(crate) fn capture_failure(error: anyhow::Error) -> anyhow::Error {
    capture_failure_with(error, CAPTURE_STOP.get())
}

/// The complete chain stays in the log; status keeps only the safe message.
fn capture_failure_with(error: anyhow::Error, stop: Option<&(Stop, String)>) -> anyhow::Error {
    let context = format!("source capture stopped: {error}");
    match stop {
        Some((stop, message)) => anyhow::Error::new(OperatorAction {
            stop: *stop,
            message: message.clone(),
        })
        .context(context),
        None => anyhow::Error::msg(context),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Context;

    #[test]
    fn operator_stops_survive_context_and_other_errors_stay_private() {
        let error = Err::<(), _>(resync("replication slot lost required WAL"))
            .context("source setup")
            .unwrap_err();
        assert_eq!(
            classify(&error),
            (
                Class::Resync,
                Some("replication slot lost required WAL".into())
            )
        );
        assert_eq!(
            format!("{error:#}"),
            "source setup: replication slot lost required WAL"
        );
        let error = config("configured tables changed").context("startup");
        assert_eq!(classify(&error).0.code(), CONFIG);

        let changed = crate::source::CaptureProgress {
            durable_lsn: flow_model::PgLsn(0),
            error: Some("contract violated".into()),
            publication_changed: true,
        }
        .failure()
        .unwrap()
        .context("capture");
        assert_eq!(
            classify(&changed),
            (Class::Resync, Some("contract violated".into()))
        );

        let secret = anyhow::anyhow!("postgres://user:secret@host failed");
        assert_eq!(classify(&secret), (Class::Failure, None));
        let transient: anyhow::Error = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap()
            .block_on(async {
                tokio::time::timeout(std::time::Duration::ZERO, std::future::pending::<()>()).await
            })
            .unwrap_err()
            .into();
        let transient = transient.context("connect to PostgreSQL");
        assert_eq!(classify(&transient), (Class::Unavailable, None));
        // An operator stop is never retried, whatever it wraps.
        let settings = transient.context(OperatorAction {
            stop: Stop::Config,
            message: "invalid settings".into(),
        });
        assert!(!crate::retry::startup_transient(&settings));
        assert_eq!(classify(&settings).0, Class::Config);
        assert_eq!(
            [
                Class::Failure,
                Class::Unavailable,
                Class::Config,
                Class::Resync
            ]
            .map(Class::code),
            [1, 75, 78, 79]
        );
    }

    #[test]
    fn a_capture_stop_persists_only_its_own_message() {
        let raw = resync("replication slot lost required WAL")
            .context("query failed for postgres://flow:secret@db");
        let stop = operator_stop(&raw).unwrap();
        // Capture sends the full chain as text; only the safe message is kept.
        let failure = capture_failure_with(anyhow::anyhow!("{raw:#}"), Some(&stop));
        assert_eq!(
            classify(&failure),
            (
                Class::Resync,
                Some("replication slot lost required WAL".into())
            )
        );
        assert!(format!("{failure:#}").contains("secret"));
        let plain = capture_failure_with(anyhow::anyhow!("disconnected"), None);
        assert_eq!(classify(&plain), (Class::Failure, None));
        assert_eq!(plain.to_string(), "source capture stopped: disconnected");
    }
}
