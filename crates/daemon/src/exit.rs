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
/// operator stop it saw keeps its class for the process exit.
static CAPTURE_STOP: OnceLock<Stop> = OnceLock::new();

pub(crate) fn remember_capture_stop(error: &anyhow::Error) {
    let stop = match classify(error).0 {
        Class::Config => Stop::Config,
        Class::Resync => Stop::Resync,
        Class::Failure | Class::Unavailable => return,
    };
    let _ = CAPTURE_STOP.set(stop);
}

/// The coordinator's error for a stopped capture actor, keeping its class.
pub(crate) fn capture_failure(error: anyhow::Error) -> anyhow::Error {
    let message = format!("source capture stopped: {error}");
    match CAPTURE_STOP.get() {
        Some(stop) => OperatorAction {
            stop: *stop,
            message,
        }
        .into(),
        None => anyhow::Error::msg(message),
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
}
