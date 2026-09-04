use flow_coordinator::{Priority, Scheduler};
use flow_model::TableId;
use std::time::{Duration, Instant};

#[test]
fn measured_service_time_backlog_age_and_global_permits_control_publication() {
    let start = Instant::now();
    let realtime = TableId(1);
    let balanced = TableId(2);
    let mut scheduler = Scheduler::new(100, 16 << 20, 10, start).unwrap();
    scheduler.push(
        realtime,
        Some(1),
        128,
        Priority::Realtime,
        Duration::ZERO,
        start,
    );
    assert_eq!(
        scheduler.next_deadline(),
        Some(start + Duration::from_millis(400))
    );
    assert_eq!(
        scheduler.take_ready(start + Duration::from_millis(399)),
        None
    );
    let now = start + Duration::from_millis(400);
    assert_eq!(scheduler.take_ready(now), Some(realtime));

    // An observed slow table loses its batching delay. It does not bypass the
    // source's catalog request budget or force unrelated tables to flush early.
    scheduler.record_completion(realtime, Duration::from_millis(800));
    scheduler.push(
        realtime,
        Some(1),
        128,
        Priority::Realtime,
        Duration::from_millis(150),
        now,
    );
    scheduler.push(
        balanced,
        Some(1),
        128,
        Priority::Balanced,
        Duration::ZERO,
        now,
    );
    assert_eq!(
        scheduler.next_deadline(),
        Some(now + Duration::from_millis(100))
    );
    assert_eq!(scheduler.take_ready(now), None);
    assert_eq!(
        scheduler.take_ready(now + Duration::from_millis(100)),
        Some(realtime)
    );
    assert_eq!(
        scheduler.next_deadline(),
        Some(now + Duration::from_millis(2700))
    );

    // Size can force a balanced table ready, while a busy publication lane
    // remains fenced until its current operation completes.
    scheduler.push(
        balanced,
        Some(1),
        16 << 20,
        Priority::Balanced,
        Duration::ZERO,
        now,
    );
    scheduler.stall(balanced, true);
    assert_eq!(scheduler.next_deadline(), None);
    scheduler.stall(balanced, false);
    assert_eq!(
        scheduler.take_ready(now + Duration::from_millis(200)),
        Some(balanced)
    );
    assert_eq!(scheduler.next_deadline(), None);
}

#[test]
fn mutation_threshold_legacy_work_and_zero_rows_preserve_deadlines() {
    let now = Instant::now();
    let mut scheduler = Scheduler::new(100, 32 << 20, 1000, now).unwrap();
    let (counted, legacy, empty) = (TableId(1), TableId(2), TableId(3));
    scheduler.push(empty, Some(0), 0, Priority::Realtime, Duration::ZERO, now);
    scheduler.push(
        counted,
        Some(99),
        1024,
        Priority::Realtime,
        Duration::ZERO,
        now,
    );
    assert_eq!(scheduler.take_ready(now), None);
    scheduler.push(counted, Some(1), 8, Priority::Realtime, Duration::ZERO, now);
    assert_eq!(scheduler.take_ready(now), Some(counted));
    scheduler.push(legacy, None, 8, Priority::Realtime, Duration::ZERO, now);
    assert_eq!(
        scheduler.take_ready(now),
        None,
        "legacy work still respects catalog permits"
    );
    assert_eq!(
        scheduler.take_ready(now + Duration::from_millis(1)),
        Some(legacy)
    );
    assert_eq!(
        scheduler.next_deadline(),
        Some(now + Duration::from_millis(400))
    );
    assert_eq!(
        scheduler.take_ready(now + Duration::from_millis(400)),
        Some(empty)
    );
}
