use flow_coordinator::{Priority, Scheduler};
use flow_model::TableId;
use std::time::{Duration, Instant};

#[test]
fn overdue_tables_progress_under_continuous_realtime_arrivals() {
    let start = Instant::now();
    let mut scheduler = Scheduler::new(1, 1024, 10, start).unwrap();
    let efficient = TableId(1);
    let realtime = TableId(2);
    scheduler.push(
        efficient,
        Some(0),
        0,
        Priority::Efficient,
        Duration::ZERO,
        start,
    );
    for tick in 0..150 {
        let now = start + Duration::from_millis(tick * 100);
        scheduler.push(
            realtime,
            Some(1),
            1,
            Priority::Realtime,
            Duration::ZERO,
            now,
        );
        if scheduler.take_ready(now) == Some(efficient) {
            assert!(tick >= 147, "respect the table's initial batching deadline");
            return;
        }
    }
    panic!("realtime arrivals starved an overdue table");
}

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

#[test]
fn continued_arrivals_and_force_preserve_overdue_order() {
    let start = Instant::now();
    let mut scheduler = Scheduler::new(1, 1024, 10, start).unwrap();
    for (table, tick) in [(1, 0), (2, 1), (1, 2)] {
        scheduler.push(
            TableId(table),
            Some(1),
            1,
            Priority::Realtime,
            Duration::ZERO,
            start + Duration::from_millis(tick),
        );
    }
    scheduler.force(TableId(1), start + Duration::from_millis(3));
    assert_eq!(scheduler.next_deadline(), Some(start));
    assert_eq!(
        scheduler.take_ready(start + Duration::from_millis(3)),
        Some(TableId(1))
    );
}
