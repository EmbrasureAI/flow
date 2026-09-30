use flow_compactor::{
    DataRewriteScope, DeleteDependency, Error, FileCandidate, Level, Policy, PublicationPressure,
};
use flow_model::FileId;
use std::collections::BTreeSet;

fn file(id: usize, bytes: u64) -> FileCandidate {
    FileCandidate {
        id: FileId(format!("data-{id}")),
        level: Level::L0,
        size_bytes: bytes,
        row_count: 100,
        deleted_rows: 0,
        delete_file_count: 0,
        age_ms: 12_000 - id as u64,
        spec_id: 0,
        partition: vec![],
    }
}
#[test]
fn hard_debt_pauses_publication_and_plan_is_deterministic_and_bounded() {
    let policy = Policy::default();
    let mut files = (0..32).map(|id| file(id, 1 << 20)).collect::<Vec<_>>();
    assert_eq!(
        policy.debt(&files).unwrap().pressure,
        PublicationPressure::Pause
    );
    let plan = policy.plan(4, 1, &files, &[]).unwrap().unwrap();
    assert_eq!(plan.input_files.len(), 32);
    assert_eq!(plan.output_level, Level::L1);
    files.reverse();
    assert_eq!(policy.plan(4, 1, &files, &[]).unwrap().unwrap(), plan);
}

#[test]
fn data_rewrite_scope_excludes_stable_files_from_plans() {
    let mut files = vec![file(0, 1 << 20), file(1, 1 << 20)];
    let mut stable = file(2, 512 << 20);
    stable.level = Level::L2;
    files.push(stable.clone());

    let l0_only = Policy {
        data_rewrite_scope: DataRewriteScope::L0Only,
        l0_soft_files: 2,
        l0_hard_files: 4,
        ..Default::default()
    };
    let plan = l0_only.plan(4, 1, &files, &[]).unwrap().unwrap();
    assert_eq!(
        plan.input_files,
        BTreeSet::from([files[0].id.clone(), files[1].id.clone()])
    );
    assert_eq!(plan.output_level, Level::L1);
    assert!(l0_only.plan(4, 1, &[stable], &[]).unwrap().is_none());

    let disabled = Policy {
        data_rewrite_scope: DataRewriteScope::Disabled,
        ..Default::default()
    };
    assert!(disabled.plan(4, 1, &files, &[]).unwrap().is_none());
}

#[test]
fn stable_small_file_limits_are_validated_independently() {
    for policy in [
        Policy {
            stable_small_soft_files: 0,
            ..Default::default()
        },
        Policy {
            stable_small_soft_files: 33,
            stable_small_hard_files: 32,
            ..Default::default()
        },
    ] {
        assert!(matches!(policy.validate(), Err(Error::InvalidPolicy(_))));
    }
}

#[test]
fn l0_only_can_relax_stable_debt_without_weakening_l0_backpressure() {
    let stable = (100..132)
        .map(|id| {
            let mut candidate = file(id, 1 << 20);
            candidate.level = Level::L1;
            candidate
        })
        .collect::<Vec<_>>();
    let strict = Policy {
        data_rewrite_scope: DataRewriteScope::L0Only,
        ..Default::default()
    };
    assert_eq!(
        strict.debt(&stable).unwrap().pressure,
        PublicationPressure::Pause
    );
    assert!(strict.plan(4, 1, &stable, &[]).unwrap().is_none());

    let diagnostic = Policy {
        data_rewrite_scope: DataRewriteScope::L0Only,
        stable_small_soft_files: 1_000_000,
        stable_small_hard_files: 2_000_000,
        ..Default::default()
    };
    assert_eq!(
        diagnostic.debt(&stable).unwrap().pressure,
        PublicationPressure::Healthy
    );
    let mut files = stable;
    files.extend((0..32).map(|id| file(id, 1 << 20)));
    assert_eq!(
        diagnostic.debt(&files).unwrap().pressure,
        PublicationPressure::Pause
    );
    let plan = diagnostic.plan(4, 1, &files, &[]).unwrap().unwrap();
    assert_eq!(plan.input_files.len(), diagnostic.max_group_files);
    assert_eq!(plan.output_level, Level::L1);
    assert!(
        files
            .iter()
            .filter(|file| plan.input_files.contains(&file.id))
            .all(|file| file.level == Level::L0)
    );

    files.retain(|file| !plan.input_files.contains(&file.id));
    let mut output = file(999, plan.input_bytes);
    output.level = plan.output_level;
    files.push(output);
    assert_eq!(
        diagnostic.debt(&files).unwrap().pressure,
        PublicationPressure::Healthy
    );
}

#[test]
fn shared_deletes_bound_reads_without_expanding_data_inputs() {
    let mut files = (0..4).map(|id| file(id, 64 << 20)).collect::<Vec<_>>();
    files[0].deleted_rows = 90;
    let dependencies = [
        DeleteDependency {
            id: FileId("delete-1".into()),
            size_bytes: 1 << 20,
            row_count: 100,
            targets: BTreeSet::from([files[0].id.clone(), files[2].id.clone()]),
        },
        DeleteDependency {
            id: FileId("delete-2".into()),
            size_bytes: 1 << 20,
            row_count: 100,
            targets: BTreeSet::from([files[2].id.clone(), files[3].id.clone()]),
        },
    ];
    let plan = Policy::default()
        .plan(1, 0, &files, &dependencies)
        .unwrap()
        .unwrap();
    assert_eq!(plan.input_files.len(), 2);
    assert_eq!(
        plan.delete_files,
        BTreeSet::from([FileId("delete-1".into())])
    );
    assert_eq!(plan.delete_input_bytes, 1 << 20);
    assert_eq!(plan.delete_input_rows, 100);
    let bounded = Policy {
        max_group_files: 2,
        max_delete_input_bytes: 1024,
        ..Default::default()
    };
    let fallback = bounded.plan(1, 0, &files, &dependencies).unwrap().unwrap();
    assert_eq!(fallback.input_files, BTreeSet::from([files[1].id.clone()]));
    assert!(fallback.delete_files.is_empty());
    let mut all_shared = dependencies.to_vec();
    all_shared.push(DeleteDependency {
        id: FileId("delete-all".into()),
        size_bytes: 1025,
        row_count: 1,
        targets: files.iter().map(|file| file.id.clone()).collect(),
    });
    assert_eq!(
        bounded.plan(1, 0, &files, &all_shared),
        Err(Error::DependencyBudget)
    );
}

#[test]
fn dependency_unions_admit_independently_bounded_groups() {
    // Each pair shares its delete inputs. Either pair fits the default limits;
    // combining them exceeds one limit, including >1M rows in the first case.
    for (label, count, bytes, rows) in [
        ("rows", 1, 1024, 600_000),
        ("bytes", 1, 20 << 20, 1000),
        ("files", 17, 1024, 1000),
    ] {
        let policy = Policy::default();
        let mut files = (0..4).map(|id| file(id, 32 << 20)).collect::<Vec<_>>();
        let mut deletes = (0..2)
            .flat_map(|pair| {
                (0..count).map(move |index| DeleteDependency {
                    id: FileId(format!("delete-{pair}-{index}")),
                    targets: BTreeSet::from([
                        FileId(format!("data-{}", pair * 2)),
                        FileId(format!("data-{}", pair * 2 + 1)),
                    ]),
                    size_bytes: bytes,
                    row_count: rows,
                })
            })
            .collect::<Vec<_>>();
        for pair in 0..2 {
            let plan = policy
                .plan(10 + pair, 0, &files, &deletes)
                .unwrap()
                .unwrap();
            assert_eq!(
                plan.input_files,
                BTreeSet::from([
                    FileId(format!("data-{}", pair * 2)),
                    FileId(format!("data-{}", pair * 2 + 1)),
                ]),
                "{label}"
            );
            assert_eq!(plan.delete_files.len(), count);
            assert_eq!(plan.delete_input_bytes, bytes * count as u64);
            assert_eq!(plan.delete_input_rows, rows * count as u64);
            files.reverse();
            deletes.reverse();
            assert_eq!(
                policy.plan(10 + pair, 0, &files, &deletes).unwrap(),
                Some(plan.clone())
            );
            files.retain(|file| !plan.input_files.contains(&file.id));
            deletes.retain(|delete| !plan.delete_files.contains(&delete.id));
        }
        assert!(policy.plan(12, 0, &files, &deletes).unwrap().is_none());
    }
}

#[test]
fn an_impossible_urgent_seed_does_not_hide_the_next_useful_group() {
    let policy = Policy::default();
    let mut files = (0..3).map(|id| file(id, 32 << 20)).collect::<Vec<_>>();
    files[0].deleted_rows = 90;
    let deletes = [
        DeleteDependency {
            id: FileId("oversized-delete".into()),
            targets: BTreeSet::from([files[0].id.clone()]),
            size_bytes: 1024,
            row_count: policy.max_delete_input_rows + 1,
        },
        DeleteDependency {
            id: FileId("bounded-delete".into()),
            targets: BTreeSet::from([files[1].id.clone(), files[2].id.clone()]),
            size_bytes: 1024,
            row_count: 100,
        },
    ];
    let plan = policy.plan(1, 0, &files, &deletes).unwrap().unwrap();
    assert_eq!(
        plan.input_files,
        BTreeSet::from([files[1].id.clone(), files[2].id.clone()])
    );
    assert_eq!(plan.delete_files, BTreeSet::from([deletes[1].id.clone()]));
}

#[test]
fn undersized_idle_groups_do_not_report_a_dependency_budget_failure() {
    let policy = Policy::default();
    let files = (0..4)
        .map(|id| {
            let mut file = file(id, 128 << 20);
            file.level = Level::L1;
            file
        })
        .collect::<Vec<_>>();
    for count in [2, 4] {
        let selected = &files[..count];
        let delete = DeleteDependency {
            id: FileId("shared-delete".into()),
            targets: selected.iter().map(|file| file.id.clone()).collect(),
            size_bytes: 1024,
            row_count: policy.max_delete_input_rows + 1,
        };
        let result = policy.plan(1, 0, selected, &[delete]);
        if count == 2 {
            assert_eq!(result, Ok(None));
        } else {
            assert_eq!(result, Err(Error::DependencyBudget));
        }
    }
}
#[test]
fn delete_density_rewrites_a_large_singleton_without_waiting_for_file_count() {
    let mut input = file(1, 512 << 20);
    input.level = Level::L2;
    input.deleted_rows = 80;
    assert_eq!(
        Policy::default().debt(&[input.clone()]).unwrap().pressure,
        PublicationPressure::Delay
    );
    let plan = Policy::default()
        .plan(1, 0, &[input], &[])
        .unwrap()
        .unwrap();
    assert_eq!(plan.input_files.len(), 1);
    assert_eq!(plan.output_level, Level::L2);
}
#[test]
fn forced_delete_rewrite_keeps_companions_within_the_size_tier() {
    let mut small = file(0, 1 << 20);
    small.level = Level::L1;
    small.deleted_rows = 80;
    let mut at_limit = file(1, 4 << 20);
    at_limit.level = Level::L1;
    let mut over_limit = file(2, (4 << 20) + 1);
    over_limit.level = Level::L1;
    let mut large = file(3, 128 << 20);
    large.level = Level::L1;
    let policy = Policy::default();
    let plan = policy
        .plan(
            1,
            0,
            &[
                small.clone(),
                at_limit.clone(),
                over_limit.clone(),
                large.clone(),
            ],
            &[],
        )
        .unwrap()
        .unwrap();
    assert_eq!(
        plan.input_files,
        BTreeSet::from([small.id.clone(), at_limit.id])
    );
    assert_eq!(plan.input_bytes, 5 << 20);
    let singleton = policy
        .plan(1, 0, &[small.clone(), over_limit, large], &[])
        .unwrap()
        .unwrap();
    assert_eq!(singleton.input_files, BTreeSet::from([small.id]));
    assert_eq!(singleton.input_bytes, 1 << 20);
}

#[test]
fn soft_aging_closes_old_singletons_and_new_files_wait() {
    let mut input = file(1, 1);
    input.age_ms = 100;
    assert!(
        Policy::default()
            .plan(1, 0, &[input.clone()], &[])
            .unwrap()
            .is_none()
    );
    input.age_ms = 20_000;
    assert!(
        Policy::default()
            .plan(1, 0, &[input], &[])
            .unwrap()
            .is_some()
    );
}

#[test]
fn fragmented_external_output_is_bounded_without_rewriting_idle_singletons() {
    let policy = Policy::default();
    let mut files = (0..40)
        .map(|id| {
            let mut candidate = file(id, 128 + id as u64 * 64);
            candidate.level = if id % 2 == 0 { Level::L1 } else { Level::L2 };
            candidate
        })
        .collect::<Vec<_>>();
    assert_eq!(
        policy.debt(&files).unwrap().pressure,
        PublicationPressure::Pause
    );
    let plan = policy.plan(1, 0, &files, &[]).unwrap().unwrap();
    assert_eq!(plan.input_files.len(), policy.max_group_files);
    assert_eq!(plan.output_level, Level::L2);
    files.retain(|file| !plan.input_files.contains(&file.id));
    let mut consolidated = file(100, plan.input_bytes);
    consolidated.level = Level::L2;
    files.push(consolidated);
    assert_eq!(
        policy.debt(&files).unwrap().pressure,
        PublicationPressure::Healthy
    );
    assert!(policy.plan(2, 0, &files, &[]).unwrap().is_none());
}

#[test]
fn delete_pressure_prioritizes_effective_reclamation_unless_l0_is_hard_limited() {
    // Retained matrix10 snapshot155: eight L0s, three L1s, two L2s and sixteen
    // deletes. The prior five-L0 plan reclaimed26,580 rows but grew delete
    // count16→18. The L2 pair reclaims133,693 with bounded dependencies.
    let mut files = [
        (Level::L1, 87_048, 59_490, 2_109_743, 13_373), // af010279
        (Level::L2, 154_245, 103_698, 3_789_889, 11_271), // 7ddca151
        (Level::L2, 63_241, 29_995, 1_607_322, 8_884),  // 30e9c51d
        (Level::L1, 122_653, 56_228, 2_982_870, 4_471), // deddc61c
        (Level::L0, 22_360, 7_159, 542_245, 3_724),
        (Level::L0, 22_438, 6_220, 544_300, 3_358),
        (Level::L0, 22_370, 5_339, 542_559, 2_989),
        (Level::L0, 22_364, 4_435, 542_919, 2_597),
        (Level::L0, 22_409, 3_427, 544_141, 2_232),
        (Level::L1, 98_758, 23_756, 2_401_180, 1_421), // b87d705a
        (Level::L0, 22_425, 2_341, 543_149, 730),
        (Level::L0, 22_456, 1_208, 545_009, 368),
        (Level::L0, 22_499, 0, 546_642, 0),
    ]
    .into_iter()
    .enumerate()
    .map(|(id, (level, rows, deleted, bytes, age))| {
        let mut candidate = file(id, bytes);
        candidate.level = level;
        candidate.row_count = rows;
        candidate.deleted_rows = deleted;
        candidate.age_ms = age;
        candidate
    })
    .collect::<Vec<_>>();
    let dependencies: &[(u64, u64, &[usize])] = &[
        (22_639, 63_200, &[0, 1, 2]),
        (12_612, 35_365, &[1]),
        (25_128, 68_888, &[0]),
        (25_128, 69_751, &[0, 3]),
        (25_128, 73_831, &[1, 2, 3]),
        (25_128, 69_135, &[1]),
        (19_150, 52_669, &[1]),
        (15_534, 49_179, &[0, 4, 5, 6, 7, 8]),
        (15_534, 44_156, &[0, 9]),
        (15_534, 44_399, &[3, 9]),
        (15_534, 43_053, &[3]),
        (15_534, 45_124, &[1, 2, 3]),
        (11_013, 31_154, &[1]),
        (19_865, 64_231, &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
        (19_896, 65_138, &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]),
        (19_939, 67_314, &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]),
    ];
    let deletes = dependencies
        .iter()
        .enumerate()
        .map(|(id, &(rows, bytes, targets))| {
            for &target in targets {
                files[target].delete_file_count += 1;
            }
            DeleteDependency {
                id: FileId(format!("delete-{id}")),
                row_count: rows,
                size_bytes: bytes,
                targets: targets
                    .iter()
                    .map(|&target| files[target].id.clone())
                    .collect(),
            }
        })
        .collect::<Vec<_>>();
    let policy = Policy::default();
    let plan = policy.plan(155, 0, &files, &deletes).unwrap().unwrap();
    assert_eq!(
        plan.input_files,
        BTreeSet::from([files[1].id.clone(), files[2].id.clone()])
    );
    assert_eq!(plan.input_bytes, 5_397_211);
    assert_eq!(plan.delete_files.len(), 10);
    assert_eq!(plan.delete_input_rows, 190_904);
    assert_eq!(plan.delete_input_bytes, 567_161);
    let l0_bytes = files
        .iter()
        .filter(|file| file.level == Level::L0)
        .map(|file| file.size_bytes)
        .sum();
    for hard_limit in [
        Policy {
            l0_soft_files: 8,
            l0_hard_files: 8,
            ..policy.clone()
        },
        Policy {
            l0_soft_bytes: l0_bytes,
            l0_hard_bytes: l0_bytes,
            ..policy.clone()
        },
        Policy {
            oldest_l0_soft_ms: 3_724,
            oldest_l0_hard_ms: 3_724,
            ..policy.clone()
        },
    ] {
        let plan = hard_limit.plan(155, 0, &files, &deletes).unwrap().unwrap();
        assert_eq!(plan.output_level, Level::L1);
        assert!(
            files
                .iter()
                .filter(|file| plan.input_files.contains(&file.id))
                .all(|file| file.level == Level::L0)
        );
    }
}

#[test]
fn target_size_waits_for_minimum_group_even_under_hard_debt() {
    let policy = Policy {
        min_group_files: 8,
        ..Default::default()
    };
    let files = (0..32)
        .map(|id| {
            let mut candidate = file(id, 120 << 20);
            candidate.level = Level::L1;
            candidate.age_ms = 24 * 60 * 60 * 1000;
            candidate
        })
        .collect::<Vec<_>>();
    assert_eq!(
        policy.debt(&files).unwrap().pressure,
        PublicationPressure::Pause
    );
    let plan = policy.plan(4, 1, &files, &[]).unwrap().unwrap();
    assert_eq!(plan.input_files.len(), 8);
    assert_eq!(plan.input_bytes, 960 << 20);
    let deletes = files
        .iter()
        .map(|file| DeleteDependency {
            id: FileId(format!("delete-{}", file.id.0)),
            size_bytes: 1,
            row_count: 1,
            targets: BTreeSet::from([file.id.clone()]),
        })
        .collect::<Vec<_>>();
    let limited = Policy {
        max_delete_input_files: 1,
        ..policy
    };
    assert!(matches!(
        limited.plan(4, 1, &files, &deletes),
        Err(Error::DependencyBudget)
    ));
}

#[test]
fn hard_l0_relief_precedes_dense_stable_rewrites() {
    let policy = Policy::default();
    let mut files = (0..32).map(|id| file(id, 1 << 20)).collect::<Vec<_>>();
    let l0 = files
        .iter()
        .map(|file| file.id.clone())
        .collect::<BTreeSet<_>>();
    for id in 32..40 {
        let mut stable = file(id, 512 << 20);
        stable.level = Level::L2;
        stable.deleted_rows = 80;
        files.push(stable);
    }
    let plan = policy.plan(4, 1, &files, &[]).unwrap().unwrap();
    assert_eq!(plan.input_files, l0);
    assert_eq!(plan.output_level, Level::L1);
    files.reverse();
    assert_eq!(policy.plan(4, 1, &files, &[]).unwrap().unwrap(), plan);
}
