use flow_compactor::{
    DataRewriteScope, Debt, DeleteDebt, DeleteDependency, DeleteLayout, Error, FileCandidate,
    Level, MergeBudget, PlanReason, Policy, PublicationPressure, TargetDeleteFile, TargetDeletes,
    TargetRanges,
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
        target_delete_files: 0,
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
    // With consolidation, the default, an L1 seed admits only companions that
    // are themselves past the deleted fraction.
    let plan = Policy::default()
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
    assert_eq!(plan.input_files, BTreeSet::from([small.id.clone()]));
    // The current policy fills the group with companions within the size tier.
    let policy = Policy {
        delete_debt: DeleteDebt::Current,
        ..Policy::default()
    };
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

/// The profiled shape: a mostly-live L2 base, sixteen epochs of position
/// deletes against it, and small hot L0 files of which one is dense.
fn hot_base_with_delete_fanout() -> (Vec<FileCandidate>, Vec<DeleteDependency>) {
    let mut base = file(0, 600 << 20);
    base.level = Level::L2;
    base.row_count = 1_000_000;
    base.deleted_rows = 90_000;
    base.age_ms = 60_000;
    let mut files = vec![base];
    for id in 1..=4 {
        let mut l0 = file(id, 1 << 20);
        l0.age_ms = 2_000;
        l0.deleted_rows = if id == 1 { 50 } else { 5 };
        files.push(l0);
    }
    let deletes = (0..16)
        .map(|epoch| {
            let mut targets = BTreeSet::from([files[0].id.clone()]);
            if epoch % 4 == 0 {
                targets.insert(files[1 + epoch / 4].id.clone());
            }
            DeleteDependency {
                id: FileId(format!("epoch-delete-{epoch}")),
                size_bytes: 100 << 10,
                row_count: 10_000,
                targets,
            }
        })
        .collect::<Vec<_>>();
    for delete in &deletes {
        for file in files
            .iter_mut()
            .filter(|file| delete.targets.contains(&file.id))
        {
            file.delete_file_count += 1;
        }
    }
    (files, deletes)
}

#[test]
fn delete_file_count_debt_selects_consolidation_not_a_base_rewrite() {
    let (files, deletes) = hot_base_with_delete_fanout();
    let base = files[0].id.clone();
    let current = Policy {
        delete_debt: DeleteDebt::Current,
        ..Policy::default()
    };
    let consolidate = Policy::default();
    assert_eq!(consolidate.delete_debt, DeleteDebt::Consolidate);
    for policy in [&current, &consolidate] {
        let debt = policy
            .debt_with_shared_deletes(&files, deletes.len())
            .unwrap();
        assert_eq!(debt.pressure, PublicationPressure::Pause);
        assert_eq!(debt.max_delete_files, 16);
        assert_eq!(debt.dense_delete_files, 1);
        assert_eq!(
            policy.delete_file_pressure(&debt),
            PublicationPressure::Pause
        );
        assert_eq!(policy.layout_pressure(&debt), PublicationPressure::Healthy);
        assert_eq!(policy.pressure_reasons(&debt), ["delete_file_count"]);
        // Shared-file pressure is unchanged from the per-file debt.
        assert_eq!(
            policy.debt_with_shared_deletes(&files, 0).unwrap(),
            Debt {
                shared_delete_files: 0,
                ..policy.debt(&files).unwrap()
            }
        );
    }
    let debt = consolidate.debt_with_shared_deletes(&files, 16).unwrap();
    assert!(consolidate.consolidates_delete_debt(&debt));
    assert!(!current.consolidates_delete_debt(&debt));
    // The current policy pays delete-file count by rewriting the whole base.
    let plan = current.plan(1, 0, &files, &deletes).unwrap().unwrap();
    assert_eq!(plan.input_files, BTreeSet::from([base.clone()]));
    assert_eq!(plan.reason, PlanReason::DeleteFileCount);
    assert_eq!(plan.output_level, Level::L2);
    // Consolidation pays it instead; data work only reclaims the dense L0
    // file under the existing L0 grouping, and never moves the base.
    let plan = consolidate.plan(1, 0, &files, &deletes).unwrap().unwrap();
    assert!(!plan.input_files.contains(&base));
    assert!(plan.input_files.contains(&files[1].id));
    assert_eq!(plan.reason, PlanReason::DeletedRows);
    assert_eq!(plan.output_level, Level::L1);
    // Without a dense file, soft delete-file count alone selects no stable
    // data rewrite; soft pressure still groups L0 files as before.
    let mut sparse = files.clone();
    sparse[1].deleted_rows = 5;
    for file in &mut sparse {
        file.delete_file_count = file.delete_file_count.min(8);
    }
    let debt = consolidate.debt_with_shared_deletes(&sparse, 8).unwrap();
    assert_eq!(debt.pressure, PublicationPressure::Delay);
    assert!(consolidate.consolidates_delete_debt(&debt));
    let soft_deletes = deletes
        .iter()
        .take(8)
        .cloned()
        .map(|mut delete| {
            delete.targets.retain(|target| *target == base);
            delete
        })
        .collect::<Vec<_>>();
    assert_eq!(
        current
            .plan(1, 0, &sparse, &soft_deletes)
            .unwrap()
            .unwrap()
            .input_files,
        BTreeSet::from([base.clone()])
    );
    let plan = consolidate
        .plan(1, 0, &sparse, &soft_deletes)
        .unwrap()
        .unwrap();
    assert!(!plan.input_files.contains(&base));
    assert_eq!(plan.reason, PlanReason::L0Debt);
    // After consolidation the base carries one shared delete file: the debt
    // is healthy again instead of re-entering hard pressure.
    let mut consolidated = sparse;
    for file in &mut consolidated {
        file.delete_file_count = 1;
    }
    let debt = consolidate
        .debt_with_shared_deletes(&consolidated, 1)
        .unwrap();
    assert_eq!(debt.pressure, PublicationPressure::Healthy);
    assert!(!consolidate.consolidates_delete_debt(&debt));
    assert!(consolidate.pressure_reasons(&debt).is_empty());
}

#[test]
fn consolidated_delete_debt_rewrites_only_files_past_the_deleted_fraction() {
    let consolidate = Policy::default();
    let mut files = Vec::new();
    for (id, level, deleted) in [
        (0, Level::L2, 350_000),
        (1, Level::L2, 10_000),
        (2, Level::L1, 400_000),
        (3, Level::L1, 1_000),
    ] {
        let mut stable = file(id, 300 << 20);
        stable.level = level;
        stable.row_count = 1_000_000;
        stable.deleted_rows = deleted;
        stable.delete_file_count = 12;
        stable.age_ms = 60_000;
        files.push(stable);
    }
    // The current policy groups each dense file with a mostly-live neighbour
    // selected only for its delete-file count.
    let current = Policy {
        delete_debt: DeleteDebt::Current,
        ..Policy::default()
    };
    let plan = current.plan(1, 0, &files, &[]).unwrap().unwrap();
    assert_eq!(
        plan.input_files,
        BTreeSet::from([files[2].id.clone(), files[3].id.clone()])
    );
    let rest = [files[0].clone(), files[1].clone(), files[3].clone()];
    let plan = current.plan(1, 0, &rest, &[]).unwrap().unwrap();
    assert_eq!(
        plan.input_files,
        BTreeSet::from([files[0].id.clone(), files[1].id.clone()])
    );
    // Consolidation leaves the mostly-live files in place.
    let plan = consolidate.plan(1, 0, &files, &[]).unwrap().unwrap();
    assert_eq!(plan.input_files, BTreeSet::from([files[2].id.clone()]));
    assert_eq!(plan.reason, PlanReason::DeletedRows);
    let plan = consolidate.plan(1, 0, &rest, &[]).unwrap().unwrap();

    assert_eq!(plan.input_files, BTreeSet::from([files[0].id.clone()]));
    assert_eq!(plan.output_level, Level::L2);
    assert_eq!(plan.reason, PlanReason::DeletedRows);
    // Mostly-live files are left to consolidation below the hard limit.
    assert!(
        consolidate
            .plan(1, 0, &[files[1].clone(), files[3].clone()], &[])
            .unwrap()
            .is_none()
    );
}

#[test]
fn consolidated_delete_debt_falls_back_to_count_driven_rewrite_at_the_hard_limit() {
    let consolidate = Policy::default();
    let mut base = file(0, 600 << 20);
    base.level = Level::L2;
    base.row_count = 1_000_000;
    base.deleted_rows = 10_000;
    base.age_ms = 60_000;
    base.delete_file_count = 16;
    // When consolidation cannot reduce the count, the data planner keeps its
    // count-driven progress guarantee.
    let plan = consolidate
        .plan(1, 0, std::slice::from_ref(&base), &[])
        .unwrap()
        .unwrap();
    assert_eq!(plan.input_files, BTreeSet::from([base.id.clone()]));
    assert_eq!(plan.reason, PlanReason::DeleteFileCount);
    base.delete_file_count = 15;
    assert!(consolidate.plan(1, 0, &[base], &[]).unwrap().is_none());
}

#[test]
fn format_v3_tables_pay_delete_file_count_with_a_data_rewrite() {
    use iceberg::spec::FormatVersion;
    let policy = Policy::default();
    let mut base = file(0, 600 << 20);
    base.level = Level::L2;
    base.row_count = 1_000_000;
    base.deleted_rows = 10_000;
    base.age_ms = 60_000;
    base.delete_file_count = policy.delete_files_soft;
    let debt = policy.debt(std::slice::from_ref(&base)).unwrap();
    assert_eq!(debt.pressure, PublicationPressure::Delay);
    for version in [FormatVersion::V1, FormatVersion::V2] {
        let v2 = policy.for_format_version(version);
        assert!(matches!(v2, std::borrow::Cow::Borrowed(_)));
        assert!(v2.consolidates_delete_debt(&debt));
        assert!(
            v2.plan(1, 0, std::slice::from_ref(&base), &[])
                .unwrap()
                .is_none()
        );
    }
    // A data file keeps at most one deletion vector, so consolidation cannot
    // lower this count; v3 rewrites the mostly-live base as before.
    let v3 = policy.for_format_version(FormatVersion::V3);
    assert_eq!(v3.delete_debt, DeleteDebt::Current);
    assert_eq!(
        format!("{:?}", *v3),
        format!(
            "{:?}",
            Policy {
                delete_debt: DeleteDebt::Current,
                ..Policy::default()
            }
        )
    );
    assert!(!v3.consolidates_delete_debt(&debt));
    let plan = v3
        .plan(1, 0, std::slice::from_ref(&base), &[])
        .unwrap()
        .unwrap();
    assert_eq!(plan.input_files, BTreeSet::from([base.id.clone()]));
    assert_eq!(plan.reason, PlanReason::DeleteFileCount);
}

fn per_target() -> Policy {
    Policy {
        delete_layout: DeleteLayout::PerTarget,
        ..Policy::default()
    }
}

fn target(name: &str, rows: &[u64], new_rows: u64, other_files: usize) -> TargetDeletes {
    TargetDeletes {
        target: FileId(name.into()),
        files: rows
            .iter()
            .enumerate()
            .map(|(index, rows)| TargetDeleteFile {
                id: FileId(format!("{name}-delete-{index}")),
                rows: *rows,
                bytes: *rows * 8,
            })
            .collect(),
        new_rows,
        other_files,
        full_size: true,
    }
}

const UNLIMITED: MergeBudget = MergeBudget {
    files: usize::MAX,
    bytes: u64::MAX,
    rows: u64::MAX,
};

fn ids(name: &str, indices: &[usize]) -> BTreeSet<FileId> {
    indices
        .iter()
        .map(|index| FileId(format!("{name}-delete-{index}")))
        .collect()
}

#[test]
fn per_target_merges_stay_within_new_rows_and_skip_large_tiers() {
    let policy = per_target();
    assert_eq!(policy.target_delete_files(), 4);
    // A 600k accumulated file and a 100k tier stay put; only the small files
    // whose rows total at most the 10k new rows join the new output.
    let base = target("base", &[600_000, 100_000, 5_000, 3_000], 10_000, 0);
    assert_eq!(
        policy.select_target_merges(&[base], UNLIMITED),
        ids("base", &[2, 3])
    );
    // A large file is merged once new deletes reach its size.
    let base = target("base", &[100_000, 1_000], 100_000, 0);
    assert_eq!(
        policy.select_target_merges(&[base], UNLIMITED),
        ids("base", &[1])
    );
    let base = target("base", &[90_000, 1_000], 100_000, 0);
    assert_eq!(
        policy.select_target_merges(&[base], UNLIMITED),
        ids("base", &[0, 1])
    );
}

#[test]
fn per_target_merges_keep_each_target_within_its_file_cap() {
    let policy = per_target();
    // Five tiers plus the new output exceed four files: the two smallest
    // tiers merge, never the largest.
    let base = target(
        "base",
        &[600_000, 200_000, 100_000, 50_000, 20_000],
        1_000,
        0,
    );
    assert_eq!(
        policy.select_target_merges(&[base], UNLIMITED),
        ids("base", &[3, 4])
    );
    // Shared files left in place count against the cap too.
    let hot = target("hot", &[6_000, 5_000], 0, 3);
    assert_eq!(
        policy.select_target_merges(&[hot], UNLIMITED),
        ids("hot", &[0, 1])
    );
    // Without new rows, files under the cap are left alone, and one file is
    // never rewritten on its own.
    assert!(
        policy
            .select_target_merges(&[target("idle", &[5_000, 6_000], 0, 0)], UNLIMITED)
            .is_empty()
    );
    assert!(
        policy
            .select_target_merges(&[target("lone", &[5_000], 0, 9)], UNLIMITED)
            .is_empty()
    );
}

#[test]
fn per_target_merges_respect_the_remaining_budget_most_files_first() {
    let policy = per_target();
    let crowded = target("crowded", &[10, 20, 30, 40, 50, 60], 0, 0);
    let quiet = target("quiet", &[1, 2], 5, 0);
    // The crowded target needs three merges to reach the cap and is served
    // first; the quiet one gets what is left.
    let budget = MergeBudget {
        files: 4,
        bytes: u64::MAX,
        rows: u64::MAX,
    };
    let mut expected = ids("crowded", &[0, 1, 2]);
    expected.extend(ids("quiet", &[0]));
    assert_eq!(
        policy.select_target_merges(&[quiet.clone(), crowded.clone()], budget),
        expected
    );
    // A row budget ends a target's selection at the first file that does not
    // fit; a lone admitted file without new rows is dropped.
    let budget = MergeBudget {
        files: usize::MAX,
        bytes: u64::MAX,
        rows: 15,
    };
    assert_eq!(
        policy.select_target_merges(&[crowded, quiet], budget),
        ids("quiet", &[0, 1])
    );
}

#[test]
fn per_target_layout_charges_only_shared_dependencies_table_wide() {
    let shared = Policy {
        min_file_age_ms: 0,
        ..Policy::default()
    };
    let per_target = Policy {
        min_file_age_ms: 0,
        ..per_target()
    };
    let mut files = (0..4).map(|id| file(id, 1 << 20)).collect::<Vec<_>>();
    for file in &mut files {
        file.level = Level::L2;
        file.size_bytes = 512 << 20;
        file.delete_file_count = 5;
    }
    // Twenty single-target files across four stable data files: the shared
    // layout charges them table-wide and falls back to a count-driven plan at
    // the hard limit. Per target they are each file's own debt, under soft.
    let deletes = (0..20)
        .map(|index| DeleteDependency {
            id: FileId(format!("delete-{index}")),
            targets: BTreeSet::from([files[index % 4].id.clone()]),
            size_bytes: 100,
            row_count: 1,
        })
        .collect::<Vec<_>>();
    assert!(
        Policy {
            delete_files_soft: 5,
            ..shared.clone()
        }
        .plan(1, 0, &files, &deletes)
        .unwrap()
        .is_some()
    );
    assert!(
        Policy {
            delete_files_soft: 5,
            ..per_target.clone()
        }
        .plan(1, 0, &files, &deletes)
        .unwrap()
        .is_none()
    );
    // Plans carry the layout to the worker's residual writer.
    let l0 = (0..24).map(|id| file(id, 1 << 20)).collect::<Vec<_>>();
    assert!(
        per_target
            .plan(1, 0, &l0, &[])
            .unwrap()
            .unwrap()
            .per_target_deletes
    );
    assert!(
        !shared
            .plan(1, 0, &l0, &[])
            .unwrap()
            .unwrap()
            .per_target_deletes
    );
    // Format v3 keeps one vector per data file and uses the shared layout.
    let v3 = per_target.for_format_version(iceberg::spec::FormatVersion::V3);
    assert_eq!(v3.delete_layout, DeleteLayout::Shared);
    let v2 = per_target.for_format_version(iceberg::spec::FormatVersion::V2);
    assert_eq!(v2.delete_layout, DeleteLayout::PerTarget);
}

#[test]
fn per_target_budget_merges_the_cheapest_full_size_files_down_to_half() {
    let policy = Policy {
        target_delete_files_max: 8,
        ..per_target()
    };
    // Eight files on two full-size targets, each within its cap of four,
    // reach the budget of eight: the cheapest merges run until four remain,
    // and the accumulated files are never read. A small target's files are
    // left for its data rewrite.
    let a = target("a", &[1_000, 10, 20, 30], 0, 0);
    let b = target("b", &[500, 5, 6, 7], 0, 0);
    let small = TargetDeletes {
        full_size: false,
        ..target("small", &[1, 2, 3, 4], 0, 0)
    };
    // Files too large to merge two in one consolidation neither count nor
    // merge.
    let settled = target("settled", &[600_000, 700_000], 0, 0);
    assert!(policy.settled_target_delete(500_001, 0));
    assert!(policy.settled_target_delete(1, (16 << 20) + 1));
    assert!(!policy.settled_target_delete(500_000, 16 << 20));
    let mut expected = ids("a", &[1, 2, 3]);
    expected.extend(ids("b", &[1, 2, 3]));
    assert_eq!(
        policy.select_target_merges(
            &[a.clone(), b.clone(), small.clone(), settled.clone()],
            UNLIMITED
        ),
        expected
    );
    assert!(
        Policy {
            target_delete_files_max: 1,
            ..policy.clone()
        }
        .select_target_merges(&[settled], UNLIMITED)
        .is_empty()
    );
    // Below the budget nothing more is merged.
    assert!(
        Policy {
            target_delete_files_max: 9,
            ..policy.clone()
        }
        .select_target_merges(&[a.clone(), b.clone(), small], UNLIMITED)
        .is_empty()
    );
    // A target with new rows has an output already, so each of its files
    // merged removes one. The merge budget still bounds the reads.
    let budget = MergeBudget {
        files: 3,
        bytes: u64::MAX,
        rows: u64::MAX,
    };
    let hot = target("hot", &[3, 200], 1, 0);
    let mut expected = ids("b", &[1, 2]);
    expected.extend(ids("hot", &[0]));
    assert_eq!(policy.select_target_merges(&[a, b, hot], budget), expected);
}

#[test]
fn per_target_budget_is_soft_delete_file_debt_only_with_extra_full_size_files() {
    let policy = Policy {
        target_delete_files_max: 8,
        ..per_target()
    };
    let mut files = (0..8).map(|id| file(id, 1 << 20)).collect::<Vec<_>>();
    for file in &mut files {
        file.level = Level::L2;
        file.size_bytes = policy.l1_target_bytes;
        file.target_delete_files = 1;
        file.delete_file_count = 1;
    }
    // One file per full-size data file cannot be merged further.
    let debt = policy.debt(&files).unwrap();
    assert_eq!(debt.target_delete_files, 8);
    assert_eq!(debt.excess_target_delete_files, 0);
    assert!(!policy.over_target_budget(&debt));
    assert_eq!(debt.pressure, PublicationPressure::Healthy);
    files[0].target_delete_files = 2;
    files[0].delete_file_count = 2;
    let debt = policy.debt(&files).unwrap();
    assert!(policy.over_target_budget(&debt));
    assert_eq!(debt.pressure, PublicationPressure::Delay);
    assert_eq!(
        policy.delete_file_pressure(&debt),
        PublicationPressure::Delay
    );
    assert!(policy.consolidates_delete_debt(&debt));
    assert_eq!(policy.pressure_reasons(&debt), ["delete_file_count"]);
    // Files on small data files and the shared layout do not count.
    let mut small = files.clone();
    small[0].size_bytes = policy.l1_target_bytes - 1;
    assert!(!policy.over_target_budget(&policy.debt(&small).unwrap()));
    let shared = Policy {
        delete_layout: DeleteLayout::Shared,
        ..policy.clone()
    };
    let debt = shared.debt(&files).unwrap();
    assert_eq!(debt.target_delete_files, 0);
    assert_eq!(debt.pressure, PublicationPressure::Healthy);
}

#[test]
fn per_target_rewrites_small_files_carrying_single_target_deletes() {
    let policy = Policy {
        min_file_age_ms: 0,
        small_file_target_deletes: 6,
        ..per_target()
    };
    // A full-size base, two small L1 files and a small L2 file carry
    // single-target delete files; another small file and an L0 file carry
    // none.
    let mut files = (0..6).map(|id| file(id, 1 << 20)).collect::<Vec<_>>();
    files[0].level = Level::L2;
    files[0].size_bytes = policy.l1_target_bytes;
    files[0].target_delete_files = 4;
    for (file, (level, count)) in files[1..5].iter_mut().zip([
        (Level::L1, 3),
        (Level::L1, 2),
        (Level::L2, 1),
        (Level::L1, 0),
    ]) {
        file.level = level;
        file.target_delete_files = count;
        file.delete_file_count = count;
    }
    files[3].size_bytes = 8 << 20;
    files[5].age_ms = 0;
    let deletes = files
        .iter()
        .flat_map(|file| {
            (0..file.target_delete_files).map(|index| DeleteDependency {
                id: FileId(format!("{}-delete-{index}", file.id.0)),
                targets: BTreeSet::from([file.id.clone()]),
                size_bytes: 100,
                row_count: 1,
            })
        })
        .collect::<Vec<_>>();
    let debt = policy.debt(&files).unwrap();
    assert_eq!(debt.small_target_delete_files, 6);
    assert!(policy.small_target_debt(&debt));
    assert_eq!(debt.pressure, PublicationPressure::Delay);
    assert_eq!(policy.pressure_reasons(&debt), ["target_deletes"]);
    // A data rewrite pays this debt, not a consolidation.
    assert!(!policy.consolidates_delete_debt(&debt));
    let plan = policy.plan(1, 0, &files, &deletes).unwrap().unwrap();
    assert_eq!(plan.reason, PlanReason::TargetDeletes);
    assert_eq!(
        plan.input_files,
        BTreeSet::from([
            files[1].id.clone(),
            files[2].id.clone(),
            files[3].id.clone()
        ])
    );
    assert_eq!(plan.delete_files.len(), 6);
    assert_eq!(plan.output_level, Level::L2);
    // One carrier is rewritten alone.
    let mut single = files.clone();
    single[1].target_delete_files = 6;
    single[2].target_delete_files = 0;
    single[3].target_delete_files = 0;
    let plan = policy.plan(1, 0, &single, &[]).unwrap().unwrap();
    assert_eq!(plan.reason, PlanReason::TargetDeletes);
    assert_eq!(plan.input_files, BTreeSet::from([files[1].id.clone()]));
    // Below the limit, or with the shared layout, small files wait.
    let below = Policy {
        small_file_target_deletes: 7,
        ..policy.clone()
    };
    assert!(!below.small_target_debt(&below.debt(&files).unwrap()));
    assert!(below.plan(1, 0, &files, &deletes).unwrap().is_none());
    let shared = Policy {
        delete_layout: DeleteLayout::Shared,
        ..policy.clone()
    };
    assert_eq!(shared.debt(&files).unwrap().small_target_delete_files, 0);
    assert!(shared.plan(1, 0, &files, &deletes).unwrap().is_none());
}

#[test]
fn per_target_pools_l0_and_small_targets_between_full_size_files() {
    let policy = Policy {
        min_file_age_ms: 0,
        ..per_target()
    };
    let full = |name: &str| FileCandidate {
        id: FileId(name.into()),
        level: Level::L2,
        size_bytes: policy.target_delete_min_file_bytes,
        ..file(0, 1)
    };
    let small = |name: &str, level| FileCandidate {
        id: FileId(name.into()),
        level,
        size_bytes: policy.target_delete_min_file_bytes - 1,
        ..file(0, 1)
    };
    let files = vec![
        small("a", Level::L0),
        small("b", Level::L1),
        full("c"),
        small("d", Level::L2),
        small("e", Level::L0),
        full("selected"),
    ];
    let ranges = policy.target_ranges(&files, &BTreeSet::from([FileId("selected".into())]));
    assert_eq!(
        ranges.pooled,
        ["a", "b", "d", "e"].map(|id| FileId(id.into())).into()
    );
    assert_eq!(ranges.breaks, BTreeSet::from([FileId("c".into())]));
    let id = |name: &str| FileId(name.into());
    // L0 and small files share a range until a full-size file lies between.
    assert!(ranges.continues(&id("a"), &id("b")));
    assert!(!ranges.continues(&id("b"), &id("d")));
    assert!(ranges.continues(&id("d"), &id("e")));
    // A full-size file always gets its own file.
    assert!(!ranges.continues(&id("b"), &id("c")));
    assert!(!ranges.continues(&id("c"), &id("d")));
    // A rewrite's small output joins the pool, a full-size one splits it.
    let mut with_outputs = ranges.clone();
    with_outputs.add(id("a1"), 1);
    with_outputs.add(id("a2"), policy.target_delete_min_file_bytes);
    assert!(with_outputs.continues(&id("a"), &id("a1")));
    assert!(!with_outputs.continues(&id("a1"), &id("b")));
    // Past MAX_BREAKS full-size files, ranges may span them.
    let mut crowded = TargetRanges {
        full_size_bytes: 10,
        ..TargetRanges::default()
    };
    crowded.add(id("a"), 1);
    crowded.add(id("z"), 1);
    for index in 0..TargetRanges::MAX_BREAKS {
        crowded.add(FileId(format!("m{index}")), 10);
    }
    assert!(!crowded.continues(&id("a"), &id("z")));
    crowded.add(id("n"), 10);
    assert!(crowded.continues(&id("a"), &id("z")));
    // Data rewrites carry the ranges to the residual writer; the shared
    // layout needs none.
    let l0 = (0..24).map(|id| file(id, 1 << 20)).collect::<Vec<_>>();
    let plan = policy.plan(1, 0, &l0, &[]).unwrap().unwrap();
    assert!(plan.target_ranges.breaks.is_empty());
    assert_eq!(
        plan.target_ranges.full_size_bytes,
        policy.target_delete_min_file_bytes
    );
    let shared = Policy {
        delete_layout: DeleteLayout::Shared,
        ..policy.clone()
    };
    assert_eq!(
        shared.plan(1, 0, &l0, &[]).unwrap().unwrap().target_ranges,
        TargetRanges::default()
    );
}
