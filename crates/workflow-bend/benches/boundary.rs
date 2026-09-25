use std::hint::black_box;
use std::path::PathBuf;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion};
use workflow_bend::compile;
use workflow_core::{counter_program, evaluate_program, History};

fn experiment() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../experiments/bend")
}

fn histories() -> [Option<i64>; 6] {
    [
        None,
        Some(0),
        Some(1),
        Some(-1),
        Some(i64::MAX),
        Some(i64::MIN),
    ]
}

fn bench_boundary(c: &mut Criterion) {
    let rust_program = counter_program();
    let mut group = c.benchmark_group("bend_boundary");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(2));
    group.bench_function("rust_eval", |b| {
        b.iter(|| {
            for counter in histories() {
                black_box(evaluate_program(
                    black_box(&rust_program),
                    black_box(&History { counter }),
                ));
            }
        })
    });
    group.finish();

    let mut load = c.benchmark_group("bend_boundary_load");
    load.sample_size(10);
    load.warm_up_time(Duration::from_secs(1));
    load.measurement_time(Duration::from_secs(3));
    let dir = experiment();
    load.bench_function("bend_compile", |b| {
        b.iter(|| black_box(compile(black_box(&dir)).expect("compile")))
    });
    load.finish();

    let program = compile(&experiment()).expect("compile");
    let mut steady = c.benchmark_group("bend_boundary_steady");
    steady.sample_size(10);
    steady.warm_up_time(Duration::from_secs(1));
    steady.measurement_time(Duration::from_secs(2));
    steady.bench_function("loaded_eval", |b| {
        b.iter(|| {
            for counter in histories() {
                black_box(evaluate_program(
                    black_box(&program),
                    black_box(&History { counter }),
                ));
            }
        })
    });
    steady.finish();
}

criterion_group!(benches, bench_boundary);
criterion_main!(benches);
