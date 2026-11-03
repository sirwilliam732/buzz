use super::*;

// Manual microbenchmark, not a production latency claim. Run the same release
// build on the intended hardware to compare dark bypass / tracking costs.
#[test]
#[ignore = "manual activation tracking microbenchmark"]
fn activation_tracking_mutex_cost() {
    const N: usize = 100_000;
    let tracker = Arc::new(WorkTracker::default());
    let started = std::time::Instant::now();
    for _ in 0..N {
        std::hint::black_box(tracker.enter("target", 0).unwrap());
    }
    println!(
        "uncontended enter/drop ns/op: {}",
        started.elapsed().as_nanos() / N as u128
    );
    let started = std::time::Instant::now();
    std::thread::scope(|scope| {
        for _ in 0..4 {
            let tracker = tracker.clone();
            scope.spawn(move || {
                for _ in 0..N / 4 {
                    std::hint::black_box(tracker.enter("target", 0).unwrap());
                }
            });
        }
    });
    println!(
        "4-thread shared-mutex enter/drop ns/op: {}",
        started.elapsed().as_nanos() / N as u128
    );
    let ticket = tracker.enter("body", 0).unwrap();
    let mut body = TicketBody {
        inner: axum::body::Body::empty(),
        ticket,
    };
    let mut cx = std::task::Context::from_waker(futures_util::task::noop_waker_ref());
    let started = std::time::Instant::now();
    for _ in 0..N {
        let _ = std::hint::black_box(http_body::Body::poll_frame(
            std::pin::Pin::new(&mut body),
            &mut cx,
        ));
    }
    println!(
        "empty protected body poll ns/op: {}",
        started.elapsed().as_nanos() / N as u128
    );
}
