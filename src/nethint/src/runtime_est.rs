pub struct RunningTimeEstimator {
    total_trials: Option<usize>,
    done_trials: usize,
    data: Vec<std::time::Duration>, // running time for each single trial
    single_start: std::time::Instant,
    running_time: std::time::Duration,
}

impl RunningTimeEstimator {
    pub fn new() -> Self {
        RunningTimeEstimator {
            total_trials: None,
            done_trials: 0,
            data: Vec::new(),
            single_start: std::time::Instant::now(),
            running_time: std::time::Duration::from_nanos(0),
        }
    }

    pub fn set_total_trials(&mut self, total_trials: usize) {
        self.total_trials = Some(total_trials);
    }

    pub fn bench_single_start(&mut self) {
        let now = std::time::Instant::now();
        self.running_time += now - self.single_start;

        if let Some(total_trials) = self.total_trials {
            if self.done_trials > 0 {
                log::info!(
                    "average speed: {:?} second/trial, time left: {:?}",
                    self.running_time / self.done_trials as u32,
                    self.running_time * (total_trials - self.done_trials) as u32
                        / self.done_trials as u32
                );
            }
        }

        self.data.push(now - self.single_start);
        self.done_trials += 1;
        self.single_start = now;
    }
}
