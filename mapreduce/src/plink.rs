use nethint::{
    app::{AppEvent, Application, Sequence},
    background_flow::{BackgroundFlowApp, BackgroundFlowPattern},
    simulator::Events,
    Duration,
};

#[derive(Debug)]
pub struct PlinkApp<'a, T> {
    dur_ms: Duration,
    inner: Box<Sequence<'a, T>>,
}

impl<'a, T: 'a> PlinkApp<'a, T>
where
    T: Default + Clone + std::fmt::Debug,
{
    pub fn new(nhosts: usize, app: Box<dyn Application<Output = T> + 'a>) -> Self {
        let dur_ms = (nhosts * 100) as _;
        let background_flow = Box::new(BackgroundFlowApp::new(
            nhosts,
            dur_ms,
            BackgroundFlowPattern::PlinkProbe,
            Some(1_000_000_000), // 80ms on 100G
            T::default(),
        ));

        let mut app_seq = Box::new(Sequence::new());
        app_seq.add(background_flow);
        app_seq.add(app);

        PlinkApp {
            dur_ms,
            inner: app_seq,
        }
    }
}

impl<'a> Application for PlinkApp<'a, Option<Duration>> {
    type Output = Option<Duration>;

    fn on_event(&mut self, event: AppEvent) -> Events {
        self.inner.on_event(event)
    }

    fn answer(&mut self) -> Option<Duration> {
        // self.inner.answer().last().unwrap().clone()
        self.inner
            .answer()
            .last()
            .unwrap()
            .map(|dur| dur + self.dur_ms * 1_000_000)
    }
}
