use nethint::{Duration, TenantId};
use std::collections::HashMap;
use nethint::hint::NetHintVersion;

use crate::message;
use crate::Node;
use litemsg::endpoint::Endpoint;

use crate::controller::app::Application;
use crate::controller::background_flow::{BackgroundFlowPattern, BackgroundFlowApp};

#[derive(Debug, Clone, Copy, PartialEq)]
enum State {
    BackgroundFlow,
    Inner,
}

use State::*;

pub struct PlinkApp {
    _dur_ms: Duration,
    state: State,
    background_flow: Box<BackgroundFlowApp>,
    inner: Box<dyn Application>,
}

impl PlinkApp {
    pub fn new(nhosts: usize, round_ms: u64, mut inner: Box<dyn Application>) -> Self {
        let dur_ms = (nhosts as u64 * round_ms) as _;
        let background_flow = Box::new(BackgroundFlowApp::new(
            std::mem::take(inner.workers_mut()),
            inner.brain().clone(),
            inner.hostname_to_node().clone(),
            inner.tenant_id(),
            nhosts,
            dur_ms,
            BackgroundFlowPattern::PlinkProbe,
            Some(10_000_000), // 8ms on 10G
        ));

        PlinkApp {
            _dur_ms: dur_ms,
            state: State::BackgroundFlow,
            background_flow,
            inner,
        }
    }

    fn handle_brain_response_event(
        &mut self,
        msg: nhagent_v2::message::Message,
    ) -> anyhow::Result<()> {
        use nhagent_v2::message::Message::*;
        let my_tenant_id = self.tenant_id();
        match msg {
            NetHintResponseV1(tenant_id, hintv1) => {
                assert_eq!(my_tenant_id, tenant_id);
                self.background_flow.vname_to_hostname = hintv1.vname_to_hostname;
                self.background_flow.start()?;
            }
            NetHintResponseV2(_tenant_id, _hintv2) => {
                panic!("impossible");
            }
            _ => {
                panic!("unexpected brain response: {:?}", msg);
            }
        }
        Ok(())
    }
}

impl Application for PlinkApp {
    fn workers(&self) -> &HashMap<Node, Endpoint> {
        match self.state {
            BackgroundFlow => self.background_flow.workers(),
            Inner => self.inner.workers(),
        }
    }

    fn workers_mut(&mut self) -> &mut HashMap<Node, Endpoint> {
        match self.state {
            BackgroundFlow => self.background_flow.workers_mut(),
            Inner => self.inner.workers_mut(),
        }
    }

    fn brain(&self) -> &Endpoint {
        self.inner.brain()
    }

    fn brain_mut(&mut self) -> &mut Endpoint {
        self.inner.brain_mut()
    }

    fn tenant_id(&self) -> TenantId {
        self.inner.tenant_id()
    }

    fn hostname_to_node(&self) -> &HashMap<String, Node> {
        self.inner.hostname_to_node()
    }

    fn start(&mut self) -> anyhow::Result<()> {
        self.inner.request_nethint(NetHintVersion::V1)?;
        Ok(())
    }

    fn on_event(&mut self, cmd: message::Command) -> anyhow::Result<bool> {
        use message::Command::*;
        match cmd {
            BrainResponse(msg) => {
                match self.state {
                    BackgroundFlow => {
                        self.handle_brain_response_event(msg)?;
                    }
                    Inner => {
                        let orig_cmd = BrainResponse(msg);
                        self.inner.on_event(orig_cmd)?;
                    }
                }
                Ok(false)
            }
            FlowComplete(ref _flow) => {
                match self.state {
                    BackgroundFlow => {
                        let finished = self.background_flow.on_event(cmd)?;
                        if finished {
                            log::info!("background flow finished");
                            // bring workers back from background flow app
                            std::mem::swap(self.inner.workers_mut(), self.background_flow.workers_mut());
                            self.state = Inner;
                            self.inner.start()?;
                        }
                        Ok(false)
                    }
                    Inner => {
                        self.inner.on_event(cmd)
                    }
                }
            }
            _ => {
                panic!("unexpected cmd: {:?}", cmd);
            }
        }
    }
}
