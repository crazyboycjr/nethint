use std::cell::RefCell;
use std::rc::Rc;

use crate::{
    app::{AppEvent, AppEventKind, Application},
    brain::{Brain, PlacementStrategy, TenantId},
    cluster::{Topology, VirtCluster},
    simulator::{Event, Events},
};

/// A Tenant takes charge of provisioning and destroy the VMs before the app starts and after the app ends.
/// It also does the network translation and tags the flows on the fly.
pub struct Tenant<'a, T> {
    app: Box<dyn Application<Output = T> + 'a>,
    tenant_id: TenantId,
    nhosts: usize,
    brain: Rc<RefCell<Brain>>,
    vcluster: Option<VirtCluster>,
    placement_strategy: PlacementStrategy,
}

impl<'a, T> std::fmt::Debug for Tenant<'a, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Tenant")
            .field("app", &self.app)
            .field("tenant_id", &self.tenant_id)
            .field("nhosts", &self.nhosts)
            .finish()
    }
}

impl<'a, T> Tenant<'a, T> {
    pub fn new(
        app: Box<dyn Application<Output = T> + 'a>,
        tenant_id: TenantId,
        nhosts: usize,
        brain: Rc<RefCell<Brain>>,
        placement_strategy: PlacementStrategy,
    ) -> Self {
        Tenant {
            app,
            tenant_id,
            brain,
            nhosts,
            vcluster: None,
            placement_strategy,
        }
    }

    fn virt_to_phys(&mut self, vname: &str) -> String {
        self.vcluster.as_ref().unwrap().translate(vname)
    }
}

impl<'a, T: Clone + std::fmt::Debug> Application for Tenant<'a, T> {
    type Output = T;

    fn on_event(&mut self, event: AppEvent) -> Events {
        let now = event.ts;
        let sim_events = match event.event {
            AppEventKind::AppStart => {
                let vcluster = self
                    .brain
                    .borrow_mut()
                    .provision(self.tenant_id, self.nhosts, self.placement_strategy)
                    .unwrap();
                self.vcluster = Some(vcluster);
                self.app
                    .on_event(AppEvent::new(now, AppEventKind::AppStart))
            }
            AppEventKind::FlowComplete(mut raw_flows) => {
                // translate raw_flows to app flows
                for f in &mut raw_flows {
                    assert!(f.flow.vsrc.is_some() && f.flow.vdst.is_some());
                    f.flow.src = f.flow.vsrc.take().unwrap();
                    f.flow.dst = f.flow.vdst.take().unwrap();
                    f.flow.tenant_id = None;
                }
                self.app
                    .on_event(AppEvent::new(now, AppEventKind::FlowComplete(raw_flows)))
            }
            AppEventKind::NetHintResponse(..) | AppEventKind::Notification(_) => {
                self.app.on_event(AppEvent::new(now, event.event))
            }
        };

        sim_events
            .into_iter()
            .map(|sim_event| {
                match sim_event {
                    Event::AppFinish => {
                        // destroy VMs
                        self.brain.borrow_mut().destroy(self.tenant_id);
                        Event::AppFinish
                    }
                    Event::FlowArrive(mut virt_flows) => {
                        // translate app flows to physical flows
                        for f in &mut virt_flows {
                            assert!(f.flow.tenant_id.is_none());
                            f.flow.tenant_id = Some(self.tenant_id);
                            assert!(f.flow.vsrc.is_none() && f.flow.vdst.is_none());
                            f.flow.vsrc = Some(f.flow.src.clone());
                            f.flow.vdst = Some(f.flow.dst.clone());
                            f.flow.src = self.virt_to_phys(&f.flow.src);
                            f.flow.dst = self.virt_to_phys(&f.flow.dst);
                        }
                        Event::FlowArrive(virt_flows)
                    }
                    Event::NetHintRequest(app_id, tenant_id, version, app_hint) => {
                        assert_eq!(app_id, 0);
                        assert_eq!(tenant_id, 0);
                        Event::NetHintRequest(app_id, self.tenant_id, version, app_hint)
                    }
                    Event::RegisterTimer(..) => {
                        unreachable!(
                            "unless the inner app is an AppGroup, which will be supported later"
                        );
                    }
                }
            })
            .collect()
    }

    fn answer(&mut self) -> T {
        self.app.answer()
    }
}
