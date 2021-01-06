use std::cell::RefCell;
use std::collections::HashMap;
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
    pname_to_vname: HashMap<String, String>,
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
    ) -> Self {
        Tenant {
            app,
            tenant_id,
            brain,
            nhosts,
            vcluster: None,
            pname_to_vname: HashMap::default(),
        }
    }

    fn virt_to_phys(&mut self, vname: &str) -> String {
        let pname = self.vcluster.as_ref().unwrap().translate(vname);
        self.pname_to_vname
            .entry(pname.clone())
            .or_insert_with(|| vname.to_owned());
        pname
    }

    fn phys_to_virt(&self, pname: &str) -> String {
        self.pname_to_vname[pname].clone()
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
                    .provision(self.tenant_id, self.nhosts, PlacementStrategy::Compact)
                    .unwrap();
                self.vcluster = Some(vcluster);
                self.app
                    .on_event(AppEvent::new(now, AppEventKind::AppStart))
            }
            AppEventKind::FlowComplete(mut raw_flows) => {
                // translate raw_flows to app flows
                for f in &mut raw_flows {
                    f.flow.src = self.phys_to_virt(&f.flow.src);
                    f.flow.dst = self.phys_to_virt(&f.flow.dst);
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
                            f.flow.src = self.virt_to_phys(&f.flow.src);
                            f.flow.dst = self.virt_to_phys(&f.flow.dst);
                        }
                        Event::FlowArrive(virt_flows)
                    }
                    Event::NetHintRequest(app_id, tenant_id, version) => {
                        assert_eq!(app_id, 0);
                        assert_eq!(tenant_id, 0);
                        Event::NetHintRequest(app_id, self.tenant_id, version)
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
