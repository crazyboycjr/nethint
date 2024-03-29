use crate::Node;
use litemsg::endpoint::Endpoint;
use std::collections::HashMap;
use nethint::hint::NetHintVersion;
use nethint::TenantId;
use crate::message;
// use nhagent::timing::{self, TimeList};

pub trait Application {
    fn workers(&self) -> &HashMap<Node, Endpoint>;

    fn workers_mut(&mut self) -> &mut HashMap<Node, Endpoint>;

    fn brain(&self) -> &Endpoint;

    fn brain_mut(&mut self) -> &mut Endpoint;

    fn tenant_id(&self) -> TenantId;

    fn hostname_to_node(&self) -> &HashMap<String, Node>;

    fn request_nethint(&mut self, version: NetHintVersion) -> anyhow::Result<()> {
        // let mut time_list = TimeList::new();
        // time_list.push_now(timing::ON_TENANT_SENT_REQ);
        // let msg = nhagent::message::Message::NetHintRequest(self.tenant_id(), version, time_list);
        let msg = nhagent_v2::message::Message::NetHintRequest(self.tenant_id(), version);
        self.brain_mut().post(msg, None)?;
        Ok(())
    }

    fn start(&mut self) -> anyhow::Result<()>;

    fn on_event(&mut self, cmd: message::Command) -> anyhow::Result<bool>;

    fn finish(&mut self) -> anyhow::Result<()> {
        for worker in self.workers_mut().values_mut() {
            worker.post(message::Command::AppFinish, None)?;
        }
        Ok(())
    }
}
