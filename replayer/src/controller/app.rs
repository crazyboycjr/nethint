use crate::Node;
use litemsg::endpoint::Endpoint;
use std::collections::HashMap;
use nethint::hint::NetHintVersion;
use nethint::TenantId;
use crate::message;

pub trait Application {
    fn workers(&self) -> &HashMap<Node, Endpoint>;

    fn workers_mut(&mut self) -> &mut HashMap<Node, Endpoint>;

    fn brain(&self) -> &Endpoint;

    fn brain_mut(&mut self) -> &mut Endpoint;

    fn tenant_id(&self) -> TenantId;

    fn request_nethint(&mut self, version: NetHintVersion) -> anyhow::Result<()> {
        let msg = nhagent::message::Message::NetHintRequest(self.tenant_id(), version);
        self.brain_mut().post(msg, None)?;
        Ok(())
    }

    fn start(&mut self) -> anyhow::Result<()>;

    fn on_event(&mut self, cmd: message::Command) -> anyhow::Result<()>;

    fn finish(&mut self) -> anyhow::Result<()> {
        for worker in self.workers_mut().values_mut() {
            worker.post(message::Command::AppFinish, None)?;
        }
        Ok(())
    }
}