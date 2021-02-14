use crate::{endpoint::Endpoint, message, Flow, Node};
use std::collections::HashMap;

pub struct MapReduceApp {
    workers: HashMap<Node, Endpoint>,
    num_remaining_flows: usize,
}

impl MapReduceApp {
    pub fn new(workers: HashMap<Node, Endpoint>) -> Self {
        MapReduceApp {
            workers,
            num_remaining_flows: 0,
        }
    }

    pub fn workers(&self) -> &HashMap<Node, Endpoint> {
        &self.workers
    }

    pub fn workers_mut(&mut self) -> &mut HashMap<Node, Endpoint> {
        &mut self.workers
    }

    pub fn start(&mut self) -> anyhow::Result<()> {
        let workers: Vec<_> = self.workers.keys().cloned().collect();
        let n = workers.len();
        let mappers = &workers[..n / 2];
        let reducers = &workers[n / 2..];

        // emit flows
        for m in mappers {
            for r in reducers {
                let flow = Flow::new(100, m.clone(), r.clone(), None);
                let cmd = message::Command::EmitFlow(flow);
                log::trace!("mapreduce::run, cmd: {:?}", cmd);
                let endpoint = self.workers.get_mut(&m).unwrap();
                endpoint.post(cmd)?;
                self.num_remaining_flows += 1;
            }
        }

        Ok(())
    }

    fn finish(&mut self) -> anyhow::Result<()> {
        for worker in self.workers.values_mut() {
            worker.post(message::Command::AppFinish)?;
        }
        Ok(())
    }

    pub fn on_event(&mut self, cmd: message::Command) -> anyhow::Result<()> {
        // wait for all flows to finish
        use message::Command::*;
        match cmd {
            FlowComplete(_flow) => {
                self.num_remaining_flows -= 1;
                if self.num_remaining_flows == 0 {
                    self.finish()?;
                }
            }
            _ => {
                panic!("unexpected cmd: {:?}", cmd);
            }
        }
        Ok(())
    }
}
