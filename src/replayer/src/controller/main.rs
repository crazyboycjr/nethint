#![feature(bindings_after_at)]
use std::collections::HashMap;

use litemsg::endpoint;
use replayer::controller::app::Application;
use replayer::controller::allreduce::AllreduceAppBuilder;
use replayer::controller::mapreduce::MapReduceAppBuilder;
use replayer::controller::rl::RLAppBuilder;
use replayer::message;
use replayer::Node;

use structopt::StructOpt;
// use nhagent::timing;

#[derive(Debug, Clone, StructOpt)]
#[structopt(name = "Controller", about = "Controller of the distributed replayer.")]
pub struct Opt {
    /// Application, possible values are "mapreduce", "allreduce", "rl"
    #[structopt(short = "a", long = "app")]
    pub app: String,

    /// The configure file
    #[structopt(short = "c", long = "config")]
    pub config: std::path::PathBuf,

    /// Output path of the timing result. If not specified, then timing is disabled.
    #[structopt(short, long = "timing")]
    pub timing: Option<std::path::PathBuf>,
}

fn main() -> anyhow::Result<()> {
    logging::init_log();

    let opts = Opt::from_args();
    log::info!("opts: {:#?}", opts);

    let num_workers: usize = std::env::var("RP_NUM_WORKER")
        .expect("RP_NUM_WORKER")
        .parse()
        .expect("RP_NUM_WORKER");
    log::debug!("num_workers: {}", num_workers);

    let controller_uri = std::env::var("RP_CONTROLLER_URI").expect("RP_CONTROLLER_URI");

    let (_listener, workers, hostname_to_node) =
        litemsg::accept_peers(&controller_uri, num_workers)?;

    let brain_uri = std::env::var("NH_CONTROLLER_URI").expect("NH_CONTROLLER_URI");
    log::info!("connecting to brain: {}", brain_uri);
    let brain = litemsg::utils::connect_retry(&brain_uri, 5)?;
    log::info!("connected to brain at {}", brain_uri);

    let brain = endpoint::Builder::new()
        .stream(brain)
        .readable(true)
        .writable(true)
        .node(brain_uri.parse().unwrap())
        .build()
        .unwrap();

    let start = std::time::Instant::now();
    log::info!("start io_loop");
    io_loop(&opts, workers, brain, hostname_to_node)?;
    let end = std::time::Instant::now();
    log::info!("end io_loop");
    println!("duration: {:?}", end - start);

    Ok(())
}

fn io_loop(
    opts: &Opt,
    workers: HashMap<Node, std::net::TcpStream>,
    brain: endpoint::Endpoint,
    hostname_to_node: HashMap<String, Node>,
) -> anyhow::Result<()> {
    let poll = mio::Poll::new()?;

    let workers = workers
        .into_iter()
        .map(|(k, stream)| {
            (
                k.clone(),
                endpoint::Builder::new()
                    .stream(stream)
                    .readable(true)
                    .writable(true)
                    .node(k)
                    .build()
                    .unwrap(),
            )
        })
        .collect();

    // initialize application
    let mut app: Box<dyn Application> = match opts.app.as_str() {
        "mapreduce" => {
            MapReduceAppBuilder::new(opts.config.clone(), workers, brain, hostname_to_node).build()
        }
        "allreduce" => {
            AllreduceAppBuilder::new(opts.config.clone(), workers, brain, hostname_to_node).build()
        }
        "rl" => {
            RLAppBuilder::new(opts.config.clone(), workers, brain, hostname_to_node).build()
        }
        a @ _ => {
            panic!("unknown app: {:?}", a);
        }
    };

    let mut events = mio::Events::with_capacity(1024);

    let mut token_table: Vec<Node> = Default::default();
    for (node, ep) in app.workers_mut().iter_mut() {
        let new_token = mio::Token(token_table.len());
        token_table.push(node.clone());

        // set nodelay
        ep.stream().set_nodelay(true)?;
        poll.register(ep.stream(), new_token, ep.interest(), mio::PollOpt::level())?;
    }

    // set nodelay
    app.brain().stream().set_nodelay(true)?;

    // add brain to poll
    poll.register(
        app.brain().stream(),
        mio::Token(token_table.len()),
        app.brain().interest(),
        mio::PollOpt::level(),
    )?;

    app.start()?;

    let mut handler = Handler::new(app.workers().len(), opts.timing.clone());

    'outer: loop {
        poll.poll(&mut events, None)?;
        for event in events.iter() {
            let rank = event.token().0;
            assert!(rank <= token_table.len());
            let ep = if rank == token_table.len() {
                app.brain_mut()
            } else {
                let node = &token_table[rank];
                app.workers_mut().get_mut(node).unwrap()
            };
            if event.readiness().is_writable() {
                if rank == token_table.len() {
                    match ep.on_send_ready::<nhagent_v2::message::Message>() {
                        Ok(..) => {}
                        Err(endpoint::Error::NothingToSend) => {}
                        Err(endpoint::Error::WouldBlock) => {}
                        Err(e) => return Err(e.into()),
                    }
                } else {
                    match ep.on_send_ready::<message::Command>() {
                        Ok(..) => {}
                        Err(endpoint::Error::NothingToSend) => {}
                        Err(endpoint::Error::WouldBlock) => {}
                        Err(e) => return Err(e.into()),
                    }
                };
            }
            if event.readiness().is_readable() {
                // warp nhagent msg into Command and ignore attachment
                let res = if rank == token_table.len() {
                    ep.on_recv_ready::<nhagent_v2::message::Message>()
                        .map(|x| message::Command::BrainResponse(x.0))
                } else {
                    ep.on_recv_ready::<message::Command>().map(|x| x.0)
                };
                match res {
                    Ok(cmd) => {
                        if handler.on_recv_complete(cmd, &mut app)? {
                            break 'outer;
                        }
                    }
                    Err(endpoint::Error::WouldBlock) => {}
                    Err(endpoint::Error::ConnectionLost) => {
                        // hopefully this will also call Drop for the ep
                        if rank < token_table.len() {
                            let node = &token_table[rank];
                            app.workers_mut().remove(node).unwrap();
                        }
                    }
                    Err(e) => return Err(e.into()),
                }
            }
        }
    }

    Ok(())
}

struct Handler {
    num_remaining: usize,
    timing: Option<std::path::PathBuf>,
}

impl Handler {
    fn new(num_workers: usize, timing: Option<std::path::PathBuf>) -> Self {
        Handler {
            num_remaining: num_workers,
            timing,
        }
    }

    fn handle_brain_response(
        &mut self,
        msg: nhagent_v2::message::Message,
        app: &mut Box<dyn Application>,
    ) -> anyhow::Result<bool> {
        use nhagent_v2::message::Message::*;
        // let my_tenant_id = app.tenant_id();
        match msg {
            // r @ ProvisionResponse(..) => {
            //     app.on_event(message::Command::BrainResponse(r))?;
            // }
            // DestroyResponse(tenant_id) => {
            //     // exit
            //     assert_eq!(my_tenant_id, tenant_id);
            //     return Ok(true);
            // }
            r @ NetHintResponseV1(..) => {
                app.on_event(message::Command::BrainResponse(r))?;
            }
            r @ NetHintResponseV2(..) => {
                app.on_event(message::Command::BrainResponse(r))?;
            }
            _ => {
                panic!("unexpected brain response: {:?}", msg);
            }
        }
        Ok(false)
    }

    fn on_recv_complete(
        &mut self,
        cmd: message::Command,
        app: &mut Box<dyn Application>,
    ) -> anyhow::Result<bool> {
        use message::Command::*;
        match cmd {
            FlowComplete(ref _flow) => {
                app.on_event(cmd)?;
            }
            LeaveNode(node) => {
                app.workers_mut().remove(&node).unwrap();
                self.num_remaining -= 1;
                if self.num_remaining == 0 {
                    // send request to destroy VMs
                    // let msg = nhagent::message::Message::DestroyRequest(app.tenant_id());
                    // app.brain_mut().post(msg, None)?;
                    return Ok(true);
                }
            }
            BrainResponse(msg) => {
                if let Some(_path) = self.timing.as_ref() {
                    match &msg {
                        nhagent_v2::message::Message::NetHintResponseV2(_tenant_id, _hintv2) => {
                            // let mut time_list = time_list.clone();
                            // time_list.push_now(timing::ON_TENANT_RECV_RES);
                            // use std::io::Write;
                            // let mut f = utils::fs::open_with_create_append(path);
                            // writeln!(f, "{}", time_list)?;
                        }
                        _ => {
                            // do nothing
                        }
                    }
                }

                let exit = self.handle_brain_response(msg, app)?;
                if exit {
                    return Ok(exit);
                }
            }
            _ => {
                log::error!("handle_msg: unexpected cmd: {:?}", cmd);
            }
        }

        Ok(false)
    }
}
