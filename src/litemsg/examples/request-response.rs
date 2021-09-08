#![feature(vec_into_raw_parts)]
// data size: (4 + (4 + (8 + 4) * 4)) * 4 = 224
// cargo run --release --example request-response -- --server --num_clients 20 --data_size 224
// cargo run --release --example request-response -- --client rdma0.danyang-01 --num_clients 20 --data_size 224
use litemsg::endpoint::{self, Endpoint};
use litemsg::Node;
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::time::{Instant, Duration};

const PORT: u16 = 9000;

lazy_static::lazy_static! {
    static ref OPTS: Opts = parse_opts();
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum Message {
    Solicited,
    Data,
}

#[derive(Debug, Clone, Default)]
struct Opts {
    data_size: usize,
    num_clients: usize,
    warmup: usize,
    iters: usize,
}

fn usage() {
    let app = std::env::args().next().unwrap();
    println!("Usage:");
    println!("  {} --server [options]", app);
    println!("  {} --client <addr> [options]", app);
    println!("Options:");
    println!("  --data_size        data size (default: 64KB)");
    println!("  --num_clients      number of clients (default: 1)");
    println!("  --warmup           warmup iterations (default: 10)");
    println!("  --iters            iterations (default: 100)");
}

fn parse_opts() -> Opts {
    let mut opts = Opts::default();
    opts.data_size = 65536; // default 64KB
    opts.num_clients = 1; // default 1
    opts.warmup = 10; // default 10
    opts.iters = 100; // default 100
    let mut parse_data_size = false;
    let mut parse_num_clients = false;
    let mut parse_warmup = false;
    let mut parse_iters = false;
    for opt in std::env::args() {
        if parse_data_size {
            opts.data_size = opt.parse().unwrap();
            parse_data_size = false;
        }
        if parse_num_clients {
            opts.num_clients = opt.parse().unwrap();
            parse_num_clients = false;
        }
        if parse_warmup {
            opts.warmup = opt.parse().unwrap();
            parse_warmup = false;
        }
        if parse_iters {
            opts.iters = opt.parse().unwrap();
            parse_iters = false;
        }
        if opt == "--num_clients" {
            parse_num_clients = true;
        }
        if opt == "--data_size" {
            parse_data_size = true;
        }
        if opt == "--warmup" {
            parse_warmup = true;
        }
        if opt == "--iters" {
            parse_iters = true;
        }
    }
    println!("opts: {:?}", opts);
    opts
}

fn main() {
    logging::init_log();
    let first_arg = std::env::args().skip(1).next().unwrap_or_else(|| {
        usage();
        panic!();
    });
    match first_arg.as_str() {
        "--server" => run_server().unwrap(),
        "--client" => run_client().unwrap(),
        _ => usage(),
    }
}

/// Each client prepares the data in advance. Server send a solicited message to each client.
/// Each client sends a response to the server. Server count the time from the first solicited
/// message to the last response.
fn run_server() -> anyhow::Result<()> {
    let listener = std::net::TcpListener::bind(("0.0.0.0", PORT))?;
    let poll = mio::Poll::new()?;

    let mut clients = Vec::with_capacity(OPTS.num_clients);
    for _ in 0..OPTS.num_clients {
        let (stream, addr) = listener.accept()?;
        let endpoint = endpoint::Builder::new()
            .stream(stream)
            .readable(true)
            .writable(true)
            .node(addr.to_string().parse().unwrap())
            .build()?;
        clients.push(endpoint);
    }

    io_run(clients, &poll, true)?;
    Ok(())
}

fn run_client() -> anyhow::Result<()> {
    let addr = std::env::args().skip(2).next().unwrap();
    let stream = std::net::TcpStream::connect((addr.clone(), PORT))?;
    let poll = mio::Poll::new()?;
    let endpoint = endpoint::Builder::new()
        .stream(stream)
        .readable(true)
        .writable(true)
        .node(Node { addr, port: PORT })
        .build()?;
    io_run(vec![endpoint], &poll, false)?;
    Ok(())
}

fn io_run(mut peers: Vec<Endpoint>, poll: &mio::Poll, is_server: bool) -> anyhow::Result<()> {
    let mut events = mio::event::Events::with_capacity(1024);
    for (i, ep) in peers.iter().enumerate() {
        ep.stream().set_nodelay(true)?;
        poll.register(
            ep.stream(),
            mio::Token(i),
            mio::Ready::readable() | mio::Ready::writable(),
            mio::PollOpt::level(),
        )?;
    }

    let mut latencies = Vec::with_capacity(OPTS.iters);

    'fin: for i in 0..OPTS.warmup + OPTS.iters {
        // server send solicited messages
        let start = Instant::now();
        let mut responses = 0;
        if is_server {
            let solicited_msg = Message::Solicited;
            for i in 0..OPTS.num_clients {
                peers[i].post(&solicited_msg, None)?;
            }
        }

        // client prepare the data to send to the server
        let mut data = Vec::with_capacity(OPTS.data_size);
        unsafe {
            data.set_len(data.capacity());
        }
        let (ptr, len, cap) = data.into_raw_parts();

        let timeout = Duration::from_millis(1);

        'out: loop {
            poll.poll(&mut events, Some(timeout))?;
            for event in events.iter() {
                let rank = event.token().0;

                if event.readiness().is_writable() {
                    match peers[rank].on_send_ready::<Message>() {
                        Ok((_cmd, _attachment)) => {
                            // let nbytes = attachment.map(|a| a.len()).unwrap_or(0);
                            // ep.post(cmd, attachment)?;
                            if !is_server {
                                break 'out;
                            }
                        }
                        Err(endpoint::Error::NothingToSend) => {}
                        Err(endpoint::Error::WouldBlock) => {}
                        Err(e) => return Err(e.into()),
                    }
                }

                if event.readiness().is_readable() {
                    match peers[rank].on_recv_ready::<Message>() {
                        Ok((cmd, _attach)) => {
                            if is_server {
                                assert!(matches!(cmd, Message::Data));
                                // server measure the latency
                                responses += 1;
                                if responses == OPTS.num_clients {
                                    let latency = start.elapsed();
                                    if i >= OPTS.warmup {
                                        latencies.push(latency);
                                        println!(
                                            "latency for {} requests: {:?}",
                                            OPTS.num_clients, latency
                                        );
                                    }
                                    std::io::stdout().flush().expect("Unable to flush stdout");
                                    break 'out;
                                }
                            } else {
                                assert!(matches!(cmd, Message::Solicited));
                                // send response
                                let data = unsafe { Vec::from_raw_parts(ptr, len, cap) };
                                peers[rank].post(Message::Data, Some(data))?;
                            }
                        }
                        Err(endpoint::Error::WouldBlock) => {}
                        Err(endpoint::Error::ConnectionLost) => {
                            break 'fin;
                        }
                        Err(e) => return Err(e.into()),
                    }
                }
            }
        }
    }

    if is_server {
        println!(
            "mean latency for {} iters: {:?}",
            OPTS.iters,
            latencies.iter().sum::<Duration>() / latencies.len() as u32
        );
        std::io::stdout().flush().expect("Unable to flush stdout");
    }
    Ok(())
}
