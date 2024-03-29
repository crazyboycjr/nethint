use litemsg::endpoint::{self, Endpoint};
use litemsg::Node;
use serde::{Deserialize, Serialize};

const PORT: u16 = 9000;

lazy_static::lazy_static! {
    static ref OPTS: Opts = parse_opts();
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum Message {
    DataChunk,
    Data,
}

#[derive(Debug, Clone, Default)]
struct Opts {
    bidirectional: bool,
    data_size: usize,
}

fn usage() {
    let app = std::env::args().next().unwrap();
    println!("Usage:");
    println!("  {} --server [options]", app);
    println!("  {} --client <addr> [options]", app);
    println!("Options:");
    println!("  --bidirectional    bidirectional traffic");
    println!("  --data_size        data size");
}

fn parse_opts() -> Opts {
    let mut opts = Opts::default();
    opts.data_size = 1_000_000; // default 1MB
    let mut parse_data_size = false;
    for opt in std::env::args() {
        if parse_data_size {
            opts.data_size = opt.parse().unwrap();
            parse_data_size = false;
        }
        if opt == "--bidirectional" {
            opts.bidirectional = true;
        }
        if opt == "--data_size" {
            parse_data_size = true;
        }
    }
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

fn run_server() -> anyhow::Result<()> {
    let listener = std::net::TcpListener::bind(("0.0.0.0", PORT))?;
    let (stream, addr) = listener.accept()?;
    let poll = mio::Poll::new()?;
    let mut endpoint = endpoint::Builder::new()
        .stream(stream)
        .readable(true)
        .writable(true)
        .node(addr.to_string().parse().unwrap())
        .build()?;

    if OPTS.bidirectional {
        // let mut data = Vec::with_capacity(OPTS.data_size);
        // unsafe {
        //     data.set_len(data.capacity());
        // }
        // endpoint.post(Message::Data, Some(data))?;

        // chunking
        let chunk_size = 1_000_000;
        for i in 0..(OPTS.data_size + chunk_size - 1) / chunk_size {
            let data_size = if (i + 1) * chunk_size <= OPTS.data_size { chunk_size } else { OPTS.data_size - i * chunk_size };
            let mut data = Vec::with_capacity(data_size);
            unsafe {
                data.set_len(data.capacity());
            }
            if i * chunk_size + data_size >= OPTS.data_size {
                endpoint.post(Message::Data, Some(data))?;
            } else {
                endpoint.post(Message::DataChunk, Some(data))?;
            }
        }
    }
    io_run(endpoint, &poll)?;
    Ok(())
}

fn run_client() -> anyhow::Result<()> {
    let addr = std::env::args().skip(2).next().unwrap();
    let stream = std::net::TcpStream::connect((addr.clone(), PORT))?;
    let poll = mio::Poll::new()?;
    let mut endpoint = endpoint::Builder::new()
        .stream(stream)
        .readable(true)
        .writable(true)
        .node(Node { addr, port: PORT })
        .build()?;
    // emit something
    // chunking
    let chunk_size = 1_000_000;
    for i in 0..(OPTS.data_size + chunk_size - 1) / chunk_size {
        let data_size = if (i + 1) * chunk_size <= OPTS.data_size { chunk_size } else { OPTS.data_size - i * chunk_size };
        let mut data = Vec::with_capacity(data_size);
        unsafe {
            data.set_len(data.capacity());
        }
        if i * chunk_size + data_size >= OPTS.data_size {
            endpoint.post(Message::Data, Some(data))?;
        } else {
            endpoint.post(Message::DataChunk, Some(data))?;
        }
    }
    io_run(endpoint, &poll)?;
    Ok(())
}

fn io_run(mut ep: Endpoint, poll: &mio::Poll) -> anyhow::Result<()> {
    log::info!("recv_buffer_size: {}", ep.stream().recv_buffer_size()?);
    let mut events = mio::event::Events::with_capacity(1024);
    poll.register(
        ep.stream(),
        mio::Token(0),
        mio::Ready::readable() | mio::Ready::writable(),
        mio::PollOpt::level(),
    )?;

    let mut meter = Meter::new();

    'out: loop {
        poll.poll(&mut events, None)?;
        for event in events.iter() {
            assert_eq!(event.token(), mio::Token(0));

            if event.readiness().is_writable() {
                match ep.on_send_ready::<Message>() {
                    Ok((cmd, attachment)) => {
                        // let nbytes = attachment.map(|a| a.len()).unwrap_or(0);
                        ep.post(cmd, attachment)?;
                    }
                    Err(endpoint::Error::NothingToSend) => {}
                    Err(endpoint::Error::WouldBlock) => {}
                    Err(e) => return Err(e.into()),
                }
            }

            if event.readiness().is_readable() {
                match ep.on_recv_ready::<Message>() {
                    Ok((_cmd, attach)) => {
                        // calculate length and speed
                        // assert!(matches!(cmd, Message::Data));
                        assert!(attach.is_some());
                        meter.add_bytes(attach.as_ref().unwrap().len() as _);

                        // ep.post(Message::Data, attach)?;
                    }
                    Err(endpoint::Error::WouldBlock) => {}
                    Err(endpoint::Error::ConnectionLost) => {
                        break 'out;
                    }
                    Err(e) => return Err(e.into()),
                }
            }
        }
    }
    Ok(())
}

#[derive(Debug)]
pub struct Meter {
    accumulated: isize,
    last_tp: std::time::Instant,
    refresh_interval: std::time::Duration,
}

impl Meter {
    fn new() -> Self {
        Meter {
            accumulated: 0,
            last_tp: std::time::Instant::now(),
            refresh_interval: std::time::Duration::new(1, 0),
        }
    }

    #[inline]
    fn add_bytes(&mut self, delta: isize) {
        self.accumulated += delta;
        let now = std::time::Instant::now();
        if now - self.last_tp >= self.refresh_interval {
            println!(
                "Speed: {} Gb/s",
                8e-9 * self.accumulated as f64 / (now - self.last_tp).as_secs_f64()
            );
            self.accumulated = 0;
            self.last_tp = now;
        }
    }
}
