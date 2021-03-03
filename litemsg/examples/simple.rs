use litemsg::endpoint::{self, Endpoint};
use litemsg::Node;
use serde::{Deserialize, Serialize};

const PORT: u16 = 9000;

#[derive(Debug, Clone, Serialize, Deserialize)]
enum Message {
    Data,
}

fn usage() {
    let app = std::env::args().next().unwrap();
    println!("Usage:");
    println!("  {} --server", app);
    println!("  {} --client <addr>", app);
}

fn main() {
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
    let endpoint = endpoint::Builder::new()
        .stream(stream)
        .readable(true)
        .writable(true)
        .node(addr.to_string().parse().unwrap())
        .build()?;
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
    let mut data = Vec::with_capacity(1_000_000);
    unsafe {
        data.set_len(data.capacity());
    }
    endpoint.post(Message::Data, Some(data))?;
    io_run(endpoint, &poll)?;
    Ok(())
}

fn io_run(mut ep: Endpoint, poll: &mio::Poll) -> anyhow::Result<()> {
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
                match ep.on_send_ready() {
                    Ok(attachment) => {
                        // let nbytes = attachment.map(|a| a.len()).unwrap_or(0);
                        ep.post(Message::Data, attachment)?;
                    }
                    Err(endpoint::Error::NothingToSend) => {}
                    Err(endpoint::Error::WouldBlock) => {}
                    Err(e) => return Err(e.into()),
                }
            }

            if event.readiness().is_readable() {
                match ep.on_recv_ready() {
                    Ok((cmd, attach)) => {
                        // calculate length and speed
                        assert!(matches!(cmd, Message::Data));
                        assert!(attach.is_some());
                        meter.add_bytes(attach.as_ref().unwrap().len() as _);

                        // let a = attach.map(|x| x.to_vec());
                        // ep.post(Message::Data, a)?;
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
