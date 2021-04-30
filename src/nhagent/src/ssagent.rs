use std::net::SocketAddr;
use structopt::StructOpt;
use utils::cmd_helper::get_command_output;
use std::time::Duration;
use std::process::Command;
use nhagent::sampler::ss_sampler::SsTcpFlows;

#[derive(Debug, Clone, StructOpt)]
#[structopt(
    name = "ssagent",
    about = "while true; do ss -tuni | nc -w0 -u 127.0.0.1 9999; sleep 0.1; done"
)]
struct Opts {
    /// Poll interval in ms
    #[structopt(short = "i", long = "interval", default_value = "100")]
    interval_ms: u64,
    /// Target address
    #[structopt(short = "t", long = "target")]
    target: Option<SocketAddr>,
}

#[inline]
fn now() -> std::time::Instant {
    std::time::Instant::now()
}

fn get_default_target() -> SocketAddr {
    let my_ip = utils::net::get_primary_ipv4("rdma0").unwrap();
    let last_field: u8 = my_ip.split('.').last().unwrap().parse().unwrap();
    // the conversion is hard coded
    // 3,4,5,6 -> 2, 35... -> 34
    let target_num = (last_field / 32 * 32 + 2).to_string();
    let numbers: Vec<&str> = my_ip
        .split('.')
        .take(3)
        .chain(std::iter::once(target_num.as_str()))
        .collect();
    let addr = format!(
        "{}.{}.{}.{}:{}",
        numbers[0], numbers[1], numbers[2], numbers[3], 5555
    );
    addr.parse().unwrap_or_else(|_| panic!("addr: {}", addr))
}

fn main() {
    logging::init_log();

    let mut opts = Opts::from_args();
    if opts.target.is_none() {
        opts.target = Some(get_default_target());
    }
    log::info!("opts: {:?}", opts);

    let sock = std::net::UdpSocket::bind("0.0.0.0:34254").expect("bind failed");
    sock.connect(opts.target.unwrap()).expect("connect failed");
    sock.set_write_timeout(Some(Duration::from_millis(opts.interval_ms / 2))).unwrap();
    let sleep_ms = Duration::from_millis(opts.interval_ms);
    let mut last_ts = now();

    loop {
        let mut cmd = Command::new("ss");
        cmd.arg("-tuni");
        let output = get_command_output(cmd).unwrap();
        let ss_flows: SsTcpFlows = output.parse().expect("fail to parse ss output");
        let ts = std::time::SystemTime::now();
        let buf = bincode::serialize(&(ts, ss_flows)).expect("fail to serialize ss_flows");
        assert!(buf.len() <= 65507);
        match sock.send(&buf) {
            Ok(_nbytes) => {}
            Err(_e) => {}
        }
        let n = now();
        if last_ts + sleep_ms > n {
            // avoid duration < 0, which will cause a panic.
            std::thread::sleep(last_ts + sleep_ms - n);
        }
        last_ts = now();
    }
}
