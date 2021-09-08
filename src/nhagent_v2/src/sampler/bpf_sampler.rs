use crate::argument::Opts;
use structopt::StructOpt;
use crate::sdn_controller;
use std::collections::HashMap;
use std::net::{UdpSocket, IpAddr};
use std::sync::mpsc;

use nethint::cluster::LinkIx;
use nethint::counterunit::{CounterType, CounterUnit};

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::SeqCst;

pub static TERMINATE: AtomicBool = AtomicBool::new(false);

pub struct TcSampler {
    interval_ms: u64,
    listen_port: u16,
    handle: Option<std::thread::JoinHandle<()>>,
    tx: mpsc::Sender<HashMap<LinkIx, Vec<CounterUnit>>>,
}

impl TcSampler {
    pub fn new(
        interval_ms: u64,
        listen_port: u16,
        tx: mpsc::Sender<HashMap<LinkIx, Vec<CounterUnit>>>,
    ) -> Self {
        TcSampler {
            interval_ms,
            listen_port,
            handle: None,
            tx,
        }
    }

    fn send_synthetic_data(
        rack_ip_table: &HashMap<IpAddr, LinkIx>,
        tx: &mpsc::Sender<HashMap<LinkIx, Vec<CounterUnit>>>,
    ) {
        use CounterType::*;
        let mut counter_hashmap: HashMap<LinkIx, Vec<CounterUnit>> = HashMap::new();

        // for each host under a rack
        for host_link_ix in rack_ip_table.values() {
            // for each VM on that host
            let mut counter = Vec::new();
            for i in 0..crate::cluster::MAX_SLOTS {
                let mut c = CounterUnit::new(&i.to_string());
                c.add_flow(Tx, 1);
                c.add_flow(TxIn, 1);
                c.add_flow(Rx, 1);
                c.add_flow(RxIn, 1);
                counter.push(c)
            }
            counter_hashmap.insert(*host_link_ix, counter);
        }
        tx.send(counter_hashmap).unwrap();
    }

    pub fn run(&mut self) {
        log::info!("starting BPF TC Sampler thread...");

        let opts = Opts::from_args();
        let tx = self.tx.clone();
        let interval = std::time::Duration::from_millis(self.interval_ms);

        let sock = if opts.shadow_id.is_none() {
            let listen_addr = ("0.0.0.0", self.listen_port); // default 6343
            let sock = UdpSocket::bind(listen_addr)
                .unwrap_or_else(|_| panic!("could'n bind to or listen on {:?}", listen_addr));
            sock.set_read_timeout(Some(std::time::Duration::from_millis(1)))
                .expect("set udp socket timeout failed");
            Some(sock)
        } else {
            // we're in emulation
            None
        };

        let mut buf = vec![0u8; 65507];
        let mut last_ts = std::time::Instant::now();
        let mut counter_hashmap: HashMap<LinkIx, Vec<CounterUnit>> = HashMap::new();

        let rack_ip_table = sdn_controller::get_rack_ip_table().expect("get rack_ip_table failed");

        self.handle = Some(std::thread::spawn(move || loop {
            let now = std::time::Instant::now();
            if now >= last_ts + interval {
                if TERMINATE.load(SeqCst) {
                    break;
                }

                if opts.shadow_id.is_some() {
                    // if we are in emulation
                    Self::send_synthetic_data(&rack_ip_table, &tx);
                    last_ts = now;
                    continue;
                }

                tx.send(counter_hashmap.clone()).unwrap();
                last_ts = now;
            }

            if opts.shadow_id.is_some() {
                // we are in emulation, sock is not initialized, just sleep until next interval
                if last_ts + interval > now {
                    std::thread::sleep(last_ts + interval - now);
                }
                continue;
            }

            match sock.as_ref().unwrap().recv_from(&mut buf) {
                Ok((nbytes, saddr)) => {
                    let buf = &buf[..nbytes];
                    let counter: Vec<CounterUnit> =
                        bincode::deserialize(&buf).expect("deserialize counter failed");
                    log::trace!("received: {:?}", counter);
                    if let Some(&link_ix) = rack_ip_table.get(&saddr.ip()) {
                        counter_hashmap.insert(link_ix, counter);
                    }
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    // ignore
                }
                Err(e) => {
                    panic!("BPF TC Sampler, recv_from: {}", e)
                }
            }
        }));
    }

    pub fn join(self) -> std::thread::Result<()> {
        self.handle
            .expect("BPF TC Sampler thread failed to start")
            .join()
    }
}
