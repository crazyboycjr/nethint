use crate::sdn_controller;
use std::collections::HashMap;
use std::net::UdpSocket;
use std::sync::mpsc;

use nethint::cluster::LinkIx;
use nethint::counterunit::CounterUnit;

pub struct TcSampler {
    interval_ms: u64,
    listen_port: u16,
    handle: Option<std::thread::JoinHandle<()>>,
    tx: mpsc::Sender<HashMap<LinkIx, Vec<CounterUnit>>>,
}

impl TcSampler {
    pub fn new(interval_ms: u64, listen_port: u16, tx: mpsc::Sender<HashMap<LinkIx, Vec<CounterUnit>>>) -> Self {
        TcSampler {
            interval_ms,
            listen_port,
            handle: None,
            tx,
        }
    }

    pub fn run(&mut self) {
        log::info!("starting BPF TC Sampler thread...");

        let tx = self.tx.clone();
        let interval = std::time::Duration::from_millis(self.interval_ms);

        let listen_addr = ("0.0.0.0", self.listen_port); // default 5555
        let sock = UdpSocket::bind(listen_addr)
            .unwrap_or_else(|_| panic!("could'n bind to or listen on {:?}", listen_addr));
        sock.set_read_timeout(Some(std::time::Duration::from_millis(1)))
            .expect("set udp socket timeout failed");

        let mut buf = vec![0u8; 65507];
        let mut last_ts = std::time::Instant::now();
        let mut counter_hashmap: HashMap<LinkIx, Vec<CounterUnit>> = HashMap::new();

        let rack_ip_table = sdn_controller::get_rack_ip_table().expect("get rack_ip_table failed");

        self.handle = Some(std::thread::spawn(move || loop {
            let now = std::time::Instant::now();
            if now >= last_ts + interval {
                tx.send(counter_hashmap.clone()).unwrap();
                last_ts = now;
            }

            match sock.recv_from(&mut buf) {
                Ok((nbytes, saddr)) => {
                    let buf = &buf[..nbytes];
                    let counter: Vec<CounterUnit> = bincode::deserialize(&buf).expect("deserialize counter failed");
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