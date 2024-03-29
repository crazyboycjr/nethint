// ./rplaunch --brain-uri 152.3.137.219:9000 --controller-ssh 192.168.211.35 --controller-uri 192.168.211.35:9000 --hostfile ~/hostfile --jobname allreduce --config ~/allreduce_single.toml

// $ scheduler -n jobname -o jobs --brain-uri 152.3.137.219:9000 -c experiment.toml
#![feature(command_access)]

use std::process::Command;

use structopt::StructOpt;

use nethint::TenantId;
use nethint::{cluster::Topology, hint::NetHintV1Real};
use std::collections::HashMap;

#[derive(Debug, StructOpt)]
#[structopt(
    name = "scheduler",
    about = "Launch jobs in batch by calling rplaunch."
)]
pub struct Opt {
    /// Job name, used to isolate potential overlapped files
    #[structopt(short = "n", long = "jobname")]
    jobname: String,

    /// configfile, this specify how the many experiments should be run
    #[structopt(short = "c", long = "config")]
    configfile: String,

    /// Output directory of log files, output/jobname_{} will be passed to rplaunch as its ouptut option
    #[structopt(short = "o", long = "output", default_value = "jobs")]
    output: std::path::PathBuf,

    /// Brain/nethint agent global leader URI, corresponding to NH_CONTROLLER_URI env, this will be directly pass to rplaunch
    #[structopt(long)]
    brain_uri: String,

    /// Output path of the timing result, passed to rplaunch and further to controller
    #[structopt(short, long)]
    timing: Option<std::path::PathBuf>,
}

// this is the overall table, the scheduler must know the VMs' hostnames and their corresponding Node (ipaddr)
#[derive(Debug, Clone)]
struct HostnameIpTable {
    table: HashMap<String, String>,
}

lazy_static::lazy_static! {
    static ref HOSTNAME_IP_TABLE: HostnameIpTable = {
        // let hostnames: Vec<String> = (0..24).map(|x| (x / 4) * 8 + x % 4).map(|x| format!("cpu{}", x)).collect();
        let hostnames: Vec<String> = (0..24).map(|x| (x / 4) * 8 + x % 4).map(|x| format!("nixos{}", x)).collect();
        let ipaddrs: Vec<&str> = include!("./vm_ip_addrs.in");
        HostnameIpTable {
            table: hostnames.into_iter().zip(ipaddrs).map(|(k, v)| (k, v.to_string())).collect()
        }
    };
}

#[derive(Debug, Clone)]
struct JobConfig {
    job_id: usize,
    output_dir: std::path::PathBuf,
    config_path: std::path::PathBuf,
    hostfile_path: std::path::PathBuf,
    controller_ssh: String,
    controller_uri: String,
}

fn request_provision(
    brain: &mut std::net::TcpStream,
    this_tenant_id: TenantId,
    nhosts_to_acquire: usize,
    allow_delay: bool,
) -> anyhow::Result<NetHintV1Real> {
    use nhagent_v2::message::Message;
    let msg = Message::ProvisionRequest(this_tenant_id, nhosts_to_acquire, allow_delay);
    litemsg::utils::send_cmd_sync(brain, &msg)?;
    let reply: Message = litemsg::utils::recv_cmd_sync(brain)?;
    match reply {
        Message::ProvisionResponse(tenant_id, hintv1) => {
            assert_eq!(tenant_id, this_tenant_id);
            Ok(hintv1)
        }
        _ => {
            panic!("unexpected reply from brain: {:?}", reply);
        }
    }
}

fn notify_batch_done(brain: &mut std::net::TcpStream) -> anyhow::Result<()> {
    use nhagent_v2::message::Message;
    let msg = Message::BatchDoneNotification;
    litemsg::utils::send_cmd_sync(brain, &msg)?;
    // no acknowlegement for this message
    Ok(())
}

fn request_destroy(brain: &mut std::net::TcpStream, tenant_id: TenantId) -> anyhow::Result<()> {
    // send request to destroy VMs
    use nhagent_v2::message::Message;
    let msg = Message::DestroyRequest(tenant_id);
    litemsg::utils::send_cmd_sync(brain, &msg)?;
    let reply: Message = litemsg::utils::recv_cmd_sync(brain)?;
    match reply {
        Message::DestroyResponse(tenant_id2) => {
            assert_eq!(tenant_id2, tenant_id);
            // do nothing and exit
        }
        _ => {
            panic!("unexpected reply from brain: {:?}", reply);
        }
    }
    Ok(())
}

fn get_ipaddrs(hintv1: &NetHintV1Real) -> Vec<String> {
    let nhosts = hintv1.vc.num_hosts();
    let mut content = Vec::new();
    for i in 0..nhosts {
        let vname = format!("host_{}", i);
        let hostname = &hintv1.vname_to_hostname[&vname];
        let ip = HOSTNAME_IP_TABLE
            .table
            .get(hostname)
            .unwrap_or_else(|| panic!("key not found: key: {}", hostname));
        // let ip = HOSTNAME_IP_TABLE.table[hostname].clone();
        content.push(ip.clone());
    }
    content
}

fn generate_hostfile(hintv1: &NetHintV1Real, hostfile_path: &std::path::PathBuf) {
    let content = get_ipaddrs(hintv1);

    // remove the previous result file
    if hostfile_path.exists() {
        std::fs::remove_file(hostfile_path).unwrap();
    }
    std::fs::write(hostfile_path, content.join("\n")).unwrap();
}

fn submit(
    opt: &Opt,
    job_id: usize,
    nhosts: usize,
    allow_delay: bool,
    job_dir: std::path::PathBuf,
    config_path: std::path::PathBuf,
) -> impl FnOnce() -> () {
    let output_dir = opt.output.clone();
    let jobname = opt.jobname.clone();
    let brain_uri = opt.brain_uri.clone();
    let timing = opt.timing.clone();

    move || {
        // send provision request to brain
        let mut brain = litemsg::utils::connect_retry(&brain_uri, 5).unwrap();
        let this_tenant_id = job_id;
        let nhosts_to_acquire = nhosts;
        let hintv1 =
            request_provision(&mut brain, this_tenant_id, nhosts_to_acquire, allow_delay).unwrap();
        log::info!(
            "hintv1: vname_to_hostname: {:?}, vcluster: {}",
            hintv1.vname_to_hostname,
            hintv1.vc.to_dot()
        );

        // construct job config according to the provision result
        std::fs::create_dir_all(&job_dir).expect("fail to create directory");

        // genenrate hostfile from hintv1
        let hostfile_path = job_dir.join("hostfile");
        generate_hostfile(&hintv1, &hostfile_path);

        // select a host as controller
        let ipaddrs = get_ipaddrs(&hintv1);
        log::info!("job_id: {}, ipaddrs: {:?}", job_id, ipaddrs);
        let controller_ssh = ipaddrs[0].clone();
        let controller_uri = format!("{}:{}", ipaddrs[0], 9900);

        let job_output_dir = job_dir.join("output");
        std::fs::create_dir_all(&job_output_dir).expect("fail to create directory");
        let jobconfig = JobConfig {
            job_id,
            output_dir: job_output_dir,
            config_path: config_path.clone(),
            hostfile_path,
            controller_ssh,
            controller_uri,
        };
        log::info!("jobconfig: {:?}", jobconfig);

        // execute rplaunch command and poll result

        let stdout_file = output_dir.join("launcher.log").with_extension("stdout");
        let stderr_file = output_dir.join("launcher.log").with_extension("stderr");

        let stdout = utils::fs::open_with_create_append(stdout_file);
        let stderr = utils::fs::open_with_create_append(stderr_file);
        let mut cmd = Command::new("./rplaunch");
        cmd.stdout(stdout).stderr(stderr);
        // cmd.arg(args);
        cmd.arg("--brain-uri").arg(brain_uri);
        cmd.arg("--jobname").arg(jobname);
        cmd.arg("--config").arg(jobconfig.config_path);
        cmd.arg("--output").arg(jobconfig.output_dir);
        cmd.arg("--hostfile").arg(jobconfig.hostfile_path);
        cmd.arg("--controller-ssh").arg(jobconfig.controller_ssh);
        cmd.arg("--controller-uri").arg(jobconfig.controller_uri);
        if let Some(path) = timing {
            cmd.arg("--timing").arg(path.to_str().unwrap());
        }

        utils::poll_cmd!(cmd, TERMINATE);

        request_destroy(&mut brain, this_tenant_id).unwrap();
    }
}

pub fn write_setting<T: serde::Serialize, P: AsRef<std::path::Path>>(setting: &T, path: P) {
    use std::io::Write;
    let mut file = std::fs::File::create(path).expect("fail to create file");
    let value = toml::Value::try_from(&setting).unwrap();
    let content = value.to_string();
    file.write_all(content.as_bytes()).unwrap();
}

mod sched_rl {
    use super::*;
    use rl::config;
    use rl::JobSpec;
    use rand::{Rng, rngs::StdRng, SeedableRng};

    fn gen_job_specs(config: &config::ExperimentConfig) -> Vec<(u64, JobSpec)> {
        let mut jobs = Vec::new();
        let mut rng = StdRng::seed_from_u64(config.seed as u64);
        let mut t = 0;
        for i in 0..config.ncases {
            let num_workers = config::get_random_job_size(&config.job_size_distribution, &mut rng);
            let root_index = rng.gen_range(0..num_workers);
            let job_spec = JobSpec::new(
                num_workers,
                config.buffer_size,
                config.num_iterations,
                root_index,
            );
            let next = config::get_random_arrival_time(config.poisson_lambda, &mut rng);
            t += next;
            log::info!("job {}: {:?}", i, job_spec);
            jobs.push((t, job_spec));
        }
        jobs
    }

    pub fn submit_jobs(opt: &Opt) {
        let config: config::ExperimentConfig = config::read_config(&opt.configfile);
        let mut brain = litemsg::utils::connect_retry(&opt.brain_uri, 5).unwrap();

        for batch_id in 0..config.batches.len() {
            let mut handles = Vec::new();
            let now0 = std::time::Instant::now();

            let jobs = gen_job_specs(&config);
            let batch = config.batches[batch_id].clone();
            for i in 0..config.ncases {
                let job_id = i;
                let start_ts = jobs[i].0;

                // prepare output directory
                let job_dir = opt
                    .output
                    .join(format!("{}_{}_{}", opt.jobname, batch_id, job_id));

                if job_dir.exists() {
                    // rm -r output_dir
                    std::fs::remove_dir_all(&job_dir).unwrap();
                }

                std::fs::create_dir_all(&job_dir).expect("fail to create directory");

                let setting = replayer::controller::rl::RLSetting {
                    job_id,
                    job_size_distribution: config.job_size_distribution.clone(),
                    buffer_size: config.buffer_size,
                    num_iterations: config.num_iterations,
                    poisson_lambda: config.poisson_lambda,
                    partially_sync: config.partially_sync,
                    seed_base: config.seed + i as u64,
                    traffic_scale: 1.0,
                    rl_policy: batch.policy,
                    probe: batch.probe.clone(),
                    nethint_level: batch.nethint_level,
                    auto_tune: batch.auto_tune,
                    num_trees: batch.num_trees,
                };

                let setting_path = job_dir.join("setting.toml");

                write_setting(&setting, &setting_path);

                let now = std::time::Instant::now();
                let start_ts_dura = std::time::Duration::from_nanos(start_ts);
                if now0 + start_ts_dura > now {
                    std::thread::sleep(now0 + start_ts_dura - now);
                }
                handles.push(std::thread::spawn(submit(
                    opt,
                    job_id,
                    jobs[i].1.num_workers,
                    config.allow_delay.unwrap_or(false),
                    job_dir,
                    setting_path,
                )));
            }

            for h in handles {
                h.join()
                    .unwrap_or_else(|e| panic!("Failed to join thread: {:?}", e));
            }

            notify_batch_done(&mut brain).unwrap();
        }
    }
}

mod sched_allreduce {
    use super::*;
    use allreduce::config;
    use allreduce::JobSpec;
    use rand::{rngs::StdRng, SeedableRng};

    fn gen_job_specs(config: &config::ExperimentConfig) -> Vec<(u64, JobSpec)> {
        let mut jobs = Vec::new();
        let mut rng = StdRng::seed_from_u64(config.seed as u64);
        let mut t = 0;
        for i in 0..config.ncases {
            let job_spec = JobSpec::new(
                config::get_random_job_size(&config.job_size_distribution, &mut rng),
                config.buffer_size,
                config.num_iterations,
            );
            let next = config::get_random_arrival_time(config.poisson_lambda, &mut rng);
            t += next;
            log::info!("job {}: {:?}", i, job_spec);
            jobs.push((t, job_spec));
        }
        jobs
    }

    pub fn submit_jobs(opt: &Opt) {
        let config: config::ExperimentConfig = config::read_config(&opt.configfile);
        let mut brain = litemsg::utils::connect_retry(&opt.brain_uri, 5).unwrap();

        for batch_id in 0..config.batches.len() {
            let mut handles = Vec::new();
            let now0 = std::time::Instant::now();

            let jobs = gen_job_specs(&config);
            let batch = config.batches[batch_id].clone();
            for i in 0..config.ncases {
                let job_id = i;
                let start_ts = jobs[i].0;

                // prepare output directory
                let job_dir = opt
                    .output
                    .join(format!("{}_{}_{}", opt.jobname, batch_id, job_id));

                if job_dir.exists() {
                    // rm -r output_dir
                    std::fs::remove_dir_all(&job_dir).unwrap();
                }

                std::fs::create_dir_all(&job_dir).expect("fail to create directory");

                let setting = replayer::controller::allreduce::AllreduceSetting {
                    job_id,
                    job_size_distribution: config.job_size_distribution.clone(),
                    buffer_size: config.buffer_size,
                    num_iterations: config.num_iterations,
                    poisson_lambda: config.poisson_lambda,
                    seed_base: config.seed + i as u64,
                    traffic_scale: 1.0,
                    allreduce_policy: batch.policy,
                    probe: batch.probe.clone(),
                    nethint_level: batch.nethint_level,
                    auto_tune: batch.auto_tune,
                    num_rings: batch.num_rings,
                };

                let setting_path = job_dir.join("setting.toml");

                write_setting(&setting, &setting_path);

                let now = std::time::Instant::now();
                let start_ts_dura = std::time::Duration::from_nanos(start_ts);
                if now0 + start_ts_dura > now {
                    std::thread::sleep(now0 + start_ts_dura - now);
                }
                handles.push(std::thread::spawn(submit(
                    opt,
                    job_id,
                    jobs[i].1.num_workers,
                    config.allow_delay.unwrap_or(false),
                    job_dir,
                    setting_path,
                )));
            }

            for h in handles {
                h.join()
                    .unwrap_or_else(|e| panic!("Failed to join thread: {:?}", e));
            }

            notify_batch_done(&mut brain).unwrap();
        }
    }
}

mod sched_mapreduce {
    use super::*;
    use mapreduce::{config, JobSpec, ShufflePattern};
    use replayer::controller::mapreduce::{MapReduceAppBuilder, MapReduceSetting};

    fn is_job_trivial(job_spec: &JobSpec) -> bool {
        match job_spec.shuffle_pat {
            ShufflePattern::FromTrace(ref record) => {
                let mut weights: Vec<u64> = vec![0u64; job_spec.num_reduce];
                for (i, (_x, y)) in record.reducers.iter().enumerate() {
                    weights[i % job_spec.num_reduce] += *y as u64;
                }
                job_spec.num_map == 1
                    || weights.iter().copied().max().unwrap() as f64
                        <= 1.05 * weights.iter().copied().min().unwrap() as f64
            }
            _ => {
                unimplemented!();
            }
        }
    }

    pub fn submit_jobs(opt: &Opt) {
        let config: config::ExperimentConfig = config::read_config(&opt.configfile);
        let mut brain = litemsg::utils::connect_retry(&opt.brain_uri, 5).unwrap();

        for batch_id in 0..config.batches.len() {
            let mut handles = Vec::new();
            let now0 = std::time::Instant::now();
            let mut is_trivial = Vec::new();

            let batch = config.batches[batch_id].clone();
            for i in 0..config.ncases {
                let job_id = i;
                let seed = i as _;

                let setting = MapReduceSetting {
                    trace: config
                        .trace
                        .clone()
                        .expect("current only support generate shuffle from trace"),
                    job_id,
                    seed_base: seed,
                    map_scale: config.map_scale.expect("must provide a map scale factor"),
                    reduce_scale: config
                        .reduce_scale
                        .expect("must provide a reduce scale factor"),
                    traffic_scale: config.traffic_scale,
                    time_scale: config.time_scale.expect("must provide a time scale factor"),
                    collocate: config.collocate,
                    mapper_policy: config.mapper_policy.clone(),
                    reducer_policy: batch.reducer_policy,
                    probe: batch.probe,
                    nethint_level: batch.nethint_level,
                };

                // get job specification
                let (start_ts, job_spec) = MapReduceAppBuilder::get_job_spec(&setting);

                is_trivial.push((job_id, is_job_trivial(&job_spec)));
                // skip trivial jobs
                if config.skip_trivial.unwrap_or(false) && is_trivial.last().unwrap().1 {
                    continue;
                }

                // prepare output directory
                let job_dir = opt
                    .output
                    .join(format!("{}_{}_{}", opt.jobname, batch_id, job_id));

                if job_dir.exists() {
                    // rm -r output_dir
                    std::fs::remove_dir_all(&job_dir).unwrap();
                }

                // mkdir -p output_dir
                std::fs::create_dir_all(&job_dir).expect("fail to create directory");

                let setting_path = job_dir.join("setting.toml");

                write_setting(&setting, &setting_path);

                let now = std::time::Instant::now();
                if now0 + std::time::Duration::from_nanos(start_ts) > now {
                    std::thread::sleep(now0 + std::time::Duration::from_nanos(start_ts) - now);
                }
                let nhosts_to_acquire = if config.collocate {
                    job_spec.num_map.max(job_spec.num_reduce)
                } else {
                    job_spec.num_map + job_spec.num_reduce
                };

                handles.push(std::thread::spawn(submit(
                    opt,
                    job_id,
                    nhosts_to_acquire,
                    config.allow_delay.unwrap_or(false),
                    job_dir,
                    setting_path,
                )));
            }

            use std::io::Write;
            let mut f = utils::fs::open_with_create_append("/tmp/trivial_jobs.txt");
            writeln!(f, "batch_id: {}, trivial jobs: {:?}", batch_id, is_trivial).unwrap();

            for h in handles {
                h.join()
                    .unwrap_or_else(|e| panic!("Failed to join thread: {:?}", e));
            }

            // After a batch of jobs is finsihed, we need to reset the background traffic for an apple-to-apple comparison
            notify_batch_done(&mut brain).unwrap();
        }
    }
}

fn schedule_jobs(opt: Opt) -> anyhow::Result<()> {
    match opt.jobname.as_str() {
        "rl" => {
            sched_rl::submit_jobs(&opt);
        }
        "allreduce" => {
            sched_allreduce::submit_jobs(&opt);
        }
        "mapreduce" => {
            sched_mapreduce::submit_jobs(&opt);
        }
        _ => panic!("unknown job {}", opt.jobname),
    };

    Ok(())
}

use nix::sys::signal;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::SeqCst;

static TERMINATE: AtomicBool = AtomicBool::new(false);

extern "C" fn handle_sigint(sig: i32) {
    log::warn!("sigint catched");
    assert_eq!(sig, signal::SIGINT as i32);
    TERMINATE.store(true, SeqCst);
}

fn main() {
    logging::init_log();

    let opt = Opt::from_args();
    log::info!("options: {:?}", opt);

    // register sigint handler
    let sig_action = signal::SigAction::new(
        signal::SigHandler::Handler(handle_sigint),
        signal::SaFlags::empty(),
        signal::SigSet::empty(),
    );
    unsafe {
        signal::sigaction(signal::SIGINT, &sig_action).expect("failed to register sighandler");
    }

    schedule_jobs(opt).unwrap();
}
