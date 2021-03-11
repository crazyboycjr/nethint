// ./rplaunch --brain-uri 152.3.137.219:9000 --controller-ssh 192.168.211.35 --controller-uri 192.168.211.35:9000 --hostfile ~/hostfile --jobname allreduce --config ~/allreduce_single.toml

// $ scheduler -n jobname -o jobs --brain-uri 152.3.137.219:9000 -c experiment.toml
#![feature(str_split_once)]
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
}

// this is the overall table, the scheduler must know the VMs' hostnames and their corresponding Node (ipaddr)
#[derive(Debug, Clone)]
struct HostnameIpTable {
    table: HashMap<String, String>,
}

lazy_static::lazy_static! {
    static ref HOSTNAME_IP_TABLE: HostnameIpTable = {
        let hostnames: Vec<String> = (0..24).map(|x| (x / 4) * 8 + x % 4).map(|x| format!("cpu{}", x)).collect();
        let ipaddrs: Vec<&str> = vec![ "192.168.211.3" , "192.168.211.4" , "192.168.211.5" , "192.168.211.6" , "192.168.211.35" , "192.168.211.36" , "192.168.211.37" , "192.168.211.38" , "192.168.211.67" , "192.168.211.68" , "192.168.211.69" , "192.168.211.70" , "192.168.211.131" , "192.168.211.132" , "192.168.211.133" , "192.168.211.134" , "192.168.211.163" , "192.168.211.164" , "192.168.211.165" , "192.168.211.166" , "192.168.211.195" , "192.168.211.196" , "192.168.211.197" , "192.168.211.198"];
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
) -> anyhow::Result<NetHintV1Real> {
    use nhagent::message::Message;
    let msg = Message::ProvisionRequest(this_tenant_id, nhosts_to_acquire);
    litemsg::utils::send_cmd_sync(brain, &msg)?;
    let reply: Message = litemsg::utils::recv_cmd_sync(brain)?;
    match reply {
        Message::ProvisionResponse(tenant_id, hintv1) => {
            assert_eq!(tenant_id, this_tenant_id);
            return Ok(hintv1);
        }
        _ => {
            panic!("unexpected reply from brain: {:?}", reply);
        }
    }
}

fn request_destroy(brain: &mut std::net::TcpStream, tenant_id: TenantId) -> anyhow::Result<()> {
    // send request to destroy VMs
    use nhagent::message::Message;
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
        let ip = HOSTNAME_IP_TABLE.table[hostname].clone();
        content.push(ip);
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
    job_dir: std::path::PathBuf,
    config_path: std::path::PathBuf,
) -> impl FnOnce() -> () {
    let output_dir = opt.output.clone();
    let jobname = opt.jobname.clone();
    let brain_uri = opt.brain_uri.clone();

    move || {
        // send provision request to brain
        let mut brain = litemsg::utils::connect_retry(&brain_uri, 5).unwrap();
        let this_tenant_id = job_id;
        let nhosts_to_acquire = nhosts;
        let hintv1 = request_provision(&mut brain, this_tenant_id, nhosts_to_acquire).unwrap();

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

        for batch_id in 0..config.batches.len() {
            let mut handles = Vec::new();
            let now0 = std::time::Instant::now();

            let jobs = gen_job_specs(&config);
            let batch = config.batches[batch_id].clone();
            for i in 0..config.ncases {
                let job_id = i;
                let start_ts = jobs[i].0;

                // prepare output directory
                let job_dir = opt.output.join(format!("{}_{}_{}", opt.jobname, batch_id, job_id));

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
                    seed_base: config.seed,
                    traffic_scale: 1.0,
                    allreduce_policy: batch.policy,
                    probe: batch.probe.clone(),
                    nethint_level: batch.nethint_level,
                    auto_tune: batch.auto_tune,
                };

                let setting_path = job_dir.join("setting.toml");

                write_setting(&setting, &setting_path);

                let now = std::time::Instant::now();
                if now0 + std::time::Duration::from_nanos(start_ts) > now {
                    std::thread::sleep(now0 + std::time::Duration::from_nanos(start_ts) - now);
                }
                handles.push(std::thread::spawn(submit(
                    opt,
                    job_id,
                    jobs[i].1.num_workers,
                    job_dir,
                    setting_path,
                )));
            }

            for h in handles {
                h.join()
                    .unwrap_or_else(|e| panic!("Failed to join thread: {:?}", e));
            }
        }
    }
}

fn schedule_jobs(opt: Opt) -> anyhow::Result<()> {
    match opt.jobname.as_str() {
        "allreduce" => {
            sched_allreduce::submit_jobs(&opt);
        }
        _ => panic!("unknown job {}", opt.jobname),
    };

    // for h in handles {
    //     h.join()
    //         .unwrap_or_else(|e| panic!("Failed to join thread: {:?}", e));
    // }

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
