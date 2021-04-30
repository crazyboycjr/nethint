// $ cat hostfile
// 192.168.211.3:22
// 192.168.211.4:22
// # 192.168.211.5:22
// 192.168.211.6:22
// $ rplaunch -f hostfile -n jobname -o output --controller-uri 192.168.211.3:9000 --controller-ssh
// 192.168.211.3
#![feature(str_split_once)]
#![feature(command_access)]

use std::process::Command;

use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "rplaunch", about = "Launcher of the distributed replayer.")]
struct Opt {
    /// Hostfile
    #[structopt(short = "f", long = "hostfile")]
    hostfile: std::path::PathBuf,

    /// Job name, used to isolate potential overlapped files
    #[structopt(short = "n", long = "jobname")]
    jobname: String,

    /// configfile
    #[structopt(short = "c", long = "config")]
    configfile: String,

    /// Output directory of log files
    #[structopt(short = "o", long = "output", default_value = "output")]
    output: std::path::PathBuf,

    /// Controller URI
    #[structopt(long)]
    controller_uri: String,

    /// Controller ssh address
    #[structopt(long)]
    controller_ssh: String,

    /// Brain/nethint agent global leader URI, corresponding to NH_CONTROLLER_URI env
    #[structopt(long)]
    brain_uri: String,

    /// Output path of the timing result, passed to controller
    #[structopt(short, long = "timing")]
    timing: Option<std::path::PathBuf>,
}

#[derive(Debug, Clone, Default)]
struct Hostfile {
    hosts: Vec<String>,
}

fn parse_from_file<P: AsRef<std::path::Path>>(path: P) -> anyhow::Result<Hostfile> {
    use std::io::BufRead;
    let mut hostfile = Hostfile::default();
    let f = std::fs::File::open(path)?;
    let reader = std::io::BufReader::new(f);
    hostfile.hosts = reader
        .lines()
        .map(|l| l.unwrap())
        .filter(|l| !l.is_empty() && !l.trim_start().starts_with("#"))
        .collect();
    Ok(hostfile)
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Role {
    Controller,
    Worker,
}

impl std::fmt::Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Controller => write!(f, "controller"),
            Self::Worker => write!(f, "worker"),
        }
    }
}

fn hash_file<P: AsRef<std::path::Path>>(path: P) -> String {
    use sha2::Digest;

    log::debug!("hash content of file: {}", path.as_ref().display());
    let content = std::fs::read(path).unwrap();

    // create a Sha256 object
    let mut hasher = sha2::Sha256::new();

    // write input message
    hasher.update(content);

    // read hash digest and consume hasher
    let result = hasher.finalize();

    format!("{:X}", result)
}

fn start_ssh(opt: &Opt, host: String, role: Role, envs: &[String]) -> impl FnOnce() -> () {
    let jobname = opt.jobname.clone();
    let output_dir = opt.output.clone();
    let mut env_str = envs.join(" ");
    let configfile = opt.configfile.clone();
    let timing = opt.timing.clone();
    if role == Role::Controller {
        env_str.push_str(&format!(" NH_CONTROLLER_URI={} ", opt.brain_uri));
    }

    move || {
        let (ip, port) = host.rsplit_once(':').or(Some((&host, "22"))).unwrap();

        let stdout_file = output_dir
            .join(format!("{}_{}.log", role, ip))
            .with_extension("stdout");
        let stderr_file = output_dir
            .join(format!("{}_{}.log", role, ip))
            .with_extension("stderr");

        let stdout = utils::fs::open_with_create_append(stdout_file);
        let stderr = utils::fs::open_with_create_append(stderr_file);
        let mut cmd = Command::new("ssh");
        cmd.stdout(stdout).stderr(stderr);
        cmd.arg("-oStrictHostKeyChecking=no")
            // .arg("-tt") // force allocating a tty
            .arg("-p")
            .arg(port)
            .arg(ip);

        // for controller, scp configfile first
        let mut controller_args = String::new();
        if role == Role::Controller {
            let hash = &hash_file(&configfile)[..7];
            let src = configfile;
            let dst = format!("/tmp/{}_setting_{}.toml", jobname, hash);
            let dst_full = format!("{}:{}", ip, dst);
            let mut scp_cmd = Command::new("scp");
            scp_cmd.arg("-P").arg(port).arg(src).arg(dst_full);
            let _output = utils::cmd_helper::get_command_output(scp_cmd);

            controller_args = format!("--app {} --config {}", jobname, dst);
            if let Some(path) = timing {
                controller_args.push_str(" --timing ");
                controller_args.push_str(path.to_str().unwrap());
            }
        }

        // TODO(cjr): also to distribute binary program to workers
        match role {
            Role::Controller => cmd.arg(format!("{} /tmp/controller {}", env_str, controller_args)),
            Role::Worker => cmd.arg(format!("{} /tmp/worker", env_str)),
        };

        utils::poll_cmd!(cmd, TERMINATE);
    }
}

fn submit(opt: Opt) -> anyhow::Result<()> {
    let hostfile = parse_from_file(&opt.hostfile)?;
    log::info!("hostfile: {:?}", hostfile);

    // create or clean directory
    let output_dir = &opt.output;
    if output_dir.exists() {
        // rm -r output_dir
        std::fs::remove_dir_all(output_dir)?;
    }
    std::fs::create_dir_all(output_dir)?;

    let envs = [
        format!("RP_CONTROLLER_URI={}", opt.controller_uri),
        format!("RP_NUM_WORKER={}", hostfile.hosts.len()),
    ];

    let mut handles = vec![];
    {
        // start Controller
        let handle = std::thread::spawn(start_ssh(
            &opt,
            opt.controller_ssh.clone(),
            Role::Controller,
            &envs,
        ));
        handles.push(handle);
        // TODO(cjr): wait for controller to start, e.g. 1s
    }

    for h in hostfile.hosts {
        let handle = std::thread::spawn(start_ssh(&opt, h, Role::Worker, &envs));
        handles.push(handle);
    }

    for h in handles {
        h.join()
            .unwrap_or_else(|e| panic!("Failed to join thread: {:?}", e));
    }

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

    submit(opt).unwrap();
}
