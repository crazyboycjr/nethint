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

fn open_or_create_append<P: AsRef<std::path::Path>>(path: P) -> std::fs::File {
    std::fs::OpenOptions::new()
        .append(true)
        .create(true)
        .open(&path)
        .unwrap_or_else(|e| panic!("fail to open or create {:?}: {}", path.as_ref(), e))
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

fn start_ssh(opt: &Opt, host: String, role: Role, envs: &[String]) -> impl FnOnce() -> () {
    let output_dir = opt.output.clone();
    let env_str = envs.join(" ");
    let s = format!("--app {} --config {}", opt.jobname, opt.configfile);

    move || {
        let (ip, port) = host.rsplit_once(':').or(Some((&host, "22"))).unwrap();

        let stdout_file = output_dir
            .join(format!("{}_{}.log", role, ip))
            .with_extension("stdout");
        let stderr_file = output_dir
            .join(format!("{}_{}.log", role, ip))
            .with_extension("stderr");

        let stdout = open_or_create_append(stdout_file);
        let stderr = open_or_create_append(stderr_file);
        let mut cmd = Command::new("ssh");
        cmd.stdout(stdout).stderr(stderr);
        cmd.arg("-oStrictHostKeyChecking=no")
            // .arg("-tt") // force allocating a tty
            .arg("-p")
            .arg(port)
            .arg(ip);

        // TODO(cjr): also to distribute binary program to workers
        match role {
            Role::Controller => cmd.arg(format!("{} /tmp/controller {}", env_str, s)),
            Role::Worker => cmd.arg(format!("{} /tmp/worker", env_str)),
        };

        let prog = cmd.get_program().to_str().unwrap();
        let args: Vec<&str> = cmd.get_args().map(|x| x.to_str().unwrap()).collect();
        let cmd_str = (std::iter::once(prog).chain(args).collect::<Vec<_>>()).join(" ");
        log::debug!("command: {}", cmd_str);

        let mut child = cmd.spawn().expect("Failed to start ssh submit command");
        use std::os::unix::process::ExitStatusExt; // for status.signal()
        loop {
            match child.try_wait() {
                Ok(Some(status)) => {
                    if !status.success() {
                        match status.code() {
                            Some(code) => {
                                log::error!("Exited with code: {}, cmd: {}", code, cmd_str)
                            }
                            None => log::error!(
                                "Process terminated by signal: {}, cmd: {}",
                                status.signal().unwrap(),
                                cmd_str,
                            ),
                        }
                    }
                    break;
                }
                Ok(None) => {
                    log::trace!("status not ready yet, sleep for 5 ms");
                    std::thread::sleep(std::time::Duration::from_millis(5));
                }
                Err(e) => {
                    panic!("Command wasn't running: {}", e);
                }
            }
            // check if kill is needed
            if TERMINATE.load(SeqCst) {
                log::warn!("killing the child process: {}", cmd_str);
                // instead of SIGKILL, we use SIGTERM here to gracefully shutdown ssh process tree.
                // SIGKILL can cause terminal control characters to mess up, which must be
                // fixed later with sth like "stty sane".
                // signal::kill(nix::unistd::Pid::from_raw(child.id() as _), signal::SIGTERM)
                //     .unwrap_or_else(|e| panic!("Failed to kill: {}", e));
                child
                    .kill()
                    .unwrap_or_else(|e| panic!("Failed to kill: {}", e));
                log::warn!("child process terminated")
            }
        }
    }
}

fn submit(opt: Opt) -> anyhow::Result<()> {
    let hostfile = parse_from_file(&opt.hostfile)?;
    log::info!("hostfile: {:?}", hostfile);

    // create or clean directory
    let output_dir = &opt.output;
    if output_dir.exists() {
        // rm -r output_dir
        std::fs::remove_dir_all(&output_dir)?;
    }
    std::fs::create_dir_all(&output_dir)?;

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
