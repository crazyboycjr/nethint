use std::process::Command;

pub fn get_command_str(cmd: &Command) -> String {
    let prog = cmd.get_program().to_str().unwrap();
    let args: Vec<&str> = cmd.get_args().map(|x| x.to_str().unwrap()).collect();
    let cmd_str = std::iter::once(prog).chain(args).collect::<Vec<_>>().join(" ");
    cmd_str
}

pub fn get_command_output(mut cmd: Command) -> anyhow::Result<String> {
    let cmd_str = get_command_str(&cmd);
    log::debug!("executing command: {}", cmd_str);

    use std::os::unix::process::ExitStatusExt; // for status.signal()
    let result = cmd.output()?;

    if !result.status.success() {
        return match result.status.code() {
            Some(code) => Err(anyhow::anyhow!(
                "Exited with code: {}, cmd: {}",
                code,
                cmd_str
            )),
            None => Err(anyhow::anyhow!(
                "Process terminated by signal: {}, cmd: {}",
                result.status.signal().unwrap(),
                cmd_str,
            )),
        };
    }

    Ok(std::str::from_utf8(&result.stdout)?.to_owned())
}

#[macro_export]
macro_rules! poll_cmd {
    ($cmd:expr, $stop_flag:expr) => {{
        let prog = $cmd.get_program().to_str().unwrap();
        let args: Vec<&str> = $cmd.get_args().map(|x| x.to_str().unwrap()).collect();
        let cmd_str = (std::iter::once(prog).chain(args).collect::<Vec<_>>()).join(" ");
        log::debug!("command: {}", cmd_str);

        use std::os::unix::process::ExitStatusExt; // for status.signal()
        let mut child = $cmd.spawn().expect("Failed to rplaunch");
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
            if $stop_flag.load(SeqCst) {
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
    }}
}