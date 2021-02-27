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

    let result = cmd.output()?;

    use std::os::unix::process::ExitStatusExt; // for status.signal()
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

    Ok(std::str::from_utf8(&result.stdout)?.trim().to_owned())
}
