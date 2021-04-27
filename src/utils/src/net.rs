use std::process::Command;
use crate::cmd_helper::get_command_output;

pub fn get_primary_ipv4(iface: &str) -> anyhow::Result<String> {
    let mut cmd = Command::new("ip");
    cmd.arg("addr").arg("show").arg(iface);
    let output = get_command_output(cmd).expect("ip addr failed to execute");
    let start = 5 + output.find("inet ").expect("inet not found in the output");
    let len = (&output[start..]).find("/").unwrap();
    Ok((&output[start..start + len]).to_owned())
}
