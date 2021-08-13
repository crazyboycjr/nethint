use std::process::Command;
use crate::cmd_helper::get_command_output;
use std::net::Ipv4Addr;

pub fn get_primary_ipv4(iface: &str) -> anyhow::Result<Ipv4Addr> {
    let mut cmd = Command::new("ip");
    cmd.arg("addr").arg("show").arg(iface);
    let output = get_command_output(cmd).expect("ip addr failed to execute");
    let start = 5 + output.find("inet ").expect("inet not found in the output");
    let len = (&output[start..]).find("/").unwrap();
    Ok((&output[start..start + len]).parse()?)
}
