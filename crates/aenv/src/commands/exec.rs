use crate::client::Client;
use crate::grpc::{build_start_request, drain_output, StartOpts};
use anyhow::Result;
use clap::Args as ClapArgs;
use envd::process::StartResponse;

#[derive(ClapArgs)]
pub struct Args {
    sandbox_id: String,
    /// Command and arguments to run. Flags intended for the remote command
    /// that collide with aenv's own flags can be escaped with a leading `--`.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true, required = true)]
    command: Vec<String>,
}

pub fn run(args: Args) -> Result<()> {
    let client = Client::from_env()?;
    let rt = super::tokio_rt()?;
    let code = rt.block_on(run_async(client, args))?;
    std::process::exit(code);
}

async fn run_async(client: Client, args: Args) -> Result<i32> {
    let mut cmd_iter = args.command.into_iter();
    let cmd = cmd_iter
        .next()
        .ok_or_else(|| anyhow::anyhow!("missing command"))?;
    let rest: Vec<String> = cmd_iter.collect();

    let sandbox = client.get_sandbox(&args.sandbox_id)?;
    let transport = client.transport(&args.sandbox_id, sandbox.envd_access_token.as_deref())?;
    let req = build_start_request(StartOpts {
        cmd: &cmd,
        args: rest,
        envs: Default::default(),
        pty: None,
        stdin: false,
    });

    let stream = transport
        .server_stream::<_, StartResponse>("Start", req)
        .await?;
    drain_output(stream).await
}
