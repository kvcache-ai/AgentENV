use runtime_policy::{
    unix_millis,
    watchdog::{self, Watchdog},
    ExecutionLease,
};
use std::{
    io::{self, Write},
    process::Command,
    time::SystemTime,
};
use uuid::Uuid;
fn main() -> anyhow::Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.get(1).map(String::as_str) == Some(watchdog::MODE) {
        return watchdog::run(args[2].parse()?);
    }
    let mut target = Command::new("sleep").arg("60").spawn()?;
    let lease = ExecutionLease {
        activation_id: Uuid::now_v7(),
        operation_id: Uuid::now_v7(),
        sequence: 0,
        expires_at_unix_ms: unix_millis(SystemTime::now())? + 700,
    };
    let _watchdog = Watchdog::start(&std::env::current_exe()?, target.id() as i32, lease)?;
    println!("{}", target.id());
    io::stdout().flush()?;
    target.wait()?;
    Ok(())
}
