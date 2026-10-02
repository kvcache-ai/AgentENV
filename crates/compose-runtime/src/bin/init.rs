#[tokio::main(flavor = "current_thread")]
async fn main() -> std::process::ExitCode {
    match aenv_compose_runtime::supervisor::run().await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error:#}");
            std::process::ExitCode::FAILURE
        }
    }
}
