Each directory is a Compose project that `TestLoadMatchesDockerCompose` copies
to a temporary directory, renaming `dotenv` files to `.env`, and loads there
with the planner and with the installed `docker compose config --format json`.
`args` lists Docker Compose project flags, one per line, and `env` sets process
environment variables as `KEY=VALUE` lines.
