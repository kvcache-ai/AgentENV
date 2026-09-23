# Use Codex with an AgentENV Sandbox

Run `aenv codex start ./my-project` to open the native Codex terminal on a
sandbox copy of your project. Chat with
Codex, follow its tool output, review changes with `/diff`, and resume the same
conversation later. Commands and file edits run in AgentENV; your original
project stays unchanged.

## Set Up Once

You need an AgentENV deployment and the `aenv` CLI configured with `aenv auth`.
See [Quick Start](../getting-started/quickstart.md) for deployment and login.
Run `aenv` on Linux with Git and Codex 0.155.1. It must be able to reach your
AgentENV deployment and model API. Node.js is needed if you install Codex with
npm:

```bash
npm install -g @openai/codex@0.155.1
codex login
aenv codex setup
```

Configure the model and authentication in Codex as usual. `aenv codex` uses
your `~/.codex` directory (or `CODEX_HOME`), including Codex's configured
credential store. For a custom Responses-compatible API, configure its provider
in Codex's `config.toml` and follow [Codex authentication](https://developers.openai.com/codex/auth/).
`aenv` does not ask for, copy, or save model API keys.

Setup saves only the template name in `~/.config/aenv/codex.json` and builds
the bundled `codex-executor` template if it does not exist;
subsequent sessions reuse it. The template files are included in `aenv`, so you
do not need a repository checkout or Python on your machine. First-time builds
use `aenv-buildctl`, installed alongside `aenv` by the CLI installer. Both the
local Codex and template use version 0.155.1.

Setup accepts `--template` and does not require a local Codex installation.
`start` and `resume` check the local Codex version; set `CODEX_BIN` if it is
installed elsewhere.

Codex 0.155.1 gives `environments.toml` precedence over the sandbox connection.
If that file exists in your Codex home, `aenv` stops before creating a sandbox;
use a separate `CODEX_HOME` without it and configure/login with Codex there.
Use the same Codex home when resuming a session. Older aenv model settings are
no longer used; rerunning `setup` removes them from `codex.json`.
For sessions created by the earlier draft with a private `session/codex-home`,
use `finish` to save the project, then `start` from that saved copy; their
conversation history is not automatically moved into your native Codex home.

## Work on Your Project

```bash
aenv codex start ~/projects/my-app
```

`aenv` creates a sandbox, uploads your project, and opens Codex's own
terminal interface. For example, ask it to fix a parser and run the tests,
then send a follow-up asking for another test case.

| In Codex | Action |
| --- | --- |
| Type a task or follow-up | Work on files in the same sandbox |
| `/diff` | Review the sandbox's Git diff |
| `/status` | Inspect the current Codex session |
| `/new` | Start a new conversation in the same sandbox |
| `/quit` | Leave Codex; `aenv` saves files and pauses the sandbox |

Input editing, tool output, scrolling, and turn interruption use Codex's native
controls. `aenv` starts Codex with automatic command approval inside the
sandbox. Model credentials stay on the machine running `aenv`.

Codex 0.155.1's `@` file picker still searches the CLI machine, even in remote
mode. To refer to sandbox files, type their relative paths in your message.
The model can read and edit those files normally.

## Continue or Finish

`aenv` prints the session directory at startup and again on exit. Use it
to restore both the sandbox and conversation:

```bash
aenv codex resume /path/to/session
```

When you are done, save one final copy and delete the sandbox:

```bash
aenv codex finish /path/to/session
```

`start` and `resume` require an interactive terminal and accept `--model`.
`start --template` selects another prepared template.

## Take Your Changes Back

By default, sessions are under `~/.local/share/aenv/codex/sessions/`.
`start --output` selects another parent directory outside your project.
Each save creates a new numbered directory:

```text
session/
  latest.txt                 # Path to the latest successful save
  saves/0001/
    project/                 # Updated project files
    changes.patch            # Changes against your uploaded files
```

Open the saved project directly, or review and apply the patch to your original
project:

```bash
cd ~/projects/my-app
git apply --check /path/to/session/saves/0001/changes.patch
git apply /path/to/session/saves/0001/changes.patch
```

For Git projects, upload includes working changes and untracked files that Git
does not ignore. Git history, dependency/cache directories, `.env` files, and
common private-key files are excluded. `.env.example` and `.env.sample` are
included. Plain directories are also supported. Relative symlinks must stay
within the project. Saves include tracked files and new files that Git does
not ignore; keep deliverables inside the project and outside ignored paths.

While `aenv` runs, it renews the sandbox's five-minute lease every
30 seconds. Exiting stops active work, saves a project copy, and pauses the
sandbox. If saving fails, the sandbox is retained and `aenv` prints the
commands to resume or recover it. `finish` retries the download before deleting;
`recover` is an alias for it. If `aenv` is killed, lease expiry auto-pauses
the sandbox. The reusable template remains available for new sessions.

Once `finish` saves the final copy, it records pending deletion before deleting
the sandbox. If deletion or its final local status update fails, rerun `finish`:
it keeps the saved copy and retries cleanup without reconnecting to the sandbox.
`resume` is unavailable once deletion has started.

If service shutdown cannot be confirmed, saving stops and the sandbox is
retained. This includes services from older drafts without a supervisor, or
services whose supervisor was forcibly killed. Stop any remaining service
processes inside that sandbox before retrying `finish`.
