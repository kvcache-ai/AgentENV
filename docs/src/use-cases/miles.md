# Train Terminal-Bench-2 with Miles

AgentENV integrates with [Miles](https://github.com/radixark/miles) as a
self-hosted sandbox backend for agentic reinforcement learning. Miles is a
high-performance reinforcement learning framework for large-scale model
post-training.

This example follows Miles' [OpenEnv](https://github.com/huggingface/OpenEnv)
recipe to train
[GLM-4.7-Flash](https://huggingface.co/zai-org/GLM-4.7-Flash) with GRPO on
[Terminal-Bench-2](https://github.com/laude-institute/terminal-bench-2).
OpenEnv provides the task interaction and evaluation interface used by the
recipe, while AgentENV runs the isolated sandbox for each episode.
Terminal-Bench-2 is a benchmark suite of terminal-based software tasks rather
than a conventional prompt-only dataset: each task includes an environment,
an instruction, and tests that determine whether the agent completed the task.

## 1. Deploy AgentENV

First deploy and authenticate with an AgentENV server by following the
[Quick Start](../getting-started/quickstart.md).

When starting AgentENV with `docker run`, publish ports 8000 and 80 and set
`AENV_SANDBOX_PROXY_DOMAINS` to an `sslip.io` domain for the AgentENV host:

```bash
docker run -d --name aenv --privileged -v /dev:/dev \
  -p 8000:8000 -p 80:8000 \
  -e AENV_SANDBOX_PROXY_DOMAINS=<ip-with-dashes>.sslip.io \
  ghcr.io/kvcache-ai/aenv-server:latest
```

Replace `<ip-with-dashes>` with the AgentENV host IP, replacing every dot with
a dash.

## 2. Point Miles at AgentENV

```bash
pip install e2b

export E2B_API_URL=http://<server>:8000
export E2B_SANDBOX_URL=http://<server>:8000

# Export your api key fetched in the last step.
export E2B_API_KEY=<your-api-key>

# Per-sandbox URLs use HTTPS by default. This deployment uses plain HTTP.
export OPENENV_E2B_URL_SCHEME=http
```

The API key authenticates requests but does not encrypt them; keep
this plain-HTTP setup on a trusted network.

## 3. Prepare Miles, OpenEnv, and Terminal-Bench-2

Run this tutorial inside an existing Miles training environment, as required
by the upstream recipe. Cloning Miles below provides the recipe scripts; it
does not install the CUDA, model-serving, or training stack.

```bash
git clone https://github.com/radixark/miles.git
git clone https://github.com/huggingface/OpenEnv.git
git clone --depth 1 https://github.com/laude-institute/terminal-bench-2.git

pip install -e ./OpenEnv/envs/tbench2_env

python ./miles/examples/experimental/openenv/make_tbench2_data.py \
  --tasks_dir ./terminal-bench-2 \
  --output /root/tbench2_train.jsonl
```

Add `--n 8` to `make_tbench2_data.py` for a small smoke subset.

## 4. Train

```bash
export OPENENV_TB2_TASKS_DIR="$(realpath ./terminal-bench-2)"
OPENENV_SANDBOX_BACKEND=e2b \
  python ./miles/examples/experimental/openenv/run-openenv-tbench2.py
```

The launcher creates one AgentENV microVM per
episode, and returns the task's canonical test result as the GRPO reward.
`e2b` uses the E2B-compatible backend pointed at the AgentENV endpoints.

For the complete training configuration, provider options, and operational
notes, see the upstream
[Miles OpenEnv Terminal-Bench-2 recipe](https://github.com/radixark/miles/blob/main/examples/experimental/openenv/README.md).
