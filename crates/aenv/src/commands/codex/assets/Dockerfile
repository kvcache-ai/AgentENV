FROM docker.io/library/node:22-bookworm-slim
ARG CODEX_VERSION=0.155.1

# Python is available for user projects; aenv's integration does not depend on it.
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl file git python3 ripgrep util-linux \
    && rm -rf /var/lib/apt/lists/* \
    && npm install --global @openai/codex@${CODEX_VERSION} \
    && npm cache clean --force \
    && useradd --create-home --shell /bin/bash --uid 1001 user \
    && mkdir -p /workspace /codex-home \
    && chown -R user:user /workspace /codex-home \
    && codex exec-server --help >/dev/null

ENV CODEX_HOME=/codex-home
ENV HOME=/home/user
WORKDIR /workspace
USER user
