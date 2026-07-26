.. raw:: html

   <div align="center">
     <picture>
       <source media="(prefers-color-scheme: dark)" srcset="assets/heading-logo-dark.svg" />
       <img src="assets/heading-logo.svg" alt="AgentENV" />
     </picture>
     <p><strong>Running agent environments at scale</strong></p>
   </div>

|coverage-status| |coverage-report| 📖 `Full documentation <https://kvcache-ai.github.io/AgentENV/>`_ 

AgentENV (AENV) is a distributed system for running agent environments at scale. Its components power agentic RL training for **Kimi K3** and an unreleased model.

----

🚀 Why AgentENV
----------------------
- **Scale across diverse environments**: AENV runs massive numbers of Firecracker environments across machines and diverse OCI-compatible images, loaded on demand via overlaybd. Local disk acts as a bounded cache, retaining hot data and evicting cold, so images can exceed disk capacity while startup stays fast cluster-wide, without pre-warming every host.
- **Make idle environments inexpensive**: Snapshot-backed environments boot or resume in under 50 ms and pause in under 100 ms. Idle environments can quickly release CPU and memory, then return when new work arrives.
- **Native snapshot and fork support**: AENV snapshots memory and filesystem changes incrementally, completing in under 100 ms even under heavy disk modification. A running environment can fork into multiple independent sandboxes for parallel agent workflows. Snapshots persist to S3-compatible object storage or a shared distributed filesystem to prevent data loss.
- **Preserve performance and density over time**: AENV delivers high-performance I/O via ublk while sharing the host page cache across storage and memory-snapshot data. Memory ballooning returns reclaimable guest memory to the host, sustaining high overcommit as environments run longer and diverge.

----

📋 Prerequisites
----------------------

- **Linux kernel 6.8+**; the install script additionally requires **Ubuntu 24.04** (see *Quick Start* below for installation options)
- ``/dev/kvm`` access for Firecracker microVM execution

----

⚡ Quick Start (Single Node)
----------------------

**1. Install and start the server**

*Option A — install script (Ubuntu 24.04)*

Install both the server and the ``aenv`` CLI, then start the server as a
systemd service:

.. code-block:: bash

   curl -fsSL https://raw.githubusercontent.com/kvcache-ai/AgentENV/main/scripts/install.sh | sudo bash
   sudo systemctl start aenv

*Option B — Docker*

Set up the server:

.. code-block:: bash

   curl -fsSL https://raw.githubusercontent.com/kvcache-ai/AgentENV/main/scripts/docker-setup.sh | sudo bash
   docker pull ghcr.io/kvcache-ai/aenv-server:latest
   docker run -d --privileged -v /dev:/dev -p 8000:8000 ghcr.io/kvcache-ai/aenv-server:latest

The server is accessible at ``http://127.0.0.1:8000`` by default. 

**2. Install the aenv CLI** *(skip if you used Option A in step 1)*

Install separately if you used the Docker method above, or if you are running
the CLI on a different machine from the server.  Supports Linux and macOS on
x86\_64 and arm64:

.. code-block:: bash

   curl -fsSL https://raw.githubusercontent.com/kvcache-ai/AgentENV/main/scripts/install-cli.sh | bash

**3. Authenticate**

.. code-block:: bash

   aenv auth
   # AENV server URL [http://localhost:8000]: http://127.0.0.1:8000
   # API key: dummy

**4. Pull a template and run a sandbox**

.. code-block:: bash

   aenv pull ubuntu:22.04 --name ubuntu
   aenv start ubuntu            # starts a sandbox and attaches an interactive shell

----


🗂 Deployment
----------------------

For Docker Compose / Kubernetes cluster deployment and build-from-source instructions,

see 📖 `Deployment <https://kvcache-ai.github.io/AgentENV/deployment/manual-compile.html>`_ .

----

🔌 E2B SDK compatibility
----------------------

AgentENV exposes an E2B-compatible HTTP API. Point ``E2B_API_URL`` at your
server and use the standard E2B Python / TypeScript SDK without any code
changes. See 📖 `E2B SDK integration <https://kvcache-ai.github.io/AgentENV/integration/e2b-sdk.html>`_ 
for setup details.

----

🛠 aenv CLI reference
----------------------

.. code-block:: bash

   # Templates
   aenv pull docker.io/library/ubuntu:latest --name ubuntu    # FROM <image> → template
   aenv template list                      # alias: aenv template ls

   # Sandboxes
   aenv start ubuntu                       # start + attach interactive shell
   aenv start ubuntu --detach              # start, print sandbox ID, don't attach
   aenv cn <sandbox-id>                    # reattach a shell
   aenv exec <sandbox-id> ls -la /         # one-shot command
   aenv ls                               

   aenv pause   <sandbox-id>
   aenv resume  <sandbox-id>
   aenv timeout <sandbox-id> 600           # extend TTL to 600 s from now
   aenv delete  <sandbox-id>               # alias: aenv rm

``aenv start`` accepts a template UUID or human-readable name/alias. ``aenv list``
outputs a table on TTY and JSON when piped; override with ``--output table|json``.

----

📑 Research Background and Citation
--------------------------------------

AgentENV builds on and integrates some of the ideas and motivation behind our
TrEnv-X research. If you find these ideas useful, please cite the following
paper:

.. code-block:: bibtex

   @article{huang2026trenvx,
     author    = {Huang, Jialiang and Ma, Teng and Liu, Zheng and Lin, Sixing and Chen, Kang and Jiang, Jinlei and Liao, Xia and Shan, Yingdi and Wu, Yongwei and Zhang, Ning and Lu, Mengting and Ma, Tao and Gong, Haifeng and Zhang, Mingxing},
     title     = {TrEnv-X: Transparently Share Serverless Execution Environments Across Different Functions and Nodes},
     year      = {2026},
     publisher = {Association for Computing Machinery},
     address   = {New York, NY, USA},
     issn      = {0734-2071},
     url       = {https://doi.org/10.1145/3805475},
     doi       = {10.1145/3805475},
     note      = {Just Accepted},
     journal   = {ACM Trans. Comput. Syst.},
     month     = mar,
     keywords  = {Serverless, Agent, Sandbox, CXL, Cold Start}
   }

.. raw:: html

   <details>
   <summary>Earlier work: TrEnv</summary>

.. code-block:: bibtex

   @inproceedings{huang2024trenv,
     author    = {Huang, Jialiang and Zhang, MingXing and Ma, Teng and Liu, Zheng and Lin, Sixing and Chen, Kang and Jiang, Jinlei and Liao, Xia and Shan, Yingdi and Zhang, Ning and Lu, Mengting and Ma, Tao and Gong, Haifeng and Wu, YongWei},
     title     = {TrEnv: Transparently Share Serverless Execution Environments Across Different Functions and Nodes},
     year      = {2024},
     isbn      = {9798400712517},
     publisher = {Association for Computing Machinery},
     address   = {New York, NY, USA},
     url       = {https://doi.org/10.1145/3694715.3695967},
     doi       = {10.1145/3694715.3695967},
     booktitle = {Proceedings of the ACM SIGOPS 30th Symposium on Operating Systems Principles},
     pages     = {421–437},
     numpages  = {17},
     keywords  = {serverless, cold start, CXL, remote memory},
     location  = {Austin, TX, USA},
     series    = {SOSP '24}
   }

.. raw:: html

   </details>

.. _Latest coverage metadata: https://github.com/kvcache-ai/AgentENV/blob/coverage-data/coverage/coverage.json
.. _Coverage workflow history: https://github.com/kvcache-ai/AgentENV/actions/workflows/coverage.yml

.. |coverage-status| image:: https://github.com/kvcache-ai/AgentENV/actions/workflows/coverage.yml/badge.svg?branch=main&event=push
   :target: https://github.com/kvcache-ai/AgentENV/actions/workflows/coverage.yml
   :alt: Coverage workflow status

.. |coverage-report| image:: https://github.com/kvcache-ai/AgentENV/blob/coverage-data/coverage/badge.svg?raw=1
   :target: https://github.com/kvcache-ai/AgentENV/blob/coverage-data/coverage/coverage.json
   :alt: Latest coverage report
