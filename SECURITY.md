# Security Policy

AgentENV runs Firecracker microVMs and manages privileged Linux resources,
including KVM, network namespaces, ublk devices, filesystems, container images,
and snapshot artifacts. Please report suspected security vulnerabilities
privately and give the maintainers an opportunity to investigate before public
disclosure.

## Reporting a vulnerability

Do **not** open a public GitHub issue, discussion, or pull request for a
suspected vulnerability.

Report vulnerabilities through
[GitHub Security Advisories](https://github.com/kvcache-ai/AgentENV/security/advisories/new).
This creates a private discussion with the maintainers and allows a coordinated
fix to be prepared.

Include as much of the following information as possible:

- the affected AgentENV release or exact commit;
- the affected component and deployment topology;
- required host, guest, registry, or API access;
- prerequisites and a minimal reproduction;
- expected and observed security impact;
- relevant logs, stack traces, or proof-of-concept code;
- whether the issue is known to be actively exploited;
- suggested mitigations or fixes, if available; and
- your preferred name and attribution for any eventual advisory.

Do not include production credentials, tokens, private keys, customer data, or
other secrets. Use synthetic test data and redact sensitive logs.

If GitHub Security Advisories cannot be used, open a public issue containing no
vulnerability details and ask a maintainer to establish a private reporting
channel.

## What to expect

Maintainers will use the private advisory to:

1. acknowledge and triage the report;
2. request additional reproduction details when necessary;
3. assess affected versions and possible mitigations;
4. develop and validate a fix; and
5. coordinate release and public disclosure with the reporter.

Response and remediation time depend on severity, reproducibility, affected
components, and maintainer availability. Please do not disclose the issue
publicly until a fix or mitigation is available and disclosure timing has been
coordinated.

## Supported versions

Security fixes are developed against the `main` branch and, when applicable,
the latest published release. Older commits, development branches, forks, and
locally modified builds are not guaranteed to receive security fixes.

When reporting a problem on an older version, first determine whether it is
also reproducible on the latest release or `main`.

## Scope

Examples of issues that should be reported privately include:

- sandbox escape or unintended host access;
- cross-sandbox data, memory, storage, network, or page-cache exposure;
- unauthorized access to sandbox, snapshot, template, or node operations;
- snapshot or image integrity and provenance bypasses;
- unsafe handling of registry, object-store, P2P, or custom-extension
  credentials;
- path traversal, command injection, or unsafe file permissions;
- privilege escalation involving KVM, ublk, network namespaces, mounts, or
  helper processes;
- secrets exposed through APIs, logs, generated configuration, MMDS, or
  persisted state;
- denial of service with a clear security impact; and
- vulnerabilities in AgentENV's use or configuration of a dependency.

General installation problems, performance regressions, feature requests, and
bugs without a security impact should use the public issue templates.

Vulnerabilities that affect an upstream dependency independently of AgentENV
should normally be reported to that upstream project. If AgentENV makes the
issue exploitable, configures the dependency unsafely, or requires a
project-specific mitigation, report it to AgentENV as well.

## Deployment security

AgentENV does not currently provide built-in API authorization. Do not expose
the AgentENV API directly to an untrusted or public network. Deploy it only on a
trusted network or behind an authentication and authorization proxy with
appropriate transport security and network controls.

Treat access to the AgentENV API, host runtime user, configuration, local state,
registry credentials, object-store credentials, snapshot repository, P2P
transport, and custom-extension service as security-sensitive.

## Security research guidelines

When investigating a possible vulnerability:

- test only systems and data that you own or are explicitly authorized to use;
- avoid privacy violations, data destruction, persistence, and disruption of
  shared services;
- use the minimum access and impact required to demonstrate the issue;
- stop testing and report immediately if you encounter real user data or
  credentials; and
- keep vulnerability details private while remediation is being coordinated.

The project cannot authorize testing against infrastructure operated by third
parties, including cloud providers, registries, or deployments owned by other
AgentENV users.
