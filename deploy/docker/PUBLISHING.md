# DHI service images

[🤖] The runtime, gateway and scheduler use digest-pinned Docker Hardened Images.
CI and `.github/scripts/build-od5.sh` check their Dockerfiles before building:

```sh
python3 .github/scripts/check_dhi.py
```

Each external `FROM` must name a literal, digest-pinned `dhi.io` image. Stages may
reuse earlier stages. External stage imports and variable roots are rejected.
The check deliberately supports only the simple syntax used by these Dockerfiles.

CI needs `DHI_USERNAME` and `DHI_PASSWORD` repository secrets for a Docker login
with DHI pull access. Local builds use the user's Docker login to `dhi.io`.
The existing `OCIR_USERNAME` and `OCIR_PASSWORD` still handle publication.

The runtime uses Debian 13's development base because setup needs a shell and
package manager. Gateway and scheduler use DHI's minimal static runtime. This
covers the three service images, not the images launched inside sandboxes or
runtime assets downloaded during setup. See the separate
[workspace-image migration](https://github.com/fl2024008/prometheus/pull/23317).

Registry publishing permissions are unchanged. Preventing monorepo CI from
pushing these images requires a separate identity and narrowing its existing
[OCIR write policy](https://github.com/fl2024008/prometheus/blob/main/devops/terraform/research-core/github_actions_oidc.tf#L107).
