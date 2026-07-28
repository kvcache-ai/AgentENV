# OpenAPI source layout

This directory contains the source files for the AgentENV HTTP API
specification. `src/api/openapi.yml` is the committed, generated specification;
do not edit it directly. Update `openapi.tmpl` or the YAML fragments here, then
run code generation to refresh both the combined specification and the Rust
server code.

## Rendering

`cargo adev codegen server` performs the following steps:

1. Read `openapi.tmpl` and collect the tags from its top-level `tags` section.
2. Render the shared component fragments:
   - `components/security.yml`
   - `components/parameters.yml`
   - `components/responses.yml`
3. Render `components/schemas/common.yml` first.
4. For each tag, in the order declared by `openapi.tmpl`, render the matching
   `components/schemas/<tag>.yml` file when it exists.
5. Keep `/health` from `openapi.tmpl`, then render each matching
   `paths/<tag>.yml` file in tag order.
6. Write the combined document to the committed `src/api/openapi.yml`.
7. Run OpenAPI Generator and update `src/api/generated`.

Tag declarations may move within `openapi.tmpl`; only the order of entries
inside the `tags` section controls the rendered schema and path order.

A tag does not have to provide both a schema file and a path file. Missing
matching files are skipped. A schema or path file whose name does not match a
declared tag is rejected so that API definitions cannot be silently omitted.

## Fragment format

Each fragment includes the section key as its first line. The renderer removes
that line and indents the remaining body into the combined document.

For example, `paths/sandboxes.yml` starts with:

```yaml
paths:
/sandboxes:
  get:
    tags: [sandboxes]
```

Schema fragments use the same convention:

```yaml
schemas:
  Sandbox:
    type: object
```

Keep models shared by multiple tags, primitive aliases, and common error types
in `components/schemas/common.yml`. Put tag-owned models in
`components/schemas/<tag>.yml`.

## Adding or changing an API

For an endpoint under an existing tag:

1. Add the operation to `paths/<tag>.yml`.
2. Set the operation's `tags` field to the same tag.
3. Add tag-owned request or response models to
   `components/schemas/<tag>.yml`.
4. Add reusable models to `components/schemas/common.yml`.
5. Reuse shared parameters and responses with `$ref`; add new shared entries
   to `components/parameters.yml` or `components/responses.yml` when needed.
6. Run `make agentenv-server`.
7. Review changes under `src/api/generated` and run the relevant Rust tests.

For a new API category:

1. Add the tag to `openapi.tmpl` in the desired rendering order.
2. Create `paths/<tag>.yml`.
3. Create `components/schemas/<tag>.yml` if the tag owns any models.
4. Follow the existing-tag workflow above.

When moving definitions between fragments without changing their contents,
`src/api/generated` should remain unchanged.

## Commands

Render the specification and regenerate the Rust Axum server:

```bash
make agentenv-server
```

Build the documentation, rendering the temporary specification first:

```bash
make docs
```
