# Manage Templates

This page provides the basic commands for managing a template.

## List templates

```bash
aenv template list        # alias: aenv template ls
aenv template list --output json
```

Displays all templates with their ID, name, build status, CPU, memory, disk
size, and last-updated timestamp. `--output` accepts `table` or `json`. It
defaults to `table` in an interactive terminal and `json` when output is piped
or redirected.

## List template builds

List the complete build history for one template:

```bash
curl -H 'X-API-Key: test-key' \
  http://127.0.0.1:8000/templates/<template-id>
```

## Check a template alias

Check whether an alias exists and resolve it to a template ID:

```bash
curl -H 'X-API-Key: test-key' \
  http://127.0.0.1:8000/templates/aliases/<alias>
```

## Delete a template

```bash
aenv template delete <template-id-or-name>   # alias: aenv template rm
```

