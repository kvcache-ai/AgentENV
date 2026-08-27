# Run Python Code

This quickstart imports the official Python image as an AgentENV template,
starts a sandbox, and executes Python code inside it. The Python program is
included directly in the command; no sample project or source file is needed.

Before starting, deploy AgentENV and configure the `aenv` CLI as described in
[Quick Start](../getting-started/quickstart.md).

## 1. Create a Python Template

Import the official Python 3.12 image and wait for the template to become
ready:

```bash
aenv pull python:3.12-slim --name python
```

## 2. Start a Sandbox

Start the template in detached mode and capture the generated sandbox ID:

```bash
SANDBOX_ID="$(aenv start python -d)"
```

## 3. Execute Python

Run this program inside the sandbox:

```bash
aenv exec "$SANDBOX_ID" python -c '
from statistics import mean

temperatures = [21.5, 23.0, 22.4, 24.1]
print(f"average temperature: {mean(temperatures):.2f} C")
'
```

Expected output:

```text
average temperature: 22.75 C
```

## 4. Delete the Sandbox and the Template

```bash
aenv delete "$SANDBOX_ID"
aenv template delete python
```