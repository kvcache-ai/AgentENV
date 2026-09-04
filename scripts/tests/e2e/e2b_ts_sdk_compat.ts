#!/usr/bin/env npx tsx
/**
 * E2B TypeScript SDK compatibility checks for AgentENV.
 */
import { Sandbox, Template, type BuildInfo } from "e2b";

function log(message: string): void {
  console.log(`[e2b-ts-sdk] ${message}`);
}

function check(condition: boolean, message: string): void {
  if (!condition) {
    throw new Error(message);
  }
}

async function retry<T>(
  action: () => Promise<T>,
  description: string,
  attempts = 10,
  delayMs = 1000,
): Promise<T> {
  let lastError: unknown;
  for (let attempt = 1; attempt <= attempts; attempt++) {
    try {
      return await action();
    } catch (error) {
      lastError = error;
      log(`${description} failed on attempt ${attempt}/${attempts}: ${error}`);
      if (attempt < attempts) {
        await new Promise((resolve) => setTimeout(resolve, delayMs));
      }
    }
  }
  throw new Error(`${description} failed after ${attempts} attempts: ${lastError}`);
}

async function main(): Promise<void> {
  const templateName =
    process.env.E2B_COMPAT_TEMPLATE_NAME ?? `e2b-ts-sdk-${Date.now()}`;
  const publicTemplate = (process.env.AENV_TEMPLATE_ID ?? "").trim();
  const derivedTemplateName = `${templateName}-from-template`;
  const baseImage = process.env.E2B_COMPAT_USER_IMAGE ?? "ghcr.io/linuxserver/baseimage-ubuntu:noble";
  const workdir = `/tmp/${templateName}`;
  const derivedWorkdir = `/tmp/${derivedTemplateName}`;
  const buildMarker = `sdk-build-marker-${Date.now()}`;
  const derivedMarker = `sdk-from-template-marker-${Date.now()}`;
  const startupMarker = `sdk-startup-marker-${Date.now()}`;
  const apiUrl = process.env.E2B_API_URL;
  const sandboxUrl = process.env.E2B_SANDBOX_URL;
  const apiKey = process.env.E2B_API_KEY;
  if (!apiUrl) throw new Error("E2B_API_URL is not set");
  if (!sandboxUrl) throw new Error("E2B_SANDBOX_URL is not set");
  if (!apiKey) throw new Error("E2B_API_KEY is not set");
  const connOpts = { apiUrl, apiKey, sandboxUrl };

  let buildInfo: BuildInfo | null = null;
  let derivedBuildInfo: BuildInfo | null = null;
  let sandbox: Sandbox | null = null;
  let derivedSandbox: Sandbox | null = null;

  try {
    log(`building template ${templateName} from ${baseImage}`);
    const template = Template()
      .fromImage(baseImage)
      .runCmd(`mkdir -p ${workdir}`)
      .setWorkdir(workdir)
      .setEnvs({ AENV_E2B_SDK_MARKER: buildMarker })
      .runCmd(`printf '%s' "$AENV_E2B_SDK_MARKER" > marker.txt`)
      .runCmd("pwd > workdir.txt")
      .setEnvs({ AENV_E2B_STARTUP_MARKER: startupMarker })
      .setStartCmd(
        `printf '%s' "$AENV_E2B_STARTUP_MARKER" > startup-ready.txt; ` +
          `exec -a "agentenv-startup-$AENV_E2B_STARTUP_MARKER" sleep 1000000`,
        `test -f startup-ready.txt && ` +
          `grep -qx "$AENV_E2B_STARTUP_MARKER" startup-ready.txt`,
      );

    buildInfo = await Template.build(template, templateName, {
      cpuCount: 1,
      memoryMB: 128,
      skipCache: true,
      ...connOpts,
    });

    check(!!buildInfo.templateId, "template build returned an empty templateId");
    check(!!buildInfo.buildId, "template build returned an empty buildId");
    check(buildInfo.name === templateName, "template build returned the wrong name");
    log(`template ready: templateId=${buildInfo.templateId} buildId=${buildInfo.buildId}`);

    log("creating sandbox from SDK-built template");
    sandbox = await Sandbox.create(templateName, {
      metadata: {
        suite: "e2b-ts-sdk",
        template: templateName,
      },
      timeoutMs: 90_000,
      secure: true,
      ...connOpts,
    });
    check(!!sandbox.sandboxId, "sandbox create returned an empty sandboxId");
    log(`sandbox created: ${sandbox.sandboxId}`);

    const listed = await Sandbox.list({ ...connOpts, limit: 20, requestTimeoutMs: 60_000}).nextItems();
    const listedIds = new Set(listed.map((item) => item.sandboxId));
    check(listedIds.has(sandbox.sandboxId), "Sandbox.list did not include created sandbox");
    log("sandbox list includes SDK-created sandbox");
    
    const result = await retry(
      () =>
        sandbox!.commands.run(
          `pid_line=$(pgrep -af '[a]gentenv-startup-${startupMarker}' | head -1); ` +
            `test -n "$pid_line"; ` +
            `printf 'marker=' && cat marker.txt && ` +
            `printf '\\nworkdir=' && cat workdir.txt && ` +
            `printf '\\nstartup=' && cat startup-ready.txt && ` +
            `printf '\\nprocess=%s' "$pid_line"`,
          {
            cwd: workdir,
            timeoutMs: 30_000,
          }
      ),
      "command execution",
    );
    check(result.exitCode === 0, `command exited with ${result.exitCode}`);
    check(result.stdout.includes(`marker=${buildMarker}`), "build marker file did not match");
    check(result.stdout.includes(`workdir=${workdir}`), "WORKDIR build step was not preserved");
    check(
      result.stdout.includes(`startup=${startupMarker}`),
      "startup ready marker file did not match",
    );
    check(
      result.stdout.includes(`agentenv-startup-${startupMarker}`),
      "startCmd process was not preserved in the template snapshot",
    );
    log("command execution returned expected build artifacts and startup state");

    if ((process.env.E2B_COMPAT_TEST_PAUSE ?? "1") !== "0") {
      log("pausing and reconnecting sandbox through SDK lifecycle APIs");
      await Sandbox.pause(sandbox.sandboxId, connOpts);
      sandbox = await Sandbox.connect(sandbox.sandboxId, {
        timeoutMs: 90_000,
        ...connOpts,
      });
      const resumed = await retry(
        () => sandbox!.commands.run("printf resumed", { timeoutMs: 30_000 }),
        "command execution after reconnect",
      );
      check(resumed.stdout === "resumed", "sandbox did not run commands after reconnect");
      log("sandbox reconnect succeeded");
    }

    await sandbox.kill();
    sandbox = null;
    log("sandbox killed");

    if (publicTemplate) {
      log(`building template ${derivedTemplateName} from template ${publicTemplate}`);
      const derivedTemplate = Template()
        .fromTemplate(publicTemplate)
        .runCmd(`mkdir -p ${derivedWorkdir}`)
        .setWorkdir(derivedWorkdir)
        .setEnvs({ AENV_E2B_SDK_FROM_TEMPLATE_MARKER: derivedMarker })
        .runCmd(`printf '%s' "$AENV_E2B_SDK_FROM_TEMPLATE_MARKER" > marker.txt`)
        .runCmd("pwd > workdir.txt");

      derivedBuildInfo = await Template.build(derivedTemplate, derivedTemplateName, {
        cpuCount: 1,
        memoryMB: 128,
        skipCache: true,
        ...connOpts,
      });

      check(!!derivedBuildInfo.templateId, "from_template build returned an empty templateId");
      check(!!derivedBuildInfo.buildId, "from_template build returned an empty buildId");
      check(
        derivedBuildInfo.name === derivedTemplateName,
        "from_template build returned the wrong name",
      );
      log(
        `from_template template ready: templateId=${derivedBuildInfo.templateId} ` +
          `buildId=${derivedBuildInfo.buildId}`,
      );

      log("creating sandbox from SDK from_template build");
      derivedSandbox = await Sandbox.create(derivedTemplateName, {
        timeoutMs: 90_000,
        metadata: {
          suite: "e2b-ts-sdk",
          template: derivedTemplateName,
          baseTemplate: publicTemplate,
        },
        secure: true,
        ...connOpts,
      });
      check(!!derivedSandbox.sandboxId, "from_template sandbox create returned an empty sandboxId");
      log(`from_template sandbox created: ${derivedSandbox.sandboxId}`);

      const derivedResult = await retry(
        () =>
          derivedSandbox!.commands.run(
            "printf 'marker=' && cat marker.txt && printf '\\nworkdir=' && cat workdir.txt",
            { cwd: derivedWorkdir, timeoutMs: 30_000 },
          ),
        "from_template command execution",
      );
      check(
        derivedResult.exitCode === 0,
        `from_template command exited with ${derivedResult.exitCode}`,
      );
      check(
        derivedResult.stdout.includes(`marker=${derivedMarker}`),
        "from_template marker file did not match",
      );
      check(
        derivedResult.stdout.includes(`workdir=${derivedWorkdir}`),
        "from_template WORKDIR build step was not preserved",
      );
      log("from_template command execution returned expected build artifacts");

      await derivedSandbox.kill();
      derivedSandbox = null;
      log("from_template sandbox killed");
    } else {
      log("AENV_TEMPLATE_ID is not set; skipping from_template compatibility check");
    }
  } finally {
    if (derivedSandbox !== null) {
      try {
        await derivedSandbox.kill();
      } catch (error) {
        log(`cleanup from_template sandbox kill failed: ${error}`);
      }
    }

    if (sandbox !== null) {
      try {
        await sandbox.kill();
      } catch (error) {
        log(`cleanup sandbox kill failed: ${error}`);
      }
    }

    if (derivedBuildInfo !== null) {
      try {
        await Sandbox.deleteSnapshot(derivedBuildInfo.templateId, connOpts);
      } catch (error) {
        log(`cleanup from_template template delete failed: ${error}`);
      }
    }

    if (buildInfo !== null) {
      try {
        await Sandbox.deleteSnapshot(buildInfo.templateId, connOpts);
      } catch (error) {
        log(`cleanup template delete failed: ${error}`);
      }
    }
  }
}

main().catch((error) => {
  log(`failed: ${error}`);
  process.exit(1);
});
