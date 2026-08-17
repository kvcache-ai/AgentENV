# AgentENV Web UI (`web/`)

Control-plane console for [Issue #6](https://github.com/kvcache-ai/AgentENV/issues/6).

## Stack

- Next.js App Router + TypeScript
- Tailwind CSS v4 + shadcn/ui
- pnpm

## Develop

```bash
cd web
pnpm install
pnpm dev
```

Open http://localhost:3000 — configure Gateway (default Compose `:8080`) under **Settings**.

## Conventions

- Talk to **Gateway HTTP only** (never Scheduler gRPC).
- Credentials live in **httpOnly cookies** (`src/lib/session.ts`); never log full secrets.
- Upstream calls: `src/lib/api/client.ts` (`gatewayFetch`).
- Feature routes live under `src/app/(console)/`.
- Use existing shadcn components in `src/components/ui/`.

## Non-goals (v1)

No browser terminal, filesystem browser, or in-sandbox process execution.
