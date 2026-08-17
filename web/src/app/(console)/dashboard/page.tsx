export const metadata = {
  title: "Dashboard — AgentENV",
};

export default function DashboardPage() {
  return (
    <div className="max-w-xl space-y-2">
      <h1 className="text-2xl font-semibold tracking-tight">Dashboard</h1>
      <p className="text-sm text-muted-foreground">
        Cluster overview panels land in a follow-up PR in this stack. Use
        Settings once connection support lands to point the console at a
        Gateway.
      </p>
    </div>
  );
}
