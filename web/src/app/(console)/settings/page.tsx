export const metadata = {
  title: "Settings — AgentENV",
};

export default function SettingsPage() {
  return (
    <div className="max-w-xl space-y-2">
      <h1 className="text-2xl font-semibold tracking-tight">Settings</h1>
      <p className="text-sm text-muted-foreground">
        Gateway connection settings and session cookies land in the next PR in
        this stack.
      </p>
    </div>
  );
}
