"use client";

import { SidebarInset, SidebarProvider, SidebarTrigger } from "@/components/ui/sidebar";
import { AppSidebar } from "@/components/app-sidebar";
import { Toaster } from "@/components/ui/sonner";

export function ConsoleShell({ children }: { children: React.ReactNode }) {
  return (
    <SidebarProvider>
      <AppSidebar />
      <SidebarInset>
        <header className="flex h-14 shrink-0 items-center gap-2 border-b border-border/70 px-4 md:px-6">
          {/* Desktop trigger is inside the sidebar; on mobile the sidebar is an
              off-canvas sheet, so it needs a trigger out here to be reachable. */}
          <SidebarTrigger className="-ml-1 md:hidden" />
          <span className="text-[0.8125rem] text-muted-foreground/65">
            Gateway-backed management console
          </span>
        </header>
        <div className="flex min-w-0 flex-1 flex-col gap-4 overflow-x-auto p-4 md:p-6">{children}</div>
      </SidebarInset>
      <Toaster richColors closeButton />
    </SidebarProvider>
  );
}
