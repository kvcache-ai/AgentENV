"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import {
  BoxIcon,
  CameraIcon,
  LayoutDashboardIcon,
  ServerIcon,
  SettingsIcon,
  LayersIcon,
} from "lucide-react";
import {
  Sidebar,
  SidebarContent,
  SidebarGroup,
  SidebarGroupLabel,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarRail,
  SidebarTrigger,
} from "@/components/ui/sidebar";

const NAV = [
  { href: "/dashboard", label: "Dashboard", icon: LayoutDashboardIcon },
  { href: "/nodes", label: "Nodes", icon: ServerIcon },
  { href: "/sandboxes", label: "Sandboxes", icon: BoxIcon },
  { href: "/snapshots", label: "Snapshots", icon: CameraIcon },
  { href: "/templates", label: "Templates", icon: LayersIcon },
  { href: "/settings", label: "Settings", icon: SettingsIcon },
] as const;

export function AppSidebar() {
  const pathname = usePathname();

  return (
    <Sidebar collapsible="icon">
      <SidebarHeader className="h-14 shrink-0 justify-center border-b border-sidebar-border px-3 group-data-[collapsible=icon]:px-0">
        <div className="flex items-center gap-2 group-data-[collapsible=icon]:justify-center">
          <Link
            href="/dashboard"
            className="flex min-w-0 flex-1 flex-col gap-px group-data-[collapsible=icon]:hidden"
          >
            <span className="label-micro text-sidebar-foreground/55">
              AgentENV
            </span>
            <span className="truncate text-[0.9375rem] leading-tight font-semibold tracking-[-0.02em]">
              Control Plane
            </span>
          </Link>
          {/* Desktop trigger lives inside the sidebar; the mobile sidebar is a
              sheet, so its trigger has to stay in the top bar. */}
          <SidebarTrigger className="hidden shrink-0 text-sidebar-foreground/60 hover:text-sidebar-foreground md:inline-flex" />
        </div>
      </SidebarHeader>
      <SidebarContent>
        <SidebarGroup className="gap-1 px-2 py-3">
          <SidebarGroupLabel className="label-micro h-auto px-2 pb-1 text-sidebar-foreground/45">
            Manage
          </SidebarGroupLabel>
          <SidebarMenu className="gap-0.5">
            {NAV.map((item) => {
              const active =
                pathname === item.href || pathname.startsWith(`${item.href}/`);
              return (
                <SidebarMenuItem key={item.href}>
                  <SidebarMenuButton
                    isActive={active}
                    render={<Link href={item.href} />}
                    tooltip={item.label}
                    className="relative h-9 text-[0.8125rem] font-medium tracking-[-0.005em] text-sidebar-foreground/70 transition-colors data-active:text-sidebar-accent-foreground before:absolute before:top-1/2 before:left-0 before:h-4 before:w-0.5 before:-translate-y-1/2 before:rounded-r-full before:bg-sidebar-primary before:opacity-0 before:transition-opacity data-active:before:opacity-100"
                  >
                    <item.icon />
                    <span>{item.label}</span>
                  </SidebarMenuButton>
                </SidebarMenuItem>
              );
            })}
          </SidebarMenu>
        </SidebarGroup>
      </SidebarContent>
      <SidebarRail />
    </Sidebar>
  );
}
