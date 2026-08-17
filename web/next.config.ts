import type { NextConfig } from "next";

const extraDevOrigins = (process.env.AENV_WEB_DEV_ORIGINS ?? "")
  .split(",")
  .map((origin) => origin.trim())
  .filter(Boolean);

const nextConfig: NextConfig = {
  output: "standalone",
  allowedDevOrigins: ["127.0.0.1", "[::1]", ...extraDevOrigins],
};

export default nextConfig;
