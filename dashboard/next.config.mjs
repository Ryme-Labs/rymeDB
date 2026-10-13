/** @type {import('next').NextConfig} */
const nextConfig = {
  output: "export",
  basePath: "/dashboard",
  assetPrefix: "/dashboard",
  reactStrictMode: true,
  typescript: { tsconfigPath: "tsconfig.app.json" },
};

export default nextConfig;
