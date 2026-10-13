const config = {
  title: "rymeDB",
  tagline: "A Rust-first realtime database and backend platform",
  url: "https://db.rymelabs.dev",
  baseUrl: "/",
  trailingSlash: true,
  favicon: "/images/rymedb-transparent.png",
  organizationName: "Ryme-Labs",
  projectName: "rymeDB",
  deploymentBranch: "main",
  onBrokenLinks: "throw",
  markdown: {
    mermaid: true,
    hooks: {
      onBrokenMarkdownLinks: "throw",
    },
  },
  presets: [
    [
      "classic",
      {
        docs: {
          path: "docs",
          routeBasePath: "/",
          sidebarPath: "./sidebars.js",
        },
        blog: false,
        theme: {
          customCss: "./docs/stylesheets/extra.css",
        },
      },
    ],
  ],
  plugins: [
    [
      "@scalar/docusaurus",
      {
        label: "REST API",
        route: "/api",
        showNavLink: false,
        configuration: {
          url: "/openapi/rest.yaml",
        },
      },
    ],
  ],
  themes: ["@docusaurus/theme-mermaid"],
  themeConfig: {
    image: "/images/rymedb-banner.png",
    colorMode: {
      defaultMode: "light",
      respectPrefersColorScheme: true,
    },
    navbar: {
      title: "rymeDB",
      logo: {
        alt: "rymeDB",
        src: "/images/rymedb-transparent.png",
      },
      items: [
        {
          type: "docSidebar",
          sidebarId: "docsSidebar",
          position: "left",
          label: "Documentation",
        },
        {
          href: "https://db.rymelabs.dev/api/",
          label: "REST API",
          position: "left",
        },
        {
          href: "https://github.com/Ryme-Labs/rymeDB",
          label: "GitHub",
          position: "right",
        },
      ],
    },
    footer: {
      style: "dark",
      links: [
        {
          title: "Documentation",
          items: [
            { label: "Get started", to: "/setup/" },
            { label: "Configuration", to: "/configuration/" },
            { label: "Troubleshooting", to: "/troubleshooting/" },
          ],
        },
        {
          title: "Project",
          items: [
            { label: "GitHub", href: "https://github.com/Ryme-Labs/rymeDB" },
            { label: "Security reports", to: "/security/" },
          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} Ryme Labs`,
    },
    prism: {
      additionalLanguages: ["bash", "json", "rust", "sql", "toml", "yaml"],
    },
  },
};

module.exports = config;
