module.exports = {
  docsSidebar: [
    "index",
    {
      type: "category",
      label: "Start here",
      collapsed: false,
      items: [
        "setup",
        "configuration",
        "auth",
        "sql",
        "isolation",
      ],
    },
    {
      type: "category",
      label: "Build and operate",
      collapsed: false,
      items: [
        "production",
        "cluster-operations",
        "backup-restore",
        "streaming",
        "troubleshooting",
        "resp",
        "postgres",
        "migration",
        "performance",
      ],
    },
    {
      type: "category",
      label: "API and clients",
      collapsed: false,
      items: [
        {
          type: "link",
          label: "REST API reference",
          href: "https://db.rymelabs.dev/api/",
        },
        "api",
        "sdk-java",
        "sdk-js",
        "sdk-rust",
      ],
    },
    {
      type: "category",
      label: "Project",
      items: ["development", "security", "sdk-release"],
    },
  ],
};
