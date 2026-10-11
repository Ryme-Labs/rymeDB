document$.subscribe(() => {
  if (!window.mermaid) return;
  window.mermaid.initialize({
    startOnLoad: false,
    securityLevel: "strict",
    theme: document.body.dataset.mdColorScheme === "slate" ? "dark" : "default",
  });
  window.mermaid.run({ querySelector: ".mermaid" });
});
