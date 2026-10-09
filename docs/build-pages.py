import argparse
import json
import shutil
from pathlib import Path

import yaml
import markdown

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"
SPEC = ROOT / "schemas" / "openapi" / "rest.yaml"
README = ROOT / "README.md"
IMAGES = ROOT / "images"
REPO = "https://github.com/Ryme-Labs/rymeDB"
SDK_DIRS = (("java", "Java SDK"), ("js", "npm SDK"), ("rust", "Rust SDK"))
SUPPORTED_SDK_DIRS = {slug for slug, _ in SDK_DIRS}

CSS = """
:root { color-scheme: light dark; }
* { box-sizing: border-box; }
body { font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; margin: 0; line-height: 1.6; }
header { padding: 12px 24px; border-bottom: 1px solid #8884; display: flex; gap: 16px; align-items: center; flex-wrap: wrap; }
header strong { font-size: 18px; }
header nav { display: flex; gap: 12px; margin-left: auto; }
.layout { display: flex; max-width: 1100px; margin: 0 auto; }
aside { width: 240px; flex-shrink: 0; padding: 24px 16px; border-right: 1px solid #8883; }
aside ul { list-style: none; padding: 0; margin: 0; }
aside li { margin: 4px 0; }
aside a { text-decoration: none; }
aside a.active { font-weight: 700; }
main { padding: 24px 32px; max-width: 860px; min-width: 0; }
pre { overflow: auto; padding: 12px 16px; border: 1px solid #8884; border-radius: 8px; }
code { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }
table { border-collapse: collapse; width: 100%; overflow-x: auto; display: block; }
th, td { border: 1px solid #8884; padding: 6px 10px; text-align: left; }
img { max-width: 100%; }
.cards { display: grid; grid-template-columns: repeat(auto-fill, minmax(220px, 1fr)); gap: 12px; }
.card { border: 1px solid #8884; border-radius: 10px; padding: 12px 16px; }
.card a { font-weight: 600; text-decoration: none; }
@media (max-width: 800px) { .layout { flex-direction: column; } aside { width: auto; border-right: 0; border-bottom: 1px solid #8883; } }
"""

MD_EXTENSIONS = ["extra", "toc", "tables", "fenced_code", "sane_lists"]


def render_md(text):
    return markdown.markdown(text, extensions=MD_EXTENSIONS)


def title_of(path, fallback):
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped.startswith("# "):
            return stripped[2:].strip()
    return fallback


def page(title, body, nav, root_prefix="."):
    return (
        "<!doctype html>\n<html lang=\"en\">\n<head>\n<meta charset=\"utf-8\">\n"
        "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n"
        "<title>" + title + " - rymeDB docs</title>\n"
        "<style>" + CSS + "</style>\n</head>\n<body>\n"
        "<header><strong>rymeDB docs</strong><nav>"
        "<a href=\"" + root_prefix + "/index.html\">Home</a>"
        "<a href=\"" + root_prefix + "/api/index.html\">API</a>"
        "<a href=\"" + REPO + "\">GitHub</a>"
        "</nav></header>\n"
        "<div class=\"layout\"><aside><ul>" + nav + "</ul></aside>"
        "<main>" + body + "</main></div>\n</body>\n</html>\n"
    )


def verify_sdk_scope():
    sdk_root = ROOT / "sdks"
    actual = {path.name for path in sdk_root.iterdir() if path.is_dir()}
    if actual != SUPPORTED_SDK_DIRS:
        unexpected = ", ".join(sorted(actual - SUPPORTED_SDK_DIRS)) or "none"
        missing = ", ".join(sorted(SUPPORTED_SDK_DIRS - actual)) or "none"
        raise RuntimeError(f"unsupported SDK scope: unexpected={unexpected}; missing={missing}")
    for slug, _ in SDK_DIRS:
        readme = sdk_root / slug / "README.md"
        if not readme.is_file():
            raise RuntimeError(f"missing SDK README: {readme}")


def build(out):
    verify_sdk_scope()
    if out.exists():
        shutil.rmtree(out)
    docs_out = out / "docs"
    api_out = out / "api"
    spec_out = out / "openapi"
    sdk_out = out / "sdks"
    docs_out.mkdir(parents=True)
    api_out.mkdir(parents=True)
    spec_out.mkdir(parents=True)
    sdk_out.mkdir(parents=True)
    (out / ".nojekyll").write_text("", encoding="utf-8")

    md_files = sorted(DOCS.glob("*.md"))
    entries = [(p.stem, title_of(p, p.stem)) for p in md_files]

    sdk_entries = []
    for slug, title in SDK_DIRS:
        readme = ROOT / "sdks" / slug / "README.md"
        sdk_entries.append((slug, title, readme))

    def nav(active="", base=".."):
        items = ["<li><a href=\"" + base + "/index.html\"" + (" class=\"active\"" if active == "home" else "") + ">Home</a></li>"]
        items.append("<li><a href=\"" + base + "/api/index.html\"" + (" class=\"active\"" if active == "api" else "") + ">API reference</a></li>")
        for slug, title in entries:
            cls = " class=\"active\"" if active == slug else ""
            items.append("<li><a href=\"" + base + "/docs/" + slug + ".html\"" + cls + ">" + title + "</a></li>")
        for slug, title, _ in sdk_entries:
            cls = " class=\"active\"" if active == "sdk-" + slug else ""
            items.append("<li><a href=\"" + base + "/sdks/" + slug + ".html\"" + cls + ">" + title + "</a></li>")
        return "".join(items)

    for path in md_files:
        body = render_md(path.read_text(encoding="utf-8"))
        (docs_out / (path.stem + ".html")).write_text(
            page(title_of(path, path.stem), body, nav(path.stem, ".."), root_prefix=".."),
            encoding="utf-8",
        )

    for slug, title, readme in sdk_entries:
        body = render_md(readme.read_text(encoding="utf-8"))
        (sdk_out / (slug + ".html")).write_text(
            page(title, body, nav("sdk-" + slug, ".."), root_prefix=".."),
            encoding="utf-8",
        )

    spec_text = SPEC.read_text(encoding="utf-8")
    (spec_out / "rest.yaml").write_text(spec_text, encoding="utf-8")
    spec_data = yaml.safe_load(spec_text)
    (spec_out / "rest.json").write_text(json.dumps(spec_data, indent=2), encoding="utf-8")
    version = str(spec_data.get("info", {}).get("version", ""))
    path_count = len(spec_data.get("paths", {}))

    cards = "".join(
        "<div class=\"card\"><a href=\"docs/" + slug + ".html\">" + title + "</a></div>"
        for slug, title in entries
    )
    sdk_cards = "".join(
        "<div class=\"card\"><a href=\"sdks/" + slug + ".html\">" + title + "</a></div>"
        for slug, title, _ in sdk_entries
    )
    readme_body = render_md(README.read_text(encoding="utf-8")) if README.exists() else ""
    home_body = (
        "<h1>rymeDB documentation</h1>\n"
        "<p>Guides from <code>./docs</code> and the REST operator API from "
        "<code>schemas/openapi/rest.yaml</code> (v" + version + ", " + str(path_count) + " paths).</p>\n"
        "<p><a href=\"api/index.html\">Open the API reference</a> · "
        "<a href=\"openapi/rest.yaml\">rest.yaml</a> · "
        "<a href=\"openapi/rest.json\">rest.json</a></p>\n"
        "<h2>Guides</h2>\n<div class=\"cards\">" + cards + "</div>\n"
        "<h2>SDKs</h2>\n<div class=\"cards\">" + sdk_cards + "</div>\n"
        "<hr>\n" + readme_body
    )
    (out / "index.html").write_text(page("Home", home_body, nav("home", "."), root_prefix="."), encoding="utf-8")

    api_body = (
        "<h1>REST API reference</h1>\n"
        "<p>Spec version " + version + " · " + str(path_count) + " paths · "
        "<a href=\"../openapi/rest.yaml\">rest.yaml</a> · "
        "<a href=\"../openapi/rest.json\">rest.json</a></p>\n"
        "<link rel=\"stylesheet\" href=\"https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui.css\">\n"
        "<div id=\"swagger-ui\"></div>\n"
        "<script src=\"https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui-bundle.js\"></script>\n"
        "<script>\nwindow.onload = function () {\n"
        "  SwaggerUIBundle({ url: \"../openapi/rest.yaml\", dom_id: \"#swagger-ui\" });\n"
        "};\n</script>\n"
    )
    (api_out / "index.html").write_text(page("API reference", api_body, nav("api", ".."), root_prefix=".."), encoding="utf-8")

    if IMAGES.exists():
        shutil.copytree(IMAGES, out / "images", dirs_exist_ok=True)

    print("pages ok: " + str(len(entries)) + " guides, " + str(len(sdk_entries)) + " SDKs, " + str(path_count) + " api paths -> " + str(out))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="_site")
    args = parser.parse_args()
    build(Path(args.out).resolve())


if __name__ == "__main__":
    main()
