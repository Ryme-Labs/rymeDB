import { cp, mkdir } from "node:fs/promises";

await mkdir("static/images", { recursive: true });
await mkdir("static/openapi", { recursive: true });
await cp("images", "static/images", { recursive: true, force: true });
await cp("schemas/openapi/rest.yaml", "static/openapi/rest.yaml");
