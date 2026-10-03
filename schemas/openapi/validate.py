import re
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
SPEC = ROOT / "schemas" / "openapi" / "rest.yaml"
ROUTER = ROOT / "crates" / "ryme-server" / "src" / "lib.rs"


def normalize(route: str) -> str:
    return re.sub(r":([A-Za-z_]+)", r"{\1}", route)


def main() -> int:
    spec = yaml.safe_load(SPEC.read_text())
    documented = set(spec.get("paths", {}))
    routed = {
        normalize(m) for m in re.findall(r'\.route\(\s*"([^"]+)"', ROUTER.read_text())
    }
    missing = sorted(routed - documented)
    extra = sorted(documented - routed)
    if missing or extra:
        if missing:
            print(f"routes missing from spec: {missing}")
        if extra:
            print(f"spec paths with no route: {extra}")
        return 1
    text = SPEC.read_text()
    refs = set(
        re.findall(r"#/components/(?:schemas|responses|parameters)/([A-Za-z]+)", text)
    )
    components = spec.get("components", {})
    defined = set(components.get("schemas", {}))
    defined |= set(components.get("responses", {}))
    defined |= set(components.get("parameters", {}))
    dangling = sorted(refs - defined)
    if dangling:
        print(f"dangling refs: {dangling}")
        return 1
    print(f"openapi ok: {len(documented)} paths, {len(defined)} components")
    return 0


if __name__ == "__main__":
    sys.exit(main())
