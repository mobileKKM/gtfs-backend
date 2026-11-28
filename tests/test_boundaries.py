"""The shared modules (gtfs_backend/*.py) are the API's base: they never import the API itself. The worker is a
separate program (worker/, Rust) that shares only the Redis/Postgres layout with it."""

import ast
from pathlib import Path

SRC = Path(__file__).parents[1] / "src" / "gtfs_backend"


def imports(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(), str(path))
    out = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            out += [a.name for a in node.names]
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            out.append(node.module)
    return out


def test_shared_modules_do_not_import_the_api():
    bad = [f"{p.name}: {name}" for p in sorted(SRC.glob("*.py")) for name in imports(p) if name.startswith("gtfs_backend.api")]
    assert not bad, "\n".join(bad)


def test_there_is_no_python_worker_left():
    assert not (SRC / "worker").exists()
