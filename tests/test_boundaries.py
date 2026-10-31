"""The worker and the API are separate programs: they share only gtfs_backend."""

import ast
import subprocess
import sys
from pathlib import Path

import pytest

SRC = Path(__file__).parents[1] / "src" / "gtfs_backend"
FORBIDDEN = {
    "shared": ("gtfs_backend.api", "gtfs_backend.worker"),  # the modules directly in gtfs_backend/
    "api": ("gtfs_backend.worker",),
    "worker": ("gtfs_backend.api",),
}


def files(package: str) -> list[Path]:
    return sorted(SRC.glob("*.py")) if package == "shared" else sorted((SRC / package).rglob("*.py"))


def imports(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(), str(path))
    out = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            out += [a.name for a in node.names]
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            out.append(node.module)
    return out


@pytest.mark.parametrize("package", sorted(FORBIDDEN))
def test_packages_keep_to_their_side(package):
    bad = [
        f"{path.relative_to(SRC)}: {name}"
        for path in files(package)
        for name in imports(path)
        if name.startswith(FORBIDDEN[package])
    ]
    assert not bad, "\n".join(bad)


def test_api_does_not_load_worker_dependencies():
    code = (
        "import sys, gtfs_backend.api.app, gtfs_backend.api.cli; "
        "print(sorted(m for m in ('pandas', 'gtfs_kit', 'pyarrow', 'google.protobuf', 'httpx', 'gtfs_backend.worker') "
        "if m in sys.modules))"
    )
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True).stdout.strip()
    assert out == "[]"
