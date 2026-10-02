#!/usr/bin/env python3
"""Install the actual Go module archive outside the repository and test it offline."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import zipfile


MODULE = "github.com/casperfrome/Dunnelean/sdk/go"


def command(arguments: list[str], directory: Path, environment: dict[str, str]) -> str:
    print("+", " ".join(arguments), flush=True)
    result = subprocess.run(arguments, cwd=directory, env=environment,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            encoding="utf-8", errors="replace")
    print(result.stdout, end="", flush=True)
    if result.returncode:
        raise subprocess.CalledProcessError(result.returncode, arguments)
    return result.stdout


def escaped_module(path: str) -> str:
    return "".join("!" + character.lower() if character.isupper() else character
                   for character in path)


def make_proxy(sdk: Path, directory: Path, version: str) -> str:
    destination = directory / escaped_module(MODULE) / "@v"
    destination.mkdir(parents=True)
    (destination / "list").write_text(version + "\n", encoding="utf-8")
    (destination / f"{version}.mod").write_bytes((sdk / "go.mod").read_bytes())
    (destination / f"{version}.info").write_text(json.dumps({
        "Version": version, "Time": "2026-10-02T00:00:00Z",
    }), encoding="utf-8")
    with zipfile.ZipFile(destination / f"{version}.zip", "w", zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(sdk.rglob("*")):
            if not path.is_file() or path.is_symlink():
                continue
            relative = path.relative_to(sdk)
            if any(part.startswith(".") for part in relative.parts):
                continue
            if path.suffix in {".exe", ".test", ".tmp"}:
                continue
            # The ZIP prefix is the canonical module path, not its proxy escaping.
            name = f"{MODULE}@{version}/{relative.as_posix()}"
            entry = zipfile.ZipInfo(name, (1980, 1, 1, 0, 0, 0))
            entry.compress_type = zipfile.ZIP_DEFLATED
            archive.writestr(entry, path.read_bytes())
    return directory.resolve().as_uri()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    repository = Path(__file__).resolve().parents[1]
    parser.add_argument("--go", default="go", help="Go executable, including an independent 64-bit installation")
    parser.add_argument("--sdk", type=Path, default=repository / "sdk/go")
    parser.add_argument("--version", default="v0.1.0")
    parser.add_argument("--published", action="store_true", help="Download the real public tag instead of a local module ZIP")
    parser.add_argument("--race", action="store_true", help="Use the Go race detector (maintainer C compiler required)")
    parser.add_argument("--report", type=Path)
    arguments = parser.parse_args()
    executable = shutil.which(arguments.go) or str(Path(arguments.go).resolve())
    with tempfile.TemporaryDirectory(prefix="dunnelean-go-installed-") as temporary:
        directory = Path(temporary)
        consumer = directory / "consumer"
        consumer.mkdir()
        environment = dict(os.environ)
        environment.pop("GOROOT", None)
        environment.update({
            "GOENV": "off", "GOTOOLCHAIN": "local", "GO111MODULE": "on",
            "CGO_ENABLED": "1" if arguments.race else "0",
            "GOPATH": str(directory / "gopath"), "GOMODCACHE": str(directory / "modules"),
            "GOCACHE": str(directory / "build-cache"),
        })
        if not arguments.published:
            proxy = make_proxy(arguments.sdk.resolve(), directory / "proxy", arguments.version)
            fallback = os.environ.get("GOPROXY", "https://proxy.golang.org,direct")
            if fallback == "off":
                fallback = "https://proxy.golang.org,direct"
            environment["GOPROXY"] = proxy + "," + fallback
            environment["GONOSUMDB"] = MODULE
        shutil.copyfile(repository / "examples/go/offline/main.go", consumer / "main.go")
        version = command([executable, "version"], consumer, environment).strip()
        architecture = command([executable, "env", "GOOS", "GOARCH"], consumer, environment).splitlines()
        if len(architecture) != 2 or "_".join(architecture) not in {
            "windows_amd64", "linux_amd64", "darwin_amd64", "darwin_arm64",
        }:
            raise ValueError(f"Use a supported 64-bit Go installation: {architecture}")
        command([executable, "mod", "init", "dunnelean-installed-acceptance"], consumer, environment)
        command([executable, "get", MODULE + "@" + arguments.version], consumer, environment)
        command([executable, "mod", "tidy"], consumer, environment)
        modules = command([executable, "list", "-m", "-json", MODULE], consumer, environment)
        module = json.loads(modules)
        installed = Path(module["Dir"]).resolve()
        if installed.is_relative_to(repository):
            raise ValueError("Acceptance must import the installed module outside the repository")
        if module.get("Replace"):
            raise ValueError("Acceptance must not use a local module replacement")
        installed_manifest = json.loads(
            (installed / "internal/nativeassets/manifest.json").read_text(encoding="utf-8"))
        if installed_manifest["version"] != arguments.version.lstrip("v"):
            raise ValueError("Installed module and bundled native versions differ")
        test = [executable, "test", "-count=1", "-timeout=5m"]
        if arguments.race:
            test.append("-race")
        command(test + [MODULE + "/..."], consumer, environment)
        command([executable, "vet", MODULE + "/..."], consumer, environment)
        command([executable, "mod", "vendor"], consumer, environment)
        # Compilation starts from an empty Go build cache, with no module access.
        # All source and native libraries must already be present in vendor.
        offline = dict(environment)
        offline.update({"GOPROXY": "off", "GOSUMDB": "off", "GONOSUMDB": "",
                        "GOMODCACHE": str(directory / "empty-modules"),
                        "GOCACHE": str(directory / "offline-build-cache"), "CGO_ENABLED": "0"})
        binary = consumer / ("offline.exe" if os.name == "nt" else "offline")
        command([executable, "build", "-mod=vendor", "-o", str(binary), "."], consumer, offline)
        output = command([str(binary), "-native-cache", str(directory / "native-cache")], consumer, offline)
        smoke = json.loads(output.strip().splitlines()[-1])
        if not smoke["offline"] or not smoke["reopened"] or not smoke["state_store_id"]:
            raise ValueError(f"Offline embedded engine acceptance failed: {smoke}")
        report = {"module": MODULE, "version": arguments.version, "go": version,
                  "platform": "_".join(architecture), "published": arguments.published,
                  "race": arguments.race, "module_directory": str(installed),
                  "native_source_commit": installed_manifest["source_commit"],
                  "native_source_hash": installed_manifest["source_hash"],
                  "offline": smoke, "result": "passed"}
        if arguments.report:
            arguments.report.parent.mkdir(parents=True, exist_ok=True)
            arguments.report.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        print(json.dumps(report, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, KeyError, subprocess.CalledProcessError) as error:
        print(f"Go package acceptance failed: {error}", file=sys.stderr)
        raise SystemExit(1)
