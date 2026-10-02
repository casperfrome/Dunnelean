#!/usr/bin/env python3
"""Build, package, and verify the native libraries shipped in the Go module.

Only the maintainer runs this script. Go consumers need neither Python nor Rust.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys


PLATFORMS = {
    "x86_64-pc-windows-msvc": ("windows", "amd64", "dunnelean_native.dll"),
    "x86_64-unknown-linux-gnu": ("linux", "amd64", "libdunnelean_native.so"),
    "x86_64-apple-darwin": ("darwin", "amd64", "libdunnelean_native.dylib"),
    "aarch64-apple-darwin": ("darwin", "arm64", "libdunnelean_native.dylib"),
}
ABI_VERSION = 1
MAX_LIBRARY_SIZE = 100 * 1024 * 1024
MAX_MODULE_SIZE = 500 * 1024 * 1024


def run(command: list[str], *, cwd: Path, env: dict[str, str] | None = None) -> str:
    result = subprocess.run(command, cwd=cwd, env=env, check=True,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            encoding="utf-8", errors="replace")
    return result.stdout


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def source_hash(repository: Path) -> str:
    """Hash native source paths and UTF-8 bytes with platform-independent LF lines."""
    files = {repository / name for name in
             ("Cargo.toml", "Cargo.lock", "rust-toolchain.toml")}
    files.update((repository / "src").rglob("*.rs"))
    native = repository / "sdk/native"
    files.update(path for path in native.rglob("*")
                 if path.is_file() and "target" not in path.relative_to(native).parts
                 and (path.suffix in {".rs", ".h"} or path.name == "Cargo.toml"))
    digest = hashlib.sha256()
    for path in sorted(files, key=lambda value: value.relative_to(repository).as_posix()):
        relative = path.relative_to(repository).as_posix()
        content = path.read_bytes().decode("utf-8").replace("\r\n", "\n").replace("\r", "\n")
        digest.update(relative.encode("utf-8") + b"\0")
        digest.update(content.encode("utf-8") + b"\0")
    return digest.hexdigest()


def native_version(repository: Path) -> str:
    manifest = (repository / "sdk/native/Cargo.toml").read_text(encoding="utf-8")
    match = re.search(r'^version\s*=\s*"([^"]+)"', manifest, re.MULTILINE)
    if not match:
        raise ValueError("sdk/native/Cargo.toml must declare a package version")
    return match.group(1)


def dumpbin_path() -> str:
    found = shutil.which("dumpbin")
    if found:
        return found
    base = Path(os.environ.get("ProgramFiles(x86)", "C:/Program Files (x86)"))
    locator = base / "Microsoft Visual Studio/Installer/vswhere.exe"
    if locator.is_file():
        installation = run([str(locator), "-latest", "-requires",
                            "Microsoft.VisualStudio.Component.VC.Tools.x86.x64",
                            "-property", "installationPath"], cwd=Path.cwd()).strip()
        if installation:
            candidates = sorted(Path(installation).glob(
                "VC/Tools/MSVC/*/bin/Hostx64/x64/dumpbin.exe"), reverse=True)
            if candidates:
                return str(candidates[0])
    raise ValueError("dumpbin.exe is required to verify the Windows DLL")


def check_dependencies(library: Path, target: str, repository: Path) -> list[str]:
    if target.endswith("windows-msvc"):
        output = run([dumpbin_path(), "/DEPENDENTS", str(library)], cwd=repository)
        dependencies = sorted(set(re.findall(r"^\s+([\w.-]+\.dll)\s*$", output,
                                              re.MULTILINE | re.IGNORECASE)))
        if not dependencies:
            raise ValueError("Windows dependency inspection returned no DLLs")
        forbidden = [name for name in dependencies
                     if name.lower().startswith(("vcruntime", "msvcp", "msvcr"))]
        if forbidden:
            raise ValueError(f"DLL requires a separately installed MSVC runtime: {forbidden}")
    elif target.endswith("linux-gnu"):
        output = run(["readelf", "-d", str(library)], cwd=repository)
        dependencies = sorted(set(re.findall(r"\(NEEDED\).*?\[([^\]]+)\]", output)))
        allowed = {"libgcc_s.so.1", "libc.so.6", "libm.so.6", "libpthread.so.0",
                   "libdl.so.2", "librt.so.1", "ld-linux-x86-64.so.2"}
        if set(dependencies) - allowed:
            raise ValueError(f"Unexpected Linux dependencies: {set(dependencies) - allowed}")
        versions = run(["readelf", "--version-info", str(library)], cwd=repository)
        required = {tuple(map(int, version.split(".")))
                    for version in re.findall(r"GLIBC_(\d+(?:\.\d+)+)", versions)}
        if required and max(required) > (2, 17):
            raise ValueError(f"Library exceeds glibc 2.17: {max(required)}")
        if "GLIBCXX_" in versions:
            raise ValueError("Library unexpectedly depends on a C++ runtime")
    else:
        output = run(["otool", "-L", str(library)], cwd=repository)
        dependencies = [line.strip().split(" (", 1)[0]
                        for line in output.splitlines()[1:] if line.strip()]
        # otool lists the dylib's own LC_ID_DYLIB before its actual dependencies.
        dependencies = [name for name in dependencies if Path(name).name != library.name]
        if any(not name.startswith(("/usr/lib/", "/System/Library/"))
               for name in dependencies):
            raise ValueError(f"Unexpected macOS dependencies: {dependencies}")
        commands = run(["otool", "-l", str(library)], cwd=repository)
        minimum = re.findall(r"\bminos\s+(\d+(?:\.\d+)+)", commands)
        if not minimum:
            minimum = re.findall(r"\bversion\s+(\d+(?:\.\d+)+)", commands)
        if not minimum or any(tuple(map(int, value.split("."))) > (12, 0, 0)
                              for value in minimum):
            raise ValueError(f"Expected a macOS deployment target of at most 12: {minimum}")
        run(["codesign", "--verify", "--verbose", str(library)], cwd=repository)
    return dependencies


def build(arguments: argparse.Namespace) -> None:
    repository = arguments.repo.resolve()
    before = source_hash(repository)
    target = arguments.target
    goos, goarch, library_name = PLATFORMS[target]
    env = dict(os.environ)
    if goos == "windows":
        env["RUSTFLAGS"] = (env.get("RUSTFLAGS", "") +
                            " -C target-feature=+crt-static").strip()
    elif goos == "darwin":
        env["MACOSX_DEPLOYMENT_TARGET"] = "12.0"
    if arguments.library:
        library = arguments.library.resolve()
    else:
        target_directory = (arguments.target_dir or repository / "target/go-native").resolve()
        command = ["cargo", "build", "--release", "--locked", "-p", "dunnelean-native",
                   "--target", target, "--target-dir", str(target_directory)]
        print("Building native library", target, flush=True)
        result = subprocess.run(command, cwd=repository, env=env)
        if result.returncode:
            raise subprocess.CalledProcessError(result.returncode, command)
        library = target_directory / target / "release" / library_name
    if goos == "darwin":
        run(["codesign", "--force", "--sign", "-", str(library)], cwd=repository)
    dependencies = check_dependencies(library, target, repository)
    payload = library.read_bytes()
    if not 0 < len(payload) <= MAX_LIBRARY_SIZE:
        raise ValueError("Native library must be nonempty and no larger than 100 MiB")
    if source_hash(repository) != before:
        raise ValueError("Native sources changed during the build; rebuild from a stable commit")
    source_commit = arguments.source_commit or run(
        ["git", "rev-parse", "HEAD"], cwd=repository).strip()
    if not re.fullmatch(r"[0-9a-f]{40}", source_commit):
        raise ValueError("source_commit must be a full lowercase Git commit SHA")
    packed = gzip.compress(payload, compresslevel=9, mtime=0)
    asset_name = f"{goos}_{goarch}.gz"
    document = {
        "schema_version": 1, "version": native_version(repository),
        "abi_version": ABI_VERSION, "source_commit": source_commit,
        "source_hash": before,
        "assets": [{"goos": goos, "goarch": goarch, "target": target,
                    "file": asset_name, "library": library_name,
                    "size": len(payload), "sha256": sha256(payload),
                    "compressed_size": len(packed), "compressed_sha256": sha256(packed),
                    "dependencies": dependencies}],
    }
    arguments.out.mkdir(parents=True, exist_ok=True)
    (arguments.out / asset_name).write_bytes(packed)
    manifest = arguments.out / f"{goos}_{goarch}.json"
    manifest.write_text(json.dumps(document, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Packaged {asset_name}: {len(payload)} native bytes, {len(packed)} compressed bytes")
    print(f"Native source SHA-256: {before}")


def verify_asset(document: dict, asset: dict, directory: Path) -> None:
    expected = PLATFORMS.get(asset["target"])
    if expected != (asset["goos"], asset["goarch"], asset["library"]):
        raise ValueError(f"Unsupported or inconsistent native target: {asset}")
    if asset["file"] != f"{asset['goos']}_{asset['goarch']}.gz":
        raise ValueError("Unexpected native asset filename")
    payload = (directory / asset["file"]).read_bytes()
    if len(payload) != asset["compressed_size"] or sha256(payload) != asset["compressed_sha256"]:
        raise ValueError(f"Compressed asset integrity failed: {asset['file']}")
    with gzip.GzipFile(fileobj=io.BytesIO(payload)) as stream:
        unpacked = stream.read(MAX_LIBRARY_SIZE + 1)
    if not 0 < asset["size"] <= MAX_LIBRARY_SIZE:
        raise ValueError("Invalid native asset size")
    if len(unpacked) != asset["size"] or sha256(unpacked) != asset["sha256"]:
        raise ValueError(f"Native asset integrity failed: {asset['file']}")
    if document["schema_version"] != 1 or document["abi_version"] != ABI_VERSION:
        raise ValueError("Unsupported native manifest schema or ABI")


def verify(arguments: argparse.Namespace) -> None:
    directory = arguments.sdk.resolve() / "internal/nativeassets"
    document = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
    if document["source_hash"] != source_hash(arguments.repo.resolve()):
        raise ValueError("Committed native assets are stale: Rust native source hash differs")
    if document["version"] != native_version(arguments.repo.resolve()):
        raise ValueError("Committed native assets have the wrong package version")
    keys = {(asset["goos"], asset["goarch"]) for asset in document["assets"]}
    expected = {(value[0], value[1]) for value in PLATFORMS.values()}
    if keys != expected or len(document["assets"]) != len(expected):
        raise ValueError("Native manifest must contain exactly the four supported platforms")
    for asset in document["assets"]:
        verify_asset(document, asset, directory)
    # Include every module file, not only gzip assets, in the distribution size check.
    module_size = sum(path.stat().st_size for path in arguments.sdk.rglob("*") if path.is_file())
    if module_size > MAX_MODULE_SIZE:
        raise ValueError("Go module exceeds the 500 MiB uncompressed ZIP limit")
    print(f"Verified four native assets, source {document['source_commit']}, module bytes {module_size}")


def assemble(arguments: argparse.Namespace) -> None:
    manifests = sorted(arguments.artifacts.rglob("*.json"))
    documents = []
    for manifest in manifests:
        document = json.loads(manifest.read_text(encoding="utf-8"))
        if "assets" in document and "source_hash" in document:
            documents.append((manifest, document))
    if len(documents) != len(PLATFORMS):
        raise ValueError(f"Expected four platform manifests, found {len(documents)}")
    header_keys = ("schema_version", "version", "abi_version", "source_commit", "source_hash")
    header = {key: documents[0][1][key] for key in header_keys}
    assets = []
    for manifest, document in documents:
        if {key: document[key] for key in header_keys} != header or len(document["assets"]) != 1:
            raise ValueError("Platform artifacts must come from the same source, version, and ABI")
        asset = document["assets"][0]
        verify_asset(document, asset, manifest.parent)
        assets.append((asset, manifest.parent / asset["file"]))
    expected = {(value[0], value[1]) for value in PLATFORMS.values()}
    if {(asset["goos"], asset["goarch"]) for asset, _ in assets} != expected:
        raise ValueError("Platform artifacts do not cover all four supported platforms")
    if header["source_hash"] != source_hash(arguments.repo.resolve()):
        raise ValueError("Artifacts do not match the current native source tree")
    destination = arguments.sdk.resolve() / "internal/nativeassets"
    destination.mkdir(parents=True, exist_ok=True)
    for asset, path in assets:
        shutil.copyfile(path, destination / asset["file"])
    header["assets"] = sorted((asset for asset, _ in assets),
                              key=lambda value: (value["goos"], value["goarch"]))
    (destination / "manifest.json").write_text(
        json.dumps(header, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    verify(arguments)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[1])
    subparsers = parser.add_subparsers(dest="command", required=True)
    builder = subparsers.add_parser("build", help="Build and package one platform")
    builder.add_argument("--target", choices=PLATFORMS, required=True)
    builder.add_argument("--out", type=Path, default=Path("dist/go-native"))
    builder.add_argument("--target-dir", type=Path)
    builder.add_argument("--library", type=Path, help="Package an already built library")
    builder.add_argument("--source-commit")
    builder.set_defaults(action=build)
    for name, action in (("assemble", assemble), ("verify", verify)):
        child = subparsers.add_parser(name)
        child.add_argument("--sdk", type=Path, default=Path("sdk/go"))
        if name == "assemble":
            child.add_argument("--artifacts", type=Path, required=True)
        child.set_defaults(action=action)
    arguments = parser.parse_args()
    try:
        arguments.action(arguments)
    except (OSError, ValueError, KeyError, subprocess.CalledProcessError) as error:
        print(f"Go native packaging failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
