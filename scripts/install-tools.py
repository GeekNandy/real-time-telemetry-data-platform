#!/usr/bin/env python3
"""Install pinned Linux/amd64 CI tools after verifying publisher checksums over HTTPS."""
import argparse
import hashlib
import io
from pathlib import Path
import platform
import tarfile
import urllib.request
import zipfile

SOURCES = {
    "helm": ("https://get.helm.sh/helm-v3.22.0-linux-amd64.tar.gz",
             "https://get.helm.sh/helm-v3.22.0-linux-amd64.tar.gz.sha256sum", "linux-amd64/helm"),
    "kind": ("https://github.com/kubernetes-sigs/kind/releases/download/v0.30.0/kind-linux-amd64",
             "https://github.com/kubernetes-sigs/kind/releases/download/v0.30.0/kind-linux-amd64.sha256sum", None),
    "kubeconform": ("https://github.com/yannh/kubeconform/releases/download/v0.7.0/kubeconform-linux-amd64.tar.gz",
                    "https://github.com/yannh/kubeconform/releases/download/v0.7.0/CHECKSUMS", "kubeconform"),
    "terraform": ("https://releases.hashicorp.com/terraform/1.13.5/terraform_1.13.5_linux_amd64.zip",
                  "https://releases.hashicorp.com/terraform/1.13.5/terraform_1.13.5_SHA256SUMS", "terraform"),
}


def fetch(url):
    with urllib.request.urlopen(url, timeout=60) as response:
        return response.read()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("tools", nargs="+", choices=SOURCES)
    args = parser.parse_args()
    if platform.system() != "Linux" or platform.machine() not in ("x86_64", "amd64"):
        parser.error("These checksum-verified artifacts target Linux/amd64 CI runners")
    args.directory.mkdir(parents=True, exist_ok=True)
    for tool in args.tools:
        url, checksum_url, member = SOURCES[tool]
        name = url.rsplit("/", 1)[1]
        lines = fetch(checksum_url).decode().splitlines()
        expected = next(line.split()[0] for line in lines if name in line or len(lines) == 1)
        data = fetch(url)
        if hashlib.sha256(data).hexdigest() != expected:
            raise SystemExit(f"Publisher checksum verification failed for {tool}")
        if url.endswith(".tar.gz"):
            with tarfile.open(fileobj=io.BytesIO(data)) as archive:
                data = archive.extractfile(member).read()
        elif url.endswith(".zip"):
            with zipfile.ZipFile(io.BytesIO(data)) as archive:
                data = archive.read(member)
        target = args.directory / tool
        target.write_bytes(data)
        target.chmod(0o755)
        print(f"Installed verified {tool}")


if __name__ == "__main__":
    main()
