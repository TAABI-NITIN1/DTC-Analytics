"""Small zero-dependency CI gate for accidentally tracked credential material."""

import re
import subprocess
from pathlib import Path


PATTERNS = {
    "private key": re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
    "OpenAI key": re.compile(r"\bsk-(?:proj-)?[A-Za-z0-9_-]{20,}\b"),
    "GitHub token": re.compile(r"\b(?:ghp|gho|ghu|ghs|ghr)_[A-Za-z0-9]{20,}\b"),
    "AWS access key": re.compile(r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b"),
}
PLACEHOLDER_MARKERS = ("your_", "replace-at-deploy-time", "test-only", "example")


def main() -> None:
    files = subprocess.check_output(["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"]).decode().split("\0")
    failures = []
    if ".env" in files:
        failures.append(".env: tracked environment file")
    for name in filter(None, files):
        path = Path(name)
        if not path.is_file() or path.stat().st_size > 2_000_000:
            continue
        try:
            content = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue
        lowered = content.lower()
        for label, pattern in PATTERNS.items():
            for match in pattern.finditer(content):
                nearby = lowered[max(0, match.start() - 40):match.end() + 40]
                if not any(marker in nearby for marker in PLACEHOLDER_MARKERS):
                    failures.append(f"{name}: possible {label}")
    if failures:
        raise SystemExit("Secret scan failed:\n" + "\n".join(sorted(set(failures))))
    print("Secret scan passed")


if __name__ == "__main__":
    main()
