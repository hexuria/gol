from pathlib import Path
import subprocess
import sys

path = Path("Cargo.lock")
text = path.read_text()
old_proxy = "[[package]]\nname = \"pulp\"\n"
new_proxy = """[[package]]
name = "proxy"
version = "0.1.0"
dependencies = [
 "axum",
 "reqwest",
 "serde_json",
 "tokio",
]

[[package]]
name = "pulp"
"""
old_server_proxy = """ "protocol",
 "redis",
"""
new_server_proxy = """ "protocol",
 "proxy",
 "redis",
"""
old_ureq = """ "typesafe-sdk",
 "uuid",
"""
new_ureq = """ "typesafe-sdk",
 "ureq",
 "uuid",
"""
for label, old, new in (
    ("proxy package", old_proxy, new_proxy),
    ("server proxy dep", old_server_proxy, new_server_proxy),
    ("server ureq dep", old_ureq, new_ureq),
):
    if old not in text:
        sys.exit(f"lock pattern missing: {label}")
    text = text.replace(old, new, 1)
path.write_text(text)
got = subprocess.check_output(["git", "hash-object", "Cargo.lock"], text=True).strip()
expected = "c6f150049c315f76193d72fe742ab610419d7d13"
if got != expected:
    sys.exit(f"Cargo.lock {got} != {expected}")
subprocess.check_call(["git", "config", "user.email", "actions@github.com"])
subprocess.check_call(["git", "config", "user.name", "github-actions"])
subprocess.check_call(["git", "add", "Cargo.lock"])
subprocess.check_call([
    "git", "rm", "-f",
    ".github/apply_inference_lock.py",
    ".github/workflows/inference-lock.yml",
])
subprocess.check_call(["git", "commit", "-m", "feat(inference): record the proxy lock"])
subprocess.check_call(["git", "push"])
