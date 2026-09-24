from pathlib import Path
import subprocess
import sys

path = Path("Cargo.lock")
text = path.read_text()
old = """name = "harness"
version = "0.1.0"
dependencies = [
 "protocol",
 "serde_json",
 "typesafe-sdk",
]
"""
new = """name = "harness"
version = "0.1.0"
dependencies = [
 "protocol",
 "serde",
 "serde_json",
 "toml 0.8.23",
 "typesafe-sdk",
]
"""
if old not in text:
    sys.exit("lock pattern missing")
path.write_text(text.replace(old, new, 1))
got = subprocess.check_output(["git", "hash-object", "Cargo.lock"], text=True).strip()
expected = "2aa20505fbebaa43d7de55208c13cd339aea8979"
if got != expected:
    sys.exit(f"Cargo.lock {got} != {expected}")
