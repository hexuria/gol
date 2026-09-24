import base64, io, pathlib, subprocess, tarfile
b64 = "".join(pathlib.Path(f".github/b64part{i}").read_text().strip() for i in range(4))
raw = base64.b64decode(b64)
tarfile.open(fileobj=io.BytesIO(raw), mode="r:gz").extractall(".")
lock = pathlib.Path("Cargo.lock")
text = lock.read_text()
old = """name = "harness"
version = "0.1.0"
dependencies = [
 "protocol",
 "serde_json",
 "typesafe-sdk",
]"""
new = """name = "harness"
version = "0.1.0"
dependencies = [
 "protocol",
 "serde",
 "serde_json",
 "toml 0.8.23",
 "typesafe-sdk",
]"""
if old not in text:
    raise SystemExit("lock pattern missing")
lock.write_text(text.replace(old, new, 1))
expected = {
    "Cargo.lock": "2aa20505fbebaa43d7de55208c13cd339aea8979",
    "crates/harness/Cargo.toml": "12cfdb94d68819d0960037e4cc481e6c2a6a24b3",
    "crates/harness/src/decider.rs": "dd2069b81c76167c932fb3b7db8a43db26536953",
    "crates/harness/src/driver.rs": "aff793b8c7878b9cf1343db72e598dfc0807abf2",
    "crates/harness/src/lib.rs": "d8ae3869d292ad5ca9f9317d8a65e25191f6ea83",
    "crates/harness/src/catalog.rs": "fa8439c1a8739591498531c8bbd94c94f9c9a030",
    "crates/harness/tests/catalog.rs": "c7cd9bdb21862972078f7c6aff13f83c00d42e8c",
}
for path, sha in expected.items():
    got = subprocess.check_output(["git", "hash-object", path], text=True).strip()
    if got != sha:
        raise SystemExit(path + " " + got + " != " + sha)
subprocess.check_call(["git", "config", "user.email", "actions@github.com"])
subprocess.check_call(["git", "config", "user.name", "github-actions"])
subprocess.check_call(["git", "add", "Cargo.lock", "crates/harness"])
subprocess.check_call(["git", "rm", "-f", ".github/workflows/catalog.yml", ".github/apply_catalog.py", ".github/catalog.b64", ".github/b64part0", ".github/b64part1", ".github/b64part2", ".github/b64part3"])
subprocess.check_call(["git", "commit", "-m", "feat(harness): load tools, MCP, and skills"])
subprocess.check_call(["git", "push"])
