import hashlib, json, pathlib, sys
root = pathlib.Path(sys.argv[1])
meta = json.loads(pathlib.Path(sys.argv[2]).read_text())
signed = len(sys.argv) > 3
sources = pathlib.Path.cwd()
version = "0.27.0"
prefix = f"Vibeke-{version}-"
cli = {f"vibeke-{platform}-{arch}" for platform in ("linux","macos") for arch in ("aarch64","x86_64")}
updates = {prefix+f"mac-{arch}.{fmt}" for arch in ("arm64","x64") for fmt in ("dmg","zip")} | {prefix+"win-x64.exe"}
payload = updates | {s+".blockmap" for s in updates} | {prefix+"linux-x86_64.AppImage",prefix+"linux-amd64.deb","latest.yml","latest-mac.yml","latest-linux.yml"} | cli
expected = payload | {s+".sha256" for s in cli} | {"install.sh"}
if signed:
    expected |= {"SHA256SUMS","SHA256SUMS.minisig","manifest.json","manifest.json.minisig"}
assert meta["tagName"] == "v"+version
assert {a["name"] for a in meta["assets"]} == expected
assert {p.name for p in root.iterdir()} == expected
def sha(path):
    with path.open("rb") as fp:
        return hashlib.file_digest(fp,"sha256").hexdigest()
checks = {}
for a in meta["assets"]:
    path = root/a["name"]
    assert path.stat().st_size == a["size"] > 0
    checks[a["name"]]=sha(path)
    assert a["digest"] == "sha256:"+checks[a["name"]], a["name"]
for name in cli:
    assert (root/(name+".sha256")).read_text().split() == [checks[name],name]
assert (root/"install.sh").read_bytes() == (sources/"scripts/install.sh").read_bytes()
if signed:
    sums={name:h for h,name in (line.split() for line in (root/"SHA256SUMS").read_text().splitlines())}
    assert sums == {n:checks[n] for n in payload}
    manifest=json.loads((root/"manifest.json").read_text())
    assert manifest["version"]==version
    assert sorted(manifest["artifacts"],key=lambda a:a["target"])==[{"target":n.removeprefix("vibeke-"),"sha256":checks[n],"url":f"https://github.com/vibeke-ai/vibeke/releases/download/v{version}/{n}"} for n in sorted(cli)]
    for name,want in (("SHA256SUMS",f"vibeke v{version}"),("manifest.json",f"vibeke v{version} version:{version}")):
        assert ("trusted comment: "+want) in (root/(name+".minisig")).read_text().splitlines()
print(f"Verified all {len(expected)} files against the release inventory and their uploaded hashes.")
