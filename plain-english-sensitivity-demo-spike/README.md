# Plain-English Sensitivity Demo Spike Issue Import

This tarball contains a GitHub CLI script that creates the Skadi spike epic and six stories for the plain-English market-risk sensitivity query demo.

## Usage

```bash
tar -xzf plain-english-sensitivity-demo-spike.tar.gz
cd plain-english-sensitivity-demo-spike
./scripts/create-plain-english-sensitivity-demo-spike.sh
```

The script assumes:

- `gh` is installed
- you are authenticated with `gh auth login`
- you have permission to create issues and labels in `iceforge-io/skadi`

## Contents

```text
scripts/create-plain-english-sensitivity-demo-spike.sh
README.md
```
