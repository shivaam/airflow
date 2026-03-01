# Apache Airflow Release Candidate Verification Guide

This document details the complete PMC-level verification process for Apache Airflow release candidates, based on verifying Airflow 3.1.7rc2 and Task SDK 1.1.7rc2.

## Overview

When Apache Airflow prepares a release, they first publish a Release Candidate (RC) for community testing. PMC members and contributors vote on whether the RC should become the official release. This guide covers the deep verification steps that go beyond simple installation testing.

## Prerequisites

- Docker installed and running
- Git
- SVN client (`svn`)
- Java (for Apache RAT license checker)
- ~10GB disk space
- ~1-2 hours time

---

## Step 1: Reproducible Package Check

### What is it?
A "reproducible build" means building the exact same source code produces byte-for-byte identical binary packages, regardless of who builds it or when. You rebuild the RC packages yourself and compare against what the Release Manager uploaded.

### Why does it matter?
- **Supply chain security** - Proves packages weren't tampered with after being built from source
- **No hidden code** - Nothing was injected that isn't in the source
- **Trust but verify** - Mathematical proof the packages are legitimate

### How to do it

```bash
# Create a clean verification folder
mkdir -p ~/airflow-rc-verify && cd ~/airflow-rc-verify

# Clone a fresh copy of the Airflow repo
git clone https://github.com/apache/airflow.git airflow-verify
cd airflow-verify

# Set version variables
VERSION=3.1.7
VERSION_SUFFIX=rc2
VERSION_RC=${VERSION}${VERSION_SUFFIX}
TASK_SDK_VERSION=1.1.7
TASK_SDK_VERSION_RC=${TASK_SDK_VERSION}${VERSION_SUFFIX}

# Checkout the RC tag
git fetch origin --tags
git checkout ${VERSION_RC}

# Set repo root for later comparison
export AIRFLOW_REPO_ROOT=$(pwd)

# Install breeze
uv tool install -e ./dev/breeze

# Build the packages (this uses Docker for consistent environment)
breeze release-management prepare-airflow-distributions --distribution-format both
breeze release-management prepare-task-sdk-distributions --distribution-format both
breeze release-management prepare-tarball --tarball-type apache_airflow --version ${VERSION} --version-suffix ${VERSION_SUFFIX}
```

### How it works internally
The build script (`scripts/in_container/run_prepare_airflow_distributions.py`) uses:
- `hatch build` to create wheel and sdist packages
- `SOURCE_DATE_EPOCH` environment variable set to a fixed timestamp from `reproducible_build.yaml`
- Docker container for consistent build environment

The fixed timestamp is the key to reproducibility - without it, embedded timestamps would differ between builds.

### Output
Your `dist/` folder now contains:
- `apache_airflow_core-3.1.7.tar.gz` and `.whl`
- `apache_airflow-3.1.7.tar.gz` and `.whl`
- `apache-airflow-3.1.7-source.tar.gz`
- Task SDK packages

---

## Step 2: SVN Checkout & Binary Comparison

### What is it?
Download the official RC packages from Apache's SVN repository and compare them byte-for-byte against your local build.

### What is SVN?
SVN (Subversion) is a version control system Apache uses to host official release artifacts:
- `https://dist.apache.org/repos/dist/dev/airflow/` - RC candidates (voting stage)
- `https://dist.apache.org/repos/dist/release/airflow/` - Final approved releases

### How to do it

```bash
# Go to verification folder
cd ~/airflow-rc-verify

# Clone Apache's SVN dist repo (shallow clone first)
svn checkout --depth=immediates https://dist.apache.org/repos/dist asf-dist

# Pull down the Airflow RC files
svn update --set-depth=infinity asf-dist/dev/airflow

# Set path variable
export PATH_TO_AIRFLOW_SVN=$(pwd)/asf-dist/dev/airflow

# Compare Airflow packages
cd ${PATH_TO_AIRFLOW_SVN}/${VERSION_RC}
for i in *.whl *.tar.gz; do
  echo "Checking $(basename $i)"
  diff "$(basename $i)" "${AIRFLOW_REPO_ROOT}/dist/$(basename $i)" && echo "OK"
done

# Compare Task SDK packages
cd ${PATH_TO_AIRFLOW_SVN}/../task-sdk/${TASK_SDK_VERSION_RC}
for i in *.whl *.tar.gz; do
  echo "Checking $(basename $i)"
  diff "$(basename $i)" "${AIRFLOW_REPO_ROOT}/dist/$(basename $i)" && echo "OK"
done
```

### Expected output
- **Success**: No output from `diff`, just "OK" for each file
- **Failure**: `Binary files X and Y differ`

The `diff` command does byte-for-byte binary comparison on `.whl` and `.tar.gz` files.

---

## Step 3: License Check (Apache RAT)

### What is it?
Apache RAT (Release Audit Tool) scans all source files to verify they have proper Apache 2.0 license headers.

### Why does it matter?
Apache has strict legal requirements:
- All source files need license headers
- No code with incompatible licenses allowed
- Protects users and contributors legally

### How to do it

```bash
# Download Apache RAT with checksum verification
wget -q https://dlcdn.apache.org//creadur/apache-rat-0.17/apache-rat-0.17-bin.tar.gz -O /tmp/apache-rat-0.17-bin.tar.gz
echo "32848673dc4fb639c33ad85172dfa9d7a4441a0144e407771c9f7eb6a9a0b7a9b557b9722af968500fae84a6e60775449d538e36e342f786f20945b1645294a0  /tmp/apache-rat-0.17-bin.tar.gz" | sha512sum -c -
tar -xzf /tmp/apache-rat-0.17-bin.tar.gz -C /tmp

# Extract the source tarball from SVN
rm -rf /tmp/apache-airflow-src
mkdir -p /tmp/apache-airflow-src
tar -xzf ${PATH_TO_AIRFLOW_SVN}/${VERSION_RC}/apache-airflow-*-source.tar.gz --strip-components 1 -C /tmp/apache-airflow-src

# Run the license check
java -jar /tmp/apache-rat-0.17/apache-rat-0.17.jar \
  --input-exclude-file /tmp/apache-airflow-src/.rat-excludes \
  /tmp/apache-airflow-src/ | grep -E "^\!|INFO:"
```

### Expected output
```
INFO: RAT summary:
INFO:   Approved:  15615
INFO:   Unapproved:  0
INFO:   Unknown:  0
```

Files with issues are marked with `!` prefix.

---

## Step 4: GPG Signature Verification

### What is it?
Verifies that packages were cryptographically signed by a trusted Apache Airflow Release Manager.

### Why does it matter?
- **Authentication** - Proves the Release Manager created these files
- **Integrity** - Proves files weren't modified after signing
- **Non-repudiation** - Signer can't deny they signed it

### GPG Version Issue
Older GPG versions (2.0.x) cannot verify newer key formats like Ed25519. If you encounter:
```
gpg: key 6B8EF080: no valid user IDs
gpg: Can't check signature: Invalid public key algorithm
```

Use Docker with a newer GPG:

```bash
docker run --rm -it -v ${PATH_TO_AIRFLOW_SVN}/${VERSION_RC}:/work ubuntu:22.04 bash -c "
  apt update && apt install -y gnupg curl
  curl https://dist.apache.org/repos/dist/release/airflow/KEYS | gpg --import
  cd /work
  for i in *.asc; do echo \"Checking \$i\"; gpg --verify \$i; done
"
```

### Expected output
```
gpg: Good signature from "Ephraim Anierobi <ephraimanierobi@apache.org>" [unknown]
```

The `[unknown]` trust warning is normal - it means you haven't personally verified the key owner's identity. The signature itself is valid.

---

## Step 5: SHA512 Checksum Verification

### What is it?
Confirms files weren't corrupted during download by comparing cryptographic hashes.

### How to do it

```bash
cd ${PATH_TO_AIRFLOW_SVN}/${VERSION_RC}

# On Linux (Amazon Linux, Ubuntu, etc.)
for i in *.sha512; do
    echo "Checking $i"
    sha512sum -c $i
done

# On macOS
for i in *.sha512; do
    echo "Checking $i"
    shasum -a 512 -c $i
done
```

Also verify Task SDK:
```bash
cd ${PATH_TO_AIRFLOW_SVN}/../task-sdk/${TASK_SDK_VERSION_RC}
for i in *.sha512; do
    echo "Checking $i"
    sha512sum -c $i
done
```

### Expected output
```
apache_airflow-3.1.7.tar.gz: OK
```

---

## Voting

After completing verification, reply to the vote email on `dev@airflow.apache.org`:

```
+1 (non-binding)

I verified the following on [your OS]:

Airflow/Airflow Core 3.1.7rc2:
- Reproducible package build: OK
- SVN artifacts match local build: OK
- Licenses (Apache RAT): OK
- Signatures: OK
- Checksums (SHA512): OK

Task SDK 1.1.7rc2:
- Reproducible package build: OK
- SVN artifacts match local build: OK
- Signatures: OK
- Checksums (SHA512): OK

No blocking issues found.
```

---

## Optional: Functional Testing

Beyond artifact verification, you can test the RC functionally:

```bash
# Start Airflow with RC version and providers
breeze start-airflow \
  --use-airflow-version 3.1.7rc2 \
  --python 3.10 \
  --backend postgres \
  --airflow-extras "amazon,google" \
  --load-default-connections
```

Then create test DAGs to verify specific functionality.

---

## Official Documentation References

- PMC verification: `dev/README_RELEASE_AIRFLOW.md` section "Verify the release candidate by PMC members"
- Contributor verification: `dev/README_RELEASE_AIRFLOW.md` section "Verify the release candidate by Contributors"
- Testing packages with Breeze: `contributing-docs/testing/testing_packages.rst`

---

## Troubleshooting

### GPG "Invalid public key algorithm"
Your GPG version is too old. Use Docker with Ubuntu 22.04 or upgrade GPG.

### `shasum` not found
On Amazon Linux/RHEL, use `sha512sum` instead of `shasum`.

### Build produces different packages
- Ensure you checked out the exact RC tag
- Ensure Docker is running (Breeze uses it for consistent builds)
- Check `reproducible_build.yaml` exists and has `source-date-epoch`

---

# Quick Summary

## What We Verified

| Step | Check | Purpose |
|------|-------|---------|
| 1 | Reproducible Build | Rebuilt packages from source - proves no tampering |
| 2 | SVN Comparison | Binary diff against official artifacts - proves match |
| 3 | Apache RAT | License headers on all files - legal compliance |
| 4 | GPG Signatures | Cryptographic proof of Release Manager identity |
| 5 | SHA512 Checksums | File integrity - no corruption |

## Key Commands Summary

```bash
# 1. Clone and build
git clone https://github.com/apache/airflow.git && cd airflow
git checkout 3.1.7rc2
breeze release-management prepare-airflow-distributions --distribution-format both

# 2. Get SVN artifacts
svn checkout --depth=immediates https://dist.apache.org/repos/dist asf-dist
svn update --set-depth=infinity asf-dist/dev/airflow

# 3. Compare (should show no diff)
diff asf-dist/dev/airflow/3.1.7rc2/apache_airflow-3.1.7.tar.gz dist/apache_airflow-3.1.7.tar.gz

# 4. License check
java -jar apache-rat-0.17.jar --input-exclude-file .rat-excludes /path/to/source

# 5. Signature check (in Docker if needed)
gpg --verify file.asc

# 6. Checksum
sha512sum -c *.sha512
```

## Time Investment
- Full PMC-level verification: ~1-2 hours
- Simple contributor verification (pip install + test): ~15 minutes

The reproducible build check is the most valuable - it mathematically proves the packages are legitimate, which is stronger than signature verification alone.
