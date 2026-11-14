# Whop's RisingWave builds

This branch builds the RisingWave image Whop runs: an official RisingWave release plus a short list of patches. Nothing else is different. The image is built the same way as the official one (`docker/Dockerfile`, `CARGO_PROFILE=production`, as upstream's `ci/scripts/docker.sh`).

## Files

| File | What it is |
|---|---|
| `patches` | The base release and the patches, in order. |
| `assemble` | Clones the base release and commits each patch on top of it. |
| `.github/workflows/whop-build.yml` | Assembles, tags, builds and publishes an image. |

## The patch list

```
base v3.1.1

pr risingwavelabs/risingwave#27977 Source backfill no longer loses rows when its Kafka reader reaches a partition EOF
commit whopio/risingwave@<sha> A note
```

- `base` is the upstream release tag to start from. Change it to move to a new release.
- `pr <owner>/<repo>#<number>` applies the whole diff of a pull request (from any repository, open or merged) as one commit.
- `commit <owner>/<repo>@<sha>` applies one commit. For a change of our own, push it to a branch of this repository and list its commit.
- The text after the reference is a note for the release page.

`assemble` reports each line:

| Result | Meaning |
|---|---|
| `applied` | The patch is in the build. |
| `in base` | The base release already contains the change. The build goes on; remove the line. |
| `conflict` | The patch does not apply to the base. The build stops. Rebase the change onto the base release, push it as a commit to this repository and list that commit. |

The last commit of every build removes `.github/workflows`, so upstream's workflows never run in this repository. It does not change the binary.

Check a list before you build:

```bash
./assemble /tmp/risingwave-patched
```

It needs `gh` with access to the listed repositories.

## Build an image

Run **Build patched RisingWave** on this branch (Actions tab, or `gh workflow run whop-build.yml --repo whopio/risingwave --ref whop`). It:

1. assembles the patched tree and pushes it as tag `<base>-whop.<n>`, with the patch list as the tag message;
2. builds the image on an ARM runner (prod runs on ARM64 only), checks that it starts, and pushes `ghcr.io/whopio/risingwave:<tag>`;
3. publishes a release with the patch list.

The image is `linux/arm64` only. On an x86 machine, run it with emulation (`--platform linux/arm64`) or test with the official image of the base release.

`SELECT version()` on a running node shows the commit of that tag.

## Run it

Prod pins the image in the monorepo: `RISINGWAVE_IMAGE` in `infra/pulumi/src/risingwave/nodes.ts`. Set it to `ghcr.io/whopio/risingwave:<tag>`. To go back to the official image, set it back to `ghcr.io/risingwavelabs/risingwave:<base>`. This works because patches change behavior only.

## Rules for patches

- Keep a patch to behavior. Do not change files under `proto/`, meta migrations or how state is stored, so the official image of the same release can always replace ours.
- Each patch needs a test that fails without it.
