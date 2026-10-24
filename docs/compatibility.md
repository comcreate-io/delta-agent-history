# Compatibility and verification

This experimental bridge uses version-sensitive public interfaces. Python portability and live desktop integration are different claims.

## Supported baseline

| Component | Baseline | Boundary |
|---|---|---|
| Python | 3.11+ | Standard-library runtime, POSIX locking and private permissions. |
| Git | Installed CLI | Checkout identity and linked-worktree common-directory checks. |
| T3 Code | 0.1.0 | Supported read-only MCP and CLI session issuance. |
| Zed Delta | 0.21.0 | Recording contracts 6–7. Reject newer maximum until reviewed. |
| Linux | Live recording exercised | Generic Python packaging and per-user systemd tooling. |
| macOS | Portable implementation and CI target | Per-user launchd tooling; no live Mac desktop integration proof yet. |
| Windows | Unsupported | POSIX-only locking and service tooling. |

The bridge does not install or license T3 or Delta, grant account access, or make either available on unsupported hardware. [Delta's installation guide](https://delta.dev/docs/installation) currently requires macOS 13+ on Apple Silicon, or a glibc Linux system on x86_64/aarch64. In particular, portable Python code does not establish Intel Mac support for the upstream app. Check upstream requirements before setup. Nix packages this bridge; upstream apps remain separate prerequisites.

Do not silently widen accepted recording versions or transplant a newer producer's wire format. Upstream can change fields, errors, or receipt semantics. Protocol updates need synthetic tests, live exact-version verification, and a compatibility update.

## Evidence and limitations

The original Linux integration exercised private conversation creation, existing-checkout attachment, server acknowledgements, completed private upload, bounded transcript readback, restart-safe duplicate replay, and latest-state recovery in a disposable repository. Source and sink tests cover interrupted delivery, scope boundaries, errors, and immutable replay with synthetic fixtures.

CI tests deterministic behavior and service rendering on Linux and macOS. It does not sign into desktop apps or upload live histories. A green macOS job does not establish end-to-end Mac recording.

Conversation coverage excludes hidden reasoning, attachments, and raw tool outputs. Recording is prospective and file coverage begins after attachment. One recorded conversation is not complete project history.

Serial polling under heavy parallel activity can cause temporary stale/catching-up reports. T3 credentials expire after 30 days and require explicit renewal. This release has no automatic renewal or failure-notification service.

## Recovery

**Verified:** latest-state file recovery in a controlled Linux test into a separate Delta-managed checkout. This is a narrow proof, not a guarantee for every repository or platform.

**Unverified:** recovering an arbitrary earlier point through this bridge. Opaque receipt versions are not interchangeable with CLI historical-version arguments. The recorder intentionally exposes no automatic restore command.

For recovery, establish the exact repository, checkout, attachment period, and target state. Prepare a separate checkout or patch and compare before applying changes. Coordinate with active agents. Do not rewind a live checkout to test a backup. Restoring files does not rewind a T3 conversation.

## Report an issue

Include OS and architecture, Python/Git/T3/Delta versions, command, sanitized error type, and a minimal synthetic reproduction. Say separately whether recording, acknowledgement, upload, transcript readback, or recovery failed. Never attach tokens, credentials, private ledgers, client trees, session environments, or unsanitized transcripts.
