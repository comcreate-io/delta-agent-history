# Recorder development

This is a harness-independent coding history recorder with optional source adapters. Read README.md and
docs/compatibility.md before changing protocol behavior.

- Run `python3 -m unittest discover -s tests -v` after code changes.
- Keep all fixtures synthetic and all repository enrollment explicit.
- Never read harness/Delta private databases or provider credential stores. The
  linked legacy recorder's own ledgers are an explicit read-only recall source.
- Retain exact immutable replay, private visibility, and bounded read-only T3
  access for its optional adapter. Acknowledgement, cloud upload, and recoverability are separate claims.
- Do not update the supported recording contract range without reviewing the
  upstream schema and adding meaningful failure tests.
- Service generation must never activate a service or issue credentials.
- Generic ingress must not require T3. Retain legacy producer IDs, message IDs,
  pending JSONL bytes, state paths, and commands; never replay legacy data under
  the generic producer. Keep generic and legacy workers in separate state.
- Hooks enqueue locally without waiting for Delta. MCP is project-bound and
  read-only; tool access never implies complete passive transcript capture.
- Keep Linux/macOS paths and service formats portable. macOS unit CI does not
  establish live T3/Delta recording on a Mac.
- Do not include real user state or assistant authorship in commits or PRs.
