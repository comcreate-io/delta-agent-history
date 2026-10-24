# Recorder development

This is an independent T3 Code to Delta adapter. Read README.md and
docs/compatibility.md before changing protocol behavior.

- Run `python3 -m unittest discover -s tests -v` after code changes.
- Keep all fixtures synthetic and all repository enrollment explicit.
- Never read T3/Delta private databases or provider credential stores.
- Retain exact immutable replay, private visibility, and bounded read-only T3
  access. Acknowledgement, cloud upload, and recoverability are separate claims.
- Do not update the supported recording contract range without reviewing the
  upstream schema and adding meaningful failure tests.
- Service generation must never activate a service or issue credentials.
- Keep Linux/macOS paths and service formats portable. macOS unit CI does not
  establish live T3/Delta recording on a Mac.
- Do not include real user state or assistant authorship in commits or PRs.
