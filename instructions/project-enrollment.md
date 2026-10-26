## New project Delta enrollment

When creating, cloning or starting substantial work in a Git project, use the
installed `delta-enroll` skill or run
`delta-history enroll --project /absolute/repository/root --check`.
Use the actual canonical Git root and the operator's selected config.

If already enrolled, run `delta-history enroll --project /absolute/repository/root`
to add/update managed Delta instructions in `AGENTS.md` and `CLAUDE.md` while
preserving other content. For a new repository, add `--accept-upload` only if the
operator has authorized this project's private Delta conversation and checkout
uploads. Existing instructions, skill installation and a matching remote do not
grant that consent to another user, machine or clone.

If consent, installation or initial config is missing, report that requirement
and continue unrelated coding work. Do not reinitialize an existing config,
invent source identities, issue credentials or edit declaratively managed files.
Honor filename, origin and checkout review holds. Use the configured capture
integration; version 0.3 workers reload approved project scope between passes.
T3 shares new enrollment only when both configs explicitly select the same
project_enrollment_file; its original recordings and credentials remain separate.
Enrollment is local setup, not proof of capture, upload or historical recovery.
