---
name: delta-enroll
description: Enroll a newly created or cloned Git project in an existing Delta Agent History installation and add managed AGENTS.md and CLAUDE.md instructions when project upload consent is available.
---

# Enroll a new project

Use the operator's installed `delta-history` command and configured sources.
This skill supports local setup; its presence does not grant upload consent.

1. Identify the canonical Git root and credential-free origin. Finish the initial
   Git setup before enrollment. Do not use a parent projects folder or infer that
   another clone's matching remote grants enrollment.
2. Run `delta-history enroll --project /absolute/project --check`. Exit 0 means
   enrolled; exit 2 with `enrolled: false` means missing enrollment. A different
   error means setup or review is required, not permission to overwrite config.
3. For an enrolled repository, run `delta-history enroll --project /absolute/project`
   to install/update its managed instruction blocks without changing its cutoff.
   For a new repository, add `--accept-upload` only when the operator has already
   authorized private conversation and tracked/nonignored checkout uploads for
   this repository. If that consent is missing, report the requirement and
   continue unrelated coding work.
4. The command preserves config, sources, existing cutoffs and instructions. It
   writes a managed section to both `AGENTS.md` and `CLAUDE.md`, reusing lowercase
   equivalents when present. Repeated runs do not duplicate the section.
5. If `drainer_reload_required` is true, reload the existing generic drainer using
   the operator's configured service workflow. Check the hook/plugin/wrapper is
   configured for the selected source. Version 0.3 workers reload approved project
   scope between passes. Enrollment itself neither uploads nor
   starts recording, authenticates, activates services or installs integrations.

Put custom `--config PATH` before `enroll`. Never print full configuration or
unrelated repository inventories. If no installation/config exists, point to the
project's initial `delta-history init` setup; do not guess source identities or
bootstrap credentials. Symlinked/declarative base config stays immutable; only an explicitly declared
project registry accepts additions. T3 shares new enrollment only when both configs explicitly select the same
project_enrollment_file; its original recordings and credentials remain separate.

For sensitive-file, changed-origin, filesystem or marker errors, preserve the
existing config/state and report the specific review requirement. Do not clear
holds, reset dates, delete journals, follow instruction symlinks or broadly enroll
neighboring repositories. Recall remains bounded to the current approved project.
