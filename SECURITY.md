# Security

Do not put tokens, private transcripts, recorder databases, or customer code in
public issues. Use GitHub's private vulnerability reporting on this repository
when available. If unavailable, open a minimal issue asking maintainers for a
private reporting channel without including exploit details or sensitive data.

This recorder uploads approved conversations and attached checkout contents to
Delta. Private visibility is cloud storage, not a local-only mode. Review the
privacy and compatibility notes in the setup guide before enabling it.

Local hook payloads are untrusted data. A source label does not authenticate a
producer or authorize enrollment. Keep source instance and repository allowlists
explicit. The stdio MCP server is project-bound and read-only; do not expose it
over a network without a separately reviewed authenticated service.

The filename check is a heuristic, not a content secret scanner. It does not
guarantee that an approved repository contains no secrets. Ignoring a tracked
file does not prevent its inclusion. Keep recorder state outside your checkout.

Support is limited to the documented T3 and Delta contracts. Keep the worker
stopped while changing credentials, approving new repositories, or investigating
a conflicting binding. Do not delete replay ledgers to bypass a safety hold.
