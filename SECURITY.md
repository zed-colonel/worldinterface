# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability in WorldInterface, please report it responsibly.

**Do not open a public GitHub issue for security vulnerabilities.**

Instead, please send a description of the vulnerability to the project maintainers
via a [GitHub Security Advisory](https://docs.github.com/en/code-security/security-advisories/guidance-on-reporting-and-writing-information-about-vulnerabilities/privately-reporting-a-security-vulnerability).

Include:
- A description of the vulnerability
- Steps to reproduce the issue
- Potential impact
- Any suggested fix, if available

We will acknowledge receipt within 48 hours and aim to provide a fix or mitigation
plan within 7 days for critical issues.

## Scope

WorldInterface is designed as an embedded/single-node workflow engine. The current
security posture assumes trusted-network deployment:

- Default bind: localhost only (`127.0.0.1:7800`)
- No built-in authentication on HTTP endpoints (expected to be fronted by a
  reverse proxy or service mesh in production)
- SQLite database and AQ WAL files should be protected by OS-level file permissions
- Filesystem connectors (`fs.read`, `fs.write`) accept arbitrary paths — access
  control should be enforced at the FlowSpec validation or runtime policy layer
- The HTTP connector (`http.request`) does not restrict target URLs — network-level
  controls or allowlists should be applied in production

## Supported Versions

Security updates are provided for the latest release only.
