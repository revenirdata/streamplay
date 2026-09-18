# Security

This is an early local development tool. It binds to `127.0.0.1` and validates browser request origins and host headers. It has no multi-user authentication and is not designed to be exposed through a public proxy.

Use isolated development topics and non-sensitive fixtures. Run snapshots contain payloads and local adapter logs. `.streamplay/` is excluded from Git, but files remain on your computer.

Please report vulnerabilities privately to info@revenirdata.com with “StreamPlay security” in the subject. Do not include secrets or private event payloads in a public issue.
