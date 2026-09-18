# Contributing to StreamPlay

Welcome. StreamPlay is early, and useful contributions are often small: a realistic event fixture, a reproducible failure, a clearer raw-data view, or a test that catches a transport edge case.

1. Read the [README](README.md) and run the process quickstart.
2. For a bug, include the scenario (with private data removed), OS, Node version, execution adapter, and expected versus observed behavior.
3. Discuss larger changes in an issue before investing heavily. Pick a narrowly scoped task from the [roadmap](docs/ROADMAP.md).
4. Make your change on a branch. Run `npm run check` and `npm test`; run Playwright for UI changes and the Docker integration for Kafka/Flink changes.
5. Open a pull request describing the problem, resulting behavior, and what you verified. State validation you could not run.

No separate contributor license agreement is required at this stage. Contributions are submitted under Apache-2.0, the repository's license. You retain credit and copyright in your own contributions. Use your own GitHub-linked commit identity; don't attribute another person's work to yourself. AI-assisted contributions are welcome when you understand, review, and validate the result.

Keep fixtures synthetic or explicitly shareable. Never submit credentials, customer data, or employer/client code you cannot license. Treat other contributors respectfully; discuss technical disagreements with evidence. Report conduct concerns to info@revenirdata.com.

## Project boundaries

- Keep transformation code in the application's existing language and project.
- Keep raw records and execution assumptions visible.
- Separate transport integration from engine lifecycle.
- Distinguish observations, assertions, and incomplete captures.
- Avoid platform expansion until the first three integration paths are useful and reliable.

The lead maintainer is [@kc-salazar](https://github.com/kc-salazar); see [MAINTAINERS.md](MAINTAINERS.md).
