# StreamPlay CLI

The CLI and browser operate on the same versioned JSON artifacts. Use the terminal for repeatable execution and CI; use the browser to design experiments and inspect evidence. `splay` is the short cross-platform command; `streamplay` is the readable form. The package also installs `sp`, but Windows PowerShell reserves that name for `Set-ItemProperty`; use `splay` there (`sp.cmd` also reaches the executable explicitly).

```text
splay start
splay lab [adapter]
splay run <scenario.json>
splay suite <suite.json>
splay plan <experiment.json>
splay simulate <fleet.json> [--allow-remote]
splay example <kafka-flink|kafka-streams|kinesis-flink> <up|down|reset>
```

From a repository checkout, use `npm run streamplay -- <command>`. During local development, `npm link` creates `splay`, `sp`, and `streamplay`. The package is still private and has no published npm release.

Examples:

```sh
npm run streamplay -- run examples/scenarios/orders.process.json
npm run streamplay -- suite examples/suites/orders.process.json
npm run streamplay -- plan examples/experiments/streaming-smoke.json
npm run streamplay -- simulate examples/simulations/mqtt-fleet.local.json
```

Commands preserve the exit status of the underlying runner. Scenario and suite artifacts remain suitable for source control; run evidence remains local under `.streamplay/` unless explicitly exported.
