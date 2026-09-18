# Trusted local application controls (experimental)

An application binding can supply local lifecycle controls, configuration, live observations and state inspection without putting application-specific behavior in StreamPlay. The operator selects and imports this code in a local launcher. The browser can invoke only the binding's declared action IDs; it cannot supply shell commands or module paths.

```js
import { workbench } from './src/server.js';
const application = {
  description: {
    name: 'My local application',
    configuration: {}, input: [],
    actions: [
      { id: 'start', label: 'Start application' },
      { id: 'apply', label: 'Apply configuration', input: 'configuration' },
      { id: 'feed', label: 'Send input', input: 'input' },
    ],
  },
  async snapshot() {
    return { status: 'ready', detail: 'Describe actual readiness here',
      inputs: [], outputs: [], logs: [], state: null };
  },
  async act(id, value) {
    // Validate value and dispatch a fixed application operation.
    // Return only after its declared completion condition holds.
    return { message: 'Action complete' };
  },
};
const server = workbench({ dataDir: '.streamplay', application,
  sample: () => currentScenario(), kafka: {} }, { local: applicationAdapter });
server.listen(4317, '127.0.0.1');
```

`description` may be a getter. Action input is omitted, `configuration`, or `input`; the latter two read their corresponding JSON editor. Optional result fields `configuration`, `input`, and `sample` update the editors after a successful action. The `sample` configuration option can be a function to provide the current application fixture on page reload.

The UI polls `GET /api/application` once per second and renders returned values as text. `POST /api/application` receives `{action, value}`. Actions serialize with each other and scenario execution. Snapshot reads remain available during startup and long actions. Existing localhost Host/Origin checks apply. The binding must validate its domain inputs, bound external calls, implement shutdown, and expose truthful failures. The server does not provide a sandbox for trusted binding code.

For queues with competing consumers, use one application-owned observer and subscribe scenario adapters to that observer. Closing a scenario adapter should detach its subscription, not consume or stop a second queue reader. Document filters, retained application state, acknowledgments, and evidence durability in run metadata. Clearing live presentation must not silently reset application state or erase saved runs.

The UI is optional: applications without a binding retain the existing scenario workbench. These controls are an experimental embedding interface, not a built-in service orchestrator or a general AWS connector. Lifecycle cleanup and application-specific state migrations remain the binding's responsibility.
