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

`description` may be a getter. Action input is omitted/`none`, `configuration`, `input`, or `controls`. The JSON options read their corresponding advanced editor; `controls` reads typed fields defined by the binding. Optional result fields `configuration`, `input`, `sample`, and `controls` update the corresponding editor or form after a successful action. The `sample` configuration option can be a function to provide the current application fixture on page reload.

For example, add these definitions to `description.controls`:

```js
[
  { id: 'deviceId', label: 'Source', type: 'select', optionsFrom: 'devices' },
  { id: 'temperature', label: 'Temperature (°C)', type: 'number', value: 21,
    min: -40, max: 120, step: 'any' },
  { id: 'intervalMs', label: 'Interval (ms)', type: 'number', value: 1000,
    min: 100, group: 'Emission settings' },
]
```

Controls support number, text, and select fields, optional values, fixed `options`, and collapsible groups. `optionsFrom: 'devices'` takes IDs from `snapshot().devices`. Polling preserves edited fields and selection. Browser validation supplements the binding's required server-side validation. Do not place credentials in form definitions or snapshots.

For a background operation, expose an `application.busy` getter until it finishes. Other actions and asserted scenarios are rejected while busy. A declared cancellation action may set `allowWhileBusy: true`; use `input: 'none'` so an unrelated invalid field cannot prevent cancellation. This exception applies to the binding's background operation, not a simultaneous HTTP action or StreamPlay scenario.

Snapshots may include `configuration`, `state`, `queues`, and `reports`, exposed in the application inspector and evidence export. Keep live capture and history summaries bounded; save complete reports separately if needed. On embedding shutdown, call `server.cancelActiveRun()` before waiting for `server.close()`, then close application observers and runtime resources.

The UI polls `GET /api/application` once per second and renders returned values as text. `POST /api/application` receives `{action, value}`. Actions serialize with each other and scenario execution. Snapshot reads remain available during startup and long actions. Existing localhost Host/Origin checks apply. The binding must validate its domain inputs, bound external calls, implement shutdown, and expose truthful failures. The server does not provide a sandbox for trusted binding code.

For queues with competing consumers, use one application-owned observer and subscribe scenario adapters to that observer. Closing a scenario adapter should detach its subscription, not consume or stop a second queue reader. Document filters, retained application state, acknowledgments, and evidence durability in run metadata. Clearing live presentation must not silently reset application state or erase saved runs.

The UI is optional: applications without a binding retain the existing scenario workbench. These controls are an experimental embedding interface, not a built-in service orchestrator or a general AWS connector. Lifecycle cleanup and application-specific state migrations remain the binding's responsibility.
