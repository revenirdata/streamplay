// SPDX-License-Identifier: Apache-2.0
// Form definitions come from the trusted startup adapter, never from imported scenarios.
export function createApplicationControls(root, definitions = []) {
  const controls = new Map(), groups = new Map();
  for (const spec of definitions) {
    if (!/^[A-Za-z]\w*$/.test(spec.id) || controls.has(spec.id)) throw new Error('Invalid application control identifier.');
    let group = groups.get(spec.group ?? 'Source controls');
    if (!group) {
      const details = document.createElement('details'), summary = document.createElement('summary');
      summary.textContent = spec.group ?? 'Source controls'; details.open = groups.size === 0;
      group = document.createElement('div'); group.className = 'application-controls-grid';
      details.append(summary, group); root.append(details); groups.set(spec.group ?? 'Source controls', group);
    }
    const wrapper = document.createElement('div'), label = document.createElement('label');
    const input = document.createElement(spec.type === 'select' ? 'select' : 'input');
    input.id = `application-control-${spec.id}`; label.htmlFor = input.id; label.textContent = spec.label;
    if (spec.type !== 'select') { input.type = spec.type === 'number' ? 'number' : 'text'; input.maxLength = 200; }
    for (const name of ['min', 'max', 'step']) if (spec[name] !== undefined) input[name] = spec[name];
    for (const option of spec.options ?? []) input.add(new Option(option.label ?? option.value, option.value));
    input.value = spec.value ?? ''; wrapper.append(label, input); group.append(wrapper);
    controls.set(spec.id, { spec, input });
  }
  return {
    read() {
      return Object.fromEntries([...controls].flatMap(([id, { spec, input }]) => {
        if (input.value === '' && spec.optional) return [];
        if (input.value === '' || !input.checkValidity()) throw new Error(`Check ${spec.label}.`);
        return [[id, spec.type === 'number' ? Number(input.value) : input.value]];
      }));
    },
    update(snapshot) {
      for (const { spec, input } of controls.values()) {
        if (spec.optionsFrom !== 'devices') continue;
        const ids = (snapshot.devices ?? []).map(device => device.id);
        if (ids.join() === [...input.options].map(option => option.value).join()) continue;
        const previous = input.value;
        input.replaceChildren(...ids.map(id => new Option(id, id)));
        if (ids.includes(previous)) input.value = previous;
      }
    },
    fill(values) { for (const [id, { input }] of controls) if (values[id] !== undefined) input.value = values[id]; }
  };
}
