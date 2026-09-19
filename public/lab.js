// SPDX-License-Identifier: Apache-2.0
import { sourcePresets, validateProfile, renderSource } from './source-profile.js';
export function initializeLab(description, { api, graph, config }) {
  if (!description) return;
  const $ = id => document.getElementById(id);
  $('lab').hidden = false;
  $('lab-name').textContent = description.name;
  let snapshot, pending = false, selected, profileLoaded = false;
  const pretty = value => JSON.stringify(value, null, 2);
  const download = (value, name) => { const url = URL.createObjectURL(new Blob([pretty(value)], { type: 'application/json' })); const link = document.createElement('a'); link.href = url; link.download = name; link.click(); setTimeout(() => URL.revokeObjectURL(url), 1000); };
  async function refresh() {
    snapshot = await api('lab');
    if (!profileLoaded) { $('lab-profile-json').value = pretty(snapshot.profile); profileLoaded = true; }
    $('lab-meter-controls').hidden = !snapshot.profile.accumulate;
    $('lab-total-controls').hidden = !snapshot.profile.accumulate;
    $('lab-status').textContent = snapshot.status;
    const selection = $('lab-device').value;
    const devices = snapshot.devices;
    if (devices.map(d => d.id).join() !== [...$('lab-device').options].map(o => o.value).join()) {
      $('lab-device').replaceChildren(...devices.map(d => new Option(d.id, d.id)));
      if (devices.some(d => d.id === selection)) $('lab-device').value = selection;
    }
    selected = devices.find(d => d.id === $('lab-device').value);
    $('lab-device-status').textContent = selected ? `${selected.active ? 'Sending' : 'Idle'} · value ${selected.rate} · ${selected.intervalMs} ms interval${snapshot.profile.accumulate ? ` · total ${selected.total.toFixed(4)}` : ''} · phase: ${selected.phase} · ${selected.sent} published` : 'Add a source';
    $('lab-fleet-summary').textContent = devices.map(d => `${d.id}: ${d.phase}, ${d.sent} published${snapshot.profile.accumulate ? `, total ${d.total.toFixed(4)}` : ''}`).join('\n');
    $('lab-episode-report').textContent = pretty(selected?.report ?? { status: 'No episode yet' });
    $('lab-queue-state').textContent = pretty(snapshot.queues ?? { status: 'No queue inspector configured' });
    $('lab-report').textContent = pretty(snapshot.report ?? { status: 'No regression report yet', checks: description.checks });
    $('lab-state').textContent = pretty(snapshot.state);
    $('lab-logs').textContent = snapshot.logs.join('\n');
    $('lab-inputs').textContent = pretty(snapshot.inputs);
    $('lab-outputs').textContent = pretty(snapshot.outputs);
    for (const button of $('lab').querySelectorAll('button[data-lab-action]')) button.disabled = pending;
    const topology = config.topology ? { ...config.topology, nodes: config.topology.nodes.map(n => n.kind === 'source' ? { ...n, identityPath: snapshot.profile.identityPath } : n) } : null;
    graph.update({ config: { ...config, topology }, snapshot });
  }
  async function act(action, value = {}) {
    pending = true; $('lab-message').textContent = `${action}…`;
    try { const response = await api('lab', { action, value }); $('lab-message').textContent = response.result?.message ?? `${action} completed`; await refresh(); }
    catch (error) { $('lab-message').textContent = error.message; }
    finally { pending = false; for (const button of $('lab').querySelectorAll('button[data-lab-action]')) button.disabled = false; }
  }
  $('lab-device').onchange = () => { const d = snapshot.devices.find(d => d.id === $('lab-device').value); $('lab-rate').value = d.rate; $('lab-interval').value = d.intervalMs; void refresh(); };
  const id = () => $('lab-device').value;
  $('lab-preset').onchange = () => { $('lab-profile-json').value = pretty(sourcePresets[$('lab-preset').value]); showPreview(); };
  function showPreview() {
    try { const profile = validateProfile(JSON.parse($('lab-profile-json').value)); $('lab-profile-preview').textContent = pretty(renderSource(profile, { id: id() || 'source-001', eventId: 'preview-only', timestamp: 'preview timestamp', index: 0, value: Number($('lab-rate').value), total: 0 })); }
    catch (error) { $('lab-profile-preview').textContent = error.message; }
  }
  $('lab-profile-preview-button').onclick = showPreview;
  $('lab-profile-apply').onclick = () => { try { void act('profile', validateProfile(JSON.parse($('lab-profile-json').value))); } catch (error) { $('lab-message').textContent = error.message; } };
  $('lab-profile-from-event').onclick = () => {
    try {
      const template = JSON.parse($('events').value)[0];
      const path = $('lab-field-path').value, kind = $('lab-field-kind').value;
      const field = { path, kind };
      if (kind === 'random') { field.min = Number($('lab-field-min').value); field.max = Number($('lab-field-max').value); }
      const profile = validateProfile({ id: 'custom-events', label: 'Custom event source', template, identityPath: $('lab-identity-path').value,
        fields: [field], accumulate: false, seed: 42 });
      $('lab-profile-json').value = pretty(profile); showPreview();
    } catch (error) { $('lab-message').textContent = error.message; }
  };
  $('lab-add').onclick = () => act('add');
  $('lab-apply').onclick = () => act('configure-device', { id: id(), rate: Number($('lab-rate').value), intervalMs: Number($('lab-interval').value) });
  $('lab-total-set').onclick = () => act('set-total', { id: id(), total: Number($('lab-total').value) });
  $('lab-send').onclick = () => act('send', { id: id() });
  $('lab-start').onclick = () => act('start', { id: id(), durationMs: Number($('lab-session').value) * 1000 });
  $('lab-stop').onclick = () => act('stop', { id: id() });
  $('lab-episode').onclick = () => {
    const value = { id: id(), baselineMs: Number($('lab-baseline').value), stopMs: Number($('lab-stop-ms').value), durationMs: Number($('lab-duration').value) };
    if ($('lab-target').value.trim()) value.targetQuantity = Number($('lab-target').value);
    if ($('lab-raw').value.trim()) { value.rawRate = Number($('lab-raw').value); value.unitsPerRawUnit = Number($('lab-conversion').value); }
    void act('episode', value);
  };
  $('lab-export').onclick = () => download(snapshot, 'streamplay-lab-evidence.json');
  $('lab-plan-export').onclick = () => download({ version: 1, profile: snapshot.profile, device: { rate: selected.rate, intervalMs: selected.intervalMs }, episode: selected.report?.phases }, 'streamplay-device-plan.json');
  $('lab-plan-import').onchange = async () => {
    try {
      const file = $('lab-plan-import').files[0]; if (!file || file.size > 100000) throw new Error('Choose a plan under 100 KB.');
      const value = JSON.parse(await file.text());
      if (value.version !== 1 || !value.device || !Array.isArray(value.episode) || value.episode.some(p => !['baseline','active','stop'].includes(p.name) || p.kind !== 'emit' || !Number.isFinite(p.durationMs))) throw new Error('Import a device episode plan exported by StreamPlay.');
      $('lab-rate').value = value.device.rate; $('lab-interval').value = value.device.intervalMs;
      if (value.profile) $('lab-profile-json').value = pretty(validateProfile(value.profile));
      $('lab-baseline').value = value.episode.find(p => p.name === 'baseline')?.durationMs ?? 0;
      $('lab-stop-ms').value = value.episode.find(p => p.name === 'stop')?.durationMs ?? 0;
      $('lab-duration').value = value.episode.find(p => p.name === 'active')?.durationMs ?? 1000;
      $('lab-raw').value = value.episode.find(p => p.name === 'active')?.value ?? value.device.rate;
      $('lab-conversion').value = 1; $('lab-target').value = '';
      $('lab-message').textContent = 'Plan imported. Apply the source profile and device settings, then start the episode. Stored total is retained; nothing starts automatically.';
    } catch (error) { $('lab-message').textContent = error.message; }
  };
  for (const [element, action] of [['lab-pause','pause-consumer'],['lab-resume','resume-consumer'],['lab-queue-check','queue-check'],['lab-checks','checks'],['lab-clear','clear']]) $(element).onclick = () => act(action);
  $('lab-configuration-apply').onclick = () => { try { void act('configure-application', JSON.parse($('lab-configuration').value)); } catch (error) { $('lab-message').textContent = error.message; } };
  $('lab-configuration-panel').hidden = !description.configuration;
  $('lab-queues').hidden = !description.queues;
  let refreshing = false;
  const poll = async () => { if (refreshing) return; refreshing = true; try { await refresh(); } catch (error) { $('lab-message').textContent = error.message; } finally { refreshing = false; } };
  void poll(); setInterval(poll, 1000);
}
