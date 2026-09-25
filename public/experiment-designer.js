// SPDX-License-Identifier: Apache-2.0
import { buildMqttFleetProfile, estimateKinesisCapacity, experimentCoverage, experimentPresets, validateExperimentPlan } from './experiment-plan.js';

export function initializeExperimentDesigner() {
  const $ = id => document.getElementById(id);
  const pretty = value => JSON.stringify(value, null, 2);
  const fields = {
    entities: ['workload', 'entities'], rate: ['workload', 'messagesPerSecondPerEntity'], duration: ['workload', 'durationSeconds'],
    payload: ['workload', 'payloadBytes'], burst: ['workload', 'burstMultiplier'], partitions: ['partitioning', 'partitions'],
    hotKey: ['partitioning', 'hotKeyPercent'], utilization: ['capacity', 'targetUtilizationPercent']
  };
  let plan = experimentPresets.smoke;
  const formatRate = value => value >= 1000 ? `${(value / 1000).toFixed(value >= 10_000 ? 0 : 1)}k/s` : `${Number(value.toFixed(2)).toLocaleString()}/s`;
  const formatBytes = value => { const units = ['B', 'KB', 'MB', 'GB', 'TB']; let amount = value, index = 0; while (amount >= 1000 && index < units.length - 1) { amount /= 1000; index++; } return `${amount.toFixed(amount >= 10 || index === 0 ? 0 : 1)} ${units[index]}`; };
  const download = (value, name) => { const url = URL.createObjectURL(new Blob([pretty(value) + '\n'], { type: 'application/json' })); const link = document.createElement('a'); link.href = url; link.download = name; link.click(); setTimeout(() => URL.revokeObjectURL(url), 1000); };

  function render() {
    const result = estimateKinesisCapacity(plan);
    $('experiment-average-rate').textContent = formatRate(result.workload.averageRecordsPerSecond);
    $('experiment-peak-rate').textContent = formatRate(result.workload.peakRecordsPerSecond);
    $('experiment-volume').textContent = formatBytes(result.workload.totalPayloadBytes);
    $('experiment-shards').textContent = result.recommendation.shards.toLocaleString();
    $('experiment-pressure').textContent = `${Math.max(result.configured.recordUtilizationPercent, result.configured.byteUtilizationPercent).toFixed(1)}%`;
    $('experiment-capacity-note').textContent = result.warnings.length ? result.warnings.join(' ') : `Configured capacity stays within the ${plan.capacity.targetUtilizationPercent}% planning target.`;
    $('experiment-capacity-note').dataset.state = result.warnings.length ? 'warning' : 'ready';
    $('experiment-result-json').textContent = pretty(result); $('experiment-json').value = pretty(plan);
  }
  function fill(value) {
    plan = validateExperimentPlan(value); $('experiment-name').value = plan.name;
    for (const [id, [section, key]] of Object.entries(fields)) $(`experiment-${id}`).value = plan[section][key];
    render();
  }
  function readEssentials() {
    const next = structuredClone(plan); next.name = $('experiment-name').value;
    for (const [id, [section, key]] of Object.entries(fields)) next[section][key] = Number($(`experiment-${id}`).value);
    return validateExperimentPlan(next);
  }
  $('experiment-preset').onchange = () => fill(experimentPresets[$('experiment-preset').value]);
  $('experiment-calculate').onclick = () => { try { plan = readEssentials(); render(); $('experiment-message').textContent = 'Plan updated. No traffic was sent.'; } catch (error) { $('experiment-message').textContent = error.message; } };
  $('experiment-apply-json').onclick = () => { try { fill(JSON.parse($('experiment-json').value)); $('experiment-message').textContent = 'Advanced plan applied. No traffic was sent.'; } catch (error) { $('experiment-message').textContent = error.message; } };
  $('experiment-export').onclick = () => download(plan, 'streamplay-experiment.json');
  $('experiment-export-evidence').onclick = () => download({ plan, capacity: estimateKinesisCapacity(plan), coverage: experimentCoverage }, 'streamplay-capacity-plan.json');
  $('experiment-export-mqtt').onclick = () => { try { const fleet = buildMqttFleetProfile(plan); download(fleet, 'streamplay-mqtt-fleet-plan.json'); $('experiment-message').textContent = fleet.unsupported.length ? `MQTT profile exported. Still needs adapters for: ${fleet.unsupported.join(', ')}.` : 'MQTT profile exported.'; } catch (error) { $('experiment-message').textContent = error.message; } };
  $('experiment-import').onchange = async () => { try { const file = $('experiment-import').files[0]; if (!file || file.size > 200_000) throw new Error('Choose an experiment JSON file under 200 KB.'); fill(JSON.parse(await file.text())); $('experiment-message').textContent = 'Experiment imported. No traffic was sent.'; } catch (error) { $('experiment-message').textContent = error.message; } };
  $('experiment-coverage').replaceChildren(...experimentCoverage.map(item => {
    const article = document.createElement('article'); article.dataset.status = item.status;
    const heading = document.createElement('div'), title = document.createElement('h3'), badge = document.createElement('span'); title.textContent = item.category; badge.textContent = item.status; heading.append(title, badge);
    const controls = document.createElement('p'), evidence = document.createElement('small'); controls.textContent = item.controls; evidence.textContent = item.evidence;
    article.append(heading, controls, evidence); return article;
  }));
  fill(plan);
}
