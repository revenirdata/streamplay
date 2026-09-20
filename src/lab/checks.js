// SPDX-License-Identifier: Apache-2.0
import { spawn } from 'node:child_process';

// Definitions are trusted startup code. No browser-supplied command, path, or arguments.
export async function runChecks(definitions = [], { signal } = {}) {
  const report = { startedAt: new Date().toISOString(), status: definitions.length ? 'passed' : 'not-configured', checks: [] };
  for (const spec of definitions) {
    signal?.throwIfAborted();
    if (!spec.name || typeof spec.command !== 'string' || !Array.isArray(spec.args) || !Number.isInteger(spec.timeoutMs) || spec.timeoutMs < 1 || spec.timeoutMs > 120000 || typeof spec.successPattern !== 'string' || !spec.successPattern) throw new Error('Trusted checks require name, command, args, bounded timeoutMs and an explicit successPattern.');
    const successPattern = new RegExp(spec.successPattern, 'm');
    const result = await new Promise(resolve => {
      let output = '', done = false, timer, timedOut = false;
      const child = spawn(spec.command, spec.args, { cwd: spec.cwd, shell: false, windowsHide: true, detached: process.platform !== 'win32', env: { ...process.env, ...spec.env }, stdio: ['ignore', 'pipe', 'pipe'] });
      const finish = result => { if (done) return; done = true; clearTimeout(timer); signal?.removeEventListener('abort', terminate); resolve({ name: spec.name, ...result, output }); };
      const capture = chunk => { output = (output + chunk.toString()).slice(-50000); };
      child.stdout.on('data', capture); child.stderr.on('data', capture);
      child.on('error', error => finish({ passed: false, error: error.message }));
      child.on('close', code => finish({ code, timedOut, cancelled: Boolean(signal?.aborted), passed: !timedOut && !signal?.aborted && code === 0 && successPattern.test(output) }));
      function terminate() {
        if (process.platform === 'win32' && child.pid) { const killer = spawn('taskkill', ['/PID', String(child.pid), '/T', '/F'], { windowsHide: true, stdio: 'ignore' }); killer.on('error', () => child.kill('SIGKILL')); }
        else if (child.pid) { try { process.kill(-child.pid, 'SIGKILL'); } catch { child.kill('SIGKILL'); } }
      }
      signal?.addEventListener('abort', terminate, { once: true });
      timer = setTimeout(() => { timedOut = true; terminate(); }, spec.timeoutMs);
    });
    report.checks.push(result); if (!result.passed) report.status = 'failed';
  }
  report.finishedAt = new Date().toISOString(); return report;
}
