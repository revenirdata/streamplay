// SPDX-License-Identifier: Apache-2.0
// Edit this file in your editor, then run the scenario again.
// A fresh Node.js process executes this code each time. This is not Flink.
import { createInterface } from 'node:readline';

if (process.send) process.send({ type: 'ready' }, () => process.disconnect());

for await (const line of createInterface({ input: process.stdin })) {
  try {
    const event = JSON.parse(line);
    if (!event || typeof event.order_id !== 'string' || !Number.isInteger(event.quantity) || !Number.isInteger(event.unit_price_cents)) {
      throw new Error('Expected order_id, integer quantity, and integer unit_price_cents.');
    }
    if (event.quantity <= 0) continue;
    // Deliberately preserves duplicates so repeated events remain visible.
    console.log(JSON.stringify({ order_id: event.order_id, total_cents: event.quantity * event.unit_price_cents }));
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}
