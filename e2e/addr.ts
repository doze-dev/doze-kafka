import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { IDENTITY, KAFKA_ADDR } from './playwright.config';

/** Where global-setup writes the address the broker bound, when it picked its own port. */
export const ADDR_FILE = join(process.cwd(), '.tmp', `kafka-addr-${process.env.E2E_PORT ?? 14092}`);

/** The broker's bootstrap address: fixed in the default run, discovered (127.0.0.1:0) in the identity run. */
export function kafkaAddr(): string {
  if (!IDENTITY) return KAFKA_ADDR;
  return readFileSync(ADDR_FILE, 'utf8').trim();
}
