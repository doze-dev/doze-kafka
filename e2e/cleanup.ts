import type { ChildProcess } from 'node:child_process';
import { rmSync } from 'node:fs';

// Removing a directory a broker was using. Two things made it fail with ENOTEMPTY: the
// broker was told to stop (SIGTERM) and the directory removed at once, while it was
// still writing into it; and a directory removed while anything is still closing a file
// in it can lose that race even after the process has exited. So wait for the process
// to be gone (killing it hard after a grace period), then remove with retries.

/** Stops a child and resolves when it has exited. */
export async function stop(child: ChildProcess | undefined, graceMs = 5000): Promise<void> {
  if (!child || child.exitCode !== null || child.signalCode !== null) return;
  await new Promise<void>((done) => {
    const hard = setTimeout(() => child.kill('SIGKILL'), graceMs);
    child.once('exit', () => { clearTimeout(hard); done(); });
    child.kill('SIGTERM');
  });
}

/** Removes a directory tree, trying again when something is still closing a file in it. */
export function removeDir(dir: string): void {
  rmSync(dir, { recursive: true, force: true, maxRetries: 10, retryDelay: 200 });
}

/** Stops the child, then removes the directory it used. */
export async function stopAndRemove(child: ChildProcess | undefined, dir: string): Promise<void> {
  await stop(child);
  removeDir(dir);
}
