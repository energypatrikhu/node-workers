import { existsSync } from 'fs';
import { availableParallelism } from 'os';
import { extname, resolve } from 'path';
import { Worker } from 'worker_threads';

const BASE_PATH = require?.main?.path || __dirname;
let DEBUG = false;

const log = (...message: Array<any>) => {
  if (DEBUG) console.log('[parallel-workers]', ...message);
};

/**
 * Switches debug mode.
 *
 * @param {boolean} value - A boolean value to enable or disable debug mode.
 */
export const debugMode = (value: boolean) => {
  DEBUG = value;
};

function getWorkerPath(workerFile: string) {
  const __filepath = workerFile.slice(0, -extname(workerFile).length);
  const __extension = extname(__filename);
  let __workerPath = resolve(BASE_PATH, `${__filepath}${__extension}`);

  if (existsSync(__workerPath)) {
    return __workerPath;
  }

  for (const extension of ['js', 'cjs', 'mjs']) {
    __workerPath = resolve(BASE_PATH, `${__filepath}.${extension}`);

    if (existsSync(__workerPath)) {
      return __workerPath;
    }
  }

  throw new Error(`Worker script not found: ${workerFile}`);
}

/**
 * Executes a worker function for each element in the array in parallel.
 *
 * @template T - The type of elements in the array.
 * @param {string} workerFile - The file path of the worker script.
 * @param {T[] | Set<T> | Map<any, T>} array - The array of elements to process.
 * @param {(data: any) => void} [callback] - An optional callback function to execute when an element is processed.
 * @param {{ maxWorkers?: number }} [options] - Optional configuration options.
 * @returns {Promise<void>} A promise that resolves when all elements have been processed.
 *
 * @example
 * // index.ts
 * import { workerForEach } from '@energypatrikhu/node-workers';
 * await workerForEach(
 *      'workers/transformNumbers.ts',
 *      [10, 20, 30, 40, 50, 60, 70, 80, 90, 100],
 *      (data: number) => {
 *          console.log(data);
 *      },
 * );
 *
 *	console.log('done');
 *
 * // workers/transformNumbers.ts
 * import { workerData, parentPort } from 'worker_threads';
 * const { index, data } = workerData;
 * parentPort?.postMessage(data * 10 * 100 ** 10);
 */
export function workerForEach<T>(
  workerFile: string,
  array: T[] | Set<T> | Map<any, T>,
  callback?: (data: any) => void,
  options?: { maxWorkers?: number },
): Promise<void> {
  return new Promise<void>((__resolve) => {
    const __workerPath = getWorkerPath(workerFile);

    const __maxWorkers = options?.maxWorkers || availableParallelism();

    log(`Using ${__maxWorkers} workers`);

    let entries: Set<{ index: number; state: string; value: T }>;

    // Transform into a 'Set' with index, state and value to indicate if its processing
    if (array instanceof Map) {
      entries = new Set<{ index: number; state: string; value: T }>(
        [...array.entries()].map(([index, value]) => ({
          index,
          state: 'pending',
          value,
        })),
      );
    } else if (array instanceof Set) {
      entries = new Set<{ index: number; state: string; value: T }>(
        [...array].map((value, index) => ({
          index,
          state: 'pending',
          value,
        })),
      );
    } else {
      entries = new Set<{ index: number; state: string; value: T }>(
        array.map((value, index) => ({
          index,
          state: 'pending',
          value,
        })),
      );
    }

    // Get next pending entry to process with index and value
    const getNextPending = () => [...entries.values()].find((entry) => entry.state === 'pending');

    // Get length of entries by state
    const getLengthByState = (state: string) => [...entries.values()].filter((entry) => entry.state === state).length;

    // Remove processed entry to free up memory
    const remove = (index: number) => {
      log('Removing entry');

      const entry = [...entries.values()].find((entry) => entry.index === index);
      if (entry) entries.delete(entry);

      // Resolve promise if all entries are processed
      if (entries.size === 0) __resolve();
    };

    // Process entries
    const processEntries = () => {
      if (entries.size === 0 || getLengthByState('processing') === __maxWorkers) return;

      const entry = getNextPending();
      if (!entry) return;

      entry.state = 'processing';

      log('Starting worker');
      const worker = new Worker(__workerPath, {
        workerData: { index: entry.index, data: entry.value },
      });

      const processResponseHandler = (data: any, error: any) => {
        log('Worker exited!');

        remove(entry.index);
        processEntries();

        if (error) {
          console.error(error);
        }

        if (callback) {
          log('Returning data');
          callback(data);
        }
      };

      worker.on('message', (data) => processResponseHandler(data, null));
      worker.on('error', (error) => processResponseHandler(null, error));
      worker.on('exit', (code) => {
        if (code !== 0) {
          processResponseHandler(null, code);
        }
      });
    };

    // Start processing entries with maxWorkers
    for (let i = 0; i < __maxWorkers; i++) {
      processEntries();
    }
  });
}

/**
 * Executes a worker function with the provided data.
 *
 * @param {string} workerFile - The file path of the worker script.
 * @param {any} workerData - The data to pass to the worker script.
 * @returns {Promise<any>} A promise that resolves with the data returned by the worker script.
 *
 * @example
 * // index.ts
 * import { worker } from '@energypatrikhu/node-workers';
 * const result = await worker('workers/transformNumber.ts', 100);
 * console.log(result);
 *
 * // workers/transformNumber.ts
 * import { workerData, parentPort } from 'worker_threads';
 * parentPort?.postMessage(workerData * 10);
 */
export function worker<T>(workerFile: string, workerData: T): Promise<T> {
  return new Promise((__resolve, __reject) => {
    const __workerPath = getWorkerPath(workerFile);

    const __worker = new Worker(__workerPath, { workerData });
    __worker.on('message', __resolve);
    __worker.on('error', __reject);
    __worker.on('exit', (code) => {
      if (code !== 0) {
        __reject(new Error(`Worker stopped with exit code ${code}`));
      }
    });
  });
}
