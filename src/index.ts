import { existsSync } from 'fs';
import { availableParallelism } from 'os';
import { extname, resolve } from 'path';
import { Worker } from 'worker_threads';

import type { WorkerForEach$Entries, WorkerForEach$Status } from './Types';

const BASE_PATH = require?.main?.path || __dirname;

/**
 * Executes a given callback function for each element in the workerData array using multiple workers.
 *
 * @template T - The type of elements in the workerData array.
 * @param {Object} options - The options for the workerForEach function.
 * @param {string} options.workerFile - The file path of the worker script.
 * @param {T[] | Set<T> | Map<any, T>} options.workerData - The array of data to be processed by the workers.
 * @param {number} [options.workers=availableParallelism()] - The number of workers to be used for processing.
 * @param {boolean} [options.logging=false] - Specifies whether to enable logging.
 * @param {(index: number, data: any) => void | Promise<void>} [options.callback] - The callback function to be executed for each processed element.
 * @returns {Promise<void>} - A promise that resolves when all elements have been processed.
 *
 * @example
 * // index.ts
 * import { workerForEach } from '@energypatrikhu/node-workers';
 * await workerForEach({
 *     workerFile: 'workers/transformNumbers.ts',
 *     workerData: [10, 20, 30, 40, 50, 60, 70, 80, 90, 100], // Can be an Array, Set or Map
 *     workers: 2, // default is 4
 *     logging: true, // default is false
 *     callback: (data: number) => { // default is undefined
 *         console.log(data);
 *     }
 * });
 * console.log('done');
 *
 * // workers/transformNumbers.ts
 * import { workerData, parentPort } from 'worker_threads';
 * const { index, data } = workerData;
 *
 * if (!parentPort) {
 *     console.error('Not running in worker thread');
 *     process.exit(1);
 * }
 * console.log(`Worker ${index} started`);
 *
 * parentPort.postMessage(data * 10 * 100 ** 10);
 */
export function workerForEach<T>({
  workerFile,
  workerData,
  workers = availableParallelism(),
  logging = false,
  callback,
}: {
  workerFile: string;
  workerData: T[] | Set<T> | Map<any, T>;
  workers: number;
  logging: boolean;
  callback?: (index: number, data: any) => void | Promise<void>;
}): Promise<void> {
  return new Promise<void>((__resolve) => {
    const log = (...message: any[]) => {
      if (logging) console.log('[workerForEach]', ...message);
    };

    const __workerPath = getWorkerPath(workerFile);
    log(`Worker path: ${__workerPath}`);

    let entries: WorkerForEach$Entries<T>;

    // Convert array to entries with index and value
    if (workerData instanceof Map) {
      if (workerData.size === 0) {
        console.error('Array is empty (Array is a Map)');
        __resolve();
        return;
      }

      log('Converting Map to entries');

      entries = new Map(Array.from(workerData).map(([_, value], index) => [index, { status: 'pending', value }]));
    } else if (workerData instanceof Set) {
      if (workerData.size === 0) {
        console.error('Array is empty (Array is a Set)');
        __resolve();
        return;
      }

      log('Converting Set to entries');

      entries = new Map(Array.from(workerData).map((value, index) => [index, { status: 'pending', value }]));
    } else {
      if (workerData.length === 0) {
        console.error('Array is empty (Array is an Array)');
        __resolve();
        return;
      }

      log('Converting Array to entries');

      entries = new Map(workerData.map((value, index) => [index, { status: 'pending', value }]));
    }

    // Get next pending entry to process with index and value
    const getEntryByStatus = (status: WorkerForEach$Status) => {
      return Array.from(entries).find(([_, entry]) => entry.status === status);
    };

    // Get length of entries by state
    const getLengthByStatus = (status: WorkerForEach$Status) => {
      return Array.from(entries).filter(([_, entry]) => entry.status === status).length;
    };

    // Remove processed entry to free up memory
    const removeEntryByIndex = (index: number) => {
      log(`Removing entry index: ${index}`);
      if (entries.has(index)) entries.delete(index);
    };

    // Process entries
    const processEntries = () => {
      if (entries.size === 0 || getLengthByStatus('processing') === workers) return;

      const entry = getEntryByStatus('pending');
      if (!entry) return;

      const [entryIndex, entryData] = entry;

      log(`Processing entry index: ${entryIndex}`);

      entryData.status = 'processing';

      log(`Creating worker for entry index: ${entryIndex}`);
      const __worker = new Worker(__workerPath, {
        workerData: { index: entryIndex, data: entryData.value },
      });

      const processResponseHandler = (data: any, error: any) => {
        log(`Processing response for entry index: ${entryIndex}`);
        if (error) {
          console.error(error);
        }

        if (callback) {
          log(`Executing callback for entry index: ${entryIndex}`);
          callback(entryIndex, data);
        }

        removeEntryByIndex(entryIndex);
        if (entries.size === 0) {
          log('All entries processed, resolving promise');
          __resolve();
          return;
        }

        processEntries();
      };

      __worker.on('message', (data) => processResponseHandler(data, null));
      __worker.on('error', (error) => processResponseHandler(null, error));
      __worker.on('exit', (code) => {
        if (code !== 0) {
          processResponseHandler(null, new Error(`Worker stopped with exit code ${code}`));
        }
      });
    };

    log('Starting workerForEach');
    // Start processing entries with maxWorkers
    for (let i = 0; i < workers; i++) {
      processEntries();
    }
  });
}

/**
 * Executes a worker file with the given worker data and returns a promise that resolves with the result.
 *
 * @template T - The type of elements in the workerData array.
 * @param {string} workerFile - The path to the worker file.
 * @param {any} workerData - The data to be passed to the worker.
 * @returns {T} A promise that resolves with the result of the worker execution.
 *
 * @example
 * // index.ts
 * import { worker } from '@energypatrikhu/node-workers';
 *
 * const data = await worker('workers/transformNumbers.ts', [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]);
 * console.log(data);
 *
 * // workers/transformNumbers.ts
 * import { workerData, parentPort } from 'worker_threads';
 *
 * if (!parentPort) {
 *     console.error('Not running in worker thread');
 *     process.exit(1);
 * }
 * console.log('Worker started');
 *
 * let sum = 0;
 * for (const number of workerData) {
 *     sum += number;
 * }
 * parentPort.postMessage(sum);
 */
export function worker<T>(workerFile: string, workerData: any): Promise<T> {
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

// ---------------------------------------------------- //
// ----------------- Helper Functions ----------------- //
// ---------------------------------------------------- //

function getWorkerPath(workerFile: string) {
  const __filepath = workerFile.slice(0, -extname(workerFile).length);
  const __extension = extname(__filename).slice(1);
  let __workerPath = resolve(BASE_PATH, `${__filepath}.${__extension}`);

  if (existsSync(__workerPath)) {
    return __workerPath;
  }

  for (const extension of ['js', 'cjs', 'mjs'].filter((ext) => ext !== __extension)) {
    __workerPath = resolve(BASE_PATH, `${__filepath}.${extension}`);

    if (existsSync(__workerPath)) {
      return __workerPath;
    }
  }

  throw new Error(`Worker script not found: ${workerFile}`);
}
