export type WorkerForEach$Status = 'pending' | 'processing';

export type WorkerForEach$Entries<T> = Map<number, { status: WorkerForEach$Status; value: T }>;
