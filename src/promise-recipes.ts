

/** Options for parallel promise utilities. */
export interface ParallelOptions {
    /** Passed to each callback; use to cancel in-flight work. */
    signal?: AbortSignal;
    /** Maximum number of callbacks running at once. Defaults to 10. */
    concurrency?: number;
}

const DEFAULT_CONCURRENCY = 10;

/**
 * Runs async callbacks from an iterable with bounded concurrency.
 *
 * Results are returned in input order, not completion order. When the input
 * is a small array (length ≤ concurrency), uses `Promise.all` directly.
 *
 * @param callbacks - Async functions to run; each receives the optional abort signal.
 * @param options - Concurrency limit and optional abort signal shared by all callbacks.
 */
export async function processParallel<T>(
    callbacks: Iterable<(signal?: AbortSignal) => Promise<T>>,
    options?: ParallelOptions
): Promise<T[]> {
    const { signal, concurrency = DEFAULT_CONCURRENCY } = options ?? {};

    if (Array.isArray(callbacks) && callbacks.length <= concurrency) {
        return Promise.all(callbacks.map((callback) => callback(signal)));
    }

    const iterator = mapIter(callbacks, (callback, index) => [index, callback] as const)[Symbol.iterator]();
    const results: (T | null)[] = [];

    async function doStep() {
        const iterResult = iterator.next();
        if (iterResult.done) {
            return null;
        }
        const [index, callback] = iterResult.value;
        results.push(null);
        const result = await callback(signal);
        results[index] = result;
        return doStep;
    }

    async function worker() {
        let stepResult = await doStep();
        while (stepResult) {
            stepResult = await stepResult();
        }
    }

    const workers = Array.from({ length: concurrency }, worker);
    await Promise.all(workers);
    return results as T[];
}

/**
 * Maps over values in parallel with bounded concurrency.
 *
 * Like `Promise.all(values.map(callback))`, but limits how many callbacks
 * run at once. Results preserve input order.
 *
 * @param values - Items to pass to each invocation of `callback`.
 * @param callback - Async mapper; receives the item and optional abort signal.
 * @param options - Concurrency limit and optional abort signal.
 */
export async function mapParallel<T, U>(
    values: Iterable<T>,
    callback: (value: T, signal?: AbortSignal) => Promise<U>,
    options?: ParallelOptions
): Promise<U[]> {
    if (Array.isArray(values) && values.length <= (options?.concurrency ?? DEFAULT_CONCURRENCY)) {
        return Promise.all(values.map((value) => callback(value, options?.signal)));
    }

    return processParallel(mapIter(values, (value) => (signal?: AbortSignal) => callback(value, signal)), options);
}


/**
 * Maps over values in parallel and returns a settled result for each entry.
 *
 * Unlike {@link mapParallel}, individual rejections do not fail the whole batch.
 * Pass `{ thenThrow: true }` to wait for all callbacks to finish, then throw
 * the first rejection reason if any entry failed.
 *
 * @param values - Items to pass to each invocation of `callback`.
 * @param callback - Async mapper; receives the item and optional abort signal.
 * @param options - Concurrency limit, optional abort signal, and `thenThrow` flag.
 */
export async function mapParallelSettled<T, U>(
    values: Iterable<T>,
    callback: (value: T, signal?: AbortSignal) => Promise<U>,
    options?: ParallelOptions & { thenThrow: true }
): Promise<Array<PromiseFulfilledResult<U>>>;
export async function mapParallelSettled<T, U>(
    values: Iterable<T>,
    callback: (value: T, signal?: AbortSignal) => Promise<U>,
    options?: ParallelOptions & { thenThrow?: boolean }
): Promise<Array<PromiseSettledResult<U>>>;
export async function mapParallelSettled<T, U>(
    values: Iterable<T>,
    callback: (value: T, signal?: AbortSignal) => Promise<U>,
    options?: ParallelOptions & { thenThrow?: boolean }
): Promise<Array<PromiseSettledResult<U>>> {
    const results = await mapParallel(values, async (value, signal): Promise<PromiseSettledResult<U>> => {
        try {
            return { status: "fulfilled", value: await callback(value, signal) };
        } catch (reason) {
            return { status: "rejected", reason };
        }
    }, options);

    if (options?.thenThrow) {
        for (const result of results) {
            if (result.status === "rejected") {
                throw result.reason;
            }
        }
    }

    return results;
}

/**
 * Maps over values in parallel and returns fulfilled values only.
 *
 * Runs {@link mapParallelSettled} with `thenThrow: true`, so every callback
 * is allowed to finish before the first rejection is thrown.
 *
 * @param values - Items to pass to each invocation of `callback`.
 * @param callback - Async mapper; receives the item and optional abort signal.
 * @param options - Concurrency limit and optional abort signal.
 */
export async function mapParallelThenThrow<T, U>(
    values: Iterable<T>,
    callback: (value: T, signal?: AbortSignal) => Promise<U>,
    options?: ParallelOptions
): Promise<U[]> {
    const settledResults = await mapParallelSettled(values, callback, {...options, thenThrow: true});
    return settledResults.map((result) => result.value);
}


/**
 * Resolves an object whose values are promises into the same keys with
 * awaited values.
 *
 * All properties are awaited in parallel. Rejects if any property rejects.
 *
 * @param properties - Object whose values are promises to await.
 */
export async function resolveProps<Props extends Record<string, Promise<unknown>>>(
    properties: Props
): Promise<{ [K in keyof Props]: Awaited<Props[K]> }> {
    const results = await Promise.all(
        Object.entries(properties).map(([key, promiseValue]) => promiseValue.then((value) => [key, value] as const)),
    );

    return Object.fromEntries(results) as { [K in keyof Props]: Awaited<Props[K]> };
}

/** Maps an iterable to another iterable, tracking the source index. */
function* mapIter<T, U>(iterable: Iterable<T>, callback: (value: T, index: number) => U): IterableIterator<U> {
    let index = 0;
    for (const value of iterable) {
        yield callback(value, index++);
    }
}