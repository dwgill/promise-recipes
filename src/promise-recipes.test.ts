import { describe, expect, it } from "vitest";
import {
    mapParallel,
    mapParallelSettled,
    mapParallelThenThrow,
    processParallel,
    resolveProps,
} from "./promise-recipes.js";

describe("processParallel", () => {
    it("returns an empty array for an empty iterable", async () => {
        const results = await processParallel([]);
        expect(results).toEqual([]);
    });

    it("returns a single resolved value", async () => {
        const results = await processParallel([async () => 42]);
        expect(results).toEqual([42]);
    });

    it("preserves result order regardless of completion time", async () => {
        const deferred = [
            Promise.withResolvers<void>(),
            Promise.withResolvers<void>(),
            Promise.withResolvers<void>(),
        ];

        const resultsPromise = processParallel([
            async () => {
                await deferred[0]!.promise;
                return "a";
            },
            async () => {
                await deferred[1]!.promise;
                return "b";
            },
            async () => {
                await deferred[2]!.promise;
                return "c";
            },
        ]);

        deferred[1]!.resolve();
        deferred[2]!.resolve();
        deferred[0]!.resolve();

        const results = await resultsPromise;
        expect(results).toEqual(["a", "b", "c"]);
    });

    it("limits concurrency to the configured maximum", async () => {
        const deferreds = Array.from({ length: 6 }, () => Promise.withResolvers<void>());
        let inFlight = 0;
        let peak = 0;

        const resultsPromise = processParallel(
            deferreds.map((deferred) => async () => {
                inFlight++;
                peak = Math.max(peak, inFlight);
                await deferred.promise;
                inFlight--;
                return 1;
            }),
            { concurrency: 2 },
        );

        await Promise.resolve();

        expect(peak).toBeLessThanOrEqual(2);
        expect(peak).toBeGreaterThan(1);
        
        for (const deferred of deferreds) {
            deferred.resolve();
        }

        await resultsPromise;
        
        expect(peak).toBeLessThanOrEqual(2);
        expect(peak).toBeGreaterThan(1);
    });

    it("runs sequentially when concurrency is 1", async () => {
        const deferreds = Array.from({ length: 4 }, () => Promise.withResolvers<void>());
        let inFlight = 0;
        let peak = 0;

        const resultsPromise = processParallel(
            deferreds.map((deferred) => async () => {
                inFlight++;
                peak = Math.max(peak, inFlight);
                await deferred.promise;
                inFlight--;
                return 1;
            }),
            { concurrency: 1 },
        );

        await Promise.resolve();

        expect(peak).toBe(1);

        for (const deferred of deferreds) {
            deferred.resolve();
        }

        await resultsPromise;

        expect(peak).toBe(1);
    });

    it("works with default concurrency when options are omitted", async () => {
        const results = await processParallel([
            async () => 1,
            async () => 2,
            async () => 3,
        ]);
        expect(results).toEqual([1, 2, 3]);
    });

    it("forwards the provided AbortSignal to each callback", async () => {
        const controller = new AbortController();
        const received: AbortSignal[] = [];

        await processParallel(
            [
                async (signal) => {
                    received.push(signal!);
                    return "a";
                },
                async (signal) => {
                    received.push(signal!);
                    return "b";
                },
            ],
            { signal: controller.signal },
        );

        expect(received).toEqual([controller.signal, controller.signal]);
    });

    it("accepts array and generator iterables", async () => {
        function* callbacks() {
            yield async () => 1;
            yield async () => 2;
        }

        const results = await processParallel(callbacks());
        expect(results).toEqual([1, 2]);
    });

    it("rejects when a callback rejects", async () => {
        await expect(
            processParallel([
                async () => "ok",
                async () => {
                    throw new Error("fail");
                },
            ]),
        ).rejects.toThrow("fail");
    });
});

describe("mapParallel", () => {
    it("maps values in order", async () => {
        const results = await mapParallel([1, 2, 3], async (value) => value * 2);
        expect(results).toEqual([2, 4, 6]);
    });

    it("forwards the provided AbortSignal to the map callback", async () => {
        const controller = new AbortController();
        let received: AbortSignal | undefined;

        await mapParallel([1], async (_value, signal) => {
            received = signal;
            return _value;
        }, { signal: controller.signal });

        expect(received).toBe(controller.signal);
    });

    it("returns an empty array for an empty iterable", async () => {
        const results = await mapParallel([], async (value) => value);
        expect(results).toEqual([]);
    });
});

describe("mapParallelSettled", () => {
    it("returns an empty array for an empty iterable", async () => {
        const results = await mapParallelSettled([], async (value) => value);
        expect(results).toEqual([]);
    });

    it("returns fulfilled results when all callbacks succeed", async () => {
        const results = await mapParallelSettled([1, 2, 3], async (value) => value * 2);
        expect(results).toEqual([
            { status: "fulfilled", value: 2 },
            { status: "fulfilled", value: 4 },
            { status: "fulfilled", value: 6 },
        ]);
    });

    it("returns rejected results without throwing when some callbacks fail", async () => {
        const results = await mapParallelSettled([1, 2, 3], async (value) => {
            if (value === 2) {
                throw new Error("fail");
            }
            return value * 10;
        });

        expect(results[0]).toEqual({ status: "fulfilled", value: 10 });
        expect(results[1]).toMatchObject({
            status: "rejected",
            reason: expect.objectContaining({ message: "fail" }),
        });
        expect(results[2]).toEqual({ status: "fulfilled", value: 30 });
    });

    it("preserves result order regardless of completion time", async () => {
        const deferred = [
            Promise.withResolvers<void>(),
            Promise.withResolvers<void>(),
            Promise.withResolvers<void>(),
        ];

        const resultsPromise = mapParallelSettled([0, 1, 2], async (index) => {
            await deferred[index]!.promise;
            return String.fromCharCode(97 + index);
        });

        deferred[1]!.resolve();
        deferred[2]!.resolve();
        deferred[0]!.resolve();

        const results = await resultsPromise;
        expect(results).toEqual([
            { status: "fulfilled", value: "a" },
            { status: "fulfilled", value: "b" },
            { status: "fulfilled", value: "c" },
        ]);
    });

    it("throws when thenThrow is true and any callback fails", async () => {
        await expect(
            mapParallelSettled([1, 2], async (value) => {
                if (value === 2) {
                    throw new Error("settled fail");
                }
                return value;
            }, { thenThrow: true }),
        ).rejects.toThrow("settled fail");
    });

    it("returns fulfilled results when thenThrow is true and all callbacks succeed", async () => {
        const results = await mapParallelSettled([1, 2], async (value) => value * 2, { thenThrow: true });
        expect(results).toEqual([
            { status: "fulfilled", value: 2 },
            { status: "fulfilled", value: 4 },
        ]);
    });

    it("forwards the provided AbortSignal to the map callback", async () => {
        const controller = new AbortController();
        let received: AbortSignal | undefined;

        await mapParallelSettled([1], async (_value, signal) => {
            received = signal;
            return _value;
        }, { signal: controller.signal });

        expect(received).toBe(controller.signal);
    });
});

describe("mapParallelThenThrow", () => {
    it("returns mapped values when all callbacks succeed", async () => {
        const results = await mapParallelThenThrow([1, 2, 3], async (value) => value * 2);
        expect(results).toEqual([2, 4, 6]);
    });

    it("returns an empty array for an empty iterable", async () => {
        const results = await mapParallelThenThrow([], async (value) => value);
        expect(results).toEqual([]);
    });

    it("throws after all callbacks settle when any callback fails", async () => {
        const third = Promise.withResolvers<void>();
        const completed: number[] = [];

        const resultPromise = mapParallelThenThrow([1, 2, 3], async (value) => {
            if (value === 2) {
                throw new Error("then throw fail");
            }
            if (value === 3) {
                await third.promise;
            }
            completed.push(value);
            return value * 10;
        });

        await Promise.resolve();
        third.resolve();

        await expect(resultPromise).rejects.toThrow("then throw fail");
        expect(completed).toContain(3);
    });

    it("forwards the provided AbortSignal to the map callback", async () => {
        const controller = new AbortController();
        let received: AbortSignal | undefined;

        await mapParallelThenThrow([1], async (_value, signal) => {
            received = signal;
            return _value;
        }, { signal: controller.signal });

        expect(received).toBe(controller.signal);
    });
});

describe("resolveProps", () => {
    it("returns an empty object for empty input", async () => {
        const result = await resolveProps({});
        expect(result).toEqual({});
    });

    it("resolves a single property", async () => {
        const result = await resolveProps({ count: Promise.resolve(42) });
        expect(result).toEqual({ count: 42 });
    });

    it("resolves multiple properties in parallel", async () => {
        const result = await resolveProps({
            a: Promise.resolve(1),
            b: Promise.resolve("two"),
            c: Promise.resolve({ nested: true }),
        });
        expect(result).toEqual({
            a: 1,
            b: "two",
            c: { nested: true },
        });
    });

    it("resolves all properties regardless of completion order", async () => {
        const slow = Promise.withResolvers<number>();
        const fast = Promise.withResolvers<string>();

        const resultPromise = resolveProps({
            slow: slow.promise,
            fast: fast.promise,
        });

        fast.resolve("done");
        slow.resolve(99);

        await expect(resultPromise).resolves.toEqual({
            slow: 99,
            fast: "done",
        });
    });

    it("rejects when any property promise rejects", async () => {
        await expect(
            resolveProps({
                ok: Promise.resolve("fine"),
                bad: Promise.reject(new Error("prop fail")),
            }),
        ).rejects.toThrow("prop fail");
    });
});
