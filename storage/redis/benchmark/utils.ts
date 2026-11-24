import { randomUUID } from "node:crypto";
import { Worker } from "node:worker_threads";
import type { RedisClientType } from "@keyv/redis";
import { openStore, preload, threadCount } from "./workload.js";

const servers = [
	{ name: "Redis", uri: process.env.REDIS_URI ?? "redis://127.0.0.1:16379" },
	{ name: "Valkey", uri: process.env.VALKEY_URI ?? "redis://127.0.0.1:16370" },
	{ name: "KiviDB", uri: process.env.KIVIDB_URI ?? "redis://127.0.0.1:16381" },
];

type Sample = { writes: number; end: number };

// used_memory from INFO memory, which Redis, Valkey and KiviDB all report.
async function usedMemory(client: RedisClientType): Promise<number> {
	const match = /^used_memory:(\d+)/m.exec(await client.info("memory"));
	if (!match) {
		throw new Error("The server did not report used_memory.");
	}
	return Number(match[1]);
}

async function measurePoint(uri: string, namespace: string, connections: number) {
	const workers: Worker[] = [];
	let ready = 0;
	let start = 0;
	const pending: Promise<Sample>[] = [];
	try {
		for (let thread = 0; thread < threadCount; thread++) {
			const sample = new Promise<Sample>((resolve, reject) => {
				const module = import.meta.url.endsWith(".ts") ? "./workload.ts" : "./workload.mjs";
				const worker = new Worker(new URL(module, import.meta.url), {
					workerData: { uri, namespace, connections, thread },
				});
				workers.push(worker);
				let result: Sample | undefined;
				worker.once("error", reject);
				worker.once("exit", (code) => {
					if (code === 0 && result) {
						resolve(result);
					} else {
						reject(new Error("A worker failed to return a result."));
					}
				});
				worker.on("message", (message: "ready" | Sample) => {
					if (message !== "ready") {
						result = message;
						return;
					}

					ready++;
					if (ready === threadCount) {
						start = performance.now();
						for (const peer of workers) {
							peer.postMessage(start + 15_000);
						}
					}
				});
			});
			pending.push(sample);
		}
		const samples = await Promise.all(pending);
		const writes = samples.reduce((total, sample) => total + sample.writes, 0);
		const seconds = (Math.max(...samples.map((sample) => sample.end)) - start) / 1000;
		return { reads: 9 * writes, writes, ops: Math.round((10 * writes) / seconds) };
	} catch (error) {
		await Promise.all(workers.map((worker) => worker.terminate()));
		throw error;
	}
}

async function runPoint(name: string, uri: string, connections: number) {
	const namespace = `keyv-bench-${randomUUID()}`;
	let stage = "connection";
	try {
		const store = openStore(uri, namespace);
		try {
			const client = (await store.getClient()) as RedisClientType;
			try {
				stage = "preload";
				const before = await usedMemory(client);
				await preload(store);
				const memory = (await usedMemory(client)) - before;
				stage = "workload or reply verification";
				const sample = await measurePoint(uri, namespace, connections);
				stage = "cleanup";
				return { ...sample, memory };
			} finally {
				await store.clear();
			}
		} finally {
			await store.disconnect(true);
		}
	} catch {
		// Client errors can contain credentials or server-supplied text.
		throw new Error(`${name} failed at ${connections} connections during ${stage}.`);
	}
}

export async function runBenchmarks(): Promise<string> {
	const rows = [
		"| Server | Connections | Operations/s | Memory for 200,000 keys | Verified reads | Verified writes |",
		"| --- | ---: | ---: | ---: | ---: | ---: |",
	];
	for (const { name, uri } of servers) {
		for (const connections of [48, 500]) {
			console.log(`${name}, ${connections} connections`);
			const { reads, writes, ops, memory } = await runPoint(name, uri, connections);
			const mb = (memory / 1024 / 1024).toFixed(1);
			rows.push(`| ${name} | ${connections} | ${ops} | ${mb} MB | ${reads} | ${writes} |`);
		}
	}
	return rows.join("\n");
}
