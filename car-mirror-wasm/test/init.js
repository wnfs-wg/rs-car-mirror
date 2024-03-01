
import { setPanicHook, initSync, push_request, push_request_streaming, pull_request, pull_handle_response_streaming, PushResponse } from "/dist/web/car_mirror_wasm.js"
import { CID } from "https://esm.sh/multiformats"
import * as UnixFS from "https://esm.sh/@ipld/unixfs"

initSync(await (await fetch("/dist/web/car_mirror_wasm_bg.wasm")).arrayBuffer());
setPanicHook();

const supportsRequestStreams = (() => {
    let duplexAccessed = false;

    const hasContentType = new Request('', {
        body: new ReadableStream(),
        method: 'POST',
        get duplex() {
            duplexAccessed = true;
            return 'half';
        },
    }).headers.has('Content-Type');

    return duplexAccessed && !hasContentType;
})();

export class MemoryBlockStore {
    store

    /** Creates a new in-memory block store. */
    constructor() {
        this.store = new Map();
    }

    /** Stores an array of bytes in the block store. */
    async getBlock(cid) {
        const decodedCid = CID.decode(cid);
        return this.store.get(decodedCid.toString());
    }

    /** Retrieves an array of bytes from the block store with given CID. */
    async putBlockKeyed(cid, bytes) {
        const decodedCid = CID.decode(cid);
        this.store.set(decodedCid.toString(), bytes);
    }

    /** Finds out whether a block is retrievable from this blockstore */
    async hasBlock(cid) {
        const decodedCid = CID.decode(cid);
        return this.store.has(decodedCid.toString());
    }
}

export async function runCarMirrorPull(serverUrl, cidString, store) {
    const cid = CID.parse(cidString);
    const url = new URL(serverUrl);
    url.pathname = `/dag/pull/${cid.toString()}`;

    let request = await pull_request(cid.bytes, store);
    while (!request.indicatesFinished()) {
        console.debug("Initiating request", url.toString(), JSON.stringify(request.toJSON()))
        const response = await fetch(url, {
            headers: {
                "Accept": "application/vnd.ipld.car",
                "Content-Type": "application/vnd.ipld.dag-cbor",
            },
            method: "POST",
            body: request.encode()
        });
        console.debug("Got response status", response.status, response.body.locked);
        if (200 <= response.status && response.status < 300) {
            response.body.getReader().releaseLock();
            request = await pull_handle_response_streaming(cid.bytes, response.body, store);
        } else {
            throw new Error(`Unexpected status code in car-mirror pull response: ${response.status}, body: ${await response.text()}`);
        }
    }
    console.debug("Finished pulling", cidString);
}

export async function runCarMirrorPush(serverUrl, cidString, store) {
    const cid = CID.parse(cidString);
    const url = new URL(serverUrl);
    url.pathname = `/dag/push/${cid.toString()}`;

    const isHTTPS = url.protocol.toLowerCase() === "https";
    const useStreaming = supportsRequestStreams && isHTTPS;

    let lastResponse = null;
    while (!lastResponse?.indicatesFinished()) {
        console.debug(`Creating push request body (supports streams? ${supportsRequestStreams} isHTTPS? ${isHTTPS})`)
        const body = useStreaming
            ? await push_request_streaming(cid.bytes, lastResponse == null ? undefined : lastResponse, store)
            : await push_request(cid.bytes, lastResponse == null ? undefined : lastResponse, store);

        console.debug("Initiating request", url.toString(), body);
        const response = await fetch(url, {
            method: "POST",
            headers: {
                "Content-Type": "application/vnd.ipld.car",
                "Accept": "application/vnd.ipld.dag-cbor",
            },
            duplex: useStreaming ? "half" : undefined,
            body,
        });

        if (!(200 <= response.status && response.status < 300)) {
            throw new Error(`Unexpected status code in car-mirror push response: ${response.status}, body: ${await response.text()}`);
        }

        const responseBytes = new Uint8Array(await response.arrayBuffer());

        lastResponse = PushResponse.decode(responseBytes);
        console.debug(`Got response (status ${response.status}):`, JSON.stringify(lastResponse.toJSON()));
    }
    console.debug("Finished pushing", cidString);
}

export async function exampleFile(store, writes) {
    const { readable, writable } = new TransformStream({}, UnixFS.withCapacity(1048576 * 32));
    // Asynchronously write blocks to blockstore
    const finishReading = (async () => {
        const reader = readable.getReader();

        while (true) {
            const { done, value: block } = await reader.read();

            if (done) {
                return;
            }

            console.debug("Adding block", block.cid.toString(), block.bytes.length);
            await store.putBlockKeyed(block.cid.bytes, block.bytes);
        }
    })();

    const writer = UnixFS.createWriter({ writable });
    const file = UnixFS.createFileWriter(writer);

    await writes(file);

    const { cid } = await file.close();
    console.debug("Computed CID", cid.toString());
    writer.close();

    await finishReading;

    return cid;
}

window.runCarMirrorPull = runCarMirrorPull;
window.runCarMirrorPush = runCarMirrorPush;
window.supportsRequestStreams = supportsRequestStreams;
window.MemoryBlockStore = MemoryBlockStore;
window.exampleFile = exampleFile;
window.CID = CID;
window.UnixFS = UnixFS;
