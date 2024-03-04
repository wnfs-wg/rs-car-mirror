import { MemoryBlockStore, exampleFile, runCarMirrorPull, runCarMirrorPush } from "./index.js"
import { CID } from "multiformats"
import { assert, suite } from 'playwright-test/taps'

const test = suite("protocol");

test("car mirror pull http", () => testPull("http"));
test("car mirror pull https", () => testPull("https"));

test("car mirror incremental push http", () => testIncrementalPush("http"));
test("car mirror incremental push https", () => testIncrementalPush("https"));

test("car mirror both push then pull http", () => testPushThenPull("http"));
test("car mirror both push then pull https", () => testPushThenPull("https"));


async function testPull(protocol) {
  const store = new MemoryBlockStore();
  // This is the sample data that the server serves. 100MB of ChaCha8 data from the 0 seed encoded as UnixFS file
  const cid = CID.parse("bafyb4ifjd76kkpos2uiv5mqifs4vi2xtywhf7pnu2pqtlg2vzmtmpyzdfa");
  await runCarMirrorPull(`${protocol}://localhost:3344/dag/pull`, cid.toString(), store);

  assert.equal(await store.hasBlock(cid.bytes), true);
  assert.equal(store.store.size > 300, true);
}


async function testIncrementalPush(protocol) {
  const storeSmall = new MemoryBlockStore();
  const wasmCidSmall = await exampleFile(storeSmall, async (file) => {
    const wasm = await (await fetch("./dist/web/car_mirror_wasm_bg.wasm")).arrayBuffer();
    for (let i = 0; i < 5; i++) {
      file.write(new Uint8Array(wasm));
    }
  });
  await runCarMirrorPush(`${protocol}://localhost:3344/dag/push`, wasmCidSmall.toString(), storeSmall);

  // and then push another time, this time more data, but sharing data with the previous push
  const storeBig = new MemoryBlockStore();
  const wasmCidBig = await exampleFile(storeBig, async (file) => {
    const wasm = await (await fetch("./dist/web/car_mirror_wasm_bg.wasm")).arrayBuffer();
    for (let i = 0; i < 10; i++) {
      file.write(new Uint8Array(wasm));
    }
  });
  await runCarMirrorPush(`${protocol}://localhost:3344/dag/push`, wasmCidBig.toString(), storeBig);
}


async function testPushThenPull(protocol) {
  let store = new MemoryBlockStore();
  const wasmCid = await exampleFile(store, async (file) => {
    const wasm = await (await fetch("./dist/web/car_mirror_wasm_bg.wasm")).arrayBuffer();
    for (let i = 0; i < 5; i++) {
      file.write(new Uint8Array(wasm));
    }
  });
  await runCarMirrorPush(`${protocol}://localhost:3344/dag/push`, wasmCid.toString(), store);

  // Clear the store
  store = new MemoryBlockStore();
  await runCarMirrorPull(`${protocol}://localhost:3344/dag/pull`, wasmCid.toString(), store);

  assert.equal(await store.hasBlock(wasmCid.bytes), true);
  assert.equal(store.store.size > 10, true);
}
