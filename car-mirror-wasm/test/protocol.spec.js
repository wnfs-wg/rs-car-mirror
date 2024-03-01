import { test, expect } from "@playwright/test";

test.beforeEach(async ({ page }) => {
  page.addListener("console", msg => {
    let logfn = console.log;
    switch (msg.type()) {
      case "debug": logfn = console.debug; break;
      case "info": logfn = console.info; break;
      case "error": logfn = console.error; break;
      case "warning": logfn = console.warning; break;
    }
    logfn(`type=${msg.type()} text=\"${msg.text()}\" src=${msg.location().url} line=${msg.location().lineNumber}`);
  });

  page.addListener("pageerror", err => {
    throw new Error(`${err.name}: ${err.message}\n${err.stack}`);
  });

  page.addListener("crash", () => console.error("Page crashed!"));

  await page.goto("/index.html");
  await page.waitForFunction(() => window.runCarMirrorPull != null);
})

test("car mirror pull http", testPull("http"));
test("car mirror pull https", testPull("https"));

test("car mirror incremental push http", testIncrementalPush("http"));
test("car mirror incremental push https", testIncrementalPush("https"));

test("car mirror both push then pull http", testPushThenPull("http"));
test("car mirror both push then pull https", testPushThenPull("https"));


function testPull(protocol) {
  return async ({ page }) => {
    const { hasRootBlock, totalBlocks } = await page.evaluate(async (protocol) => {
      const store = new MemoryBlockStore();
      // This is the sample data that the server serves. 100MB of ChaCha8 data from the 0 seed encoded as UnixFS file
      const cid = CID.parse("bafyb4ifjd76kkpos2uiv5mqifs4vi2xtywhf7pnu2pqtlg2vzmtmpyzdfa");
      await runCarMirrorPull(`${protocol}://localhost:3344/dag/pull`, cid.toString(), store);
      return ({
        hasRootBlock: await store.hasBlock(cid.bytes),
        totalBlocks: store.store.size
      });
    }, protocol);

    expect(hasRootBlock).toBeTruthy();
    expect(totalBlocks).toBeGreaterThan(300);
  }
}


function testIncrementalPush(protocol) {
  return async ({ page }) => {
    const success = await page.evaluate(async (protocol) => {
      const store = new MemoryBlockStore();
      const wasmCid = await exampleFile(store, async (file) => {
        const wasm = await (await fetch("./dist/web/car_mirror_wasm_bg.wasm")).arrayBuffer();
        for (let i = 0; i < 5; i++) {
          file.write(new Uint8Array(wasm));
        }
      });
      await runCarMirrorPush(`${protocol}://localhost:3344/dag/push`, wasmCid.toString(), store);
      return "success";
    }, protocol);
    expect(success).toBe("success");

    // and then push another time, this time more data, but sharing data with the previous push
    const secondSuccess = await page.evaluate(async (protocol) => {
      const store = new MemoryBlockStore();
      const wasmCid = await exampleFile(store, async (file) => {
        const wasm = await (await fetch("./dist/web/car_mirror_wasm_bg.wasm")).arrayBuffer();
        for (let i = 0; i < 10; i++) {
          file.write(new Uint8Array(wasm));
        }
      });
      await runCarMirrorPush(`${protocol}://localhost:3344/dag/push`, wasmCid.toString(), store);
      return "success";
    }, protocol);
    expect(secondSuccess).toBe("success");
  }
}


function testPushThenPull(protocol) {
  return async ({ page }) => {
    const { hasRootBlock, totalBlocks } = await page.evaluate(async (protocol) => {
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

      return ({
        hasRootBlock: await store.hasBlock(wasmCid.bytes),
        totalBlocks: store.store.size
      });
    }, protocol);
    expect(hasRootBlock).toBeTruthy();
    expect(totalBlocks).toBeGreaterThan(10);
  }
}
