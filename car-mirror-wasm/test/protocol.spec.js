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

test("car mirror pull", async ({ page }) => {
  const { hasRootBlock, totalBlocks } = await page.evaluate(async () => {
    const store = new MemoryBlockStore();
    // This is the sample data that the server serves. 100MB of ChaCha8 data from the 0 seed encoded as UnixFS file
    const cid = CID.parse("bafyb4ifjd76kkpos2uiv5mqifs4vi2xtywhf7pnu2pqtlg2vzmtmpyzdfa");
    await runCarMirrorPull("http://localhost:3344/dag/pull", cid.toString(), store);
    return ({
      hasRootBlock: await store.hasBlock(cid.bytes),
      totalBlocks: store.store.size
    });
  });

  expect(hasRootBlock).toBeTruthy();
  expect(totalBlocks).toBeGreaterThan(300);
});

test("car mirror push & incremental", async ({ page }) => {
  const success = await page.evaluate(async () => {
    const store = new MemoryBlockStore();
    const wasmCid = await exampleFile(store, async (file) => {
      const wasm = await (await fetch("./dist/web/car_mirror_wasm_bg.wasm")).arrayBuffer();
      for (let i = 0; i < 5; i++) {
        file.write(new Uint8Array(wasm));
      }
    });
    await runCarMirrorPush("http://localhost:3344/dag/push", wasmCid.toString(), store);
    return "success";
  });
  expect(success).toBe("success");

  // and then push another time, this time more data, but sharing data with the previous push
  const secondSuccess = await page.evaluate(async () => {
    const store = new MemoryBlockStore();
    const wasmCid = await exampleFile(store, async (file) => {
      const wasm = await (await fetch("./dist/web/car_mirror_wasm_bg.wasm")).arrayBuffer();
      for (let i = 0; i < 10; i++) {
        file.write(new Uint8Array(wasm));
      }
    });
    await runCarMirrorPush("http://localhost:3344/dag/push", wasmCid.toString(), store);
    return "success";
  });
  expect(secondSuccess).toBe("success");
});

test("car mirror push then pull", async ({ page }) => {
  const { hasRootBlock, totalBlocks } = await page.evaluate(async () => {
    let store = new MemoryBlockStore();
    const wasmCid = await exampleFile(store, async (file) => {
      const wasm = await (await fetch("./dist/web/car_mirror_wasm_bg.wasm")).arrayBuffer();
      for (let i = 0; i < 5; i++) {
        file.write(new Uint8Array(wasm));
      }
    });
    await runCarMirrorPush("http://localhost:3344/dag/push", wasmCid.toString(), store);

    // Clear the store
    store = new MemoryBlockStore();
    await runCarMirrorPull("http://localhost:3344/dag/pull", wasmCid.toString(), store);

    return ({
      hasRootBlock: await store.hasBlock(wasmCid.bytes),
      totalBlocks: store.store.size
    });
  });
  expect(hasRootBlock).toBeTruthy();
  expect(totalBlocks).toBeGreaterThan(10);
})
