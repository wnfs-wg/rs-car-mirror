//! The bindgen API for WNFS block store.

use anyhow::{Result, anyhow};
use bytes::Bytes;
use js_sys::{Promise, Uint8Array};
use wasm_bindgen::{JsCast, prelude::wasm_bindgen};
use wasm_bindgen_futures::JsFuture;
use wnfs_common::{BlockStore as WnfsBlockStore, BlockStoreError, Cid};

//--------------------------------------------------------------------------------------------------
// Externs
//--------------------------------------------------------------------------------------------------

#[wasm_bindgen(typescript_custom_section)]
const TS_BLOCKSTORE: &'static str = r#"
export interface BlockStore {
    putBlockKeyed(cid: Uint8Array, bytes: Uint8Array): Promise<void>;
    getBlock(cid: Uint8Array): Promise<Uint8Array | undefined>;
    hasBlock(cid: Uint8Array): Promise<boolean>;
}
"#;

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(typescript_type = "BlockStore")]
    pub type BlockStore;

    #[wasm_bindgen(method, js_name = "putBlockKeyed")]
    pub(crate) fn put_block_keyed(store: &BlockStore, cid: Vec<u8>, bytes: Vec<u8>) -> Promise;

    #[wasm_bindgen(method, js_name = "getBlock")]
    pub(crate) fn get_block(store: &BlockStore, cid: Vec<u8>) -> Promise;

    #[wasm_bindgen(method, js_name = "hasBlock")]
    pub(crate) fn has_block(store: &BlockStore, cid: Vec<u8>) -> Promise;
}

//--------------------------------------------------------------------------------------------------
// Type Definitions
//--------------------------------------------------------------------------------------------------

/// A block store provided by the host (JavaScript) for custom implementation like connection to the IPFS network.
#[wasm_bindgen]
pub struct ForeignBlockStore(pub(crate) BlockStore);

impl Clone for ForeignBlockStore {
    fn clone(&self) -> Self {
        Self(BlockStore::unchecked_from_js(self.0.clone()))
    }
}

//--------------------------------------------------------------------------------------------------
// Implementations
//--------------------------------------------------------------------------------------------------

impl WnfsBlockStore for ForeignBlockStore {
    async fn put_block_keyed(
        &self,
        cid: Cid,
        bytes: impl Into<Bytes>,
    ) -> Result<(), BlockStoreError> {
        let bytes: Bytes = bytes.into();

        JsFuture::from(self.0.put_block_keyed(cid.to_bytes(), bytes.into()))
            .await
            .map_err(|e| anyhow!("Cannot put block: {e:?}"))?;

        Ok(())
    }

    async fn get_block(&self, cid: &Cid) -> Result<Bytes, BlockStoreError> {
        let value = JsFuture::from(self.0.get_block(cid.to_bytes()))
            .await
            .map_err(|e| anyhow!("Cannot get block: {e:?}"))?;

        if value.is_undefined() {
            return Err(BlockStoreError::CIDNotFound(*cid));
        }

        // Convert the value to a vector of bytes.
        let bytes = Uint8Array::new(&value).to_vec();
        Ok(Bytes::from(bytes))
    }

    async fn has_block(&self, cid: &Cid) -> Result<bool, BlockStoreError> {
        let value = JsFuture::from(self.0.has_block(cid.to_bytes()))
            .await
            .map_err(|e| anyhow!("Cannot run has_block: {e:?}"))?;

        Ok(js_sys::Boolean::from(value).value_of())
    }
}
