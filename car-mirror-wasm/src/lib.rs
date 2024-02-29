#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(missing_debug_implementations, missing_docs, rust_2018_idioms)]
#![deny(unreachable_pub)]

//! car-mirror

#[cfg(target = "wasm32-unknown-unknown")]
pub mod blockstore;
#[cfg(target = "wasm32-unknown-unknown")]
pub mod messages;
#[cfg(target = "wasm32-unknown-unknown")]
mod utils;

#[cfg(target = "wasm32-unknown-unknown")]
mod exports {
    use crate::blockstore::{BlockStore, ForeignBlockStore};
    use crate::messages::{PullRequest, PushResponse};
    use crate::utils::{handle_jserr, parse_cid};
    use car_mirror::{cache::NoCache, common::Config};
    use futures::TryStreamExt;
    use js_sys::{Error, Promise, Uint8Array};
    use std::rc::Rc;
    use tokio_util::compat::FuturesAsyncReadCompatExt;
    use wasm_bindgen::{prelude::wasm_bindgen, JsValue};
    use wasm_bindgen_futures::future_to_promise;
    use wasm_streams::ReadableStream;

    #[wasm_bindgen]
    pub fn push_request(
        root_cid: Vec<u8>,
        last_response: Option<PushResponse>,
        store: BlockStore,
    ) -> Result<Promise, Error> {
        let store = ForeignBlockStore(store);
        let root = parse_cid(root_cid)?;
        let last_response = if let Some(push_response) = last_response {
            Some(Rc::try_unwrap(push_response.0).unwrap_or_else(|rc| rc.as_ref().clone()))
        } else {
            None
        };

        Ok(future_to_promise(async move {
            let car_file =
                car_mirror::push::request(root, last_response, &Config::default(), &store, NoCache)
                    .await
                    .map_err(handle_jserr)?;

            let uint8array = Uint8Array::from(car_file.bytes.as_ref());

            Ok(uint8array.into())
        }))
    }

    #[wasm_bindgen]
    pub fn push_request_streaming(
        root_cid: Vec<u8>,
        last_response: Option<PushResponse>,
        store: BlockStore,
    ) -> Result<Promise, Error> {
        let store = ForeignBlockStore(store);
        let root = parse_cid(root_cid)?;
        let last_response = if let Some(push_response) = last_response {
            Some(Rc::try_unwrap(push_response.0).unwrap_or_else(|rc| rc.as_ref().clone()))
        } else {
            None
        };

        Ok(future_to_promise(async move {
            let car_stream =
                car_mirror::push::request_streaming(root, last_response, store.clone(), NoCache)
                    .await
                    .map_err(handle_jserr)?;

            let js_car_stream = car_stream
                .map_ok(|bytes| JsValue::from(Uint8Array::from(bytes.as_ref())))
                .map_err(handle_jserr);

            Ok(ReadableStream::from_stream(js_car_stream).into_raw().into())
        }))
    }

    #[wasm_bindgen]
    pub fn pull_request(root_cid: Vec<u8>, store: BlockStore) -> Result<Promise, Error> {
        let store = ForeignBlockStore(store);
        let root = parse_cid(root_cid)?;

        Ok(future_to_promise(async move {
            let pull_request =
                car_mirror::pull::request(root, None, &Config::default(), store, NoCache)
                    .await
                    .map_err(handle_jserr)?;

            Ok(PullRequest(Rc::new(pull_request)).into())
        }))
    }

    #[wasm_bindgen]
    pub fn pull_handle_response_streaming(
        root_cid: Vec<u8>,
        stream: web_sys::ReadableStream,
        store: BlockStore,
    ) -> Result<Promise, Error> {
        let store = ForeignBlockStore(store);
        let root = parse_cid(root_cid)?;
        let stream = ReadableStream::from_raw(stream);

        Ok(future_to_promise(async move {
            let pull_request = car_mirror::pull::handle_response_streaming(
                root,
                stream.into_async_read().compat(),
                &Config::default(),
                store,
                NoCache,
            )
            .await
            .map_err(handle_jserr)?;

            Ok(PullRequest(Rc::new(pull_request)).into())
        }))
    }

    //------------------------------------------------------------------------------
    // Utilities
    //------------------------------------------------------------------------------

    /// Panic hook lets us get better error messages if our Rust code ever panics.
    ///
    /// For more details see
    /// <https://github.com/rustwasm/console_error_panic_hook#readme>
    #[wasm_bindgen(js_name = "setPanicHook")]
    pub fn set_panic_hook() {
        #[cfg(feature = "console_error_panic_hook")]
        console_error_panic_hook::set_once();
    }

    #[wasm_bindgen]
    extern "C" {
        // For alerting
        pub(crate) fn alert(s: &str);
        // For logging in the console.
        #[wasm_bindgen(js_namespace = console)]
        pub fn log(s: &str);
    }
}

#[cfg(target = "wasm32-unknown-unknown")]
pub use exports::*;
