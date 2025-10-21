use crate::{
    blockstore::{BlockStore, ForeignBlockStore},
    messages::{PullRequest, PushResponse},
    utils::{handle_jserr, parse_cid},
};
use bytes::BytesMut;
use car_mirror::{cache::NoCache, common::Config};
use futures::{StreamExt, TryStreamExt};
use js_sys::{Error, Promise, Uint8Array};
use std::rc::Rc;
use tokio_util::compat::FuturesAsyncReadCompatExt;
use wasm_bindgen::{JsValue, prelude::wasm_bindgen};
use wasm_bindgen_futures::future_to_promise;
use wasm_streams::ReadableStream;

/// Compute the bytes for a non-streaming push request, given
/// the byte-encoded root CID, the PushResponse from the last round,
/// except in the case of the first round, and a BlockStore.
///
/// Returns a promise that resolves to a `Uint8Array` of car file
/// bytes.
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

/// Creates a stream of bytes for a streaming push request, given
/// the byte-encoded root CID, the PushResponse from the last round,
/// except in the case of the first round, and a BlockStore.
///
/// Returns a promise that resolves to a `ReadableStream<Uint8Array>`
/// of car file frames.
///
/// This function is unlikely to work in browsers, unless you're
/// using a Chrome-based browser that supports half-duplex fetch
/// requests and the car mirror server supports HTTP2.
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

/// Compute the pull request for given byte-encoded root CID with
/// given BlockStore state.
///
/// Returns a promise that resolves to an instance of the `PullRequest`
/// class.
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

/// Handle a response from a car-mirror pull request in a streaming way,
/// givena byte-encoded root CID, a `ReadableStream<Uint8Array>` and a
/// `BlockStore`.
///
/// This function may return before draining the whole `stream` with
/// updates about the latest receiver state.
///
/// In that case, the request should be interrupted and a new one should
/// be started.
///
/// Returns a promise that resolves to an instance of the `PullRequest`
/// class.
#[wasm_bindgen]
pub fn pull_handle_response_streaming(
    root_cid: Vec<u8>,
    readable_stream: web_sys::ReadableStream,
    store: BlockStore,
) -> Result<Promise, Error> {
    let store = ForeignBlockStore(store);
    let root = parse_cid(root_cid)?;
    let readable_stream = ReadableStream::from_raw(readable_stream);

    Ok(future_to_promise(async move {
        let pull_request = match readable_stream.try_into_async_read() {
            Ok(async_read) => car_mirror::pull::handle_response_streaming(
                root,
                async_read.compat(),
                &Config::default(),
                store,
                NoCache,
            )
            .await
            .map_err(handle_jserr)?,

            // If BYOB readers are unsupported:
            Err((_, readable_stream)) => {
                let stream = readable_stream
                        .into_stream()
                        .map(|result| result.and_then(convert_jsvalue_to_bytes))
                        .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "Error while trying to read item from stream or trying to convert the item into bytes on the rust side."));

                let async_read = tokio_util::io::StreamReader::new(stream);

                car_mirror::pull::handle_response_streaming(
                    root,
                    async_read,
                    &Config::default(),
                    store,
                    NoCache,
                )
                .await
                .map_err(handle_jserr)?
            }
        };
        Ok(PullRequest(Rc::new(pull_request)).into())
    }))
}

fn convert_jsvalue_to_bytes(js_value: JsValue) -> Result<BytesMut, JsValue> {
    let uint8array = Uint8Array::new(&js_value);

    let mut result = BytesMut::with_capacity(uint8array.length() as usize);
    result.resize(uint8array.length() as usize, 0);
    uint8array.copy_to(&mut result);

    Ok(result)
}
