use js_sys::Error;
use wasm_bindgen::JsValue;
use wnfs_common::Cid;

pub(crate) fn parse_cid(bytes: Vec<u8>) -> Result<Cid, Error> {
    Cid::read_bytes(&bytes[..]).map_err(|e| Error::new(&format!("Couldn't parse CID: {e:?}")))
}

pub(crate) fn handle_jserr<E: ToString>(e: E) -> JsValue {
    JsValue::from(Error::new(&e.to_string()))
}

pub(crate) fn handle_err<E: ToString>(e: E) -> Error {
    Error::new(&e.to_string())
}
