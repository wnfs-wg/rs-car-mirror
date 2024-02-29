use std::rc::Rc;

use crate::utils::handle_err;
use js_sys::Error;
use wasm_bindgen::{prelude::wasm_bindgen, JsValue};

#[wasm_bindgen]
pub struct PullRequest(pub(crate) Rc<car_mirror::messages::PullRequest>);

#[wasm_bindgen]
pub struct PushResponse(pub(crate) Rc<car_mirror::messages::PushResponse>);

#[wasm_bindgen]
impl PullRequest {
    #[wasm_bindgen(js_name = "fromJSON")]
    pub fn from_json(value: JsValue) -> Result<PullRequest, Error> {
        Ok(Self(Rc::new(
            serde_wasm_bindgen::from_value(value).map_err(handle_err)?,
        )))
    }

    #[wasm_bindgen(js_name = "toJSON")]
    pub fn to_json(&self) -> Result<JsValue, Error> {
        serde_wasm_bindgen::to_value(self.0.as_ref()).map_err(handle_err)
    }

    #[wasm_bindgen(js_name = "encode")]
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        self.0.to_dag_cbor().map_err(handle_err)
    }

    #[wasm_bindgen(js_name = "indicatesFinished")]
    pub fn indicates_finished(&self) -> bool {
        self.0.indicates_finished()
    }
}

#[wasm_bindgen]
impl PushResponse {
    #[wasm_bindgen(js_name = "fromJSON")]
    pub fn from_json(value: JsValue) -> Result<PushResponse, Error> {
        Ok(Self(Rc::new(
            serde_wasm_bindgen::from_value(value).map_err(handle_err)?,
        )))
    }

    #[wasm_bindgen(js_name = "toJSON")]
    pub fn to_json(&self) -> Result<JsValue, Error> {
        serde_wasm_bindgen::to_value(self.0.as_ref()).map_err(handle_err)
    }

    #[wasm_bindgen(js_name = "decode")]
    pub fn decode(bytes: Vec<u8>) -> Result<PushResponse, Error> {
        let response =
            car_mirror::messages::PushResponse::from_dag_cbor(&bytes).map_err(handle_err)?;
        Ok(Self(Rc::new(response)))
    }

    #[wasm_bindgen(js_name = "indicatesFinished")]
    pub fn indicates_finished(&self) -> bool {
        self.0.indicates_finished()
    }
}
