//! Shared HTTP body alias and downstream response constructors.

use std::convert::Infallible;

use bytes::Bytes;
use http::{Response, StatusCode};
use http_body_util::{BodyExt, Full, combinators::UnsyncBoxBody};

use crate::BoxError;

pub(crate) type ResBody = UnsyncBoxBody<Bytes, BoxError>;

pub(crate) fn full_body(bytes: Bytes) -> ResBody {
    Full::new(bytes)
        .map_err(|e: Infallible| match e {})
        .boxed_unsync()
}

pub(crate) fn empty_body() -> ResBody {
    full_body(Bytes::new())
}

pub(crate) fn raw_json_response(code: StatusCode, body: Bytes) -> Response<ResBody> {
    Response::builder()
        .status(code)
        .header("content-type", "application/json")
        .body(full_body(body))
        .expect("response builder")
}

pub(crate) fn empty_json_response(code: StatusCode) -> Response<ResBody> {
    raw_json_response(code, Bytes::new())
}

pub(crate) fn text_response(code: StatusCode, msg: impl Into<Bytes>) -> Response<ResBody> {
    Response::builder()
        .status(code)
        .header("content-type", "text/plain; charset=utf-8")
        .body(full_body(msg.into()))
        .expect("response builder")
}

pub(crate) fn uri_too_long_response() -> Response<ResBody> {
    text_response(
        StatusCode::URI_TOO_LONG,
        Bytes::from_static(b"request URI too long"),
    )
}
