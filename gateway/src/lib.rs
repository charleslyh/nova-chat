//! HTTP 接入层 + 装配壳。
//!
//! 承载协议接入（[`routes`] / [`sse`]）、鉴权（[`auth`]）、优雅停机
//! （[`shutdown`]）与应用状态（[`state`]）。能力层在 `nova-responses`
//! （crates/responses），本 crate 依赖它，并在此（装配壳 [`main`]）挂载具体
//! backend —— 生产 REST 进程复用本库、只替换 backend，与验证装配壳同构。

pub mod auth;
pub mod error;
pub mod routes;
pub mod shutdown;
pub mod sse;
pub mod state;

pub use auth::KeyTable;
pub use state::AppState;
