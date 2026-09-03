//! Responses 能力层 + HTTP 接入层 + 后台维护。
//!
//! 承载 responses 用例编排（无 axum 的 [`service`]）与协议接入（[`routes`] /
//! [`sse`]），以及 sweeper / 优雅停机。
//!
//! 本 crate **不依赖任何具体存储适配器**：账本、上下文、事件缓冲全部经
//! `nova-responses-core` 端口注入，由装配方（生产 gateway 或验证层）决定用
//! mem 还是 sql + redis。这正是 D25「能力层抽离」的落地——mem 适配器只存在于
//! 验证层，生产 gateway 永远连真实后端。

pub mod auth;
pub mod clock;
pub mod config;
pub mod error;
pub mod metrics;
pub mod routes;
pub mod service;
pub mod sse;
pub mod state;
pub mod sweeper;
pub mod shutdown;

pub use auth::KeyTable;
pub use clock::SystemClock;
pub use config::{Config, RawConfig};
pub use metrics::CountingMetrics;
pub use service::{CreateResult, ResponsesService, ServiceError};
pub use state::AppState;
