//! 能力层：用例编排，无 HTTP 依赖（D25 ⑤）。
//!
//! 接入层负责协议解析、鉴权、路由与 HTTP 翻译；本层只编排业务主线，输入领域值、输出
//! 领域结果。

mod conversations;
mod error;
mod responses;
mod sweep;

pub use conversations::ConversationsService;
pub use error::{ContextError, ServiceError};
pub use responses::{ResponsesDeps, ResponsesService};
