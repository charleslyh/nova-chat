//! 能力层：responses 用例编排，无 axum 依赖（D25 ⑤）。
//!
//! 接入层（`routes`）负责协议解析、鉴权、路由与 HTTP 翻译；本层只编排业务
//! 主线，输入领域请求、输出领域结果。

pub mod conversations;
pub mod responses;

pub use conversations::{ConversationTail, ConversationsService, TranscriptError};
pub use responses::{ContextSource, CreateResult, ResponsesService, ServiceError};
