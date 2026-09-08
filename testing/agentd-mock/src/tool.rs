//! Tool execution for the mock runner: the [`ToolExecutor`] trait plus a
//! deterministic calculator.
//!
//! The trait was previously `nova-responses-core::ToolExecutor`. It is not part
//! of the storage/domain contract — only the mock agent runner executes tools —
//! so it lives here.

use async_trait::async_trait;
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ToolError {
    #[error("no tool named `{0}` is available")]
    UnknownTool(String),
    #[error("tool `{name}` failed: {message}")]
    Execution { name: String, message: String },
}

/// Executes the tools the runner offers to the model.
#[async_trait]
pub trait ToolExecutor: Send + Sync {
    fn name(&self) -> &str;

    /// Run `tool` with the JSON `arguments` the model supplied. `Err` terminates
    /// the response; a recoverable failure should be returned as `Ok` text.
    async fn call(&self, tool: &str, arguments: &str) -> Result<String, ToolError>;
}

/// The "no tools are configured" executor.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoopToolExecutor;

#[async_trait]
impl ToolExecutor for NoopToolExecutor {
    fn name(&self) -> &str {
        "none"
    }

    async fn call(&self, tool: &str, _arguments: &str) -> Result<String, ToolError> {
        Err(ToolError::UnknownTool(tool.to_string()))
    }
}

/// The tool name the model may call.
pub const TOOL_NAME: &str = "calculate";

/// A tool executor that evaluates a single `expression` argument.
#[derive(Debug, Clone, Copy, Default)]
pub struct CalculatorTool;

#[async_trait]
impl ToolExecutor for CalculatorTool {
    fn name(&self) -> &str {
        "calculator"
    }

    async fn call(&self, tool: &str, arguments: &str) -> Result<String, ToolError> {
        if tool != TOOL_NAME {
            return Err(ToolError::UnknownTool(tool.to_string()));
        }

        let expr = match parse_expression(arguments) {
            Ok(expr) => expr,
            Err(message) => return Ok(format!("error: {message}")),
        };

        match evaluate(&expr) {
            Ok(value) => Ok(format_number(value)),
            Err(message) => Ok(format!("error: {message}")),
        }
    }
}

fn parse_expression(arguments: &str) -> Result<String, String> {
    let value: serde_json::Value =
        serde_json::from_str(arguments).map_err(|e| format!("arguments are not JSON: {e}"))?;
    match value.get("expression") {
        Some(serde_json::Value::String(s)) => Ok(s.clone()),
        Some(_) => Err("argument `expression` must be a string".to_string()),
        None => Err("missing `expression` argument".to_string()),
    }
}

fn format_number(value: f64) -> String {
    if !value.is_finite() {
        return value.to_string();
    }
    let rounded = (value * 1e10).round() / 1e10;
    if rounded == rounded.trunc() && rounded.abs() < 1e15 {
        return format!("{}", rounded as i64);
    }
    let mut s = format!("{rounded}");
    if s.contains('.') {
        while s.ends_with('0') {
            s.pop();
        }
        if s.ends_with('.') {
            s.pop();
        }
    }
    s
}

fn evaluate(input: &str) -> Result<f64, String> {
    let mut parser = Parser {
        bytes: input.as_bytes(),
        pos: 0,
    };
    let value = parser.parse_expr()?;
    parser.skip_whitespace();
    if parser.pos != parser.bytes.len() {
        return Err(format!(
            "unexpected character `{}` at position {}",
            input.as_bytes()[parser.pos] as char,
            parser.pos
        ));
    }
    Ok(value)
}

struct Parser<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl Parser<'_> {
    fn skip_whitespace(&mut self) {
        while self.pos < self.bytes.len() && self.bytes[self.pos].is_ascii_whitespace() {
            self.pos += 1;
        }
    }

    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.pos).copied()
    }

    fn parse_expr(&mut self) -> Result<f64, String> {
        let mut value = self.parse_term()?;
        loop {
            self.skip_whitespace();
            match self.peek() {
                Some(b'+') => {
                    self.pos += 1;
                    value += self.parse_term()?;
                }
                Some(b'-') => {
                    self.pos += 1;
                    value -= self.parse_term()?;
                }
                _ => break,
            }
        }
        Ok(value)
    }

    fn parse_term(&mut self) -> Result<f64, String> {
        let mut value = self.parse_factor()?;
        loop {
            self.skip_whitespace();
            match self.peek() {
                Some(b'*') => {
                    self.pos += 1;
                    value *= self.parse_factor()?;
                }
                Some(b'/') => {
                    self.pos += 1;
                    let divisor = self.parse_factor()?;
                    if divisor == 0.0 {
                        return Err("division by zero".to_string());
                    }
                    value /= divisor;
                }
                _ => break,
            }
        }
        Ok(value)
    }

    fn parse_factor(&mut self) -> Result<f64, String> {
        self.skip_whitespace();
        match self.peek() {
            Some(b'+') => {
                self.pos += 1;
                self.parse_factor()
            }
            Some(b'-') => {
                self.pos += 1;
                Ok(-self.parse_factor()?)
            }
            Some(b'(') => {
                self.pos += 1;
                let value = self.parse_expr()?;
                self.skip_whitespace();
                match self.peek() {
                    Some(b')') => {
                        self.pos += 1;
                        Ok(value)
                    }
                    _ => Err("missing closing parenthesis".to_string()),
                }
            }
            Some(c) if c.is_ascii_digit() || c == b'.' => self.parse_number(),
            Some(c) => Err(format!("unexpected character `{}`", c as char)),
            None => Err("unexpected end of expression".to_string()),
        }
    }

    fn parse_number(&mut self) -> Result<f64, String> {
        let start = self.pos;
        while let Some(c) = self.peek() {
            if c.is_ascii_digit() || c == b'.' {
                self.pos += 1;
            } else {
                break;
            }
        }
        let raw = std::str::from_utf8(&self.bytes[start..self.pos]).unwrap_or("");
        raw.parse::<f64>()
            .map_err(|_| format!("invalid number `{raw}`"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn evaluates_basic_arithmetic() {
        let t = CalculatorTool;
        assert_eq!(
            t.call(TOOL_NAME, r#"{"expression":"3 * 7"}"#).await.unwrap(),
            "21"
        );
        assert_eq!(
            t.call(TOOL_NAME, r#"{"expression":"1+2*3"}"#).await.unwrap(),
            "7"
        );
        assert_eq!(
            t.call(TOOL_NAME, r#"{"expression":"(1+2)*3"}"#).await.unwrap(),
            "9"
        );
    }

    #[tokio::test]
    async fn an_unknown_tool_name_is_a_contract_violation() {
        let t = CalculatorTool;
        assert_eq!(
            t.call("lookup", "{}").await,
            Err(ToolError::UnknownTool("lookup".to_string()))
        );
    }
}
