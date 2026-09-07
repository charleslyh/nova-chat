//! A [`ToolExecutor`] that evaluates arithmetic expressions.
//!
//! This is the concrete form of "the agent offers a tool": the model declares a
//! call to `calculate`, the agent loop hands it here through the [`ToolExecutor`]
//! port, and the result is fed back as a `function_call_output` item. Being a
//! separate adapter keeps the agent loop — and every other caller of the port —
//! unaware of how a tool is implemented (same shape as an MCP bridge or a
//! registry of pure functions).
//!
//! # Why a calculator
//!
//! Arithmetic is deterministic, needs no network and no state, and is trivial to
//! verify by hand. That makes it the cheapest possible end-to-end probe of the
//! whole tool-calling pipeline: request `tools` → model `function_call` →
//! execution → `function_call_output` → model answer → streamed transcript.
//!
//! # Failure policy (matches the port's contract)
//!
//! A *recoverable* mistake — a malformed expression, division by zero, a missing
//! argument — is returned as `Ok` text so the model can read the error and retry
//! with a corrected call. Only naming a tool this executor does not hold is an
//! `Err`, since that is a contract violation, not a runtime failure.

use async_trait::async_trait;
use nova_responses_core::{ToolError, ToolExecutor};

/// The tool name the model may call. Everything else is `UnknownTool`.
pub const TOOL_NAME: &str = "calculate";

/// A tool executor that evaluates a single `expression` argument.
///
/// The arguments are the JSON the model produced, kept opaque and parsed here:
/// `{"expression": "3 * 7"}` → `"21"`.
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
            // A malformed arguments payload is the model's mistake, not ours:
            // surface it as readable text so the model can correct the call.
            Err(message) => return Ok(format!("error: {message}")),
        };

        match evaluate(&expr) {
            Ok(value) => Ok(format_number(value)),
            Err(message) => Ok(format!("error: {message}")),
        }
    }
}

/// Extract the `expression` field from the model's JSON arguments.
fn parse_expression(arguments: &str) -> Result<String, String> {
    let value: serde_json::Value =
        serde_json::from_str(arguments).map_err(|e| format!("arguments are not JSON: {e}"))?;
    match value.get("expression") {
        Some(serde_json::Value::String(s)) => Ok(s.clone()),
        Some(_) => Err("argument `expression` must be a string".to_string()),
        None => Err("missing `expression` argument".to_string()),
    }
}

/// Format a computed value for the model.
///
/// Floating-point arithmetic accumulates tiny errors (e.g.
/// `123*1234/123 - 123*23.4` evaluates to `-1644.1999999999998`). A long dirty
/// number makes the model distrust the result and re-call the tool, so round to
/// 10 significant decimal places and strip trailing zeros before returning.
fn format_number(value: f64) -> String {
    if !value.is_finite() {
        return value.to_string();
    }
    // Round to 10 decimal places to swallow accumulated FP error.
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

/// Evaluate an arithmetic expression with the four basic operators, parentheses
/// and a unary minus. No functions, no variables, no assignments — a calculator,
/// not a programming language.
///
/// Grammar (recursive descent):
/// ```text
/// expr   := term (('+' | '-') term)*
/// term   := factor (('*' | '/') factor)*
/// factor := ('-' | '+') factor | primary
/// primary:= NUMBER | '(' expr ')'
/// ```
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
            t.call(TOOL_NAME, r#"{"expression":"3 * 7"}"#)
                .await
                .unwrap(),
            "21"
        );
        assert_eq!(
            t.call(TOOL_NAME, r#"{"expression":"1+2*3"}"#)
                .await
                .unwrap(),
            "7"
        );
        assert_eq!(
            t.call(TOOL_NAME, r#"{"expression":"(1+2)*3"}"#)
                .await
                .unwrap(),
            "9"
        );
    }

    #[tokio::test]
    async fn handles_floats_and_unary_minus() {
        let t = CalculatorTool;
        assert_eq!(
            t.call(TOOL_NAME, r#"{"expression":"7 / 2"}"#)
                .await
                .unwrap(),
            "3.5"
        );
        assert_eq!(
            t.call(TOOL_NAME, r#"{"expression":"-3 + 5"}"#)
                .await
                .unwrap(),
            "2"
        );
    }

    #[tokio::test]
    async fn rounds_away_floating_point_error() {
        let t = CalculatorTool;
        // 123*1234/123 - 123*23.4 evaluates to -1644.1999999999998 in f64;
        // the formatted answer must be the clean -1644.2.
        assert_eq!(
            t.call(TOOL_NAME, r#"{"expression":"123*1234/123 - 123*23.4"}"#)
                .await
                .unwrap(),
            "-1644.2"
        );
        assert_eq!(
            t.call(TOOL_NAME, r#"{"expression":"0.1 + 0.2"}"#)
                .await
                .unwrap(),
            "0.3"
        );
    }

    #[tokio::test]
    async fn reports_recoverable_errors_as_text() {
        let t = CalculatorTool;
        // Division by zero is returned as readable text, not a hard failure.
        assert_eq!(
            t.call(TOOL_NAME, r#"{"expression":"1/0"}"#).await.unwrap(),
            "error: division by zero"
        );
        // Malformed arguments surface the reason so the model can retry.
        let err = t.call(TOOL_NAME, "not json").await.unwrap();
        assert!(
            err.starts_with("error: arguments are not JSON:"),
            "unexpected message: {err}"
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

    #[test]
    fn rejects_malformed_expressions() {
        for (expr, needle) in [
            ("1 +", "unexpected end"),
            ("1 + (2 * 3", "missing closing parenthesis"),
            ("1 + @", "unexpected character"),
            ("1.2.3", "invalid number"),
        ] {
            let err = evaluate(expr).unwrap_err();
            assert!(err.contains(needle), "expr={expr:?} err={err:?}");
        }
    }
}
