-- Per-response `tool_choice` (D22), in outbound provider shape: a mode string
-- (`"auto"` / `"none"` / `"required"`) or a specific function
-- (`{ "type": "function", "function": { "name" } }`). NULL means "provider default".
ALTER TABLE responses ADD COLUMN IF NOT EXISTS tool_choice JSONB;
