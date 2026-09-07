-- Functions offered to the model this turn (D22 `tools` request field), stored
-- per-response rather than as a static deployment config so one fleet can serve
-- callers with different tool sets. Outbound provider shape: an array of
-- `{ "type": "function", "function": { "name", "description", "parameters", "strict" } }`.
ALTER TABLE responses ADD COLUMN IF NOT EXISTS tools JSONB NOT NULL DEFAULT '[]'::jsonb;
