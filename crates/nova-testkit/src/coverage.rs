use std::collections::{HashMap, HashSet};
use std::path::Path;

use anyhow::Result;
use serde::Serialize;

use crate::oracles::builtin_oracles;
use crate::scenario::ScenarioSpec;

#[derive(Debug, Clone)]
pub struct KnownRequirement {
    pub id: String,
}

/// Iteration-0 known FR/CR/INV subset for coverage matrix.
pub fn known_requirements() -> Vec<KnownRequirement> {
    [
        "FR-1", "FR-2.1", "FR-2.2", "FR-2.3", "FR-2.4", "FR-2.5", "FR-2.6", "FR-21", "CR-1",
        "CR-3", "CR-7", "CR-10", "INV-1", "INV-2", "INV-35", "D4", "D11", "D14", "D15", "D16",
        "D17",
    ]
    .into_iter()
    .map(|id| KnownRequirement { id: id.into() })
    .collect()
}

#[derive(Debug, Default, Serialize)]
pub struct CoverageRegistry {
    pub covered: HashMap<String, Vec<String>>,
    pub unknown_refs: Vec<String>,
    pub gaps: Vec<String>,
}

impl CoverageRegistry {
    pub fn from_scenarios(scenario_dirs: &[&Path]) -> Result<Self> {
        let known: HashSet<String> = known_requirements().into_iter().map(|k| k.id).collect();
        let mut covered: HashMap<String, Vec<String>> = HashMap::new();
        let mut unknown_refs = Vec::new();

        for o in builtin_oracles() {
            for c in o.covers() {
                covered
                    .entry((*c).to_string())
                    .or_default()
                    .push(format!("oracle:{}", o.id()));
            }
        }

        // L0 conformance covers (not YAML)
        for c in ["INV-1", "INV-2", "D11", "D14", "CR-1"] {
            covered
                .entry(c.to_string())
                .or_default()
                .push("conformance:l0".into());
        }
        covered
            .entry("D16".into())
            .or_default()
            .push("docs:task-profiles".into());

        for dir in scenario_dirs {
            if !dir.exists() {
                continue;
            }
            for ent in std::fs::read_dir(dir)? {
                let p = ent?.path();
                if p.extension().and_then(|x| x.to_str()) != Some("yaml")
                    && p.extension().and_then(|x| x.to_str()) != Some("yml")
                {
                    continue;
                }
                let text = std::fs::read_to_string(&p)?;
                let spec: ScenarioSpec = serde_yaml::from_str(&text)?;
                for c in &spec.covers {
                    if !known.contains(c) {
                        unknown_refs.push(format!("{} in {}", c, p.display()));
                    }
                    covered
                        .entry(c.clone())
                        .or_default()
                        .push(format!("scenario:{}", spec.id));
                }
            }
        }

        let mut gaps = Vec::new();
        for k in &known {
            if !covered.contains_key(k) {
                gaps.push(k.clone());
            }
        }
        gaps.sort();
        unknown_refs.sort();

        Ok(Self {
            covered,
            unknown_refs,
            gaps,
        })
    }

    pub fn render_markdown(&self) -> String {
        let mut out = String::from("# Traceability matrix\n\n");
        out.push_str("| Requirement | Sources |\n|---|---|\n");
        let mut keys: Vec<_> = self.covered.keys().cloned().collect();
        keys.sort();
        for k in keys {
            let src = self.covered[&k].join(", ");
            out.push_str(&format!("| {k} | {src} |\n"));
        }
        out.push_str("\n## Gaps (registered, uncovered)\n\n");
        if self.gaps.is_empty() {
            out.push_str("_none_\n");
        } else {
            for g in &self.gaps {
                out.push_str(&format!("- {g}\n"));
            }
        }
        out.push_str("\n## Unknown covers refs\n\n");
        if self.unknown_refs.is_empty() {
            out.push_str("_none_\n");
        } else {
            for u in &self.unknown_refs {
                out.push_str(&format!("- {u}\n"));
            }
        }
        out
    }
}
