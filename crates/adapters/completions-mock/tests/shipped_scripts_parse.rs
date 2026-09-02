//! The scripts shipped in `testing/config/` must parse against the current types.
//!
//! Without this, renaming a `Script` variant leaves the YAML silently stale: the
//! mock agent then fails at startup inside an L2 run, where it surfaces as a
//! scenario timeout rather than as "the script no longer matches the code".

use adapters_completions_mock::ScriptedScheduler;

#[test]
fn every_shipped_script_parses() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../testing/config");
    let mut checked = 0;

    for entry in std::fs::read_dir(&dir).expect("testing/config must exist") {
        let path = entry.expect("dir entry").path();
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or_default();
        if !name.contains("script") || path.extension().and_then(|e| e.to_str()) != Some("yaml") {
            continue;
        }
        let src = std::fs::read_to_string(&path).expect("read script");
        ScriptedScheduler::from_yaml(&src)
            .unwrap_or_else(|e| panic!("{} no longer parses: {e}", path.display()));
        checked += 1;
    }

    assert!(
        checked > 0,
        "found no *script*.yaml under {} — this test would otherwise pass vacuously",
        dir.display()
    );
}
