# STYLE.md — writing standard (easy to read)

- Naming: `snake_case` fns/vars, `PascalCase` types, `SCREAMING` consts. Keep Vietnamese domain words (`gia`, `khoi_luong`, `dong_bar`, `xu_ly`) exactly as C++; English for infra (`Core`, `Producer`, `Closer`).
- Size: fn <=40 lines, file <=400 lines, nesting <=3 (early return). One concept per fn.
- Safety: no `unwrap`/`expect`/`panic!` in lib (return `Result`/`Option`); every `unsafe` needs `// SAFETY: <invariant>` + test; every `pub fn` has doc with `O()` + example.
- Structure: `// 1. guard -> 2. fold -> 3. policy -> 4. publish` numbered steps in hot fns. No abbreviations beyond `ts/qty/idx/seq`.
- Tests: each module has `#[cfg(test)]` with 3 tests: empty/edge, golden vs C++ vector, concurrency (loom/miri where unsafe).
- Commits: `feat(core): ...`, `perf(fp): ...`, refs `closes B-NNN`; update RULES.md in same change.
