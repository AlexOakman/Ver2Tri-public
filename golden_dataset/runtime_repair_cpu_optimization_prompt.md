=== CPU / RESOURCE ERROR GUIDANCE ===
Use this guidance only for Trino resource failures such as EXCEEDED_CPU_LIMIT,
INSUFFICIENT_RESOURCES or EXCEEDED_LOCAL_MEMORY_LIMIT.

Goal: make the part executable without changing business semantics.

Investigation policy:
- Prefer EXPLAIN or read-only diagnostic queries before making structural changes.
- Identify whether the bottleneck is a heavy scan, expensive join/exchange, repeated CTE scan,
  regex/json/string processing, or a large source joined before filtering.
- If the part depends on previous runtime tables, inspect those tables and related parts first.

Safe optimization patterns:
- Materialize a heavy/reused CTE as a runtime temp table only when it reduces repeated scans or volatility.
- Pre-filter large source tables through small IN/prep runtime tables based on already computed keys.
- Avoid direct joins to very large source tables when a narrow relevant-key table is available.
- For CPU-heavy expressions, simplify expressions only when semantics are preserved and evidence supports it.

Hard constraints:
- Do not change joins, filters, keys, date windows, output columns or source semantics just to make the query faster.
- Do not replace semantic patterns such as INTERPOLATE/range logic with ROW_NUMBER shortcuts.
- If the fix changes query structure materially, note that compare/per-column validation is required after runtime passes.
- Save one focused fix to one part; keep unrelated formatting and logic unchanged.
