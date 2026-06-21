"""
DSPy Signatures for Vertica → Trino SQL migration.
Contains translation contracts and quality assessment signatures.
"""

import dspy


REPAIR_MODE_GUIDANCE = """
Repair Mode:
- If context_hint contains a repair instruction block, treat the task as REPAIR MODE.
- In REPAIR MODE, the highest priority is fixing only the triggered pattern/validation issue.
- Use current_trino and any structured repair context from context_hint as the working SQL.
- Keep the output as close as possible to current_trino except for the minimal required fix.
- Respect allowed_change_scope and forbidden_change_scope from context_hint strictly.
- Never rewrite unrelated JOIN logic, filters, grouping, selected columns, or aliases unless the repair issue explicitly points there.
"""


class VerticaToTrino(dspy.Signature):
    """
    You are an expert SQL translator specializing in Vertica to Trino migration.
    Convert Vertica SQL to valid Trino SQL preserving exact logic and semantics.

    Critical Translation Rules:
    - Never use USING in JOIN output. Always rewrite JOIN ... USING(...) into explicit JOIN ... ON left.col = right.col conditions
    - Parameters: Always cast colon parameters like :first_date, :last_date, :launch_id, :actual_date to the target Trino type before use, for example CAST(:first_date AS DATE), CAST(:last_date AS DATE), CAST(:launch_id AS BIGINT)
    - Typed literals: Never compare date or timestamp columns to plain string literals; use typed DATE/TIMESTAMP literals or explicit CAST, for example DATE '2024-01-01'
    - QUALIFY: Rewrite QUALIFY into a subquery or CTE that computes the window expression, then filter in an outer WHERE
    - Never reference v_temp_schema in output Trino SQL; remove the schema prefix and keep the temporary table name stable
    - Never emit DELETE statements in Trino output for this project
    - Never keep version_id in final Trino SQL if project rules forbid it
    - INTERPOLATE JOIN: Replace with range join using LEAD() window function and date range conditions
    - CAST syntax: Use CAST(expression AS type) instead of Vertica's expression::type
    - Data types: Replace NUMERIC with DECIMAL. No NUMERIC type in Trino
    - Division: Explicitly cast operands to DECIMAL to avoid integer division (1/10 becomes 0 in Trino, not 0.1)
    - Window functions: IGNORE NULLS goes outside function parentheses before OVER(), not inside
    - Hints: Remove all Vertica optimizer hints (/*+ direct */, /*+jtype(...)*/, /*+distrib(...)*/)
    - Partition filtering: Trino requires mandatory partition column filters (e.g., WHERE event_date >= CAST('2024-01-01' AS DATE))
    - String matching: Replace ILIKE with lower(column) LIKE 'pattern'
    - DECODE: Convert to CASE WHEN statements
    - EXPLODE: Replace with UNNEST(array_column) AS t(alias)
    - LISTAGG: Use ARRAY_JOIN(ARRAY_AGG(column ORDER BY ...), separator)
    - IMPLode: Use ARRAY_AGG instead of implode
    - PERCENTILE_DISC: Implement using window functions (ROW_NUMBER, COUNT) and CEIL/FLOOR logic
    - TRANSPOSE: Replace with UNNEST of ARRAY[column_names] and ARRAY[column_values]
    - Date arithmetic: Use INTERVAL 'N' DAY/MONTH instead of simple subtraction
    - TIMESTAMPADD/TIMESTAMPDIFF: Use date_diff() or date_add() with INTERVAL
    - JSON functions: JsonLookupA -> coalesce(json_extract(...), json_extract(...))
    - MapItems/MapJsonExtractor: Use CROSS JOIN LATERAL with map_entries and unnest
    - Conditional functions: conditional_change_event -> combination of LAG and SUM window functions
    - Distance: Replace Distance(lat, lon...) with ST_Distance(to_spherical_geography(ST_Point(lon, lat)), ...)
    - Bitwise shift: Use bitwise_left_shift(value, positions) instead of <<
    - toCryptoHash: Returns VARBINARY, wrap with from_utf8() or to_hex() if string needed
    - Hash: Keep as is (custom function available)
    - Approximate functions: approximate_median -> APPROX_PERCENTILE(column, 0.5)
    - Null handling: Replace NULLIFZERO and ZEROIFNULL with COALESCE or CASE WHEN
    - ISUTF8: Replace with regex pattern matching
    - If/Case: Can use IF(condition, true_value, false_value) for simple logic in Trino

    Schema Context:
    - Replace v_monitor system tables with information_schema
    - Remove SEGMENTED BY clauses (Vertica-specific storage)
    - Remove EXPORT TO VERTICA statements

    Output Requirements:
    - Return clean SQL without markdown code blocks
    - Preserve all CTEs (WITH clauses) structure
    - Maintain table aliases and column references except where explicitly changed above
    """
    
    vertica_sql: str = dspy.InputField(desc="Original Vertica SQL code to translate")
    context_hint: str = dspy.InputField(
        desc="Context from previously translated parts: table schemas, aliases, known column types", 
        default=""
    )
    
    trino_sql: str = dspy.OutputField(
        desc="Translated Trino SQL code only, no explanations, no markdown formatting"
    )


class VerticaToTrinoProgram(dspy.Module):
    """Явная DSPy-программа для compile path с именованным predictor."""

    def __init__(self):
        super().__init__()
        self.translate = dspy.Predict(VerticaToTrino)

    def forward(self, vertica_sql: str, context_hint: str = "", part_type: str = ""):
        return self.translate(
            vertica_sql=vertica_sql,
            context_hint=context_hint,
            part_type=part_type,
        )


class SQLJudge(dspy.Signature):
    """
    Expert SQL Quality Judge for Vertica to Trino translation validation.
    Compare original Vertica SQL with translated Trino SQL and assess equivalence.
    
    Evaluation Criteria:
    1. Semantic Equivalence (60%): Does Trino SQL produce identical results to Vertica SQL?
       - Check JOIN logic preservation (especially USING vs ON conversions)
       - Verify window function behavior (frame specifications, ORDER BY)
       - Check NULL handling (IGNORE NULLS placement)
       - Verify date arithmetic and interval calculations
    
    2. Syntax Correctness (20%): Is the Trino SQL valid?
       - No Vertica-specific functions remaining (no :: casts, no DECODE, no ILIKE)
       - Proper CAST syntax
       - Correct window function placement of IGNORE NULLS
       - Valid Trino data types (DECIMAL not NUMERIC)
    
    3. Logic Preservation (20%): Are there logic changes that affect results?
       - Division behavior (integer vs decimal)
       - Date difference calculations (calendar days vs actual time)
       - Aggregation behavior (LISTAGG vs ARRAY_JOIN equivalence)
       - JSON extraction logic
    
    Scoring Guide:
    - 1.0: Perfect translation, ready for production
    - 0.8-0.9: Minor issues, might work but needs review
    - 0.5-0.7: Major logic differences or syntax errors
    - <0.5: Critical errors, translation failed
    """
    
    vertica_sql: str = dspy.InputField(desc="Original Vertica SQL")
    trino_sql: str = dspy.InputField(desc="Translated Trino SQL to evaluate")
    reference_trino: str = dspy.InputField(
        desc="Reference/ground truth Trino SQL if available for comparison",
        default=""
    )
    
    semantic_equivalence: bool = dspy.OutputField(
        desc="True if Trino SQL produces exactly the same results as Vertica SQL"
    )
    syntax_correctness: int = dspy.OutputField(
        desc="Score 0-10 for Trino SQL validity and syntax correctness"
    )

    logic_issues: str = dspy.OutputField(
        desc="JSON-serialized list of identified logic differences or potential bugs. Example: '[\"Issue 1\", \"Issue 2\"]' or '[]' if no issues"
    )
    score: float = dspy.OutputField(
        desc="Final quality score from 0.0 to 1.0"
    )
