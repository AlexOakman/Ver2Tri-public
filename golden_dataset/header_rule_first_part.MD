# Task: Translate Vertica SQL (Header + DDL) to Trino SQL

You are given a Vertica SQL script containing a header comment block and a DDL statement. Your goal is to produce an equivalent Trino SQL script with a translated header and DDL, following the rules below. Output only the final Trino SQL code, without any additional commentary.

---

## Input Format

The input consists of:

1. **Header** – a comment block starting with `/* @header` and ending with `*/`. It contains metadata like datamart, type, engine, keys, params, scheduled runs, etc.
2. **DDL** – a `CREATE TABLE` statement (possibly with `IF NOT EXISTS`) in Vertica syntax.

Example:

```
/* @header
datamart: analytics.source_table
type: INCREMENTAL_REFRESH
engine: Vertica
date_col: event_date
archive: false
days_to_keep: 5480
keys:
  - user_id
  - event_date
params:
  - name: launch_id
  - name: first_date
  - name: last_date
scheduled:
  - first_date: $TODAY[-1]
    last_date: $TODAY[-1]
unit: Analytics
*/

CREATE TABLE IF NOT EXISTS target_schema.table_name
(
    event_date date,
    user_id int,
    ...
)
ORDER BY user_id, event_date
SEGMENTED BY HASH(user_id) ALL NODES;
```

---

## Translation Rules

### 1. Header Translation

- **Always preserve** the original `datamart` name, but append `_trino` to it (e.g., `analytics.source_table` -> `analytics.source_table_trino`).
- **Always preserve** the original `type` value (`INCREMENTAL_REFRESH` or `FULL_REFRESH`).  
- **Engine** must be set to `Trino` (capitalized exactly as shown).
- **date_col**:  
  - If original header contains `date_col`, keep it.  
  - If not present but `type` is `INCREMENTAL_REFRESH`, you must infer it from the DDL (the column that should be used for partitioning). Usually it is the date column used for incremental logic. If uncertain, set it to `event_date`.
  - For `INCREMENTAL_REFRESH`, prefer `date_co` as the partitioning column when it already exists or can be derived naturally from the source logic.
  - Never use a `TIMESTAMP` / `TIMESTAMP(6)` column as `date_col`. If the natural partition key is a timestamp column, create an additional `DATE` column with a similar semantic name (for example, `start_time` → `start_date`) and use that new `DATE` column as `date_col`.
- **days_to_keep**:  
  - If present in original header, keep the original value.  
  - If not present and `type` is `INCREMENTAL_REFRESH`, set to `365`.
- **keys**:  
  - If present, keep the original list.  
  - If not present, use `- key_column1` as placeholder; but better to infer from the DDL (primary key or typical distribution key).
- **params**:  
  - If present, keep the original list.  
  - For `INCREMENTAL_REFRESH`, ensure at least `launch_id`, `first_date`, `last_date` are present. Add them if missing.
- **scheduled**:  
  - Always rewrite as separate list items – one per parameter.  
  - If original `scheduled` contains combined entries like `first_date: $TODAY[-1]` and `last_date: $TODAY[-1]` on the same line, split them into two separate items.  
  - For `INCREMENTAL_REFRESH`, **always add** `- user_name: trino-daily` as the last item under `scheduled`.
  - For `FULL_REFRESH`, **always add** `- user_name: trino-daily` as the only item under `scheduled` (or as the last item if other items exist).
- **unit**:  
  - If present in original header, keep the original value.  
  - If not present, use `MY_UNIT`.

**Important**: The header must be a valid YAML-like block inside `/* @header ... */`. Indentation matters – use 2 spaces for lists.

### 2. DDL Translation

- **Table name**: Append `_trino` to the original table name. Preserve the schema prefix if present (e.g., `target_schema.table_name` -> `target_schema.table_name_trino`).
- **Column order**:
  - **If `type` is `INCREMENTAL_REFRESH`**: Move the column specified by `date_col` to the **last position** in the column list. All other columns keep their original order.
  - **If `type` is `FULL_REFRESH`**: Add a new constant column at the end named `constant` with data type `BIGINT`. This column can be used for partitioning.
- **Data types**: Convert Vertica types to Trino types according to the mapping below (see **Data Type Mapping**).
- **Constraints and storage clauses**: Remove Vertica‑specific clauses like `ORDER BY`, `SEGMENTED BY`, `ALL NODES`, etc. They are not valid in Trino.
- **Column constraints**: Do not use `NULL` or `NOT NULL` in Trino `CREATE TABLE` column definitions. The target Trino catalog used by this migration flow does not support these column constraints in DDL. Keep only the column name and data type.
- **Partitioning**:
  - **For `INCREMENTAL_REFRESH`**: Add `WITH (partitioned_by = ARRAY['date_col'])` after the column list. Replace `date_col` with the actual column name (e.g., `event_date`).
  - **For `FULL_REFRESH`**: Add `WITH (partitioned_by = ARRAY['constant'])`.
  - Trino partition columns must be `DATE`-typed for this migration flow. Do not partition by `TIMESTAMP` / `TIMESTAMP(6)`.
  - If the target incremental datamart should be partitioned by business date and `date_co` is expected, ensure the final DDL contains `date_co DATE` and partition by `date_co`.
- **Optional session setting**: Add a generic session statement only when the project configuration explicitly requires overwrite semantics. Do not assume an environment-specific catalog command by default.

### 3. Data Type Mapping

| Vertica Type              | Trino Type                 |
|---------------------------|----------------------------|
| `INT`                     | `BIGINT`                   |
| `NUMERIC` / `DECIMAL`     | `DECIMAL(20,8)`            |
| `FLOAT`                   | `DOUBLE`                   |
| `LONG VARCHAR`            | `VARCHAR(60000)`           |
| `VARCHAR` (with length)   | Same length, keep as is    |
| `TIMESTAMP`               | `TIMESTAMP(6)`             |
| `DATE`                    | `DATE`                     |
| `BOOLEAN`                 | `BOOLEAN`                  |
| `BIGINT`                  | `BIGINT` (unchanged)       |

If a type is not listed, preserve the original type name (Trino may support it).

---

## Example

**Input** (excerpt):

```
/* @header
datamart: analytics.source_table
type: INCREMENTAL_REFRESH
engine: Vertica
date_col: event_date
days_to_keep: 5480
keys:
  - user_id
  - event_date
params:
  - name: launch_id
  - name: first_date
  - name: last_date
scheduled:
  - first_date: $TODAY[-1]
    last_date: $TODAY[-1]
unit: Public Analytics
*/

CREATE TABLE IF NOT EXISTS target_schema.table_name
(
    event_date date,
    user_id int,
    external_user_id int,
    ...
    user_group_id int
)
ORDER BY user_id, event_date
SEGMENTED BY HASH(user_id) ALL NODES;
```

**Output**:

```
/* @header
datamart: analytics.source_table_trino
type: INCREMENTAL_REFRESH
engine: Trino
date_col: event_date
days_to_keep: 5480
keys:
  - user_id
  - event_date
params:
  - name: launch_id
  - name: first_date
  - name: last_date
scheduled:
  - first_date: $TODAY[-1]
    last_date: $TODAY[-1]
    user_name: trino-daily
unit: Public Analytics
*/

CREATE TABLE IF NOT EXISTS target_schema.table_name_trino
(
    user_id BIGINT,
    external_user_id BIGINT,
    ... (all columns except event_date, in original order)
    user_group_id BIGINT,
    event_date DATE
)
WITH (partitioned_by = ARRAY['event_date']);
```

---

## Output Format

- **No extra text** before or after the SQL code.
- The header comment block must be exactly as described.
- The DDL must be valid Trino syntax with proper indentation (use spaces, not tabs).
- Do not add environment-specific session statements unless the task context explicitly asks for one.

Now, apply these rules to the input provided below and generate the corresponding Trino SQL.
