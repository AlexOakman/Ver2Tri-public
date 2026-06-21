; Do not add extra comments.
[INSERT_RULES]
; Build the final INSERT block depending on refresh type.

; 1. FULL_REFRESH:
;    - If the project requires an overwrite session statement, add the configured generic statement before INSERT.
;    - Add ", 1 as constant" at the end of SELECT as a dummy partition.

insert into target_schema.{{name}}
select
    cast(:launch_id as bigint) as launch_id,
    *,
    1 as constant
from temp_table_as_select;

; 2. INCREMENTAL_REFRESH:
;    - Add a session statement only when the project configuration explicitly requires it.
;    - Do not add the constant; partitioning comes from the date column.

insert into target_schema.{{name}}
select
    cast(:launch_id as bigint) as launch_id,
    *
from temp_table_as_select;
; Do not add extra comments.
