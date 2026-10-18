-- Fails (returns a row) unless stg_fragments holds exactly N fragments numbered 1..N with no gaps.
-- N comes from vars.expected_fragments in dbt_project.yml (21).
with stats as (
    select
        count(*)                 as n_rows,
        count(distinct order_no) as n_distinct,
        min(order_no)            as min_no,
        max(order_no)            as max_no
    from {{ ref('stg_fragments') }}
)

select *
from stats
where n_rows     != {{ var('expected_fragments') }}
   or n_distinct != {{ var('expected_fragments') }}
   or min_no     != 1
   or max_no     != {{ var('expected_fragments') }}
   or min_no is null
