-- One clean row per fragment.
-- An exact duplicate (same order_no AND same word) is a redelivered message: group by collapses it.
-- Two DIFFERENT words for one order_no stay as two rows, so the unique test fails and the gate stops the flow.
select
    cast(trim(order_no) as integer) as order_no,   -- arrives as a string; '10' must sort after '2'
    nullif(trim(word), '')          as word,       -- trim spaces; empty -> NULL (caught by not_null)
    min(received_at)                as received_at -- earliest copy if it arrived twice
from {{ source('raw', 'fragments') }}
group by 1, 2
