with src as (
    select
        cast(trim(order_no) as integer) as order_no,   
        nullif(trim(word), '')          as word,  
        received_at
    from {{ source('raw', 'fragments') }}
),

ranked as (
    select
        *,
        row_number() over (partition by order_no order by received_at) as rn
    from src
)

select order_no, word, received_at
from ranked
where rn = 1
