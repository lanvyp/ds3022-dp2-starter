select
    string_agg(word, ' ' order by order_no) as phrase,
    count(*)                                as n_words
from {{ ref('stg_fragments') }}
