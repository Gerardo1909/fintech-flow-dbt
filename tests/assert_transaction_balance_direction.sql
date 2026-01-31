select *
from {{ ref('fct_transactions') }}
where transaction_type_description = 'income'
  and balance_after_transaction < (balance_after_transaction - amount)
