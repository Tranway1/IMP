select sum(lo_revenue) as revenue
from lineorder
         left join dates on lo_orderdate = d_datekey
where d_year = 1993
  and lo_discount between 1 and 3
  and lo_quantity < 25;


SELECT last_statement_duration_us / 1000000.0 last_statement_duration_seconds
FROM current_session;