select sum(lo_revenue) as revenue
from lineorder
         left join dates on lo_orderdate = d_datekey
where d_yearmonthnum = 199401
  and lo_discount between 4 and 6
  and lo_quantity between 26 and 35;

SELECT last_statement_duration_us / 1000000.0 last_statement_duration_seconds
FROM current_session;