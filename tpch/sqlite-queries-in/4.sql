select
	o.orderpriority,
	count(*) as order_count
from
	orders o
where
	o.orderdate >= date('1996-01-01')
	and o.orderdate < date('1996-01-01', '+3 month')
	and exists (
		select
			*
		from
			lineitem l
		where
			l.orderkey = o.orderkey
			and l.commitdate < l.receiptdate
	)
group by
	o.orderpriority
order by
	o.orderpriority;