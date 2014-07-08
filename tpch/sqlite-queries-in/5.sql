select
	n.name,
	sum(l.extendedprice * (1 - l.discount)) as revenue
from
	customer c,
	orders o,
	lineitem l,
	supplier s,
	nation n,
	region r
where
	c.custkey = o.custkey
	and l.orderkey = o.orderkey
	and l.suppkey = s.suppkey
	and c.nationkey = s.nationkey
	and s.nationkey = n.nationkey
	and n.regionkey = r.regionkey
	and r.name = 'ASIA'
	and o.orderdate >= date('1996-01-01')
	and o.orderdate < date('1996-01-01', '+1 year')
group by
	n.name
order by
	revenue desc;