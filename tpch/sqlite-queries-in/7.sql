select
	supp_nation,
	cust_nation,
	l_year,
	sum(volume)
from
	(
		select
			n1.name as supp_nation,
			n2.name as cust_nation,
			strftime('%Y', l.shipdate) as l_year,
			l.extendedprice * (1 - l.discount) as volume
		from
			supplier s,
			lineitem l,
			orders o,
			customer c,
			nation n1,
			nation n2
		where
			s.suppkey = l.suppkey
			and o.orderkey = l.orderkey
			and c.custkey = o.custkey
			and s.nationkey = n1.nationkey
			and c.nationkey = n2.nationkey
			and (
				(n1.name = 'UNITED KINGDOM' and n2.name = 'UNITED STATES')
				or (n1.name = 'UNITED STATES' and n2.name = 'UNITED KINGDOM')
			)
			and l.shipdate between date('1995-01-01') and date('1996-12-31')
	) as shipping
group by
	supp_nation,
	cust_nation,
	l_year
order by
	supp_nation,
	cust_nation,
	l_year;