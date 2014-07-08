select
	ps.partkey,
	sum(ps.supplycost * ps.availqty) as value
from
	partsupp ps,
	supplier s,
	nation n
where
	ps.suppkey = s.suppkey
	and s.nationkey = n.nationkey
	and n.name = 'FRANCE'
group by
	ps.partkey having
		sum(ps.supplycost * ps.availqty) > (
			select
				sum(ps.supplycost * ps.availqty) * 0.01
			from
				partsupp ps,
				supplier s,
				nation n
			where
				ps.suppkey = s.suppkey
				and s.nationkey = n.nationkey
				and n.name = 'FRANCE'
		)
order by
	value desc;