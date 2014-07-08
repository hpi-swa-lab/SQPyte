select
	p.brand,
	p.type,
	p.size,
	count(distinct ps.suppkey) as supplier_cnt
from
	partsupp ps,
	part p
where
	p.partkey = ps.partkey
	and p.brand <> 'Brand#33'
	and p.type not like 'STANDARD%'
	and p.size in (2, 4, 6, 12, 17, 28, 49, 50)
	and ps.suppkey not in (
		select
			s.suppkey
		from
			supplier s
		where
			s.comment like '%Customer%Complaints%'
	)
group by
	p.brand,
	p.type,
	p.size
order by
	supplier_cnt desc,
	p.brand,
	p.type,
	p.size;