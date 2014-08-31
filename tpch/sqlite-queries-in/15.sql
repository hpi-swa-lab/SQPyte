select
	suppkey,
	name,
	address,
	phone,
	totarevenue
from
	supplier,
	revenue
where
	suppkey = supplier_no
	and totarevenue = (
		select
			max(totarevenue)
		from
			revenue
	)
order by
	suppkey;