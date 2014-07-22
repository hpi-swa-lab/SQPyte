create view revenue as
	select
		suppkey as supplier_no,
		sum(extendedprice * (1 - discount)) as totarevenue
	from
		lineitem
	where
		shipdate >= date('1995-01-01')
		and shipdate < date('1995-01-01', '+3 month')
	group by
		suppkey;
