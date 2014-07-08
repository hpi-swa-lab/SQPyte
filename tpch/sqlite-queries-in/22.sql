select
	cntrycode,
	count(*) as numcust,
	sum(acctbal) as totacctbal
from
	(
		select
			substr(c.phone, 1, 2) as cntrycode,
			c.acctbal
		from
			customer c
		where
			substr(c.phone, 1, 2) in
				('10', '11', '13', '17', '25', '29', '34')
			and c.acctbal > (
				select
					avg(acctbal)
				from
					customer
				where
					acctbal > 0.00
					and substr(phone, 1, 2) in
						('10', '11', '13', '17', '25', '29', '34')
			)
			and not exists (
				select
					*
				from
					orders o
				where
					o.custkey = c.custkey
			)
	) as custsale
group by
	cntrycode
order by
	cntrycode;
	