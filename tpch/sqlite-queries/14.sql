/* TPC_H Query 14 - Promotion Effect */
select
	100.00 * sum(case
		when p.type like 'PROMO%'
			then l.extendedprice * (1 - l.discount)
		else 0
	end) / sum(l.extendedprice * (1 - l.discount)) as promo_revenue
from
	lineitem l,
	part p
where
	l.partkey = p.partkey
	and l.shipdate >= date('1995-01-01')
	and l.shipdate < date('1995-01-01', '+1 month');

/*
Not implemented: 8
Divide
Function
IfNot
Multiply
Real
RealAffinity
String8
Subtract

Unique opcodes: 28
AggFinal
AggStep
Close
Column
Divide
Function
Ge
Goto
Halt
IfNot
Integer
Lt
Multiply
MustBeInt
Next
NotExists
Null
OpenRead
Real
RealAffinity
ResultRow
Rewind
String8
Subtract
TableLock
Trace
Transaction
VerifyCookie


0|Trace|0|0|0||00|
1|Null|0|3|0||00|
2|Null|0|4|0||00|
3|Null|0|5|0||00|
4|Null|0|1|0||00|
5|Null|0|2|0||00|
6|Goto|0|53|0||00|
7|OpenRead|0|8|0|11|00|
8|OpenRead|1|2|0|5|00|
9|Rewind|0|44|0||00|
10|Column|0|10|6||00|
11|String8|0|8|0|1995-01-01|00|
12|Function|1|8|7|date(-1)|01|
13|Lt|7|43|6|collseq(BINARY)|6a|
14|String8|0|9|0|1995-01-01|00|
15|String8|0|10|0|+1 month|00|
16|Function|3|9|7|date(-1)|02|
17|Ge|7|43|6|collseq(BINARY)|6a|
18|Column|0|1|7||00|
19|MustBeInt|7|43|0||00|
20|NotExists|1|43|7||00|
21|String8|0|11|0|PROMO%|00|
22|Column|1|4|12||00|
23|Function|1|11|7|like(2)|02|
24|IfNot|7|33|1||00|
25|Column|0|5|7||00|
26|RealAffinity|7|0|0||00|
27|Integer|1|14|0||00|
28|Column|0|6|15||00|
29|RealAffinity|15|0|0||00|
30|Subtract|15|14|13||00|
31|Multiply|13|7|9||00|
32|Goto|0|34|0||00|
33|Integer|0|9|0||00|
34|AggStep|0|9|1|sum(1)|01|
35|Column|0|5|15||00|
36|RealAffinity|15|0|0||00|
37|Integer|1|13|0||00|
38|Column|0|6|14||00|
39|RealAffinity|14|0|0||00|
40|Subtract|14|13|7||00|
41|Multiply|7|15|11||00|
42|AggStep|0|11|2|sum(1)|01|
43|Next|0|10|0||01|
44|Close|0|0|0||00|
45|Close|1|0|0||00|
46|AggFinal|1|1|0|sum(1)|00|
47|AggFinal|2|1|0|sum(1)|00|
48|Real|0|15|0|100|00|
49|Multiply|1|15|14||00|
50|Divide|2|14|16||00|
51|ResultRow|16|1|0||00|
52|Halt|0|0|0||00|
53|Transaction|0|0|0||00|
54|VerifyCookie|0|27|0||00|
55|TableLock|0|8|0|LineItem|00|
56|TableLock|0|2|0|Part|00|
57|Goto|0|7|0||00|
*/