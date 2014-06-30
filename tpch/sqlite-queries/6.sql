/* TPC_H Query 6 - Forecasting Revenue Change */
select
	sum(l.extendedprice * l.discount) as revenue
from
	lineitem l
where
	l.shipdate >= date('1996-01-01')
	and l.shipdate < date('1996-01-01', '+1 year')
	and l.discount between 0.04 and 0.07
	and l.quantity < 25;

/*
Not implemented: 5
Function
Multiply
Real
RealAffinity
String8

Unique opcodes: 25
AggFinal
AggStep
Close
Column
Copy
Function
Ge
Goto
Gt
Halt
Integer
Lt
Multiply
Next
Null
OpenRead
Real
RealAffinity
ResultRow
Rewind
String8
TableLock
Trace
Transaction
VerifyCookie


0|Trace|0|0|0||00|
1|Null|0|2|0||00|
2|Null|0|3|0||00|
3|Null|0|1|0||00|
4|Real|0|4|0|0.04|00|
5|Real|0|5|0|0.07000000000000001|00|
6|Integer|25|6|0||00|
7|Goto|0|36|0||00|
8|OpenRead|0|8|0|11|00|
9|Rewind|0|31|0||00|
10|Column|0|10|7||00|
11|String8|0|9|0|1996-01-01|00|
12|Function|1|9|8|date(-1)|01|
13|Lt|8|30|7|collseq(BINARY)|6a|
14|String8|0|10|0|1996-01-01|00|
15|String8|0|11|0|+1 year|00|
16|Function|3|10|8|date(-1)|02|
17|Ge|8|30|7|collseq(BINARY)|6a|
18|Column|0|6|8||00|
19|RealAffinity|8|0|0||00|
20|Lt|4|30|8|collseq(BINARY)|6d|
21|Gt|5|30|8|collseq(BINARY)|6d|
22|Column|0|4|12||00|
23|Ge|6|30|12|collseq(BINARY)|6c|
24|Column|0|5|12||00|
25|RealAffinity|12|0|0||00|
26|Column|0|6|8||00|
27|RealAffinity|8|0|0||00|
28|Multiply|8|12|10||00|
29|AggStep|0|10|1|sum(1)|01|
30|Next|0|10|0||01|
31|Close|0|0|0||00|
32|AggFinal|1|1|0|sum(1)|00|
33|Copy|1|14|0||00|
34|ResultRow|14|1|0||00|
35|Halt|0|0|0||00|
36|Transaction|0|0|0||00|
37|VerifyCookie|0|27|0||00|
38|TableLock|0|8|0|LineItem|00|
39|Goto|0|8|0||00|
*/