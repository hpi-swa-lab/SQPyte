/* TPC_H Query 17 - Small-Quantity-Order Revenue */
select
	sum(l.extendedprice) / 7.0 as avg_yearly
from
	lineitem l,
	part p
where
	p.partkey = l.partkey
	and p.brand = 'Brand#45'
	and p.container = 'SM PKG'
	and l.quantity < (
		select
			0.2 * avg(quantity)
		from
			lineitem
		where
			partkey = p.partkey
	);

/*
Not implemented: 13
Divide
IdxGE
IdxRowid
IfZero
IsNull
Move
Multiply
Real
RealAffinity
Rowid
Seek
SeekGe
String8


Unique opcodes: 31
AggFinal
AggStep
Close
Column
Divide
Ge
Goto
Halt
IdxGE
IdxRowid
IfZero
Integer
IsNull
Move
Multiply
Ne
Next
Null
OpenRead
Real
RealAffinity
ResultRow
Rewind
Rowid
Seek
SeekGe
String8
TableLock
Trace
Transaction
VerifyCookie


0|Trace|0|0|0||00|
1|Null|0|2|0||00|
2|Null|0|1|0||00|
3|String8|0|3|0|Brand#45|00|
4|String8|0|4|0|SM PKG|00|
5|Goto|0|57|0||00|
6|OpenRead|1|2|0|7|00|
7|OpenRead|0|8|0|6|00|
8|OpenRead|3|20|0|keyinfo(1,BINARY)|00|
9|Rewind|1|49|0||00|
10|Column|1|3|5||00|
11|Ne|3|48|5|collseq(BINARY)|6a|
12|Column|1|6|6||00|
13|Ne|4|48|6|collseq(BINARY)|6a|
14|Rowid|1|8|0||00|
15|IsNull|8|48|0||00|
16|SeekGe|3|48|8|1|00|
17|IdxGE|3|48|8|1|01|
18|IdxRowid|3|7|0||00|
19|Seek|0|7|0||00|
20|Column|0|4|9||00|
21|Null|0|11|0||00|
22|Integer|1|12|0||00|
23|Null|0|14|0||00|
24|Null|0|13|0||00|
25|OpenRead|2|8|0|5|00|
26|OpenRead|4|20|0|keyinfo(1,BINARY)|00|
27|Rowid|1|15|0||00|
28|IsNull|15|36|0||00|
29|SeekGe|4|36|15|1|00|
30|IdxGE|4|36|15|1|01|
31|IdxRowid|4|6|0||00|
32|Seek|2|6|0||00|
33|Column|2|4|16||00|
34|AggStep|0|16|13|avg(1)|01|
35|Next|4|30|0||00|
36|Close|2|0|0||00|
37|Close|4|0|0||00|
38|AggFinal|13|1|0|avg(1)|00|
39|Real|0|6|0|0.2|00|
40|Multiply|13|6|17||00|
41|Move|17|11|1||00|
42|IfZero|12|43|-1||00|
43|Ge|11|47|9|collseq(BINARY)|6c|
44|Column|0|5|16||00|
45|RealAffinity|16|0|0||00|
46|AggStep|0|16|1|sum(1)|01|
47|Next|3|17|0||00|
48|Next|1|10|0||01|
49|Close|1|0|0||00|
50|Close|0|0|0||00|
51|Close|3|0|0||00|
52|AggFinal|1|1|0|sum(1)|00|
53|Real|0|7|0|7|00|
54|Divide|7|1|18||00|
55|ResultRow|18|1|0||00|
56|Halt|0|0|0||00|
57|Transaction|0|0|0||00|
58|VerifyCookie|0|27|0||00|
59|TableLock|0|2|0|Part|00|
60|TableLock|0|8|0|LineItem|00|
61|Goto|0|6|0||00|
*/