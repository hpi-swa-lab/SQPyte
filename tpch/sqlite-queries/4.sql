/* TPC_H Query 4 - Order Priority Checking */
select
	o.orderpriority,
	count(*) as order_count
from
	orders o
where
	o.orderdate >= date('1996-01-01')
	and o.orderdate < date('1996-01-01', '+3 month')
	and exists (
		select
			*
		from
			lineitem l
		where
			l.orderkey = o.orderkey
			and l.commitdate < l.receiptdate
	)
group by
	o.orderpriority
order by
	o.orderpriority;

/*
Not implemented: 23
Compare
Function
Gosub
IdxGE
IdxRowid
IfNot
IfPos
IfZero
IsNull
Jump
MakeRecord
Move
OpenPseudo
Return
Seek
SeekGe
Sequence
SorterData
SorterInsert
SorterNext
SorterOpen
SorterSort
String8

Unique opcodes: 40
AggFinal
AggStep
Close
Column
Compare
Copy
Function
Ge
Gosub
Goto
Halt
IdxGE
IdxRowid
IfNot
IfPos
IfZero
Integer
IsNull
Jump
MakeRecord
Move
Next
Null
OpenPseudo
OpenRead
ResultRow
Return
Seek
SeekGe
Sequence
SorterData
SorterInsert
SorterNext
SorterOpen
SorterSort
String8
TableLock
Trace
Transaction
VerifyCookie


0|Trace|0|0|0||00|
1|SorterOpen|2|2|0|keyinfo(1,BINARY)|00|
2|Integer|0|4|0||00|
3|Integer|0|3|0||00|
4|Null|0|7|7||00|
5|Gosub|6|73|0||00|
6|Goto|0|77|0||00|
7|OpenRead|0|9|0|6|00|
8|OpenRead|3|24|0|keyinfo(1,BINARY)|00|
9|String8|0|10|0|1996-01-01|00|
10|Function|1|10|9|date(-1)|01|
11|IsNull|9|46|0||00|
12|SeekGe|3|46|9|1|00|
13|String8|0|11|0|1996-01-01|00|
14|String8|0|12|0|+3 month|00|
15|Function|3|11|9|date(-1)|02|
16|IsNull|9|46|0||00|
17|IdxGE|3|46|9|1|00|
18|Column|3|0|13||00|
19|IsNull|13|45|0||00|
20|IdxRowid|3|13|0||00|
21|Seek|0|13|0||00|
22|Integer|0|15|0||00|
23|Integer|1|16|0||00|
24|OpenRead|1|8|0|16|00|
25|OpenRead|4|16|0|keyinfo(2,BINARY,BINARY)|00|
26|IdxRowid|3|17|0||00|
27|IsNull|17|38|0||00|
28|SeekGe|4|38|17|1|00|
29|IdxGE|4|38|17|1|01|
30|IdxRowid|4|18|0||00|
31|Seek|1|18|0||00|
32|Column|1|11|19||00|
33|Column|1|12|20||00|
34|Ge|20|37|19|collseq(BINARY)|6a|
35|Integer|1|15|0||00|
36|IfZero|16|38|-1||00|
37|Next|4|29|0||00|
38|Close|1|0|0||00|
39|Close|4|0|0||00|
40|IfNot|15|45|1||00|
41|Column|0|5|11||00|
42|Sequence|2|12|0||00|
43|MakeRecord|11|2|13||00|
44|SorterInsert|2|13|0||00|
45|Next|3|17|0||00|
46|Close|0|0|0||00|
47|Close|3|0|0||00|
48|OpenPseudo|5|13|2||00|
49|SorterSort|2|76|0||00|
50|SorterData|2|13|0||00|
51|Column|5|0|8||20|
52|Compare|7|8|1|keyinfo(1,BINARY)|00|
53|Jump|54|58|54||00|
54|Move|8|7|1||00|
55|Gosub|5|66|0||00|
56|IfPos|4|76|0||00|
57|Gosub|6|73|0||00|
58|AggStep|0|0|2|count(0)|00|
59|Column|5|0|1||00|
60|Integer|1|3|0||00|
61|SorterNext|2|50|0||00|
62|Gosub|5|66|0||00|
63|Goto|0|76|0||00|
64|Integer|1|4|0||00|
65|Return|5|0|0||00|
66|IfPos|3|68|0||00|
67|Return|5|0|0||00|
68|AggFinal|2|0|0|count(0)|00|
69|Copy|1|37|0||00|
70|Copy|2|38|0||00|
71|ResultRow|37|2|0||00|
72|Return|5|0|0||00|
73|Null|0|1|0||00|
74|Null|0|2|0||00|
75|Return|6|0|0||00|
76|Halt|0|0|0||00|
77|Transaction|0|0|0||00|
78|VerifyCookie|0|27|0||00|
79|TableLock|0|9|0|Orders|00|
80|TableLock|0|8|0|LineItem|00|
81|Goto|0|7|0||00|
*/