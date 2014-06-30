/* TPC_H Query 12 - Shipping Modes and Order Priority */
select
	l.shipmode,
	sum(case
		when o.orderpriority = '1-URGENT'
			or o.orderpriority = '2-HIGH'
			then 1
		else 0
	end) as high_line_count,
	sum(case
		when o.orderpriority <> '1-URGENT'
			and o.orderpriority <> '2-HIGH'
			then 1
		else 0
	end) as low_line_count
from
	orders o,
	lineitem l
where
	o.orderkey = l.orderkey
	and l.shipmode in ('AIR', 'REG AIR')
	and l.commitdate < l.receiptdate
	and l.shipdate < l.commitdate
	and l.receiptdate >= date('1995-01-01')
	and l.receiptdate < date('1995-01-01', '+1 year')
group by
	l.shipmode
order by
	l.shipmode;

/*
Not implemented: 22
Affinity
Compare
Function
Gosub
IdxInsert
IfPos
IsNull
Jump
MakeRecord
Move
NotFound
Once
OnceEphemeral
OpenPseudo
Return
Sequence
SorterData
SorterInsert
SorterNext
SorterOpen
SorterSort
String8

Unique opcodes: 45
Affinity
AggFinal
AggStep
Close
Column
Compare
Copy
Eq
Function
Ge
Gosub
Goto
Halt
IdxInsert
IfPos
Integer
IsNull
Jump
Lt
MakeRecord
Move
MustBeInt
Ne
Next
NotExists
NotFound
Null
Once
OpenEphemeral
OpenPseudo
OpenRead
ResultRow
Return
Rewind
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
1|SorterOpen|2|3|0|keyinfo(1,BINARY)|00|
2|Integer|0|6|0||00|
3|Integer|0|5|0||00|
4|Null|0|9|9||00|
5|Gosub|8|94|0||00|
6|Goto|0|100|0||00|
7|OpenRead|1|8|0|15|00|
8|OpenRead|0|9|0|6|00|
9|Rewind|1|46|0||00|
10|Null|0|11|0||00|
11|Once|0|21|0||00|
12|Null|0|11|0||00|
13|OpenEphemeral|4|1|0|keyinfo(1,BINARY)|00|
14|Null|0|13|0||00|
15|String8|0|12|0|AIR|00|
16|MakeRecord|12|1|13|b|00|
17|IdxInsert|4|13|0||00|
18|String8|0|12|0|REG AIR|00|
19|MakeRecord|12|1|13|b|00|
20|IdxInsert|4|13|0||00|
21|Column|1|14|13||00|
22|IsNull|13|45|0||00|
23|Affinity|13|1|0|b|00|
24|NotFound|4|45|13|1|00|
25|Column|1|11|13||00|
26|Column|1|12|12||00|
27|Ge|12|45|13|collseq(BINARY)|6a|
28|Column|1|10|14||00|
29|Ge|13|45|14|collseq(BINARY)|6a|
30|String8|0|16|0|1995-01-01|00|
31|Function|1|16|15|date(-1)|01|
32|Lt|15|45|12|collseq(BINARY)|6a|
33|String8|0|17|0|1995-01-01|00|
34|String8|0|18|0|+1 year|00|
35|Function|3|17|15|date(-1)|02|
36|Ge|15|45|12|collseq(BINARY)|6a|
37|Column|1|0|15||00|
38|MustBeInt|15|45|0||00|
39|NotExists|0|45|15||00|
40|Column|1|14|19||00|
41|Sequence|2|20|0||00|
42|Column|0|5|21||00|
43|MakeRecord|19|3|15||00|
44|SorterInsert|2|15|0||00|
45|Next|1|10|0||01|
46|Close|1|0|0||00|
47|Close|0|0|0||00|
48|OpenPseudo|5|15|3||00|
49|SorterSort|2|99|0||00|
50|SorterData|2|15|0||00|
51|Column|5|0|10||20|
52|Compare|9|10|1|keyinfo(1,BINARY)|00|
53|Jump|54|58|54||00|
54|Move|10|9|1||00|
55|Gosub|7|85|0||00|
56|IfPos|6|99|0||00|
57|Gosub|8|94|0||00|
58|Column|5|2|14||00|
59|String8|0|22|0|1-URGENT|00|
60|Eq|22|64|14|collseq(BINARY)|62|
61|Column|5|2|22||00|
62|String8|0|14|0|2-HIGH|00|
63|Ne|14|66|22|collseq(BINARY)|6a|
64|Integer|1|19|0||00|
65|Goto|0|67|0||00|
66|Integer|0|19|0||00|
67|AggStep|0|19|2|sum(1)|01|
68|Column|5|2|14||00|
69|String8|0|22|0|1-URGENT|00|
70|Eq|22|76|14|collseq(BINARY)|6a|
71|Column|5|2|22||00|
72|String8|0|14|0|2-HIGH|00|
73|Eq|14|76|22|collseq(BINARY)|6a|
74|Integer|1|20|0||00|
75|Goto|0|77|0||00|
76|Integer|0|20|0||00|
77|AggStep|0|20|3|sum(1)|01|
78|Column|5|0|1||00|
79|Integer|1|5|0||00|
80|SorterNext|2|50|0||00|
81|Gosub|7|85|0||00|
82|Goto|0|99|0||00|
83|Integer|1|6|0||00|
84|Return|7|0|0||00|
85|IfPos|5|87|0||00|
86|Return|7|0|0||00|
87|AggFinal|2|1|0|sum(1)|00|
88|AggFinal|3|1|0|sum(1)|00|
89|Copy|1|23|0||00|
90|Copy|2|24|0||00|
91|Copy|3|25|0||00|
92|ResultRow|23|3|0||00|
93|Return|7|0|0||00|
94|Null|0|1|0||00|
95|Null|0|4|0||00|
96|Null|0|2|0||00|
97|Null|0|3|0||00|
98|Return|8|0|0||00|
99|Halt|0|0|0||00|
100|Transaction|0|0|0||00|
101|VerifyCookie|0|27|0||00|
102|TableLock|0|8|0|LineItem|00|
103|TableLock|0|9|0|Orders|00|
104|Goto|0|7|0||00|
*/