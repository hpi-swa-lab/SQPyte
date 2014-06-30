/* TPC_H Query 5 - Local Supplier Volume */
select
	n.name,
	sum(l.extendedprice * (1 - l.discount)) as revenue
from
	customer c,
	orders o,
	lineitem l,
	supplier s,
	nation n,
	region r
where
	c.custkey = o.custkey
	and l.orderkey = o.orderkey
	and l.suppkey = s.suppkey
	and c.nationkey = s.nationkey
	and s.nationkey = n.nationkey
	and n.regionkey = r.regionkey
	and r.name = 'ASIA'
	and o.orderdate >= date('1996-01-01')
	and o.orderdate < date('1996-01-01', '+1 year')
group by
	n.name
order by
	revenue desc;

/*
Not implemented: 26
Affinity
Compare
Function
Gosub
IdxGE
IdxRowid
IfPos
IsNull
Jump
MakeRecord
Move
Multiply
OpenPseudo
RealAffinity
Return
SCopy
Seek
SeekGe
Sequence
SorterData
SorterInsert
SorterNext
SorterOpen
SorterSort
String8
Subtract

Unique opcodes: 45
Affinity
AggFinal
AggStep
Close
Column
Compare
Copy
Function
Gosub
Goto
Halt
IdxGE
IdxRowid
IfPos
Integer
IsNull
Jump
MakeRecord
Move
Multiply
MustBeInt
Ne
Next
NotExists
Null
OpenPseudo
OpenRead
RealAffinity
ResultRow
Return
SCopy
Seek
SeekGe
Sequence
SorterData
SorterInsert
SorterNext
SorterOpen
SorterSort
String8
Subtract
TableLock
Trace
Transaction
VerifyCookie


0|Trace|0|0|0||00|
1|SorterOpen|6|3|0|keyinfo(1,-BINARY)|00|
2|SorterOpen|7|4|0|keyinfo(1,BINARY)|00|
3|Integer|0|6|0||00|
4|Integer|0|5|0||00|
5|Null|0|9|9||00|
6|Gosub|8|109|0||00|
7|String8|0|11|0|ASIA|00|
8|Goto|0|125|0||00|
9|OpenRead|1|9|0|5|00|
10|OpenRead|8|24|0|keyinfo(1,BINARY)|00|
11|OpenRead|0|5|0|4|00|
12|OpenRead|9|18|0|keyinfo(1,BINARY)|00|
13|OpenRead|4|6|0|3|00|
14|OpenRead|5|7|0|2|00|
15|OpenRead|2|8|0|7|00|
16|OpenRead|10|21|0|keyinfo(1,BINARY)|00|
17|String8|0|13|0|1996-01-01|00|
18|Function|1|13|12|date(-1)|01|
19|IsNull|12|66|0||00|
20|SeekGe|8|66|12|1|00|
21|String8|0|14|0|1996-01-01|00|
22|String8|0|15|0|+1 year|00|
23|Function|3|14|12|date(-1)|02|
24|IsNull|12|66|0||00|
25|IdxGE|8|66|12|1|00|
26|Column|8|0|16||00|
27|IsNull|16|65|0||00|
28|IdxRowid|8|16|0||00|
29|Seek|1|16|0||00|
30|Column|1|1|17||00|
31|MustBeInt|17|65|0||00|
32|NotExists|0|65|17||00|
33|Column|0|3|18||00|
34|IsNull|18|65|0||00|
35|Affinity|18|1|0|d|00|
36|SeekGe|9|65|18|1|00|
37|IdxGE|9|65|18|1|01|
38|Column|9|0|19||00|
39|MustBeInt|19|64|0||00|
40|NotExists|4|64|19||00|
41|Column|4|2|20||00|
42|MustBeInt|20|64|0||00|
43|NotExists|5|64|20||00|
44|Column|5|1|21||00|
45|Ne|11|64|21|collseq(BINARY)|6a|
46|IdxRowid|9|23|0||00|
47|IsNull|23|64|0||00|
48|SeekGe|10|64|23|1|00|
49|IdxGE|10|64|23|1|01|
50|IdxRowid|10|22|0||00|
51|Seek|2|22|0||00|
52|Column|2|0|24||00|
53|IdxRowid|8|25|0||00|
54|Ne|25|63|24|collseq(BINARY)|6b|
55|Column|4|1|26||00|
56|Sequence|7|27|0||00|
57|Column|2|5|28||00|
58|RealAffinity|28|0|0||00|
59|Column|2|6|29||00|
60|RealAffinity|29|0|0||00|
61|MakeRecord|26|4|22||00|
62|SorterInsert|7|22|0||00|
63|Next|10|49|0||00|
64|Next|9|37|0||00|
65|Next|8|25|0||00|
66|Close|1|0|0||00|
67|Close|8|0|0||00|
68|Close|0|0|0||00|
69|Close|9|0|0||00|
70|Close|4|0|0||00|
71|Close|5|0|0||00|
72|Close|2|0|0||00|
73|Close|10|0|0||00|
74|OpenPseudo|11|22|4||00|
75|SorterSort|7|114|0||00|
76|SorterData|7|22|0||00|
77|Column|11|0|10||20|
78|Compare|9|10|1|keyinfo(1,BINARY)|00|
79|Jump|80|84|80||00|
80|Move|10|9|1||00|
81|Gosub|7|97|0||00|
82|IfPos|6|114|0||00|
83|Gosub|8|109|0||00|
84|Column|11|2|21||00|
85|Integer|1|19|0||00|
86|Column|11|3|25||00|
87|Subtract|25|19|20||00|
88|Multiply|20|21|26||00|
89|AggStep|0|26|2|sum(1)|01|
90|Column|11|0|1||00|
91|Integer|1|5|0||00|
92|SorterNext|7|76|0||00|
93|Gosub|7|97|0||00|
94|Goto|0|114|0||00|
95|Integer|1|6|0||00|
96|Return|7|0|0||00|
97|IfPos|5|99|0||00|
98|Return|7|0|0||00|
99|AggFinal|2|1|0|sum(1)|00|
100|Copy|1|30|0||00|
101|Copy|2|31|0||00|
102|MakeRecord|30|2|20||00|
103|SCopy|2|27|0||00|
104|Sequence|6|28|0||00|
105|Move|20|29|1||00|
106|MakeRecord|27|3|21||00|
107|SorterInsert|6|21|0||00|
108|Return|7|0|0||00|
109|Null|0|1|0||00|
110|Null|0|3|0||00|
111|Null|0|4|0||00|
112|Null|0|2|0||00|
113|Return|8|0|0||00|
114|OpenPseudo|12|20|2||00|
115|OpenPseudo|13|32|3||00|
116|SorterSort|6|123|0||00|
117|SorterData|6|32|0||00|
118|Column|13|2|20||20|
119|Column|12|0|30||20|
120|Column|12|1|31||00|
121|ResultRow|30|2|0||00|
122|SorterNext|6|117|0||00|
123|Close|12|0|0||00|
124|Halt|0|0|0||00|
125|Transaction|0|0|0||00|
126|VerifyCookie|0|27|0||00|
127|TableLock|0|9|0|Orders|00|
128|TableLock|0|5|0|Customer|00|
129|TableLock|0|3|0|Supplier|00|
130|TableLock|0|6|0|Nation|00|
131|TableLock|0|7|0|Region|00|
132|TableLock|0|8|0|LineItem|00|
133|Goto|0|9|0||00|
*/