/* TPC_H Query 21 - Suppliers Who Kept Orders Waiting */
select
	s.name,
	count(*) as numwait
from
	supplier s,
	lineitem l1,
	orders o,
	nation n
where
	s.suppkey = l1.suppkey
	and o.orderkey = l1.orderkey
	and o.orderstatus = 'F'
	and l1.receiptdate > l1.commitdate
	and exists (
		select
			*
		from
			lineitem l2
		where
			l2.orderkey = l1.orderkey
			and l2.suppkey <> l1.suppkey
	)
	and not exists (
		select
			*
		from
			lineitem l3
		where
			l3.orderkey = l1.orderkey
			and l3.suppkey <> l1.suppkey
			and l3.receiptdate > l3.commitdate
	)
	and s.nationkey = n.nationkey
	and n.name = 'CANADA'
group by
	s.name
order by
	numwait desc,
	s.name;

/*
Not implemented: 25
Affinity
Compare
Gosub
IdxGe
IdxRowid
If
IfNot
IfPos
IfZero
IsNull
MakeRecord
Move
OpenPseudo
Return
Rowid
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

Unique opcodes: 48
Affinity
AggFinal
AggStep
Close
Column
Compare
Copy
Eq
Gosub
Goto
Halt
IdxGE
IdxRowid
If
IfNot
IfPos
IfZero
Integer
IsNull
Jump
Le
MakeRecord
Move
MustBeInt
Ne
Next
NotExists
Null
OpenPseudo
OpenRead
ResultRow
Return
Rewind
Rowid
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
TableLock
Trace
Transaction
VerifyCookie


0|Trace|0|0|0||00|
1|SorterOpen|6|4|0|keyinfo(2,-BINARY,BINARY)|00|
2|SorterOpen|7|2|0|keyinfo(1,BINARY)|00|
3|Integer|0|4|0||00|
4|Integer|0|3|0||00|
5|Null|0|7|7||00|
6|Gosub|6|120|0||00|
7|String8|0|9|0|F|00|
8|String8|0|10|0|CANADA|00|
9|Goto|0|134|0||00|
10|OpenRead|2|9|0|3|00|
11|OpenRead|1|8|0|13|00|
12|OpenRead|8|16|0|keyinfo(2,BINARY,BINARY)|00|
13|OpenRead|0|3|0|4|00|
14|OpenRead|3|6|0|2|00|
15|Rewind|2|84|0||00|
16|Column|2|2|11||00|
17|Ne|9|83|11|collseq(BINARY)|6a|
18|Rowid|2|13|0||00|
19|IsNull|13|83|0||00|
20|SeekGe|8|83|13|1|00|
21|IdxGE|8|83|13|1|01|
22|IdxRowid|8|12|0||00|
23|Seek|1|12|0||00|
24|Column|1|12|14||00|
25|Column|1|11|15||00|
26|Le|15|82|14|collseq(BINARY)|6a|
27|Integer|0|17|0||00|
28|Integer|1|18|0||00|
29|OpenRead|4|8|0|16|00|
30|OpenRead|9|16|0|keyinfo(2,BINARY,BINARY)|00|
31|Column|8|0|19||00|
32|IsNull|19|44|0||00|
33|Affinity|19|1|0|d|00|
34|SeekGe|9|44|19|1|00|
35|IdxGE|9|44|19|1|01|
36|IdxRowid|9|15|0||00|
37|Seek|4|15|0||00|
38|Column|4|2|14||00|
39|Column|1|2|11||00|
40|Eq|11|43|14|collseq(BINARY)|6b|
41|Integer|1|17|0||00|
42|IfZero|18|44|-1||00|
43|Next|9|35|0||00|
44|Close|4|0|0||00|
45|Close|9|0|0||00|
46|IfNot|17|82|1||00|
47|Integer|0|36|0||00|
48|Integer|1|37|0||00|
49|OpenRead|5|8|0|16|00|
50|OpenRead|10|16|0|keyinfo(2,BINARY,BINARY)|00|
51|Column|8|0|38||00|
52|IsNull|38|67|0||00|
53|Affinity|38|1|0|d|00|
54|SeekGe|10|67|38|1|00|
55|IdxGE|10|67|38|1|01|
56|IdxRowid|10|11|0||00|
57|Seek|5|11|0||00|
58|Column|5|2|14||00|
59|Column|1|2|15||00|
60|Eq|15|66|14|collseq(BINARY)|6b|
61|Column|5|12|39||00|
62|Column|5|11|40||00|
63|Le|40|66|39|collseq(BINARY)|6a|
64|Integer|1|36|0||00|
65|IfZero|37|67|-1||00|
66|Next|10|55|0||00|
67|Close|5|0|0||00|
68|Close|10|0|0||00|
69|If|36|82|1||00|
70|Column|1|2|12||00|
71|MustBeInt|12|82|0||00|
72|NotExists|0|82|12||00|
73|Column|0|3|16||00|
74|MustBeInt|16|82|0||00|
75|NotExists|3|82|16||00|
76|Column|3|1|40||00|
77|Ne|10|82|40|collseq(BINARY)|6a|
78|Column|0|1|57||00|
79|Sequence|7|58|0||00|
80|MakeRecord|57|2|40||00|
81|SorterInsert|7|40|0||00|
82|Next|8|21|0||00|
83|Next|2|16|0||01|
84|Close|2|0|0||00|
85|Close|1|0|0||00|
86|Close|8|0|0||00|
87|Close|0|0|0||00|
88|Close|3|0|0||00|
89|OpenPseudo|11|40|2||00|
90|SorterSort|7|123|0||00|
91|SorterData|7|40|0||00|
92|Column|11|0|8||20|
93|Compare|7|8|1|keyinfo(1,BINARY)|00|
94|Jump|95|99|95||00|
95|Move|8|7|1||00|
96|Gosub|5|107|0||00|
97|IfPos|4|123|0||00|
98|Gosub|6|120|0||00|
99|AggStep|0|0|2|count(0)|00|
100|Column|11|0|1||00|
101|Integer|1|3|0||00|
102|SorterNext|7|91|0||00|
103|Gosub|5|107|0||00|
104|Goto|0|123|0||00|
105|Integer|1|4|0||00|
106|Return|5|0|0||00|
107|IfPos|3|109|0||00|
108|Return|5|0|0||00|
109|AggFinal|2|0|0|count(0)|00|
110|Copy|1|59|0||00|
111|Copy|2|60|0||00|
112|MakeRecord|59|2|16||00|
113|SCopy|2|61|0||00|
114|SCopy|1|62|0||00|
115|Sequence|6|63|0||00|
116|Move|16|64|1||00|
117|MakeRecord|61|4|12||00|
118|SorterInsert|6|12|0||00|
119|Return|5|0|0||00|
120|Null|0|1|0||00|
121|Null|0|2|0||00|
122|Return|6|0|0||00|
123|OpenPseudo|12|16|2||00|
124|OpenPseudo|13|65|4||00|
125|SorterSort|6|132|0||00|
126|SorterData|6|65|0||00|
127|Column|13|3|16||20|
128|Column|12|0|59||20|
129|Column|12|1|60||00|
130|ResultRow|59|2|0||00|
131|SorterNext|6|126|0||00|
132|Close|12|0|0||00|
133|Halt|0|0|0||00|
134|Transaction|0|0|0||00|
135|VerifyCookie|0|27|0||00|
136|TableLock|0|9|0|Orders|00|
137|TableLock|0|8|0|LineItem|00|
138|TableLock|0|3|0|Supplier|00|
139|TableLock|0|6|0|Nation|00|
140|Goto|0|10|0||00|
*/