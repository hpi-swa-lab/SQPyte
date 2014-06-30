/* TPC_H Query 9 - Product Type Profit Measure */
select
	nation,
	o_year,
	sum(amount) as sum_profit
from
	(
		select
			n.name as nation,
			strftime('%Y', o.orderdate) as o_year,
			l.extendedprice * (1 - l.discount) - ps.supplycost * l.quantity as amount
		from
			part p,
			supplier s,
			lineitem l,
			partsupp ps,
			orders o,
			nation n
		where
			s.suppkey = l.suppkey
			and ps.suppkey = l.suppkey
			and ps.partkey = l.partkey
			and p.partkey = l.partkey
			and o.orderkey = l.orderkey
			and s.nationkey = n.nationkey
			and p.name like '%red%'
	) as profit
group by
	nation,
	o_year
order by
	nation,
	o_year desc;

/*
Not implemented: 28
Affinity
Compare
Function
Gosub
IdxGe
IdxRowid
IfNot
IfPos
IsNull
Jump
MakeRecord
Move
Multiply
OpenPseudo
RealAffinity
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
Subtract

Unique opcodes: 47
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
IfNot
IfPos
Integer
IsNull
Jump
MakeRecord
Move
Multiply
MustBeInt
Next
NotExists
Null
OpenPseudo
OpenRead
RealAffinity
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
Subtract
TableLock
Trace
Transaction
VerifyCookie


0|Trace|0|0|0||00|
1|SorterOpen|7|4|0|keyinfo(2,BINARY,-BINARY)|00|
2|SorterOpen|8|8|0|keyinfo(2,BINARY,BINARY)|00|
3|Integer|0|9|0||00|
4|Integer|0|8|0||00|
5|Null|0|12|13||00|
6|Gosub|11|118|0||00|
7|Goto|0|138|0||00|
8|OpenRead|1|2|0|2|00|
9|OpenRead|3|8|0|7|00|
10|OpenRead|9|20|0|keyinfo(1,BINARY)|00|
11|OpenRead|2|3|0|4|00|
12|OpenRead|5|9|0|5|00|
13|OpenRead|6|6|0|2|00|
14|OpenRead|4|4|0|4|00|
15|OpenRead|10|12|0|keyinfo(2,BINARY,BINARY)|00|
16|Rewind|1|63|0||00|
17|String8|0|17|0|%red%|00|
18|Column|1|1|18||00|
19|Function|1|17|16|like(2)|02|
20|IfNot|16|62|1||00|
21|Rowid|1|19|0||00|
22|IsNull|19|62|0||00|
23|SeekGe|9|62|19|1|00|
24|IdxGE|9|62|19|1|01|
25|IdxRowid|9|16|0||00|
26|Seek|3|16|0||00|
27|Column|3|2|20||00|
28|MustBeInt|20|61|0||00|
29|NotExists|2|61|20||00|
30|Column|3|0|21||00|
31|MustBeInt|21|61|0||00|
32|NotExists|5|61|21||00|
33|Column|2|3|22||00|
34|MustBeInt|22|61|0||00|
35|NotExists|6|61|22||00|
36|Column|9|0|23||00|
37|IsNull|23|61|0||00|
38|SCopy|20|24|0||00|
39|IsNull|24|61|0||00|
40|Affinity|23|2|0|dd|00|
41|SeekGe|10|61|23|2|00|
42|IdxGE|10|61|23|2|01|
43|IdxRowid|10|25|0||00|
44|Seek|4|25|0||00|
45|Column|6|1|26||00|
46|String8|0|17|0|%Y|00|
47|Column|5|4|18||00|
48|Function|1|17|27|strftime(-1)|02|
49|Sequence|8|28|0||00|
50|Column|5|4|29||00|
51|Column|3|5|30||00|
52|RealAffinity|30|0|0||00|
53|Column|3|6|31||00|
54|RealAffinity|31|0|0||00|
55|Column|4|3|32||00|
56|RealAffinity|32|0|0||00|
57|Column|3|4|33||00|
58|MakeRecord|26|8|25||00|
59|SorterInsert|8|25|0||00|
60|Next|10|42|0||00|
61|Next|9|24|0||00|
62|Next|1|17|0||01|
63|Close|1|0|0||00|
64|Close|3|0|0||00|
65|Close|9|0|0||00|
66|Close|2|0|0||00|
67|Close|5|0|0||00|
68|Close|6|0|0||00|
69|Close|4|0|0||00|
70|Close|10|0|0||00|
71|OpenPseudo|11|25|8||00|
72|SorterSort|8|126|0||00|
73|SorterData|8|25|0||00|
74|Column|11|0|14||20|
75|Column|11|1|15||00|
76|Compare|12|14|2|keyinfo(2,BINARY,BINARY)|00|
77|Jump|78|82|78||00|
78|Move|14|12|2||00|
79|Gosub|10|100|0||00|
80|IfPos|9|126|0||00|
81|Gosub|11|118|0||00|
82|Column|11|4|21||00|
83|Integer|1|34|0||00|
84|Column|11|5|35||00|
85|Subtract|35|34|16||00|
86|Multiply|16|21|22||00|
87|Column|11|6|21||00|
88|Column|11|7|35||00|
89|Multiply|35|21|16||00|
90|Subtract|16|22|26||00|
91|AggStep|0|26|3|sum(1)|01|
92|Column|11|0|1||00|
93|Column|11|3|2||00|
94|Integer|1|8|0||00|
95|SorterNext|8|73|0||00|
96|Gosub|10|100|0||00|
97|Goto|0|126|0||00|
98|Integer|1|9|0||00|
99|Return|10|0|0||00|
100|IfPos|8|102|0||00|
101|Return|10|0|0||00|
102|AggFinal|3|1|0|sum(1)|00|
103|Copy|1|36|0||00|
104|String8|0|27|0|%Y|00|
105|Copy|2|28|0||00|
106|Function|1|27|37|strftime(-1)|02|
107|Copy|3|38|0||00|
108|MakeRecord|36|3|16||00|
109|SCopy|1|29|0||00|
110|String8|0|39|0|%Y|00|
111|Copy|2|40|0||00|
112|Function|1|39|30|strftime(-1)|02|
113|Sequence|7|31|0||00|
114|Move|16|32|1||00|
115|MakeRecord|29|4|22||00|
116|SorterInsert|7|22|0||00|
117|Return|10|0|0||00|
118|Null|0|1|0||00|
119|Null|0|2|0||00|
120|Null|0|4|0||00|
121|Null|0|5|0||00|
122|Null|0|6|0||00|
123|Null|0|7|0||00|
124|Null|0|3|0||00|
125|Return|11|0|0||00|
126|OpenPseudo|12|16|3||00|
127|OpenPseudo|13|41|4||00|
128|SorterSort|7|136|0||00|
129|SorterData|7|41|0||00|
130|Column|13|3|16||20|
131|Column|12|0|36||20|
132|Column|12|1|37||00|
133|Column|12|2|38||00|
134|ResultRow|36|3|0||00|
135|SorterNext|7|129|0||00|
136|Close|12|0|0||00|
137|Halt|0|0|0||00|
138|Transaction|0|0|0||00|
139|VerifyCookie|0|27|0||00|
140|TableLock|0|2|0|Part|00|
141|TableLock|0|8|0|LineItem|00|
142|TableLock|0|3|0|Supplier|00|
143|TableLock|0|9|0|Orders|00|
144|TableLock|0|6|0|Nation|00|
145|TableLock|0|4|0|PartSupp|00|
146|Goto|0|8|0||00|
*/