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


0|Init|0|126|0||00|
1|SorterOpen|7|4|0|k(2,B,-B)|00|
2|SorterOpen|8|8|0|k(2,B,B)|00|
3|Integer|0|9|0||00|
4|Integer|0|8|0||00|
5|Null|0|12|13||00|
6|Gosub|11|112|0||00|
7|OpenRead|3|8|0|7|00|
8|OpenRead|1|2|0|2|00|
9|OpenRead|2|3|0|4|00|
10|OpenRead|5|9|0|5|00|
11|OpenRead|6|6|0|2|00|
12|OpenRead|4|4|0|4|00|
13|OpenRead|9|12|0|k(3,nil,nil,nil)|00|
14|Rewind|3|61|0||00|
15|Column|3|1|16||00|
16|MustBeInt|16|60|0||00|
17|NotExists|1|60|16||00|
18|Column|3|1|17||00|
19|Ne|17|60|16|(BINARY)|6b|
20|Column|1|1|20||00|
21|Function|1|19|18|like(2)|02|
22|IfNot|18|60|1||00|
23|Column|3|2|21||00|
24|MustBeInt|21|60|0||00|
25|NotExists|2|60|21||00|
26|Column|3|2|18||00|
27|Ne|18|60|21|(BINARY)|6b|
28|Column|3|0|22||00|
29|MustBeInt|22|60|0||00|
30|NotExists|5|60|22||00|
31|Column|2|3|23||00|
32|MustBeInt|23|60|0||00|
33|NotExists|6|60|23||00|
34|SCopy|17|24|0||00|
35|IsNull|24|60|0||00|
36|SCopy|18|25|0||00|
37|IsNull|25|60|0||00|
38|Affinity|24|2|0|dd|00|
39|SeekGE|9|60|24|2|00|
40|IdxGT|9|60|24|2|00|
41|IdxRowid|9|26|0||00|
42|Seek|4|26|0||00|
43|Column|9|1|27||00|
44|Ne|18|59|27|(BINARY)|6b|
45|Column|6|1|29||00|
46|Column|5|4|38||00|
47|Function|1|37|30|strftime(-1)|02|
48|Sequence|8|31|0||00|
49|Column|5|4|32||00|
50|Column|3|5|33||00|
51|RealAffinity|33|0|0||00|
52|Column|3|6|34||00|
53|RealAffinity|34|0|0||00|
54|Column|4|3|35||00|
55|RealAffinity|35|0|0||00|
56|Column|3|4|36||00|
57|MakeRecord|29|8|27||00|
58|SorterInsert|8|27|0||00|
59|Next|9|40|1||00|
60|Next|3|15|0||01|
61|Close|3|0|0||00|
62|Close|1|0|0||00|
63|Close|2|0|0||00|
64|Close|5|0|0||00|
65|Close|6|0|0||00|
66|Close|4|0|0||00|
67|Close|9|0|0||00|
68|OpenPseudo|10|27|8||00|
69|SorterSort|8|114|0||00|
70|SorterData|8|27|0||00|
71|Column|10|0|14||20|
72|Column|10|1|15||00|
73|Compare|12|14|2|k(2,B,B)|00|
74|Jump|75|79|75||00|
75|Move|14|12|2||00|
76|Gosub|10|96|0||00|
77|IfPos|9|114|0||00|
78|Gosub|11|112|0||00|
79|Column|10|4|39||00|
80|Column|10|5|42||00|
81|Subtract|42|41|40||00|
82|Multiply|40|39|28||00|
83|Column|10|6|39||00|
84|Column|10|7|42||00|
85|Multiply|42|39|40||00|
86|Subtract|40|28|29||00|
87|AggStep|0|29|3|sum(1)|01|
88|Column|10|0|1||00|
89|Column|10|3|2||00|
90|Integer|1|8|0||00|
91|SorterNext|8|70|0||00|
92|Gosub|10|96|0||00|
93|Goto|0|114|0||00|
94|Integer|1|9|0||00|
95|Return|10|0|0||00|
96|IfPos|8|98|0||00|
97|Return|10|0|0||00|
98|AggFinal|3|1|0|sum(1)|00|
99|Copy|1|43|0||00|
100|Copy|2|47|0||00|
101|Function|1|46|44|strftime(-1)|02|
102|Copy|3|45|0||00|
103|MakeRecord|43|3|40||00|
104|SCopy|1|49|0||00|
105|Copy|2|54|0||00|
106|Function|1|53|50|strftime(-1)|02|
107|Sequence|7|51|0||00|
108|Move|40|52|1||00|
109|MakeRecord|49|4|48||00|
110|SorterInsert|7|48|0||00|
111|Return|10|0|0||00|
112|Null|0|1|7||00|
113|Return|11|0|0||00|
114|OpenPseudo|11|40|3||00|
115|OpenPseudo|12|55|4||00|
116|SorterSort|7|124|0||00|
117|SorterData|7|55|0||00|
118|Column|12|3|40||20|
119|Column|11|0|43||20|
120|Column|11|1|44||00|
121|Column|11|2|45||00|
122|ResultRow|43|3|0||00|
123|SorterNext|7|117|0||00|
124|Close|11|0|0||00|
125|Halt|0|0|0||00|
126|Transaction|0|0|23|0|01|
127|TableLock|0|8|0|LineItem|00|
128|TableLock|0|2|0|Part|00|
129|TableLock|0|3|0|Supplier|00|
130|TableLock|0|9|0|Orders|00|
131|TableLock|0|6|0|Nation|00|
132|TableLock|0|4|0|PartSupp|00|
133|String8|0|19|0|%red%|00|
134|String8|0|37|0|%Y|00|
135|Integer|1|41|0||00|
136|String8|0|46|0|%Y|00|
137|String8|0|53|0|%Y|00|
138|Goto|0|1|0||00|
*/
