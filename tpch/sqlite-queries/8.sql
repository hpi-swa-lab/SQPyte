/* TPC_H Query 8 - National Market Share */
select
	o_year,
	sum(case
		when nation = 'UNITED STATES' then volume
		else 0
	end) / sum(volume) as mkt_share
from
	(
		select
			strftime('%Y', o.orderdate) as o_year,
			l.extendedprice * (1 - l.discount) as volume,
			n2.name as nation
		from
			part p,
			supplier s,
			lineitem l,
			orders o,
			customer c,
			nation n1,
			nation n2,
			region r
		where
			p.partkey = l.partkey
			and s.suppkey = l.suppkey
			and l.orderkey = o.orderkey
			and o.custkey = c.custkey
			and c.nationkey = n1.nationkey
			and n1.regionkey = r.regionkey
			and r.name = 'EUROPE'
			and s.nationkey = n2.nationkey
			and o.orderdate between date('1995-01-01') and date('1996-12-31')
			and p.type = 'PROMO BRUSHED BRASS'
	) as all_nations
group by
	o_year
order by
	o_year;

/*
Not implemented: 22
Compare
Divide
Function
Gosub
IsNull
Jump
MakeRecord
Move
Multiply
OpenPseudo
RealAffinity
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
Subtract

Unique opcodes: 44
AggFinal
AggStep
Close
Column
Compare
Copy
Divide
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
1|SorterOpen|9|6|0|keyinfo(1,BINARY)|00|
2|Integer|0|8|0||00|
3|Integer|0|7|0||00|
4|Null|0|11|11||00|
5|Gosub|10|126|0||00|
6|String8|0|13|0|EUROPE|00|
7|String8|0|14|0|PROMO BRUSHED BRASS|00|
8|Goto|0|134|0||00|
9|OpenRead|4|9|0|5|00|
10|OpenRead|10|24|0|keyinfo(1,BINARY)|00|
11|OpenRead|5|5|0|4|00|
12|OpenRead|6|6|0|3|00|
13|OpenRead|8|7|0|2|00|
14|OpenRead|3|8|0|7|00|
15|OpenRead|11|16|0|keyinfo(2,BINARY,BINARY)|00|
16|OpenRead|1|2|0|5|00|
17|OpenRead|2|3|0|4|00|
18|OpenRead|7|6|0|2|00|
19|String8|0|16|0|1995-01-01|00|
20|Function|1|16|15|date(-1)|01|
21|IsNull|15|72|0||00|
22|SeekGe|10|72|15|1|00|
23|String8|0|16|0|1996-12-31|00|
24|Function|1|16|15|date(-1)|01|
25|IsNull|15|72|0||00|
26|IdxGE|10|72|15|1|01|
27|Column|10|0|17||00|
28|IsNull|17|71|0||00|
29|IdxRowid|10|17|0||00|
30|Seek|4|17|0||00|
31|Column|4|1|18||00|
32|MustBeInt|18|71|0||00|
33|NotExists|5|71|18||00|
34|Column|5|3|19||00|
35|MustBeInt|19|71|0||00|
36|NotExists|6|71|19||00|
37|Column|6|2|20||00|
38|MustBeInt|20|71|0||00|
39|NotExists|8|71|20||00|
40|Column|8|1|21||00|
41|Ne|13|71|21|collseq(BINARY)|6a|
42|IsNull|17|71|0||00|
43|SeekGe|11|71|17|1|00|
44|IdxGE|11|71|17|1|01|
45|IdxRowid|11|23|0||00|
46|Seek|3|23|0||00|
47|Column|3|1|22||00|
48|MustBeInt|22|70|0||00|
49|NotExists|1|70|22||00|
50|Column|1|4|24||00|
51|Ne|14|70|24|collseq(BINARY)|6a|
52|Column|3|2|25||00|
53|MustBeInt|25|70|0||00|
54|NotExists|2|70|25||00|
55|Column|2|3|26||00|
56|MustBeInt|26|70|0||00|
57|NotExists|7|70|26||00|
58|String8|0|33|0|%Y|00|
59|Column|10|0|34||00|
60|Function|1|33|27|strftime(-1)|02|
61|Sequence|9|28|0||00|
62|Column|10|0|29||00|
63|Column|7|1|30||00|
64|Column|3|5|31||00|
65|RealAffinity|31|0|0||00|
66|Column|3|6|32||00|
67|RealAffinity|32|0|0||00|
68|MakeRecord|27|6|23||00|
69|SorterInsert|9|23|0||00|
70|Next|11|44|0||00|
71|Next|10|26|0||00|
72|Close|4|0|0||00|
73|Close|10|0|0||00|
74|Close|5|0|0||00|
75|Close|6|0|0||00|
76|Close|8|0|0||00|
77|Close|3|0|0||00|
78|Close|11|0|0||00|
79|Close|1|0|0||00|
80|Close|2|0|0||00|
81|Close|7|0|0||00|
82|OpenPseudo|12|23|6||00|
83|SorterSort|9|133|0||00|
84|SorterData|9|23|0||00|
85|Column|12|0|12||20|
86|Compare|11|12|1|keyinfo(1,BINARY)|00|
87|Jump|88|92|88||00|
88|Move|12|11|1||00|
89|Gosub|9|116|0||00|
90|IfPos|8|133|0||00|
91|Gosub|10|126|0||00|
92|Column|12|3|21||00|
93|String8|0|26|0|UNITED STATES|00|
94|Ne|26|101|21|collseq(BINARY)|6a|
95|Column|12|4|26||00|
96|Integer|1|25|0||00|
97|Column|12|5|24||00|
98|Subtract|24|25|21||00|
99|Multiply|21|26|27||00|
100|Goto|0|102|0||00|
101|Integer|0|27|0||00|
102|AggStep|0|27|2|sum(1)|01|
103|Column|12|4|21||00|
104|Integer|1|24|0||00|
105|Column|12|5|25||00|
106|Subtract|25|24|26||00|
107|Multiply|26|21|28||00|
108|AggStep|0|28|3|sum(1)|01|
109|Column|12|2|1||00|
110|Integer|1|7|0||00|
111|SorterNext|9|84|0||00|
112|Gosub|9|116|0||00|
113|Goto|0|133|0||00|
114|Integer|1|8|0||00|
115|Return|9|0|0||00|
116|IfPos|7|118|0||00|
117|Return|9|0|0||00|
118|AggFinal|2|1|0|sum(1)|00|
119|AggFinal|3|1|0|sum(1)|00|
120|String8|0|29|0|%Y|00|
121|Copy|1|30|0||00|
122|Function|1|29|35|strftime(-1)|02|
123|Divide|3|2|36||00|
124|ResultRow|35|2|0||00|
125|Return|9|0|0||00|
126|Null|0|1|0||00|
127|Null|0|4|0||00|
128|Null|0|5|0||00|
129|Null|0|6|0||00|
130|Null|0|2|0||00|
131|Null|0|3|0||00|
132|Return|10|0|0||00|
133|Halt|0|0|0||00|
134|Transaction|0|0|0||00|
135|VerifyCookie|0|27|0||00|
136|TableLock|0|9|0|Orders|00|
137|TableLock|0|5|0|Customer|00|
138|TableLock|0|6|0|Nation|00|
139|TableLock|0|7|0|Region|00|
140|TableLock|0|8|0|LineItem|00|
141|TableLock|0|2|0|Part|00|
142|TableLock|0|3|0|Supplier|00|
143|Goto|0|9|0||00|
*/