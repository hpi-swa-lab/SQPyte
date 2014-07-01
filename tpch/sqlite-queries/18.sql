/* TPC_H Query 18 - Large Volume Customer */
select
	c.name,
	c.custkey,
	o.orderkey,
	o.orderdate,
	o.totalprice,
	sum(l.quantity)
from
	customer c,
	orders o,
	lineitem l
where
	o.orderkey in (
		select
			orderkey
		from
			lineitem
		group by
			orderkey having
				sum(quantity) > 300
	)
	and c.custkey = o.custkey
	and o.orderkey = l.orderkey
group by
	c.name,
	c.custkey,
	o.orderkey,
	o.orderdate,
	o.totalprice
order by
	o.totalprice desc,
	o.orderdate;

/*
Not implemented: 26
Compare
Gosub
IdxGe
IdxInsert
IdxRowid
IfPos
IsNull
Jump
MakeRecord
Move
Noop
Once
OnceEphemeral
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

Unique opcodes: 46
AggFinal
AggStep
Close
Column
Compare
Copy
Gosub
Goto
Halt
IdxGE
IdxInsert
IdxRowid
IfPos
Integer
IsNull
Jump
Le
MakeRecord
Move
MustBeInt
Next
Noop
NotExists
Null
Once
OpenEphemeral
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
TableLock
Trace
Transaction
VerifyCookie


0|Trace|0|0|0||00|
1|SorterOpen|4|4|0|keyinfo(2,-BINARY,BINARY)|00|
2|SorterOpen|5|7|0|keyinfo(5,BINARY,BINARY)|00|
3|Integer|0|9|0||00|
4|Integer|0|8|0||00|
5|Null|0|12|16||00|
6|Gosub|11|128|0||00|
7|Goto|0|151|0||00|
8|OpenRead|1|9|0|5|00|
9|OpenRead|0|5|0|2|00|
10|OpenRead|2|8|0|5|00|
11|OpenRead|6|16|0|keyinfo(2,BINARY,BINARY)|00|
12|Once|0|55|0||00|
13|OpenEphemeral|8|1|0|keyinfo(1,BINARY)|08|
14|Noop|0|0|0||00|
15|Integer|0|27|0||00|
16|Integer|0|26|0||00|
17|Null|0|30|30||00|
18|Gosub|29|51|0||00|
19|OpenRead|3|8|0|5|00|
20|OpenRead|10|16|0|keyinfo(2,BINARY,BINARY)|00|
21|Rewind|10|36|32|0|00|
22|IdxRowid|10|32|0||00|
23|Seek|3|32|0||00|
24|Column|10|0|31||00|
25|Compare|30|31|1|keyinfo(1,BINARY)|00|
26|Jump|27|31|27||00|
27|Move|31|30|1||00|
28|Gosub|28|42|0||00|
29|IfPos|27|55|0||00|
30|Gosub|29|51|0||00|
31|Column|3|4|33||00|
32|AggStep|0|33|24|sum(1)|01|
33|Column|10|0|23||00|
34|Integer|1|26|0||00|
35|Next|10|22|0||00|
36|Close|3|0|0||00|
37|Close|10|0|0||00|
38|Gosub|28|42|0||00|
39|Goto|0|55|0||00|
40|Integer|1|27|0||00|
41|Return|28|0|0||00|
42|IfPos|26|44|0||00|
43|Return|28|0|0||00|
44|AggFinal|24|1|0|sum(1)|00|
45|Integer|300|32|0||00|
46|Le|32|43|24||6a|
47|SCopy|23|34|0||00|
48|MakeRecord|34|1|32|c|00|
49|IdxInsert|8|32|0||00|
50|Return|28|0|0||00|
51|Null|0|23|0||00|
52|Null|0|25|0||00|
53|Null|0|24|0||00|
54|Return|29|0|0||00|
55|Rewind|8|80|0||00|
56|Column|8|0|22||00|
57|IsNull|22|79|0||00|
58|MustBeInt|22|79|0||00|
59|NotExists|1|79|22||00|
60|Column|1|1|32||00|
61|MustBeInt|32|79|0||00|
62|NotExists|0|79|32||00|
63|IsNull|22|79|0||00|
64|SeekGe|6|79|22|1|00|
65|IdxGE|6|79|22|1|01|
66|IdxRowid|6|35|0||00|
67|Seek|2|35|0||00|
68|Column|0|1|36||00|
69|Rowid|0|37|0||00|
70|Rowid|1|38|0||00|
71|Column|1|4|39||00|
72|Column|1|3|40||00|
73|RealAffinity|40|0|0||00|
74|Sequence|5|41|0||00|
75|Column|2|4|42||00|
76|MakeRecord|36|7|35||00|
77|SorterInsert|5|35|0||00|
78|Next|6|65|0||00|
79|Next|8|56|0||00|
80|Close|1|0|0||00|
81|Close|0|0|0||00|
82|Close|2|0|0||00|
83|Close|6|0|0||00|
84|OpenPseudo|11|35|7||00|
85|SorterSort|5|136|0||00|
86|SorterData|5|35|0||00|
87|Column|11|0|17||20|
88|Column|11|1|18||00|
89|Column|11|2|19||00|
90|Column|11|3|20||00|
91|Column|11|4|21||00|
92|Compare|12|17|5|keyinfo(5,BINARY,BINARY)|00|
93|Jump|94|98|94||00|
94|Move|17|12|5||00|
95|Gosub|10|111|0||00|
96|IfPos|9|136|0||00|
97|Gosub|11|128|0||00|
98|Column|11|6|36||00|
99|AggStep|0|36|6|sum(1)|01|
100|Column|11|0|1||00|
101|Column|11|1|2||00|
102|Column|11|2|3||00|
103|Column|11|3|4||00|
104|Column|11|4|5||00|
105|Integer|1|8|0||00|
106|SorterNext|5|86|0||00|
107|Gosub|10|111|0||00|
108|Goto|0|136|0||00|
109|Integer|1|9|0||00|
110|Return|10|0|0||00|
111|IfPos|8|113|0||00|
112|Return|10|0|0||00|
113|AggFinal|6|1|0|sum(1)|00|
114|Copy|1|43|0||00|
115|Copy|2|44|0||00|
116|Copy|3|45|0||00|
117|Copy|4|46|0||00|
118|Copy|5|47|0||00|
119|Copy|6|48|0||00|
120|MakeRecord|43|6|32||00|
121|SCopy|5|37|0||00|
122|SCopy|4|38|0||00|
123|Sequence|4|39|0||00|
124|Move|32|40|1||00|
125|MakeRecord|37|4|49||00|
126|SorterInsert|4|49|0||00|
127|Return|10|0|0||00|
128|Null|0|1|0||00|
129|Null|0|2|0||00|
130|Null|0|3|0||00|
131|Null|0|4|0||00|
132|Null|0|5|0||00|
133|Null|0|7|0||00|
134|Null|0|6|0||00|
135|Return|11|0|0||00|
136|OpenPseudo|12|32|6||00|
137|OpenPseudo|13|50|4||00|
138|SorterSort|4|149|0||00|
139|SorterData|4|50|0||00|
140|Column|13|3|32||20|
141|Column|12|0|43||20|
142|Column|12|1|44||00|
143|Column|12|2|45||00|
144|Column|12|3|46||00|
145|Column|12|4|47||00|
146|Column|12|5|48||00|
147|ResultRow|43|6|0||00|
148|SorterNext|4|139|0||00|
149|Close|12|0|0||00|
150|Halt|0|0|0||00|
151|Transaction|0|0|0||00|
152|VerifyCookie|0|27|0||00|
153|TableLock|0|9|0|Orders|00|
154|TableLock|0|5|0|Customer|00|
155|TableLock|0|8|0|LineItem|00|
156|Goto|0|8|0||00|
*/