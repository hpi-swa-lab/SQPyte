/* TPC_H Query 11 - Important Stock Identification */
select
	ps.partkey,
	sum(ps.supplycost * ps.availqty) as value
from
	partsupp ps,
	supplier s,
	nation n
where
	ps.suppkey = s.suppkey
	and s.nationkey = n.nationkey
	and n.name = 'FRANCE'
group by
	ps.partkey having
		sum(ps.supplycost * ps.availqty) > (
			select
				sum(ps.supplycost * ps.availqty) * 0.01
			from
				partsupp ps,
				supplier s,
				nation n
			where
				ps.suppkey = s.suppkey
				and s.nationkey = n.nationkey
				and n.name = 'FRANCE'
		)
order by
	value desc;

/*
Not implemented: 28
Compare
Gosub
IdxGe
IdxRowid
IfPos
IfZero
IsNull
Jump
MakeRecord
Move
Multiply
Noop
Once
OpenPseudo
Real
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

Unique opcodes: 49
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
IdxRowid
IfPos
IfZero
Integer
IsNull
Jump
Le
MakeRecord
Move
Multiply
MustBeInt
Ne
Next
Noop
NotExists
Null
Once
OpenPseudo
OpenRead
Real
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
TableLock
Trace
Transaction
VerifyCookie


0|Trace|0|0|0||00|
1|SorterOpen|6|3|0|keyinfo(1,-BINARY)|00|
2|Noop|0|0|0||00|
3|Integer|0|6|0||00|
4|Integer|0|5|0||00|
5|Null|0|9|9||00|
6|Gosub|8|101|0||00|
7|String8|0|11|0|FRANCE|00|
8|Goto|0|117|0||00|
9|OpenRead|0|4|0|4|00|
10|OpenRead|8|12|0|keyinfo(2,BINARY,BINARY)|00|
11|OpenRead|1|3|0|4|00|
12|OpenRead|2|6|0|2|00|
13|Rewind|8|39|12|0|00|
14|IdxRowid|8|12|0||00|
15|Seek|0|12|0||00|
16|Column|8|1|13||00|
17|MustBeInt|13|38|0||00|
18|NotExists|1|38|13||00|
19|Column|1|3|14||00|
20|MustBeInt|14|38|0||00|
21|NotExists|2|38|14||00|
22|Column|2|1|15||00|
23|Ne|11|38|15|collseq(BINARY)|6a|
24|Column|8|0|10||00|
25|Compare|9|10|1|keyinfo(1,BINARY)|00|
26|Jump|27|31|27||00|
27|Move|10|9|1||00|
28|Gosub|7|47|0||00|
29|IfPos|6|106|0||00|
30|Gosub|8|101|0||00|
31|Column|0|3|15||00|
32|RealAffinity|15|0|0||00|
33|Column|0|2|14||00|
34|Multiply|14|15|17||00|
35|AggStep|0|17|2|sum(1)|01|
36|Column|8|0|1||00|
37|Integer|1|5|0||00|
38|Next|8|14|0||00|
39|Close|0|0|0||00|
40|Close|8|0|0||00|
41|Close|1|0|0||00|
42|Close|2|0|0||00|
43|Gosub|7|47|0||00|
44|Goto|0|106|0||00|
45|Integer|1|6|0||00|
46|Return|7|0|0||00|
47|IfPos|5|49|0||00|
48|Return|7|0|0||00|
49|AggFinal|2|1|0|sum(1)|00|
50|Once|0|91|0||00|
51|Null|0|18|0||00|
52|Integer|1|19|0||00|
53|Null|0|21|0||00|
54|Null|0|22|0||00|
55|Null|0|20|0||00|
56|OpenRead|5|6|0|2|00|
57|OpenRead|9|18|0|keyinfo(1,BINARY)|00|
58|OpenRead|3|4|0|4|00|
59|OpenRead|10|23|0|keyinfo(1,BINARY)|00|
60|Rewind|5|82|0||00|
61|Column|5|1|15||00|
62|String8|0|13|0|FRANCE|00|
63|Ne|13|81|15|collseq(BINARY)|6a|
64|Rowid|5|23|0||00|
65|IsNull|23|81|0||00|
66|SeekGe|9|81|23|1|00|
67|IdxGE|9|81|23|1|01|
68|IdxRowid|9|24|0||00|
69|IsNull|24|80|0||00|
70|SeekGe|10|80|24|1|00|
71|IdxGE|10|80|24|1|01|
72|IdxRowid|10|13|0||00|
73|Seek|3|13|0||00|
74|Column|3|3|13||00|
75|RealAffinity|13|0|0||00|
76|Column|3|2|15||00|
77|Multiply|15|13|17||00|
78|AggStep|0|17|20|sum(1)|01|
79|Next|10|71|0||00|
80|Next|9|67|0||00|
81|Next|5|61|0||01|
82|Close|5|0|0||00|
83|Close|9|0|0||00|
84|Close|3|0|0||00|
85|Close|10|0|0||00|
86|AggFinal|20|1|0|sum(1)|00|
87|Real|0|15|0|0.01|00|
88|Multiply|15|20|25||00|
89|Move|25|18|1||00|
90|IfZero|19|91|-1||00|
91|Le|18|48|2||6a|
92|Copy|1|26|0||00|
93|Copy|2|27|0||00|
94|MakeRecord|26|2|14||00|
95|SCopy|2|28|0||00|
96|Sequence|6|29|0||00|
97|Move|14|30|1||00|
98|MakeRecord|28|3|15||00|
99|SorterInsert|6|15|0||00|
100|Return|7|0|0||00|
101|Null|0|1|0||00|
102|Null|0|3|0||00|
103|Null|0|4|0||00|
104|Null|0|2|0||00|
105|Return|8|0|0||00|
106|OpenPseudo|11|14|2||00|
107|OpenPseudo|12|31|3||00|
108|SorterSort|6|115|0||00|
109|SorterData|6|31|0||00|
110|Column|12|2|14||20|
111|Column|11|0|26||20|
112|Column|11|1|27||00|
113|ResultRow|26|2|0||00|
114|SorterNext|6|109|0||00|
115|Close|11|0|0||00|
116|Halt|0|0|0||00|
117|Transaction|0|0|0||00|
118|VerifyCookie|0|27|0||00|
119|TableLock|0|4|0|PartSupp|00|
120|TableLock|0|3|0|Supplier|00|
121|TableLock|0|6|0|Nation|00|
122|Goto|0|9|0||00|
*/