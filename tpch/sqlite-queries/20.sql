/* TPC_H Query 20 - Potential Part Promotion */
select
	s.name,
	s.address
from
	supplier s,
	nation n
where
	s.suppkey in (
		select
			ps.suppkey
		from
			partsupp ps
		where
			ps.partkey in (
				select
					partkey
				from
					part
				where
					name like 'b%'
			)
			and ps.availqty > (
				select
					0.5 * sum(l.quantity)
				from
					lineitem l
				where
					l.partkey = ps.partkey
					and l.suppkey = ps.suppkey
					and l.shipdate >= date('1995-01-01')
					and l.shipdate < date('1995-01-01', '+1 year')
			)
	)
	and s.nationkey = n.nationkey
	and n.name = 'BRAZIL'
order by
	s.name;

/*
Not implemented: 25
Affinity
Function
IdxGe
IdxInsert
IdxRowid
IfNot
IfZero
IsNull
MakeRecord
Move
Multiply
Once
OnceEphemeral
OpenPseudo
Real
Rowid
Seek
SeekGe
Sequence
SorterData
SorterInsert
SorterNext
SorterOpen
SorterSort
String8

Unique opcodes: 47
Affinity
AggFinal
AggStep
Close
Column
Function
Ge
Goto
Halt
IdxGE
IdxInsert
IdxRowid
IfNot
IfZero
Integer
IsNull
Le
Lt
MakeRecord
Move
Multiply
MustBeInt
Ne
Next
NotExists
Null
Once
OpenEphemeral
OpenPseudo
OpenRead
Real
ResultRow
Rewind
Rowid
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
1|SorterOpen|5|3|0|keyinfo(1,BINARY)|00|
2|String8|0|1|0|BRAZIL|00|
3|Goto|0|106|0||00|
4|OpenRead|0|3|0|4|00|
5|OpenRead|1|6|0|2|00|
6|Once|0|74|0||00|
7|OpenEphemeral|7|1|0|keyinfo(1,BINARY)|08|
8|OpenRead|2|4|0|3|00|
9|OpenRead|8|12|0|keyinfo(2,BINARY,BINARY)|00|
10|Once|1|23|0||00|
11|OpenEphemeral|10|1|0|keyinfo(1,BINARY)|08|
12|OpenRead|3|2|0|2|00|
13|Rewind|3|22|0||00|
14|String8|0|5|0|b%|00|
15|Column|3|1|6||00|
16|Function|1|5|4|like(2)|02|
17|IfNot|4|21|1||00|
18|Rowid|3|7|0||00|
19|MakeRecord|7|1|4|c|00|
20|IdxInsert|10|4|0||00|
21|Next|3|14|0||01|
22|Close|3|0|0||00|
23|Rewind|10|72|0||00|
24|Column|10|0|3||00|
25|IsNull|3|71|0||00|
26|Affinity|3|1|0|d|00|
27|SeekGe|8|71|3|1|00|
28|IdxGE|8|71|3|1|01|
29|IdxRowid|8|4|0||00|
30|Seek|2|4|0||00|
31|Column|2|2|8||00|
32|Null|0|10|0||00|
33|Integer|1|11|0||00|
34|Null|0|13|0||00|
35|Null|0|12|0||00|
36|OpenRead|4|8|0|11|00|
37|OpenRead|11|21|0|keyinfo(1,BINARY)|00|
38|Column|8|1|14||00|
39|IsNull|14|59|0||00|
40|Affinity|14|1|0|d|00|
41|SeekGe|11|59|14|1|00|
42|IdxGE|11|59|14|1|01|
43|IdxRowid|11|15|0||00|
44|Seek|4|15|0||00|
45|Column|4|1|16||00|
46|Column|8|0|17||00|
47|Ne|17|58|16|collseq(BINARY)|6b|
48|Column|4|10|18||00|
49|String8|0|5|0|1995-01-01|00|
50|Function|1|5|19|date(-1)|01|
51|Lt|19|58|18|collseq(BINARY)|6a|
52|String8|0|20|0|1995-01-01|00|
53|String8|0|21|0|+1 year|00|
54|Function|3|20|19|date(-1)|02|
55|Ge|19|58|18|collseq(BINARY)|6a|
56|Column|4|4|20||00|
57|AggStep|0|20|12|sum(1)|01|
58|Next|11|42|0||00|
59|Close|4|0|0||00|
60|Close|11|0|0||00|
61|AggFinal|12|1|0|sum(1)|00|
62|Real|0|17|0|0.5|00|
63|Multiply|12|17|22||00|
64|Move|22|10|1||00|
65|IfZero|11|66|-1||00|
66|Le|10|70|8|collseq(BINARY)|6c|
67|Column|8|1|23||00|
68|MakeRecord|23|1|4|c|00|
69|IdxInsert|7|4|0||00|
70|Next|8|28|0||00|
71|Next|10|24|0||00|
72|Close|2|0|0||00|
73|Close|8|0|0||00|
74|Rewind|7|93|0||00|
75|Column|7|0|2||00|
76|IsNull|2|92|0||00|
77|MustBeInt|2|92|0||00|
78|NotExists|0|92|2||00|
79|Column|0|3|4||00|
80|MustBeInt|4|92|0||00|
81|NotExists|1|92|4||00|
82|Column|1|1|8||00|
83|Ne|1|92|8|collseq(BINARY)|6a|
84|Column|0|1|24||00|
85|Column|0|2|25||00|
86|MakeRecord|24|2|8||00|
87|Column|0|1|26||00|
88|Sequence|5|27|0||00|
89|Move|8|28|1||00|
90|MakeRecord|26|3|4||00|
91|SorterInsert|5|4|0||00|
92|Next|7|75|0||00|
93|Close|0|0|0||00|
94|Close|1|0|0||00|
95|OpenPseudo|12|8|2||00|
96|OpenPseudo|13|29|3||00|
97|SorterSort|5|104|0||00|
98|SorterData|5|29|0||00|
99|Column|13|2|8||20|
100|Column|12|0|24||20|
101|Column|12|1|25||00|
102|ResultRow|24|2|0||00|
103|SorterNext|5|98|0||00|
104|Close|12|0|0||00|
105|Halt|0|0|0||00|
106|Transaction|0|0|0||00|
107|VerifyCookie|0|27|0||00|
108|TableLock|0|3|0|Supplier|00|
109|TableLock|0|6|0|Nation|00|
110|TableLock|0|4|0|PartSupp|00|
111|TableLock|0|2|0|Part|00|
112|TableLock|0|8|0|LineItem|00|
113|Goto|0|4|0||00|
*/