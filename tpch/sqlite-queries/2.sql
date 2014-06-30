/* TPC_H Query 2 - Minimum Cost Supplier */
select
	s.acctbal,
	s.name,
	n.name,
	p.partkey,
	p.mfgr,
	s.address,
	s.phone,
	s.comment
from
	part p,
	supplier s,
	partsupp ps,
	nation n,
	region r
where
	p.partkey = ps.partkey
	and s.suppkey = ps.suppkey
	and p.size = 35
	and p.type like 'MEDIUM%'
	and s.nationkey = n.nationkey
	and n.regionkey = r.regionkey
	and r.name = 'EUROPE'
	and ps.supplycost = (
		select
			min(ps.supplycost)
		from
			partsupp ps,
			supplier s,
			nation n,
			region r
		where
			p.partkey = ps.partkey
			and s.suppkey = ps.suppkey
			and s.nationkey = n.nationkey
			and n.regionkey = r.regionkey
			and r.name = 'EUROPE'
	)
order by
	s.acctbal desc,
	n.name,
	s.name,
	p.partkey;

/*
Not implemented: 22
CollSeq
Function
IdxGE
IdxRowid
IfNot
IfZero
IsNull
MakeRecord
Move
OpenPseudo
RealAffinity
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

Unique opcodes: 41
AggFinal
AggStep
Close
CollSeq
Column
Function
Goto
Halt
IdxGE
IdxRowid
IfNot
IfZero
Integer
IsNull
MakeRecord
Move
MustBeInt
Ne
Next
NotExists
Null
OpenPseudo
OpenRead
RealAffinity
ResultRow
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
1|SorterOpen|9|6|0|keyinfo(4,-BINARY,BINARY)|00|
2|Integer|35|1|0||00|
3|String8|0|2|0|EUROPE|00|
4|Goto|0|123|0||00|
5|OpenRead|0|2|0|6|00|
6|OpenRead|2|4|0|4|00|
7|OpenRead|10|12|0|keyinfo(2,BINARY,BINARY)|00|
8|OpenRead|1|3|0|7|00|
9|OpenRead|3|6|0|3|00|
10|OpenRead|4|7|0|2|00|
11|Rewind|0|100|0||00|
12|Column|0|5|3||00|
13|Ne|1|99|3|collseq(BINARY)|6c|
14|String8|0|5|0|MEDIUM%|00|
15|Column|0|4|6||00|
16|Function|1|5|4|like(2)|02|
17|IfNot|4|99|1||00|
18|Rowid|0|7|0||00|
19|IsNull|7|99|0||00|
20|SeekGe|10|99|7|1|00|
21|IdxGE|10|99|7|1|01|
22|IdxRowid|10|4|0||00|
23|Seek|2|4|0||00|
24|Column|2|3|8||00|
25|RealAffinity|8|0|0||00|
26|Null|0|10|0||00|
27|Integer|1|11|0||00|
28|Null|0|13|0||00|
29|Null|0|12|0||00|
30|OpenRead|5|4|0|4|00|
31|OpenRead|11|12|0|keyinfo(2,BINARY,BINARY)|00|
32|OpenRead|6|3|0|4|00|
33|OpenRead|7|6|0|3|00|
34|OpenRead|8|7|0|2|00|
35|Rowid|0|14|0||00|
36|IsNull|14|58|0||00|
37|SeekGe|11|58|14|1|00|
38|IdxGE|11|58|14|1|01|
39|IdxRowid|11|3|0||00|
40|Seek|5|3|0||00|
41|Column|11|1|15||00|
42|MustBeInt|15|57|0||00|
43|NotExists|6|57|15||00|
44|Column|6|3|16||00|
45|MustBeInt|16|57|0||00|
46|NotExists|7|57|16||00|
47|Column|7|2|17||00|
48|MustBeInt|17|57|0||00|
49|NotExists|8|57|17||00|
50|Column|8|1|18||00|
51|String8|0|19|0|EUROPE|00|
52|Ne|19|57|18|collseq(BINARY)|6a|
53|Column|5|3|5||00|
54|RealAffinity|5|0|0||00|
55|CollSeq|0|0|0|collseq(BINARY)|00|
56|AggStep|0|5|12|min(1)|01|
57|Next|11|38|0||00|
58|Close|5|0|0||00|
59|Close|11|0|0||00|
60|Close|6|0|0||00|
61|Close|7|0|0||00|
62|Close|8|0|0||00|
63|AggFinal|12|1|0|min(1)|00|
64|SCopy|12|20|0||00|
65|Move|20|10|1||00|
66|IfZero|11|67|-1||00|
67|Ne|10|98|8|collseq(BINARY)|6d|
68|Column|10|1|8||00|
69|MustBeInt|8|98|0||00|
70|NotExists|1|98|8||00|
71|Column|1|3|9||00|
72|MustBeInt|9|98|0||00|
73|NotExists|3|98|9||00|
74|Column|3|2|18||00|
75|MustBeInt|18|98|0||00|
76|NotExists|4|98|18||00|
77|Column|4|1|17||00|
78|Ne|2|98|17|collseq(BINARY)|6a|
79|Column|1|5|21||00|
80|RealAffinity|21|0|0||00|
81|Column|1|1|22||00|
82|Column|3|1|23||00|
83|Rowid|0|24|0||00|
84|Column|0|2|25||00|
85|Column|1|2|26||00|
86|Column|1|4|27||00|
87|Column|1|6|28||00|
88|MakeRecord|21|8|17||00|
89|Column|1|5|29||00|
90|RealAffinity|29|0|0||00|
91|Column|3|1|30||00|
92|Column|1|1|31||00|
93|Rowid|0|32|0||00|
94|Sequence|9|33|0||00|
95|Move|17|34|1||00|
96|MakeRecord|29|6|18||00|
97|SorterInsert|9|18|0||00|
98|Next|10|21|0||00|
99|Next|0|12|0||01|
100|Close|0|0|0||00|
101|Close|2|0|0||00|
102|Close|10|0|0||00|
103|Close|1|0|0||00|
104|Close|3|0|0||00|
105|Close|4|0|0||00|
106|OpenPseudo|12|17|8||00|
107|OpenPseudo|13|35|6||00|
108|SorterSort|9|121|0||00|
109|SorterData|9|35|0||00|
110|Column|13|5|17||20|
111|Column|12|0|21||20|
112|Column|12|1|22||00|
113|Column|12|2|23||00|
114|Column|12|3|24||00|
115|Column|12|4|25||00|
116|Column|12|5|26||00|
117|Column|12|6|27||00|
118|Column|12|7|28||00|
119|ResultRow|21|8|0||00|
120|SorterNext|9|109|0||00|
121|Close|12|0|0||00|
122|Halt|0|0|0||00|
123|Transaction|0|0|0||00|
124|VerifyCookie|0|27|0||00|
125|TableLock|0|2|0|Part|00|
126|TableLock|0|4|0|PartSupp|00|
127|TableLock|0|3|0|Supplier|00|
128|TableLock|0|6|0|Nation|00|
129|TableLock|0|7|0|Region|00|
130|Goto|0|5|0||00|
*/