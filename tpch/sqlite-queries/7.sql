/* TPC_H Query 7 - Volume Shipping */
select
	supp_nation,
	cust_nation,
	l_year,
	sum(volume)
from
	(
		select
			n1.name as supp_nation,
			n2.name as cust_nation,
			strftime('%Y', l.shipdate) as l_year,
			l.extendedprice * (1 - l.discount) as volume
		from
			supplier s,
			lineitem l,
			orders o,
			customer c,
			nation n1,
			nation n2
		where
			s.suppkey = l.suppkey
			and o.orderkey = l.orderkey
			and c.custkey = o.custkey
			and s.nationkey = n1.nationkey
			and c.nationkey = n2.nationkey
			and (
				(n1.name = 'UNITED KINGDOM' and n2.name = 'UNITED STATES')
				or (n1.name = 'UNITED STATES' and n2.name = 'UNITED KINGDOM')
			)
			and l.shipdate between date('1995-01-01') and date('1996-12-31')
	) as shipping
group by
	supp_nation,
	cust_nation,
	l_year
order by
	supp_nation,
	cust_nation,
	l_year;

/*
Not implemented: 19
Compare
Function
Gosub
IfPos
Jump
MakeRecord
Move
Multiply
OpenPseudo
RealAffinity
Return
Sequence
SorterData
SorterInsert
SorterNext
SorterOpen
SorterSort
String8
Subtract

Unique opcodes: 42
AggFinal
AggStep
Close
Column
Compare
Copy
Eq
Function
Gosub
Goto
Gt
Halt
IfPos
Integer
Jump
Lt
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
Rewind
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
1|SorterOpen|7|7|0|keyinfo(3,BINARY,BINARY)|00|
2|Integer|0|8|0||00|
3|Integer|0|7|0||00|
4|Null|0|11|13||00|
5|Gosub|10|106|0||00|
6|String8|0|17|0|UNITED KINGDOM|00|
7|String8|0|18|0|UNITED STATES|00|
8|String8|0|19|0|UNITED STATES|00|
9|String8|0|20|0|UNITED KINGDOM|00|
10|Goto|0|114|0||00|
11|OpenRead|2|8|0|11|00|
12|OpenRead|1|3|0|4|00|
13|OpenRead|3|9|0|2|00|
14|OpenRead|4|5|0|4|00|
15|OpenRead|5|6|0|2|00|
16|OpenRead|6|6|0|2|00|
17|Rewind|2|62|0||00|
18|Column|2|10|21||00|
19|String8|0|23|0|1995-01-01|00|
20|Function|1|23|22|date(-1)|01|
21|Lt|22|61|21|collseq(BINARY)|6a|
22|String8|0|23|0|1996-12-31|00|
23|Function|1|23|22|date(-1)|01|
24|Gt|22|61|21|collseq(BINARY)|6a|
25|Column|2|2|22||00|
26|MustBeInt|22|61|0||00|
27|NotExists|1|61|22||00|
28|Column|2|0|24||00|
29|MustBeInt|24|61|0||00|
30|NotExists|3|61|24||00|
31|Column|3|1|25||00|
32|MustBeInt|25|61|0||00|
33|NotExists|4|61|25||00|
34|Column|1|3|26||00|
35|MustBeInt|26|61|0||00|
36|NotExists|5|61|26||00|
37|Column|4|3|27||00|
38|MustBeInt|27|61|0||00|
39|NotExists|6|61|27||00|
40|Column|5|1|28||00|
41|Ne|17|44|28|collseq(BINARY)|6a|
42|Column|6|1|29||00|
43|Eq|18|48|29|collseq(BINARY)|62|
44|Column|5|1|29||00|
45|Ne|19|61|29|collseq(BINARY)|6a|
46|Column|6|1|28||00|
47|Ne|20|61|28|collseq(BINARY)|6a|
48|Column|5|1|31||00|
49|Column|6|1|32||00|
50|String8|0|38|0|%Y|00|
51|Column|2|10|39||00|
52|Function|1|38|33|strftime(-1)|02|
53|Sequence|7|34|0||00|
54|Column|2|10|35||00|
55|Column|2|5|36||00|
56|RealAffinity|36|0|0||00|
57|Column|2|6|37||00|
58|RealAffinity|37|0|0||00|
59|MakeRecord|31|7|26||00|
60|SorterInsert|7|26|0||00|
61|Next|2|18|0||01|
62|Close|2|0|0||00|
63|Close|1|0|0||00|
64|Close|3|0|0||00|
65|Close|4|0|0||00|
66|Close|5|0|0||00|
67|Close|6|0|0||00|
68|OpenPseudo|8|26|7||00|
69|SorterSort|7|113|0||00|
70|SorterData|7|26|0||00|
71|Column|8|0|14||20|
72|Column|8|1|15||00|
73|Column|8|2|16||00|
74|Compare|11|14|3|keyinfo(3,BINARY,BINARY)|00|
75|Jump|76|80|76||00|
76|Move|14|11|3||00|
77|Gosub|9|95|0||00|
78|IfPos|8|113|0||00|
79|Gosub|10|106|0||00|
80|Column|8|5|25||00|
81|Integer|1|27|0||00|
82|Column|8|6|28||00|
83|Subtract|28|27|24||00|
84|Multiply|24|25|31||00|
85|AggStep|0|31|4|sum(1)|01|
86|Column|8|0|1||00|
87|Column|8|1|2||00|
88|Column|8|4|3||00|
89|Integer|1|7|0||00|
90|SorterNext|7|70|0||00|
91|Gosub|9|95|0||00|
92|Goto|0|113|0||00|
93|Integer|1|8|0||00|
94|Return|9|0|0||00|
95|IfPos|7|97|0||00|
96|Return|9|0|0||00|
97|AggFinal|4|1|0|sum(1)|00|
98|Copy|1|40|0||00|
99|Copy|2|41|0||00|
100|String8|0|32|0|%Y|00|
101|Copy|3|33|0||00|
102|Function|1|32|42|strftime(-1)|02|
103|Copy|4|43|0||00|
104|ResultRow|40|4|0||00|
105|Return|9|0|0||00|
106|Null|0|1|0||00|
107|Null|0|2|0||00|
108|Null|0|3|0||00|
109|Null|0|5|0||00|
110|Null|0|6|0||00|
111|Null|0|4|0||00|
112|Return|10|0|0||00|
113|Halt|0|0|0||00|
114|Transaction|0|0|0||00|
115|VerifyCookie|0|27|0||00|
116|TableLock|0|8|0|LineItem|00|
117|TableLock|0|3|0|Supplier|00|
118|TableLock|0|9|0|Orders|00|
119|TableLock|0|5|0|Customer|00|
120|TableLock|0|6|0|Nation|00|
121|Goto|0|11|0||00|
*/