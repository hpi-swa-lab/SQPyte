/* TPC_H Query 22 - Global Sales Opportunity */
select
	cntrycode,
	count(*) as numcust,
	sum(acctbal) as totacctbal
from
	(
		select
			substr(c.phone, 1, 2) as cntrycode,
			c.acctbal
		from
			customer c
		where
			substr(c.phone, 1, 2) in
				('10', '11', '13', '17', '25', '29', '34')
			and c.acctbal > (
				select
					avg(acctbal)
				from
					customer
				where
					acctbal > 0.00
					and substr(phone, 1, 2) in
						('10', '11', '13', '17', '25', '29', '34')
			)
			and not exists (
				select
					*
				from
					orders o
				where
					o.custkey = c.custkey
			)
	) as custsale
group by
	cntrycode
order by
	cntrycode;

/*
Not implemented:32
Affinity
Compare
Function
Gosub
IdxGe
IdxInsert
IdxRowid
If
IfPos
IfZero
IsNull
Jump
MakeRecord
Move
NotFound
Once
OnceEphemeral
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

Unique opcodes: 50
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
IdxInsert
IdxRowid
If
IfPos
IfZero
Integer
IsNull
Jump
Le
MakeRecord
Move
Next
NotFound
Null
Once
OpenEphemeral
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
1|SorterOpen|4|4|0|keyinfo(1,BINARY)|00|
2|Integer|0|6|0||00|
3|Integer|0|5|0||00|
4|Null|0|9|9||00|
5|Gosub|8|158|0||00|
6|Goto|0|164|0||00|
7|OpenRead|1|5|0|6|00|
8|Rewind|1|125|0||00|
9|Null|0|11|0||00|
10|Once|0|35|0||00|
11|Null|0|11|0||00|
12|OpenEphemeral|6|1|0|keyinfo(1,nil)|00|
13|Null|0|13|0||00|
14|String8|0|12|0|10|00|
15|MakeRecord|12|1|13|b|00|
16|IdxInsert|6|13|0||00|
17|String8|0|12|0|11|00|
18|MakeRecord|12|1|13|b|00|
19|IdxInsert|6|13|0||00|
20|String8|0|12|0|13|00|
21|MakeRecord|12|1|13|b|00|
22|IdxInsert|6|13|0||00|
23|String8|0|12|0|17|00|
24|MakeRecord|12|1|13|b|00|
25|IdxInsert|6|13|0||00|
26|String8|0|12|0|25|00|
27|MakeRecord|12|1|13|b|00|
28|IdxInsert|6|13|0||00|
29|String8|0|12|0|29|00|
30|MakeRecord|12|1|13|b|00|
31|IdxInsert|6|13|0||00|
32|String8|0|12|0|34|00|
33|MakeRecord|12|1|13|b|00|
34|IdxInsert|6|13|0||00|
35|Column|1|4|14||00|
36|Integer|1|15|0||00|
37|Integer|2|16|0||00|
38|Function|6|14|13|substr(3)|03|
39|IsNull|13|124|0||00|
40|Affinity|13|1|0|b|00|
41|NotFound|6|124|13|1|00|
42|Column|1|5|13||00|
43|RealAffinity|13|0|0||00|
44|Once|1|97|0||00|
45|Null|0|17|0||00|
46|Integer|1|18|0||00|
47|Null|0|20|0||00|
48|Null|0|19|0||00|
49|OpenRead|2|5|0|6|00|
50|Rewind|2|92|0||00|
51|Column|2|5|21||00|
52|RealAffinity|21|0|0||00|
53|Real|0|22|0|0|00|
54|Le|22|91|21|collseq(BINARY)|6d|
55|Null|0|23|0||00|
56|Once|2|81|0||00|
57|Null|0|23|0||00|
58|OpenEphemeral|8|1|0|keyinfo(1,nil)|00|
59|Null|0|24|0||00|
60|String8|0|22|0|10|00|
61|MakeRecord|22|1|24|b|00|
62|IdxInsert|8|24|0||00|
63|String8|0|22|0|11|00|
64|MakeRecord|22|1|24|b|00|
65|IdxInsert|8|24|0||00|
66|String8|0|22|0|13|00|
67|MakeRecord|22|1|24|b|00|
68|IdxInsert|8|24|0||00|
69|String8|0|22|0|17|00|
70|MakeRecord|22|1|24|b|00|
71|IdxInsert|8|24|0||00|
72|String8|0|22|0|25|00|
73|MakeRecord|22|1|24|b|00|
74|IdxInsert|8|24|0||00|
75|String8|0|22|0|29|00|
76|MakeRecord|22|1|24|b|00|
77|IdxInsert|8|24|0||00|
78|String8|0|22|0|34|00|
79|MakeRecord|22|1|24|b|00|
80|IdxInsert|8|24|0||00|
81|Column|2|4|14||00|
82|Integer|1|15|0||00|
83|Integer|2|16|0||00|
84|Function|6|14|24|substr(3)|03|
85|IsNull|24|91|0||00|
86|Affinity|24|1|0|b|00|
87|NotFound|8|91|24|1|00|
88|Column|2|5|14||00|
89|RealAffinity|14|0|0||00|
90|AggStep|0|14|19|avg(1)|01|
91|Next|2|51|0||01|
92|Close|2|0|0||00|
93|AggFinal|19|1|0|avg(1)|00|
94|SCopy|19|25|0||00|
95|Move|25|17|1||00|
96|IfZero|18|97|-1||00|
97|Le|17|124|13|collseq(BINARY)|6d|
98|Integer|0|26|0||00|
99|Integer|1|27|0||00|
100|OpenRead|3|9|0|9|00|
101|OpenRead|9|22|0|keyinfo(1,BINARY)|00|
102|Rowid|1|28|0||00|
103|IsNull|28|111|0||00|
104|SeekGe|9|111|28|1|00|
105|IdxGE|9|111|28|1|01|
106|IdxRowid|9|12|0||00|
107|Seek|3|12|0||00|
108|Integer|1|26|0||00|
109|IfZero|27|111|-1||00|
110|Next|9|105|0||00|
111|Close|3|0|0||00|
112|Close|9|0|0||00|
113|If|26|124|1||00|
114|Column|1|4|42||00|
115|Integer|1|43|0||00|
116|Integer|2|44|0||00|
117|Function|6|42|38|substr(3)|03|
118|Sequence|4|39|0||00|
119|Column|1|4|40||00|
120|Column|1|5|41||00|
121|RealAffinity|41|0|0||00|
122|MakeRecord|38|4|13||00|
123|SorterInsert|4|13|0||00|
124|Next|1|9|0||01|
125|Close|1|0|0||00|
126|OpenPseudo|10|13|4||00|
127|SorterSort|4|163|0||00|
128|SorterData|4|13|0||00|
129|Column|10|0|10||20|
130|Compare|9|10|1|keyinfo(1,BINARY)|00|
131|Jump|132|136|132||00|
132|Move|10|9|1||00|
133|Gosub|7|146|0||00|
134|IfPos|6|163|0||00|
135|Gosub|8|158|0||00|
136|AggStep|0|0|2|count(0)|00|
137|Column|10|3|38||00|
138|AggStep|0|38|3|sum(1)|01|
139|Column|10|2|1||00|
140|Integer|1|5|0||00|
141|SorterNext|4|128|0||00|
142|Gosub|7|146|0||00|
143|Goto|0|163|0||00|
144|Integer|1|6|0||00|
145|Return|7|0|0||00|
146|IfPos|5|148|0||00|
147|Return|7|0|0||00|
148|AggFinal|2|0|0|count(0)|00|
149|AggFinal|3|1|0|sum(1)|00|
150|Copy|1|39|0||00|
151|Integer|1|40|0||00|
152|Integer|2|41|0||00|
153|Function|6|39|45|substr(3)|03|
154|Copy|2|46|0||00|
155|Copy|3|47|0||00|
156|ResultRow|45|3|0||00|
157|Return|7|0|0||00|
158|Null|0|1|0||00|
159|Null|0|4|0||00|
160|Null|0|2|0||00|
161|Null|0|3|0||00|
162|Return|8|0|0||00|
163|Halt|0|0|0||00|
164|Transaction|0|0|0||00|
165|VerifyCookie|0|27|0||00|
166|TableLock|0|5|0|Customer|00|
167|TableLock|0|9|0|Orders|00|
168|Goto|0|7|0||00|
*/