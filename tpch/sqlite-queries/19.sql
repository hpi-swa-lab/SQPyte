/* TPC_H Query 19 - Discounted Revenue */
select
	sum(l.extendedprice* (1 - l.discount)) as revenue
from
	lineitem l,
	part p
where
	(
		p.partkey = l.partkey
		and p.brand = 'Brand#12'
		and p.container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
		and l.quantity >= 1 and l.quantity <= 1 + 10
		and p.size between 1 and 5
		and l.shipmode in ('AIR', 'AIR REG')
		and l.shipinstruct = 'DELIVER IN PERSON'
	)
	or
	(
		p.partkey = l.partkey
		and p.brand = 'Brand#23'
		and p.container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
		and l.quantity >= 10 and l.quantity <= 10 + 10
		and p.size between 1 and 10
		and l.shipmode in ('AIR', 'AIR REG')
		and l.shipinstruct = 'DELIVER IN PERSON'
	)
	or
	(
		p.partkey = l.partkey
		and p.brand = 'Brand#34'
		and p.container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
		and l.quantity >= 20 and l.quantity <= 20 + 10
		and p.size between 1 and 15
		and l.shipmode in ('AIR', 'AIR REG')
		and l.shipinstruct = 'DELIVER IN PERSON'
	);

/*
Not implemented: 15
Add
Affinity
Gosub
IdxInsert
IsNull
MakeRecord
Multiply
NotFound
Once
OpenEphemeral
RealAffinity
Return
RowSetTest
String8
Subtract

Unique opcodes: 37
Add
Affinity
AggFinal
AggStep
Close
Column
Copy
Gosub
Goto
Gt
Halt
IdxInsert
Integer
IsNull
Lt
MakeRecord
Multiply
MustBeInt
Ne
Next
NotExists
NotFound
Null
Once
OpenEphemeral
OpenRead
RealAffinity
ResultRow
Return
Rewind
RowSetTest
String8
Subtract
TableLock
Trace
Transaction
VerifyCookie


0|Trace|0|0|0||00|
1|Null|0|2|0||00|
2|Null|0|3|0||00|
3|Null|0|1|0||00|
4|String8|0|4|0|Brand#12|00|
5|Integer|1|5|0||00|
6|Integer|1|7|0||00|
7|Integer|10|8|0||00|
8|Add|8|7|6||00|
9|Integer|1|9|0||00|
10|Integer|5|10|0||00|
11|String8|0|11|0|DELIVER IN PERSON|00|
12|String8|0|12|0|Brand#23|00|
13|Integer|10|13|0||00|
14|Integer|10|8|0||00|
15|Integer|10|7|0||00|
16|Add|7|8|14||00|
17|Integer|1|15|0||00|
18|Integer|10|16|0||00|
19|String8|0|17|0|DELIVER IN PERSON|00|
20|String8|0|18|0|Brand#34|00|
21|Integer|20|19|0||00|
22|Integer|20|7|0||00|
23|Integer|10|8|0||00|
24|Add|8|7|20||00|
25|Integer|1|21|0||00|
26|Integer|15|22|0||00|
27|String8|0|23|0|DELIVER IN PERSON|00|
28|Goto|0|204|0||00|
29|OpenRead|0|8|0|15|00|
30|OpenRead|1|2|0|7|00|
31|Rewind|0|198|0||00|
32|Null|0|25|0||00|
33|Integer|187|24|0||00|
34|Column|0|1|8||00|
35|MustBeInt|8|85|0||00|
36|NotExists|1|85|8||00|
37|Column|1|3|7||00|
38|Ne|4|85|7|collseq(BINARY)|6a|
39|Null|0|28|0||00|
40|Once|0|56|0||00|
41|Null|0|28|0||00|
42|OpenEphemeral|3|1|0|keyinfo(1,BINARY)|00|
43|Null|0|29|0||00|
44|String8|0|27|0|SM CASE|00|
45|MakeRecord|27|1|29|b|00|
46|IdxInsert|3|29|0||00|
47|String8|0|27|0|SM BOX|00|
48|MakeRecord|27|1|29|b|00|
49|IdxInsert|3|29|0||00|
50|String8|0|27|0|SM PACK|00|
51|MakeRecord|27|1|29|b|00|
52|IdxInsert|3|29|0||00|
53|String8|0|27|0|SM PKG|00|
54|MakeRecord|27|1|29|b|00|
55|IdxInsert|3|29|0||00|
56|Column|1|6|29||00|
57|IsNull|29|85|0||00|
58|Affinity|29|1|0|b|00|
59|NotFound|3|85|29|1|00|
60|Column|0|4|29||00|
61|Lt|5|85|29|collseq(BINARY)|6c|
62|Gt|6|85|29|collseq(BINARY)|6c|
63|Column|1|5|27||00|
64|Lt|9|85|27|collseq(BINARY)|6c|
65|Gt|10|85|27|collseq(BINARY)|6c|
66|Null|0|31|0||00|
67|Once|1|77|0||00|
68|Null|0|31|0||00|
69|OpenEphemeral|5|1|0|keyinfo(1,BINARY)|00|
70|Null|0|32|0||00|
71|String8|0|30|0|AIR|00|
72|MakeRecord|30|1|32|b|00|
73|IdxInsert|5|32|0||00|
74|String8|0|30|0|AIR REG|00|
75|MakeRecord|30|1|32|b|00|
76|IdxInsert|5|32|0||00|
77|Column|0|14|32||00|
78|IsNull|32|85|0||00|
79|Affinity|32|1|0|b|00|
80|NotFound|5|85|32|1|00|
81|Column|0|13|32||00|
82|Ne|11|85|32|collseq(BINARY)|6a|
83|RowSetTest|25|85|8|0|00|
84|Gosub|24|188|0||00|
85|Column|0|1|32||00|
86|MustBeInt|32|136|0||00|
87|NotExists|1|136|32||00|
88|Column|1|3|27||00|
89|Ne|12|136|27|collseq(BINARY)|6a|
90|Null|0|33|0||00|
91|Once|2|107|0||00|
92|Null|0|33|0||00|
93|OpenEphemeral|7|1|0|keyinfo(1,BINARY)|00|
94|Null|0|30|0||00|
95|String8|0|7|0|MED BAG|00|
96|MakeRecord|7|1|30|b|00|
97|IdxInsert|7|30|0||00|
98|String8|0|7|0|MED BOX|00|
99|MakeRecord|7|1|30|b|00|
100|IdxInsert|7|30|0||00|
101|String8|0|7|0|MED PKG|00|
102|MakeRecord|7|1|30|b|00|
103|IdxInsert|7|30|0||00|
104|String8|0|7|0|MED PACK|00|
105|MakeRecord|7|1|30|b|00|
106|IdxInsert|7|30|0||00|
107|Column|1|6|30||00|
108|IsNull|30|136|0||00|
109|Affinity|30|1|0|b|00|
110|NotFound|7|136|30|1|00|
111|Column|0|4|30||00|
112|Lt|13|136|30|collseq(BINARY)|6c|
113|Gt|14|136|30|collseq(BINARY)|6c|
114|Column|1|5|7||00|
115|Lt|15|136|7|collseq(BINARY)|6c|
116|Gt|16|136|7|collseq(BINARY)|6c|
117|Null|0|35|0||00|
118|Once|3|128|0||00|
119|Null|0|35|0||00|
120|OpenEphemeral|9|1|0|keyinfo(1,BINARY)|00|
121|Null|0|36|0||00|
122|String8|0|34|0|AIR|00|
123|MakeRecord|34|1|36|b|00|
124|IdxInsert|9|36|0||00|
125|String8|0|34|0|AIR REG|00|
126|MakeRecord|34|1|36|b|00|
127|IdxInsert|9|36|0||00|
128|Column|0|14|36||00|
129|IsNull|36|136|0||00|
130|Affinity|36|1|0|b|00|
131|NotFound|9|136|36|1|00|
132|Column|0|13|36||00|
133|Ne|17|136|36|collseq(BINARY)|6a|
134|RowSetTest|25|136|32|1|00|
135|Gosub|24|188|0||00|
136|Column|0|1|36||00|
137|MustBeInt|36|187|0||00|
138|NotExists|1|187|36||00|
139|Column|1|3|7||00|
140|Ne|18|187|7|collseq(BINARY)|6a|
141|Null|0|37|0||00|
142|Once|4|158|0||00|
143|Null|0|37|0||00|
144|OpenEphemeral|11|1|0|keyinfo(1,BINARY)|00|
145|Null|0|34|0||00|
146|String8|0|27|0|LG CASE|00|
147|MakeRecord|27|1|34|b|00|
148|IdxInsert|11|34|0||00|
149|String8|0|27|0|LG BOX|00|
150|MakeRecord|27|1|34|b|00|
151|IdxInsert|11|34|0||00|
152|String8|0|27|0|LG PACK|00|
153|MakeRecord|27|1|34|b|00|
154|IdxInsert|11|34|0||00|
155|String8|0|27|0|LG PKG|00|
156|MakeRecord|27|1|34|b|00|
157|IdxInsert|11|34|0||00|
158|Column|1|6|34||00|
159|IsNull|34|187|0||00|
160|Affinity|34|1|0|b|00|
161|NotFound|11|187|34|1|00|
162|Column|0|4|34||00|
163|Lt|19|187|34|collseq(BINARY)|6c|
164|Gt|20|187|34|collseq(BINARY)|6c|
165|Column|1|5|27||00|
166|Lt|21|187|27|collseq(BINARY)|6c|
167|Gt|22|187|27|collseq(BINARY)|6c|
168|Null|0|39|0||00|
169|Once|5|179|0||00|
170|Null|0|39|0||00|
171|OpenEphemeral|13|1|0|keyinfo(1,BINARY)|00|
172|Null|0|40|0||00|
173|String8|0|38|0|AIR|00|
174|MakeRecord|38|1|40|b|00|
175|IdxInsert|13|40|0||00|
176|String8|0|38|0|AIR REG|00|
177|MakeRecord|38|1|40|b|00|
178|IdxInsert|13|40|0||00|
179|Column|0|14|40||00|
180|IsNull|40|187|0||00|
181|Affinity|40|1|0|b|00|
182|NotFound|13|187|40|1|00|
183|Column|0|13|40||00|
184|Ne|23|187|40|collseq(BINARY)|6a|
185|RowSetTest|25|187|36|-1|00|
186|Gosub|24|188|0||00|
187|Goto|0|197|0||00|
188|Column|0|5|40||00|
189|RealAffinity|40|0|0||00|
190|Integer|1|7|0||00|
191|Column|0|6|38||00|
192|RealAffinity|38|0|0||00|
193|Subtract|38|7|27||00|
194|Multiply|27|40|41||00|
195|AggStep|0|41|1|sum(1)|01|
196|Return|24|0|0||00|
197|Next|0|32|0||01|
198|Close|0|0|0||00|
199|Close|1|0|0||00|
200|AggFinal|1|1|0|sum(1)|00|
201|Copy|1|42|0||00|
202|ResultRow|42|1|0||00|
203|Halt|0|0|0||00|
204|Transaction|0|0|0||00|
205|VerifyCookie|0|27|0||00|
206|TableLock|0|8|0|LineItem|00|
207|TableLock|0|2|0|Part|00|
208|Goto|0|29|0||00|
*/