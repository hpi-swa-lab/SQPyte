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



0|Init|0|176|0||00|
1|Null|0|1|3||00|
2|OpenRead|0|8|0|15|00|
3|OpenRead|1|2|0|7|00|
4|Rewind|0|170|0||00|
5|Null|0|5|0||00|
6|Integer|160|4|0||00|
7|Column|0|1|7||00|
8|MustBeInt|7|58|0||00|
9|NotExists|1|58|7||00|
10|Column|1|3|8||00|
11|Ne|9|58|8|(BINARY)|6a|
12|Null|0|10|0||00|
13|Once|0|29|0||00|
14|Null|0|10|0||00|
15|OpenEphemeral|4|1|0|k(1,B)|00|
16|Null|0|12|0||00|
17|String8|0|11|0|SM CASE|00|
18|MakeRecord|11|1|12|b|00|
19|IdxInsert|4|12|0||00|
20|String8|0|11|0|SM BOX|00|
21|MakeRecord|11|1|12|b|00|
22|IdxInsert|4|12|0||00|
23|String8|0|11|0|SM PACK|00|
24|MakeRecord|11|1|12|b|00|
25|IdxInsert|4|12|0||00|
26|String8|0|11|0|SM PKG|00|
27|MakeRecord|11|1|12|b|00|
28|IdxInsert|4|12|0||00|
29|Column|1|6|12||00|
30|IsNull|12|58|0||00|
31|Affinity|12|1|0|b|00|
32|NotFound|4|58|12|1|00|
33|Column|0|4|12||00|
34|Lt|13|58|12|(BINARY)|6c|
35|Gt|14|58|12|(BINARY)|6c|
36|Column|1|5|11||00|
37|Lt|13|58|11|(BINARY)|6c|
38|Gt|16|58|11|(BINARY)|6c|
39|Null|0|17|0||00|
40|Once|1|50|0||00|
41|Null|0|17|0||00|
42|OpenEphemeral|6|1|0|k(1,B)|00|
43|Null|0|18|0||00|
44|String8|0|15|0|AIR|00|
45|MakeRecord|15|1|18|b|00|
46|IdxInsert|6|18|0||00|
47|String8|0|15|0|AIR REG|00|
48|MakeRecord|15|1|18|b|00|
49|IdxInsert|6|18|0||00|
50|Column|0|14|18||00|
51|IsNull|18|58|0||00|
52|Affinity|18|1|0|b|00|
53|NotFound|6|58|18|1|00|
54|Column|0|13|18||00|
55|Ne|19|58|18|(BINARY)|6a|
56|RowSetTest|5|58|7|0|00|
57|Gosub|4|161|0||00|
58|Column|0|1|20||00|
59|MustBeInt|20|109|0||00|
60|NotExists|1|109|20||00|
61|Column|1|3|18||00|
62|Ne|21|109|18|(BINARY)|6a|
63|Null|0|22|0||00|
64|Once|2|80|0||00|
65|Null|0|22|0||00|
66|OpenEphemeral|8|1|0|k(1,B)|00|
67|Null|0|8|0||00|
68|String8|0|11|0|MED BAG|00|
69|MakeRecord|11|1|8|b|00|
70|IdxInsert|8|8|0||00|
71|String8|0|11|0|MED BOX|00|
72|MakeRecord|11|1|8|b|00|
73|IdxInsert|8|8|0||00|
74|String8|0|11|0|MED PKG|00|
75|MakeRecord|11|1|8|b|00|
76|IdxInsert|8|8|0||00|
77|String8|0|11|0|MED PACK|00|
78|MakeRecord|11|1|8|b|00|
79|IdxInsert|8|8|0||00|
80|Column|1|6|8||00|
81|IsNull|8|109|0||00|
82|Affinity|8|1|0|b|00|
83|NotFound|8|109|8|1|00|
84|Column|0|4|8||00|
85|Lt|23|109|8|(BINARY)|6c|
86|Gt|24|109|8|(BINARY)|6c|
87|Column|1|5|11||00|
88|Lt|13|109|11|(BINARY)|6c|
89|Gt|23|109|11|(BINARY)|6c|
90|Null|0|25|0||00|
91|Once|3|101|0||00|
92|Null|0|25|0||00|
93|OpenEphemeral|10|1|0|k(1,B)|00|
94|Null|0|26|0||00|
95|String8|0|15|0|AIR|00|
96|MakeRecord|15|1|26|b|00|
97|IdxInsert|10|26|0||00|
98|String8|0|15|0|AIR REG|00|
99|MakeRecord|15|1|26|b|00|
100|IdxInsert|10|26|0||00|
101|Column|0|14|26||00|
102|IsNull|26|109|0||00|
103|Affinity|26|1|0|b|00|
104|NotFound|10|109|26|1|00|
105|Column|0|13|26||00|
106|Ne|19|109|26|(BINARY)|6a|
107|RowSetTest|5|109|20|1|00|
108|Gosub|4|161|0||00|
109|Column|0|1|27||00|
110|MustBeInt|27|160|0||00|
111|NotExists|1|160|27||00|
112|Column|1|3|26||00|
113|Ne|28|160|26|(BINARY)|6a|
114|Null|0|29|0||00|
115|Once|4|131|0||00|
116|Null|0|29|0||00|
117|OpenEphemeral|12|1|0|k(1,B)|00|
118|Null|0|18|0||00|
119|String8|0|11|0|LG CASE|00|
120|MakeRecord|11|1|18|b|00|
121|IdxInsert|12|18|0||00|
122|String8|0|11|0|LG BOX|00|
123|MakeRecord|11|1|18|b|00|
124|IdxInsert|12|18|0||00|
125|String8|0|11|0|LG PACK|00|
126|MakeRecord|11|1|18|b|00|
127|IdxInsert|12|18|0||00|
128|String8|0|11|0|LG PKG|00|
129|MakeRecord|11|1|18|b|00|
130|IdxInsert|12|18|0||00|
131|Column|1|6|18||00|
132|IsNull|18|160|0||00|
133|Affinity|18|1|0|b|00|
134|NotFound|12|160|18|1|00|
135|Column|0|4|18||00|
136|Lt|30|160|18|(BINARY)|6c|
137|Gt|31|160|18|(BINARY)|6c|
138|Column|1|5|11||00|
139|Lt|13|160|11|(BINARY)|6c|
140|Gt|32|160|11|(BINARY)|6c|
141|Null|0|33|0||00|
142|Once|5|152|0||00|
143|Null|0|33|0||00|
144|OpenEphemeral|14|1|0|k(1,B)|00|
145|Null|0|34|0||00|
146|String8|0|15|0|AIR|00|
147|MakeRecord|15|1|34|b|00|
148|IdxInsert|14|34|0||00|
149|String8|0|15|0|AIR REG|00|
150|MakeRecord|15|1|34|b|00|
151|IdxInsert|14|34|0||00|
152|Column|0|14|34||00|
153|IsNull|34|160|0||00|
154|Affinity|34|1|0|b|00|
155|NotFound|14|160|34|1|00|
156|Column|0|13|34||00|
157|Ne|19|160|34|(BINARY)|6a|
158|RowSetTest|5|160|27|-1|00|
159|Gosub|4|161|0||00|
160|Goto|0|169|0||00|
161|Column|0|5|34||00|
162|RealAffinity|34|0|0||00|
163|Column|0|6|26||00|
164|RealAffinity|26|0|0||00|
165|Subtract|26|13|11||00|
166|Multiply|11|34|35||00|
167|AggStep|0|35|1|sum(1)|01|
168|Return|4|0|0||00|
169|Next|0|5|0||01|
170|Close|0|0|0||00|
171|Close|1|0|0||00|
172|AggFinal|1|1|0|sum(1)|00|
173|Copy|1|36|0||00|
174|ResultRow|36|1|0||00|
175|Halt|0|0|0||00|
176|Transaction|0|0|23|0|01|
177|TableLock|0|8|0|LineItem|00|
178|TableLock|0|2|0|Part|00|
179|String8|0|9|0|Brand#12|00|
180|Integer|1|13|0||00|
181|Integer|1|26|0||00|
182|Integer|10|34|0||00|
183|Add|34|26|14||00|
184|Integer|5|16|0||00|
185|String8|0|19|0|DELIVER IN PERSON|00|
186|String8|0|21|0|Brand#23|00|
187|Integer|10|23|0||00|
188|Integer|10|34|0||00|
189|Integer|10|26|0||00|
190|Add|26|34|24||00|
191|String8|0|28|0|Brand#34|00|
192|Integer|20|30|0||00|
193|Integer|20|26|0||00|
194|Integer|10|34|0||00|
195|Add|34|26|31||00|
196|Integer|15|32|0||00|
197|Goto|0|1|0||00|

*/
