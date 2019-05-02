-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select rshift(null, 1) vn, rshift(1, null) vn1, rshift(null, null) vn2, rshift(3, 6) v1, rshift(1024,3) v2 from test;
> VN   VN1  VN2  V1 V2
> ---- ---- ---- -- ---
> null null null 0  128
> rows: 1
