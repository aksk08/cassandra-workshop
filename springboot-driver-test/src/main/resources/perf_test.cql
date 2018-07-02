use test;
create table perf_test (
pk_date text,
id text,
sec_id int,
process_id text,
status int,
comment text,
add_text text,
created_dt date,
primary key (pk_date, id, sec_id)
);


select count(*) from perf_test;
truncate table perf_test;
