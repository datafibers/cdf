drop database if exists kdb_uk_prod cascade;
create database if not exists kdb_uk_prod;
use kdb_uk_prod;

drop table if exists spot_rate;
create table if not exists spot_rate (
currency_pair string,
bid_rate string,
ask_rate string
) partitioned by (run_date string)
row format delimited fields terminated by '|' lines terminated by '\n';

alter table spot_rate add partition(run_date=20200119);
load data local inpath 'src/test/data/kdb_uk_prod/spot_rate' overwrite into table spot_rate partition(run_date=20200119);

alter table spot_rate add partition(run_date=20200120);
load data local inpath 'src/test/data/kdb_uk_prod/spot_rate' overwrite into table spot_rate partition(run_date=20200120);