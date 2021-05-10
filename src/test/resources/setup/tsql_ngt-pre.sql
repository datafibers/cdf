drop database if exists ngt_xds_prod cascade;
create database if not exists ngt_xds_prod;
use ngt_xds_prod;

drop table if exists ngt_tree_attributes;
create table if not exists ngt_tree_attributes (
grid string,
profile string,
amount double
) partitioned by (run_date string)
row format delimited fields terminated by '|' lines terminated by '\n';

alter table spot_rate add partition(run_date=20200119);
load data local inpath 'src/test/data/ngt_xds_prod/test' overwrite into table ngt_tree_attributes partition(run_date=20200119);

alter table spot_rate add partition(run_date=20200120);
load data local inpath 'src/test/data/ngt_xds_prod/test' overwrite into table ngt_tree_attributes partition(run_date=20200120);