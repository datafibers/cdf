insert overwrite directory '${DE_OUTPUT_ROOT_PATH}/direct-insert/run_date=${cob}' using parquet
select
'${para_1}' as para_1,
'${para_2}' as para_2,
'${cob}' as cob,
'${ppd}' as previous_processed_date
from
file_alias; --driver

${cp_test001};

${cp_test002};

select ${cp_test003} from file_alias;