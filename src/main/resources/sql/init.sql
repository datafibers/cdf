-- this is comment line

-- this is example of code reused
set cp_test001 = select *, '${cob}' as cob, '${ppd}' as ppd from spot_rate;

set cp_test002 =
select *,
'${cob}' as cob,
'${ppd}' as ppd
from spot_rate;

set cp_test003 = *, '${cob}' as cob, '${ppd}' as ppd;

-- this is to set common variables
set DEFAULT_STRING_SET = ('null', '');
set DEFAULT_PERIOD = 12;
