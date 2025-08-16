select
`_id`,
accountid,
userid,
tenantid,
offset,
balance,
accinterest,
lastinterest,
accinterestu,
lastinterestu,
accinterestbtc,
lastinterestbtc,
cur_time,
dt,
ht
from ods_src_mongo_spot.ods_financeasset

-- 这是一个小时任务表，同步频率为小时级别
-- 其中tenantid是