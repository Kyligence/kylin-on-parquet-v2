--
-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--
select * from(
  select cal_dt, lstg_format_name, sum(price) as GMV,
  100*sum(price)/first_value(sum(price)) over (partition by lstg_format_name order by cast(cal_dt as timestamp)) as "last_day",
  first_value(sum(price)) over (partition by lstg_format_name order by cast(cal_dt as timestamp))
  from test_kylin_fact as "last_year"
  where cal_dt between '2013-01-08' and '2013-01-15' or cal_dt between '2013-01-07' and '2013-01-15' or cal_dt between '2012-01-01' and '2012-01-15'
  group by cal_dt, lstg_format_name
)t
where cal_dt between '2013-01-06' and '2013-01-15'
