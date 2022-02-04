# -*- coding: utf-8 -*-
from dish_utilities import redshift
import pandas as pd
import datetime
from StorageObj import S3Obj
import os
import subprocess
import sys

try:
    from sklearn.model_selection import train_test_split
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", 'scikit-learn'])
finally:
	from sklearn.model_selection import train_test_split
	from sklearn.tree import DecisionTreeClassifier
	from sklearn import metrics



S3O = S3Obj()
rs = redshift()

# set the enviroment variable
env = os.environ['DEPLOY_ENVIRONMENT']

# set the s3 variables
s3bucket = 'dish-media-sales-'+env
metrics_prefix = 'sbx_vm_adsales_pi/performance_clearance_model_metrics/'
roc_prefix = 'sbx_vm_adsales_pi/performance_clearance_model_roc/'
output_prefix = 'sbx_vm_adsales_pi/performance_clearance_model_output/'


today_str = datetime.datetime.today().strftime('%Y-%m-%d')

# Query to pull data for model training and validation
training_query_results = rs.query("""
select base.*, nvl(aired_metrics.median_aired_rate,0)median_aired_rate, nvl(aired_metrics.mean_aired_rate,0)mean_aired_rate,nvl(aired_metrics.units_cleared,0)units_cleared
, coalesce(av.avails,0) avails
from
(select clndr_dt, dl_unt_id, dl_typ, grss_bkd_amt, dy_prt_nm, inv_lnth_in_sec
, case when sllg_ttl_nm like '%%Swim%%' and ntwrk_cd like 'TOON' then 'ADSM'
	when sllg_ttl_nm like '%%Nite%%' and  ntwrk_cd like 'NICK' then 'NAN'
	else ntwrk_cd 
	end as ntwrk_cd
, case when inv_typ_cd = 'AGG' then 'AGG'
	when inv_typ_cd = 'NCC' then 'NCC'
	else 'CM'
	end inv_typ_cd 
, case
when airdate <> -2 then True
else False
end aird_ind

from 
(
SELECT dl_nbr, dl_ver_nbr, dl_ln_nbr, dl_unt_id, vrsn_sts, dl_sts_chng_nbr, dl_sts, Adt.Clndr_dt airdate, grss_bkd_amt, ntwrk_cd, dy_prt_nm, dl_typ, inv_lnth_in_sec, inv_typ_cd, week.clndr_dt,sllg_ttl_nm,
row_number() over(partition by dl_nbr, dl_unt_id,dl_ln_nbr order by dl_ver_nbr desc, dl_sts_chng_nbr desc, dm_inv_fct_sk desc) rn  -- this ranking is done to ensure current relevent row is picked (latest row will have rnk = 1)
from adsls_dm.dm_inv_fct fct -- dm_spot_data
INNER JOIN dim.DL_UNT_DIM dim ON fct.DL_UNT_DIM_SK = dim.dl_unt_dim_sk AND dim.del_ind IS FALSE  -- dm_spot_data
LEFT JOIN dim.sllg_ttl_rate_crd_dim st ON fct.SLLG_TTL_RATE_CRD_DIM_SK = st.SLLG_TTL_RATE_CRD_DIM_SK  -- dm_ratecard_data
LEFT JOIN dim.clndr_dim Adt ON fct.AIRD_DIM_CLNDR_SK = Adt.CLNDR_DIM_SK -- Aired Date
LEFT OUTER JOIN dim.clndr_dim Week ON fct.wk_strt_dt_sk = week.CLNDR_DIM_SK -- Week
where fct.wk_strt_dt_sk >= to_char(date_add('day',-35,NEXT_DAY(current_date,'M')), 'YYYYMMDD')
and fct.wk_strt_dt_sk <= to_char(date_add('day',-7,NEXT_DAY(current_date,'M')),'YYYYMMDD')
and dim.dl_unt_wk_dt <= concat(to_char(date_add('day',-7,NEXT_DAY(current_date,'M')), 'YYYY-MM-DD'), ' 00:00:00')
and dim.dl_unt_wk_dt  >= concat(to_char(date_add('day',-35,NEXT_DAY(current_date,'M')), 'YYYY-MM-DD'), ' 00:00:00')
  and vrsn_sts = dl_sts  -- ensures latest status is picked
AND (clst_clfn LIKE 'STANDALONE UNIT%%' OR clst_clfn LIKE 'MEMBER LINE%%') -- ensures no duplication when units are cluster units
AND DL_STS IN ('O','PO', 'H', 'DR', 'PML') -- relevant status you have in netezza
AND dl_typ in ('Direct Response','Latino DR','Programmatic Linear')
and chnl_ctg_nm <> 'Non Air Time'
)x WHERE rn=1 ) base
 
left join
 
-- Aired and scheduled metrics
(select week_nm, ntwrk_cd ntwrk_cd_aur, dy_prt_nm dy_prt_nm_aur
, case when inv_typ_cd = 'AGG' then 'AGG'
	when inv_typ_cd = 'NCC' then 'NCC'
	else 'CM'
	end inv_typ_cd_aur
, coalesce(median(equiv_aired_dollars),0) median_aired_rate
, coalesce(avg(equiv_aired_dollars),0) mean_aired_rate
, coalesce(sum(aired_eqvlnt_unts),0) units_cleared
from (
SELECT ntwrk_cd as ntwrk_cd_orig 
, case when sllg_ttl_nm like '%%Swim%%' and ntwrk_cd like 'TOON' then 'ADSM'
	when sllg_ttl_nm like '%%Nite%%' and  ntwrk_cd like 'NICK' then 'NAN'
	else ntwrk_cd 
	end as ntwrk_cd
, case when fct.aird_dim_clndr_sk <> -2 or fct.schd_dim_clndr_sk <>-2 then grss_bkd_amt
	else 0 
	end as aired_bkd_amt
, case when fct.aird_dim_clndr_sk <> -2 or fct.schd_dim_clndr_sk <>-2  then eqvlnt_unts 
	else 0 
	end as aired_eqvlnt_unts
, (case when fct.aird_dim_clndr_sk <> -2  or fct.schd_dim_clndr_sk <>-2  then grss_bkd_amt else 0 end) / nullif((case when fct.aird_dim_clndr_sk <> -2  or fct.schd_dim_clndr_sk <>-2 then eqvlnt_unts else 0 end),0) as equiv_aired_dollars
,chnl_ctg_nm, dy_prt_nm, inv_typ_cd, eqvlnt_unts, grss_bkd_amt, dl_nbr, dl_ver_nbr, dl_ln_nbr, dl_unt_id, vrsn_sts, dl_sts_chng_nbr, dl_sts, aird_dim_clndr_sk, schd_dim_clndr_sk, dl_typ, week.clndr_dt week_nm,
row_number() over(partition by dl_nbr, dl_unt_id,dl_ln_nbr order by dl_ver_nbr desc, dl_sts_chng_nbr desc, dm_inv_fct_sk desc) rn  -- this ranking is done to ensure current relevent row is picked (latest row will have rnk = 1)
from adsls_dm.dm_inv_fct fct -- dm_spot_data
INNER JOIN dim.DL_UNT_DIM dim ON fct.DL_UNT_DIM_SK = dim.dl_unt_dim_sk AND dim.del_ind IS FALSE  -- dm_spot_data
LEFT JOIN dim.sllg_ttl_rate_crd_dim st ON fct.SLLG_TTL_RATE_CRD_DIM_SK = st.SLLG_TTL_RATE_CRD_DIM_SK  -- dm_ratecard_data
LEFT OUTER JOIN dim.clndr_dim Week ON fct.wk_strt_dt_sk = week.CLNDR_DIM_SK -- Week
where fct.wk_strt_dt_sk >= to_char(date_add('day',-35,NEXT_DAY(current_date,'M')), 'YYYYMMDD')
and fct.wk_strt_dt_sk <= to_char(date_add('day',-7,NEXT_DAY(current_date,'M')),'YYYYMMDD')
and dim.dl_unt_wk_dt <= concat(to_char(date_add('day',-7,NEXT_DAY(current_date,'M')), 'YYYY-MM-DD'), ' 00:00:00')
and dim.dl_unt_wk_dt  >= concat(to_char(date_add('day',-35,NEXT_DAY(current_date,'M')), 'YYYY-MM-DD'), ' 00:00:00')
and vrsn_sts = dl_sts  -- ensures latest status is picked
AND (clst_clfn LIKE 'STANDALONE UNIT%%' OR clst_clfn LIKE 'MEMBER LINE%%') -- ensures no duplication when units are cluster units
AND DL_STS IN ('PO','DR') -- relevant status you have in netezza
AND inv_typ_cd in ('CM','DR','IT','TT','RT','BN','AGG','NCC')
)x 
WHERE rn=1 
group by 1,2,3,4) aired_metrics

on base.clndr_dt = aired_metrics.week_nm and base.ntwrk_cd = aired_metrics.ntwrk_cd_aur and base.dy_prt_nm = aired_metrics.dy_prt_nm_aur and base.inv_typ_cd = aired_metrics.inv_typ_cd_aur

left join

(----AVAILS DATA
select c.ntwrk_cd as ntwrk_cd_orig,
case 
when c.selling_title like '%%Swim%%' and c.ntwrk_cd like 'TOON' then 'ADSM'
when c.selling_title like '%%Nite%%' and  c.ntwrk_cd like 'NICK' then 'NAN'
else c.ntwrk_cd 
end as ntwrk_cd_av
, daypart, inventory_type_code, week_start_date
, equiv_capacity-nvl(sold_units,0) avails
from((select category_name, outlet, selling_title, daypart, inventory_type_code, week_start_date, sum(equiv_capacity) as equiv_capacity
from adsls_dm.DM_CAPACITY_DATA cd
where outlet not in ('Univision E','Univision W','Unimas'' E','Unimas'' W','Al Jazeera')
and category_name not in ('Non Air Time', 'Originals', 'Specials', 'Live Sports')
and inventory_type_code not in ('NAIR')
and selling_title not like '%%Dummy%%'
and date(week_start_date) >= date_add('day',-35,NEXT_DAY(current_date,'M')) and date(week_start_date) <= date_add('day',-7,NEXT_DAY(current_date,'M'))
group by 1,2,3,4,5,6)a
left join
(select distinct ntwrk_nm, ntwrk_cd from dim.sllg_ttl_rate_crd_dim)b
on a.outlet = b.ntwrk_nm)c

left join

(---Sold Units
select ntwrk_nm
, ntwrk_cd as ntwrk_cd_orig,
case 
when sllg_ttl_nm like '%%Swim%%' and ntwrk_cd like 'TOON' then 'ADSM'
when sllg_ttl_nm like '%%Nite%%' and  ntwrk_cd like 'NICK' then 'NAN'
else ntwrk_cd 
end as ntwrk_cd
, dy_prt_nm as dy_prt_nm
, case 
when inv_typ_cd = 'AGG' then 'AGG'
when inv_typ_cd = 'NCC' then 'NCC'
else 'CM'
end inv_typ_cd
, wk_nm
,sum(EQVLNT_UNTS) sold_units

from 
(
SELECT ntwrk_nm, ntwrk_cd, sllg_ttl_nm, dy_prt_nm, inv_typ_cd, eqvlnt_unts, dl_nbr, dl_ver_nbr, dl_ln_nbr, dl_unt_id, vrsn_sts, dl_sts_chng_nbr, dl_sts, week.brdcst_wk_bgn_dt as wk_nm, 
row_number() over(partition by dl_nbr, dl_unt_id,dl_ln_nbr order by dl_ver_nbr desc, dl_sts_chng_nbr desc, dm_inv_fct_sk desc) rn  -- this ranking is done to ensure current relevent row is picked (latest row will have rnk = 1)
from adsls_dm.dm_inv_fct fct -- dm_spot_data
INNER JOIN dim.DL_UNT_DIM dim ON fct.DL_UNT_DIM_SK = dim.dl_unt_dim_sk AND dim.del_ind IS FALSE  -- dm_spot_data
LEFT JOIN dim.sllg_ttl_rate_crd_dim st ON fct.SLLG_TTL_RATE_CRD_DIM_SK = st.SLLG_TTL_RATE_CRD_DIM_SK  -- dm_ratecard_data
LEFT OUTER JOIN dim.clndr_dim Week ON fct.wk_strt_dt_sk = week.CLNDR_DIM_SK -- Week
where fct.wk_strt_dt_sk >= to_char(date_add('day',-35,NEXT_DAY(current_date,'M')), 'YYYYMMDD')
and fct.wk_strt_dt_sk <= to_char(date_add('day',-7,NEXT_DAY(current_date,'M')),'YYYYMMDD')
and dim.dl_unt_wk_dt >= concat(to_char(date_add('day',-35,NEXT_DAY(current_date,'M')), 'YYYY-MM-DD'), ' 00:00:00')
and dim.dl_unt_wk_dt <= concat(to_char(date_add('day',-7,NEXT_DAY(current_date,'M')),'YYYY-MM-DD'), ' 00:00:00')
and vrsn_sts = dl_sts  -- ensures latest status is picked
AND (clst_clfn LIKE 'STANDALONE UNIT%%' OR clst_clfn LIKE 'MEMBER LINE%%') -- ensures no duplication when units are cluster units
AND DL_STS IN ('H','O') -- relevant status you have in netezza
)x 
WHERE rn=1 
group by 1,2,3,4,5,6)d

on c.ntwrk_cd = d.ntwrk_cd
and
c.daypart = d.dy_prt_nm
and
c.inventory_type_code = d.inv_typ_cd
and
c.week_start_date = d.wk_nm)av

on base.clndr_dt = av.week_start_date and base.ntwrk_cd = av.ntwrk_cd_av and base.dy_prt_nm = av.daypart and base.inv_typ_cd = av.inventory_type_code
""")

# NEEDS COLUMN NAMES ADDED
spot_data = pd.DataFrame.from_records(training_query_results,columns =['clndr_dt', 'dl_unt_id', 'dl_typ', 'grss_bkd_amt', 'dy_prt_nm', 'inv_lnth_in_sec', 'ntwrk_cd', 'inv_typ_cd', 'aird_ind', 'median_aired_rate', 'mean_aired_rate', 'units_cleared', 'avails'])

# Query to pull production data for predictions
prod_query_results = rs.query("""
select base.*, coalesce(aired_metrics.median_aired_rate,0) median_aired_rate, coalesce(aired_metrics.mean_aired_rate,0) mean_aired_rate, coalesce(aired_metrics.units_cleared,0) units_cleared
, coalesce(av.avails,0) avails
, cast(to_char(current_date, 'YYYYMMDD') as bigint) rundate
from
(select clndr_dt, dl_nbr, dl_unt_id, dl_typ, grss_bkd_amt, dy_prt_nm, inv_lnth_in_sec 
, case when sllg_ttl_nm like '%%Swim%%' and ntwrk_cd like 'TOON' then 'ADSM'
	when sllg_ttl_nm like '%%Nite%%' and  ntwrk_cd like 'NICK' then 'NAN'
	else ntwrk_cd 
	end as ntwrk_cd
, case when inv_typ_cd = 'AGG' then 'AGG'
	when inv_typ_cd = 'NCC' then 'NCC'
	else 'CM'
	end inv_typ_cd 
, case
when airdate <> -2 then True
else False
end aird_ind
from 
(
SELECT dl_nbr, dl_ver_nbr, dl_ln_nbr, dl_unt_id, vrsn_sts, dl_sts_chng_nbr, dl_sts, Adt.Clndr_dt airdate, grss_bkd_amt, ntwrk_cd, dy_prt_nm, dl_typ, inv_lnth_in_sec, inv_typ_cd, week.clndr_dt, sllg_ttl_nm,
row_number() over(partition by dl_nbr, dl_unt_id,dl_ln_nbr order by dl_ver_nbr desc, dl_sts_chng_nbr desc, dm_inv_fct_sk desc) rn  -- this ranking is done to ensure current relevent row is picked (latest row will have rnk = 1)
from adsls_dm.dm_inv_fct fct -- dm_spot_data
INNER JOIN dim.DL_UNT_DIM dim ON fct.DL_UNT_DIM_SK = dim.dl_unt_dim_sk AND dim.del_ind IS FALSE  -- dm_spot_data
LEFT JOIN dim.sllg_ttl_rate_crd_dim st ON fct.SLLG_TTL_RATE_CRD_DIM_SK = st.SLLG_TTL_RATE_CRD_DIM_SK  -- dm_ratecard_data
LEFT JOIN dim.clndr_dim Adt ON fct.AIRD_DIM_CLNDR_SK = Adt.CLNDR_DIM_SK -- Aired Date
LEFT JOIN dim.ADVTSR_DIM advt ON fct.ADVTSR_DIM_SK = advt.ADVTSR_DIM_SK -- Advertiser
LEFT JOIN dim.agcy_loc_dim loc_dim ON fct.AGCY_LOC_DIM_SK = loc_dim.AGCY_LOC_DIM_SK -- Agency Location data
LEFT JOIN dim.EMP_DIM emp ON fct.ACCNT_EXCTVE_DIM_SK = emp.emp_dim_sk -- AE information
LEFT OUTER JOIN dim.clndr_dim Week ON fct.wk_strt_dt_sk = week.CLNDR_DIM_SK -- Week
where fct.wk_strt_dt_sk >= to_char(date_add('day',-7,NEXT_DAY(current_date,'M')), 'YYYYMMDD')
and dim.dl_unt_wk_dt  >= concat(to_char(date_add('day',-7,NEXT_DAY(current_date,'M')), 'YYYY-MM-DD'), ' 00:00:00')
and vrsn_sts = dl_sts  -- ensures latest status is picked
AND (clst_clfn LIKE 'STANDALONE UNIT%%' OR clst_clfn LIKE 'MEMBER LINE%%') -- ensures no duplication when units are cluster units
AND DL_STS IN ('O','PO', 'H', 'DR', 'PML') -- relevant status you have in netezza
AND dl_typ in ('Direct Response','Latino DR','Programmatic Linear')
and chnl_ctg_nm <> 'Non Air Time'
)x WHERE rn=1) base
 
left join
 
-- Aired and scheduled metrics
(select ntwrk_cd ntwrk_cd_aur, dy_prt_nm dy_prt_nm_aur
, case when inv_typ_cd = 'AGG' then 'AGG'
	when inv_typ_cd = 'NCC' then 'NCC'
	else 'CM'
	end inv_typ_cd_aur
, coalesce(median(equiv_aired_dollars),0) median_aired_rate
, coalesce(avg(equiv_aired_dollars),0) mean_aired_rate
, coalesce(sum(aired_eqvlnt_unts),0) units_cleared
from (
SELECT ntwrk_cd as ntwrk_cd_orig 
, case when sllg_ttl_nm like '%%Swim%%' and ntwrk_cd like 'TOON' then 'ADSM'
	when sllg_ttl_nm like '%%Nite%%' and  ntwrk_cd like 'NICK' then 'NAN'
	else ntwrk_cd 
	end as ntwrk_cd
, case when fct.aird_dim_clndr_sk <> -2 or fct.schd_dim_clndr_sk <>-2 then grss_bkd_amt
	else 0 
	end as aired_bkd_amt
, case when fct.aird_dim_clndr_sk <> -2 or fct.schd_dim_clndr_sk <>-2  then eqvlnt_unts 
	else 0 
	end as aired_eqvlnt_unts
, (case when fct.aird_dim_clndr_sk <> -2  or fct.schd_dim_clndr_sk <>-2  then grss_bkd_amt else 0 end) / nullif((case when fct.aird_dim_clndr_sk <> -2  or fct.schd_dim_clndr_sk <>-2 then eqvlnt_unts else 0 end),0) as equiv_aired_dollars
,chnl_ctg_nm, dy_prt_nm, inv_typ_cd, eqvlnt_unts, grss_bkd_amt, dl_nbr, dl_ver_nbr, dl_ln_nbr, dl_unt_id, vrsn_sts, dl_sts_chng_nbr, dl_sts, aird_dim_clndr_sk, schd_dim_clndr_sk, dl_typ, week.clndr_dt week_nm,
row_number() over(partition by dl_nbr, dl_unt_id,dl_ln_nbr order by dl_ver_nbr desc, dl_sts_chng_nbr desc, dm_inv_fct_sk desc) rn  -- this ranking is done to ensure current relevent row is picked (latest row will have rnk = 1)
from adsls_dm.dm_inv_fct fct -- dm_spot_data
INNER JOIN dim.DL_UNT_DIM dim ON fct.DL_UNT_DIM_SK = dim.dl_unt_dim_sk AND dim.del_ind IS FALSE  -- dm_spot_data
LEFT JOIN dim.sllg_ttl_rate_crd_dim st ON fct.SLLG_TTL_RATE_CRD_DIM_SK = st.SLLG_TTL_RATE_CRD_DIM_SK  -- dm_ratecard_data
LEFT OUTER JOIN dim.clndr_dim Week ON fct.wk_strt_dt_sk = week.CLNDR_DIM_SK -- Week
where fct.wk_strt_dt_sk >= to_char(date_add('day',-35,NEXT_DAY(current_date,'M')), 'YYYYMMDD')
and fct.wk_strt_dt_sk <= to_char(date_add('day',-7,NEXT_DAY(current_date,'M')),'YYYYMMDD')
and dim.dl_unt_wk_dt <= concat(to_char(date_add('day',-7,NEXT_DAY(current_date,'M')), 'YYYY-MM-DD'), ' 00:00:00')
and dim.dl_unt_wk_dt  >= concat(to_char(date_add('day',-35,NEXT_DAY(current_date,'M')), 'YYYY-MM-DD'), ' 00:00:00')
and vrsn_sts = dl_sts  -- ensures latest status is picked
AND (clst_clfn LIKE 'STANDALONE UNIT%%' OR clst_clfn LIKE 'MEMBER LINE%%') -- ensures no duplication when units are cluster units
AND DL_STS IN ('PO','DR') -- relevant status you have in netezza
AND inv_typ_cd in ('CM','DR','IT','TT','RT','BN','AGG','NCC')
)x 
WHERE rn=1 
group by 1,2,3) aired_metrics

on base.ntwrk_cd = aired_metrics.ntwrk_cd_aur and base.dy_prt_nm = aired_metrics.dy_prt_nm_aur and base.inv_typ_cd = aired_metrics.inv_typ_cd_aur

left join

(----AVAILS DATA
select c.ntwrk_cd as ntwrk_cd_orig,
case 
when c.selling_title like '%%Swim%%' and c.ntwrk_cd like 'TOON' then 'ADSM'
when c.selling_title like '%%Nite%%' and  c.ntwrk_cd like 'NICK' then 'NAN'
else c.ntwrk_cd 
end as ntwrk_cd_av
, daypart, inventory_type_code, week_start_date
, equiv_capacity-nvl(sold_units,0) avails
from((select category_name, outlet, selling_title, daypart, inventory_type_code, week_start_date, sum(equiv_capacity) as equiv_capacity
from adsls_dm.DM_CAPACITY_DATA cd
where outlet not in ('Univision E','Univision W','Unimas'' E','Unimas'' W','Al Jazeera')
and category_name not in ('Non Air Time', 'Originals', 'Specials', 'Live Sports')
and inventory_type_code not in ('NAIR')
and selling_title not like '%%Dummy%%'
and date(week_start_date) >= date_add('day',-35,NEXT_DAY(current_date,'M')) and date(week_start_date) <= date_add('day',-7,NEXT_DAY(current_date,'M'))
group by 1,2,3,4,5,6)a
left join
(select distinct ntwrk_nm, ntwrk_cd from dim.sllg_ttl_rate_crd_dim)b
on a.outlet = b.ntwrk_nm)c

left join

(---Sold Units
select ntwrk_nm
, ntwrk_cd as ntwrk_cd_orig,
case 
when sllg_ttl_nm like '%%Swim%%' and ntwrk_cd like 'TOON' then 'ADSM'
when sllg_ttl_nm like '%%Nite%%' and  ntwrk_cd like 'NICK' then 'NAN'
else ntwrk_cd 
end as ntwrk_cd
, dy_prt_nm as dy_prt_nm
, case 
when inv_typ_cd = 'AGG' then 'AGG'
when inv_typ_cd = 'NCC' then 'NCC'
else 'CM'
end inv_typ_cd
, wk_nm
,sum(EQVLNT_UNTS) sold_units

from 
(
SELECT ntwrk_nm, ntwrk_cd, sllg_ttl_nm, dy_prt_nm, inv_typ_cd, eqvlnt_unts, dl_nbr, dl_ver_nbr, dl_ln_nbr, dl_unt_id, vrsn_sts, dl_sts_chng_nbr, dl_sts, week.brdcst_wk_bgn_dt as wk_nm, 
row_number() over(partition by dl_nbr, dl_unt_id,dl_ln_nbr order by dl_ver_nbr desc, dl_sts_chng_nbr desc, dm_inv_fct_sk desc) rn  -- this ranking is done to ensure current relevent row is picked (latest row will have rnk = 1)
from adsls_dm.dm_inv_fct fct -- dm_spot_data
INNER JOIN dim.DL_UNT_DIM dim ON fct.DL_UNT_DIM_SK = dim.dl_unt_dim_sk AND dim.del_ind IS FALSE  -- dm_spot_data
LEFT JOIN dim.sllg_ttl_rate_crd_dim st ON fct.SLLG_TTL_RATE_CRD_DIM_SK = st.SLLG_TTL_RATE_CRD_DIM_SK  -- dm_ratecard_data
LEFT OUTER JOIN dim.clndr_dim Week ON fct.wk_strt_dt_sk = week.CLNDR_DIM_SK -- Week
where fct.wk_strt_dt_sk >= to_char(date_add('day',-35,NEXT_DAY(current_date,'M')), 'YYYYMMDD')
and dim.dl_unt_wk_dt >= concat(to_char(date_add('day',-35,NEXT_DAY(current_date,'M')), 'YYYY-MM-DD'), ' 00:00:00')
and vrsn_sts = dl_sts  -- ensures latest status is picked
AND (clst_clfn LIKE 'STANDALONE UNIT%%' OR clst_clfn LIKE 'MEMBER LINE%%') -- ensures no duplication when units are cluster units
AND DL_STS IN ('H','O') -- relevant status you have in netezza
)x 
WHERE rn=1 
group by 1,2,3,4,5,6)d

on c.ntwrk_cd = d.ntwrk_cd
and
c.daypart = d.dy_prt_nm
and
c.inventory_type_code = d.inv_typ_cd
and
c.week_start_date = d.wk_nm)av

on base.clndr_dt = av.week_start_date and base.ntwrk_cd = av.ntwrk_cd_av and base.dy_prt_nm = av.daypart and base.inv_typ_cd = av.inventory_type_code
""")

# Create data frame for model ouput
prod_output_data = pd.DataFrame.from_records(prod_query_results,columns =['clndr_dt', 'dl_nbr', 'dl_unt_id', 'dl_typ', 'grss_bkd_amt', 'dy_prt_nm', 'inv_lnth_in_sec', 'ntwrk_cd', 'inv_typ_cd', 'aird_ind', 'median_aired_rate', 'mean_aired_rate', 'units_cleared', 'avails','rundate'])

# Make a copy of the prod data to be used in the model 
prod_model_data = pd.DataFrame.from_records(prod_query_results,columns =['clndr_dt', 'dl_nbr', 'dl_unt_id', 'dl_typ', 'grss_bkd_amt', 'dy_prt_nm', 'inv_lnth_in_sec', 'ntwrk_cd', 'inv_typ_cd', 'aird_ind', 'median_aired_rate', 'mean_aired_rate', 'units_cleared', 'avails','rundate'])

rs.cursor.close()

# Clean data for non-numeric columns
class_vars = ['dl_typ','ntwrk_cd','dy_prt_nm','inv_typ_cd','aird_ind']

for c in class_vars: 
    x=spot_data[c].value_counts()
    item_type_mapping={}
    item_list=x.index
    for i in range(0,len(item_list)):
        item_type_mapping[item_list[i]]=i
    spot_data[c]=spot_data[c].map(lambda x:item_type_mapping[x]) 
        
for c in class_vars: 
    x=prod_model_data[c].value_counts()
    item_type_mapping={}
    item_list=x.index
    for i in range(0,len(item_list)):
        item_type_mapping[item_list[i]]=i
    prod_model_data[c]=prod_model_data[c].map(lambda x:item_type_mapping[x]) 
    
# Create features and target variables
feature_cols = ['dl_typ', 'grss_bkd_amt', 'ntwrk_cd', 'dy_prt_nm', 'inv_lnth_in_sec', 'inv_typ_cd','avails','units_cleared','mean_aired_rate','median_aired_rate']
X = spot_data[feature_cols] # Features
X_prod = prod_model_data[feature_cols] # Prod Features
y = spot_data.aird_ind # Target variable

# Partition Data into traing and validation sets
X_train,X_test,y_train,y_test=train_test_split(X,y,test_size=0.25,random_state=0)

# Create Decision Tree classifer object
clf = DecisionTreeClassifier()

# Train Decision Tree Classifer
clf = clf.fit(X_train,y_train)

# Predict the response for test dataset
y_pred = clf.predict(X_test)

# Measure Model Accuarcy
# Need to add logic to write the accuracy dataset 
accuracy = metrics.accuracy_score(y_test, y_pred)
precision = metrics.precision_score(y_test, y_pred)
recall = metrics.recall_score(y_test, y_pred)
y_pred_proba = clf.predict_proba(X_test)[::,1]
fpr, tpr, _ = metrics.roc_curve(y_test,  y_pred_proba)
auc = metrics.roc_auc_score(y_test, y_pred_proba)
cnf_matrix = metrics.confusion_matrix(y_test, y_pred)
tn = cnf_matrix[0, 0]
fn = cnf_matrix[0, 1]
fp = cnf_matrix[1, 0]
tp = cnf_matrix[1, 1]


roc_curve_df = pd.DataFrame({'fpr':fpr, 'tpr':tpr})
roc_curve_df['updated'] = today_str
measure_vals = {"accuracy": [accuracy], "precision": [precision], "recall": [recall], "auc": [auc], "tn": [tn], "fn": [fn], "fp": [fp], "tp": [tp], "update": [today_str] }
measure_vals_df = pd.DataFrame.from_dict(measure_vals, orient='columns')

S3O.to_csv_on_s3(dataframe=roc_curve_df, bucket=s3bucket, key=roc_prefix+'roc_curve_'+today_str+'.csv', d = ",")
S3O.to_csv_on_s3(dataframe=measure_vals_df, bucket=s3bucket, key=metrics_prefix+'model_performance_'+today_str+'.csv', d = ",")

print("Model Performance:")
print("Accuracy:",accuracy)
print("Precision:",precision)
print("Recall:",recall)
print("AUC:",auc)

# Predict clearance on prod data
prod_pred = clf.predict(X_prod)

# Merge predictions with original data set
prod_output_data['aird_pred'] = prod_pred

# Remove unneeded columns from output data set
drop_cols = [9,10,11]
prod_output_data.drop(prod_output_data.columns[drop_cols],axis=1,inplace=True)

# Write the results to s3
S3O.to_parquet_on_s3(dataframe=prod_output_data, bucket=s3bucket, key=output_prefix+'clearance_predictions_'+today_str+'.parquet')
