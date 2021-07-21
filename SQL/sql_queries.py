def trial_period_sql():
    query="""
    with AA_LTV_sub as  
 (
 select * from 
 (
Select A1.*,
activation_dt as start_dt,
date_trunc( activation_dt, Month) as Signup_month,
cdm_udf.LTV_SIGNUP_PLAN_CALC(signup_plan_cd) as signup_plan,
case when subscription_platform_cd in ('RECURLY','PSP') then 'RECURLY' else subscription_platform_cd end as subscription_platform,

--- clean version of trial period offered
cdm_udf.LTV_TRIAL_PERIOD_CALC(signup_plan_cd, signup_trial_period_desc, sku_cd, trial_start_dt, paid_start_dt) as Trial_Period
     
 from `i-dss-cdm-data-dev.ent_vw.subscription_fct` A1
where A1.src_system_id=@src_system_id 
and subscription_platform_cd not in ('Apple iOS', 'Apple TV')    
and activation_dt<@till_date
and lower(signup_plan_cd) not like '%annual%' 
)  
where Trial_Period is not null
)


, LTV_stg2 as (
select ov.Signup_month,
ov.Trial_Period,
ov.signup_plan,
case when total_starts is null then 0 else total_starts end as total_starts,
Subs_Retained_1 ,
case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 77  MONTH)) then Subs_Retained_2 else 0 end as Subs_Retained_2,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 76  MONTH)) then Subs_Retained_3 else 0 end as Subs_Retained_3,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 75  MONTH)) then Subs_Retained_4 else 0 end as Subs_Retained_4,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 74  MONTH)) then Subs_Retained_5 else 0 end as Subs_Retained_5,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 73  MONTH)) then Subs_Retained_6 else 0 end as Subs_Retained_6,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 72  MONTH)) then Subs_Retained_7 else 0 end as Subs_Retained_7,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 71  MONTH)) then Subs_Retained_8 else 0 end as Subs_Retained_8,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 70  MONTH)) then Subs_Retained_9 else 0 end as Subs_Retained_9,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 69  MONTH)) then Subs_Retained_10 else 0 end as Subs_Retained_10,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 68  MONTH)) then Subs_Retained_11 else 0 end as Subs_Retained_11,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 67  MONTH)) then Subs_Retained_12 else 0 end as Subs_Retained_12,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 66  MONTH)) then Subs_Retained_13 else 0 end as Subs_Retained_13,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 65  MONTH)) then Subs_Retained_14 else 0 end as Subs_Retained_14,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 64  MONTH)) then Subs_Retained_15 else 0 end as Subs_Retained_15,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 63  MONTH)) then Subs_Retained_16 else 0 end as Subs_Retained_16,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 62  MONTH)) then Subs_Retained_17 else 0 end as Subs_Retained_17,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 61  MONTH)) then Subs_Retained_18 else 0 end as Subs_Retained_18,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 60  MONTH)) then Subs_Retained_19 else 0 end as Subs_Retained_19,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 59  MONTH)) then Subs_Retained_20 else 0 end as Subs_Retained_20,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 58  MONTH)) then Subs_Retained_21 else 0 end as Subs_Retained_21,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 57  MONTH)) then Subs_Retained_22 else 0 end as Subs_Retained_22,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 56  MONTH)) then Subs_Retained_23 else 0 end as Subs_Retained_23,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 55  MONTH)) then Subs_Retained_24 else 0 end as Subs_Retained_24,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 54  MONTH)) then Subs_Retained_25 else 0 end as Subs_Retained_25,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 53  MONTH)) then Subs_Retained_26 else 0 end as Subs_Retained_26,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 52  MONTH)) then Subs_Retained_27 else 0 end as Subs_Retained_27,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 51  MONTH)) then Subs_Retained_28 else 0 end as Subs_Retained_28,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 50  MONTH)) then Subs_Retained_29 else 0 end as Subs_Retained_29,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 49  MONTH)) then Subs_Retained_30 else 0 end as Subs_Retained_30,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 48  MONTH)) then Subs_Retained_31 else 0 end as Subs_Retained_31,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 47  MONTH)) then Subs_Retained_32 else 0 end as Subs_Retained_32,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 46  MONTH)) then Subs_Retained_33 else 0 end as Subs_Retained_33,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 45  MONTH)) then Subs_Retained_34 else 0 end as Subs_Retained_34,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 44  MONTH)) then Subs_Retained_35 else 0 end as Subs_Retained_35,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 43  MONTH)) then Subs_Retained_36 else 0 end as Subs_Retained_36,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 42  MONTH)) then Subs_Retained_37 else 0 end as Subs_Retained_37,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 41  MONTH)) then Subs_Retained_38 else 0 end as Subs_Retained_38,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 40  MONTH)) then Subs_Retained_39 else 0 end as Subs_Retained_39,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 39  MONTH)) then Subs_Retained_40 else 0 end as Subs_Retained_40,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 38  MONTH)) then Subs_Retained_41 else 0 end as Subs_Retained_41,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 37  MONTH)) then Subs_Retained_42 else 0 end as Subs_Retained_42,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 36  MONTH)) then Subs_Retained_43 else 0 end as Subs_Retained_43,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 35  MONTH)) then Subs_Retained_44 else 0 end as Subs_Retained_44,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 34  MONTH)) then Subs_Retained_45 else 0 end as Subs_Retained_45,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 33  MONTH)) then Subs_Retained_46 else 0 end as Subs_Retained_46,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 32  MONTH)) then Subs_Retained_47 else 0 end as Subs_Retained_47,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 31  MONTH)) then Subs_Retained_48 else 0 end as Subs_Retained_48,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 30  MONTH)) then Subs_Retained_49 else 0 end as Subs_Retained_49,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 29  MONTH)) then Subs_Retained_50 else 0 end as Subs_Retained_50,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 28  MONTH)) then Subs_Retained_51 else 0 end as Subs_Retained_51,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 27  MONTH)) then Subs_Retained_52 else 0 end as Subs_Retained_52,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 26  MONTH)) then Subs_Retained_53 else 0 end as Subs_Retained_53,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 25  MONTH)) then Subs_Retained_54 else 0 end as Subs_Retained_54,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 24  MONTH)) then Subs_Retained_55 else 0 end as Subs_Retained_55,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 23  MONTH)) then Subs_Retained_56 else 0 end as Subs_Retained_56,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 22  MONTH)) then Subs_Retained_57 else 0 end as Subs_Retained_57,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 21  MONTH)) then Subs_Retained_58 else 0 end as Subs_Retained_58,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 20  MONTH)) then Subs_Retained_59 else 0 end as Subs_Retained_59,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 19  MONTH)) then Subs_Retained_60 else 0 end as Subs_Retained_60,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 18  MONTH)) then Subs_Retained_61 else 0 end as Subs_Retained_61,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 17  MONTH)) then Subs_Retained_62 else 0 end as Subs_Retained_62,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 16  MONTH)) then Subs_Retained_63 else 0 end as Subs_Retained_63,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 15  MONTH)) then Subs_Retained_64 else 0 end as Subs_Retained_64,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 14  MONTH)) then Subs_Retained_65 else 0 end as Subs_Retained_65,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 13  MONTH)) then Subs_Retained_66 else 0 end as Subs_Retained_66,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 12  MONTH)) then Subs_Retained_67 else 0 end as Subs_Retained_67,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 11  MONTH)) then Subs_Retained_68 else 0 end as Subs_Retained_68,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 10  MONTH)) then Subs_Retained_69 else 0 end as Subs_Retained_69,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 9  MONTH)) then Subs_Retained_70 else 0 end as Subs_Retained_70,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 8  MONTH)) then Subs_Retained_71 else 0 end as Subs_Retained_71,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 7  MONTH)) then Subs_Retained_72 else 0 end as Subs_Retained_72,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 6  MONTH)) then Subs_Retained_73 else 0 end as Subs_Retained_73,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 5  MONTH)) then Subs_Retained_74 else 0 end as Subs_Retained_74,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 4  MONTH)) then Subs_Retained_75 else 0 end as Subs_Retained_75,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 3  MONTH)) then Subs_Retained_76 else 0 end as Subs_Retained_76,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL  2   MONTH)) then Subs_Retained_77 else 0 end as Subs_Retained_77,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL  1   MONTH)) then Subs_Retained_78 else 0 end as Subs_Retained_78,
    case when ov.Signup_month<@start_date then Subs_Retained_79 else 0 end as Subs_Retained_79

from 
(

select * from
 
 (
 
 Select t.*,running_sum as Cum_cancels ,
 begin_paid_subs as starting_paid_subs
 from 

  (
 select Signup_month,
Trial_Period,
signup_plan,
 subsequent_month
 from
       (
       select distinct t.Signup_month,signup_plan,Trial_Period from (select r.* from AA_LTV_sub r)  dt 
       cross join
	   (select DATE_TRUNC(day_dt,MONTH) as Signup_month from `i-dss-cdm-data-dev.dw_vw.days` where day_dt between @start_date and @end_date) t -- Change the date range here 
 
      )
      cross join 
      (
      select s.* from 
      (select row_number() over (order by day_dt) subsequent_month from `i-dss-cdm-data-dev.dw_vw.days`) s where subsequent_month <=  (SELECT DATE_DIFF(@end_date,@start_date,MONTH)+2)) sb -- change the subsequent months
      ) t
    
      left join (
	  select Signup_Month,
    Trial_Period,
    signup_plan,
            subsequent_month as mn,
            SUM (cancels) OVER (PARTITION BY  Signup_month,signup_plan,Trial_Period ORDER BY subsequent_month) AS running_sum
            from (
             select Signup_month,
             Trial_Period,
             signup_plan,
			 case when (Days_actv/30)<=1 then 1 else CEILING(Days_actv/30) end as subsequent_month,
			 sum(users) as cancels
			 from (
             select Signup_month, 
             Trial_Period,
             signup_plan,
			 Days_actv,
			 count(distinct subscription_guid ) as users
			 from 
                               (
                               select t.*, 
                               DATE_DIFF(cast(case when expiration_dt is null OR expiration_dt>=@till_date then @till_date else expiration_dt end as date), cast( start_dt as Date),DAY) +1 as Days_actv
							   from AA_LTV_sub t
 
                               ) a1 
                               where cast(expiration_dt as date)<=@till_date
                               group by Signup_month, Trial_Period,signup_plan,
                               Days_actv) a2
            group by Signup_month,Trial_Period,signup_plan,
            case when (Days_actv/30)<=1 then 1
       else CEILING(Days_actv/30) end
       ) a3 
       ) can
       on (t.Signup_month=can.Signup_month
       and t.subsequent_month=can.mn
       and t.Trial_Period=can.Trial_Period
       and t.signup_plan=can.signup_plan)    
        left join ( 
       select Signup_month,   
       Trial_Period,signup_plan,
       count(distinct subscription_guid) as begin_paid_subs
                               from 
                               (
                               select t.*, 
                               DATE_DIFF(cast(case when expiration_dt is null OR expiration_dt>=@till_date then @till_date  else expiration_dt end as date), cast( start_dt as Date),DAY) +1 as Days_actv
							   from AA_LTV_sub t
 
                               ) a1 
                               group by Signup_month    ,Trial_Period   ,signup_plan                   
                               ) a4
                               on (t.Signup_month=a4.Signup_month
                               and t.Trial_Period=a4.Trial_Period
                               and t.signup_plan=a4.signup_plan)
       ) a5
        PIVOT(SUM(starting_paid_subs-Cum_cancels) as subs_retained FOR subsequent_month in
        (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,
        21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,
        39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,
        57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,
        75,76,77,78,79))
) ov
left join ( 
       select Signup_month,   
       Trial_Period,
       signup_plan,
       count(distinct subscription_guid ) as total_starts
                               from 
                               (
                               select t.*, 
                               DATE_DIFF(cast(case when expiration_dt is null OR expiration_dt>=@till_date then @till_date else expiration_dt end as date), cast( start_dt as Date),DAY) +1 as Days_actv
							   from AA_LTV_sub t
 
                               ) a1 
                               group by Signup_month,Trial_Period ,signup_plan                            
                               ) strt
                               on (ov.Signup_month=strt.Signup_month
                               and ov.Trial_Period=strt.Trial_Period
                               and ov.signup_plan=strt.signup_plan)
order by 2,1

)

-- select * from LTV_stg2
, LTV_stg3 as (
select Signup_month,Trial_Period,
signup_plan,
total_starts,
coalesce((lag(total_starts,1)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l1,
coalesce((lag(total_starts,2)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l2,
    coalesce((lag(total_starts,3)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l3,
    coalesce((lag(total_starts,4)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l4,
    coalesce((lag(total_starts,5)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l5,
    coalesce((lag(total_starts,6)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l6,
    coalesce((lag(total_starts,7)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l7,
    coalesce((lag(total_starts,8)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l8,
    coalesce((lag(total_starts,9)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l9,
    coalesce((lag(total_starts,10)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l10,
    coalesce((lag(total_starts,11)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l11,
    coalesce((lag(total_starts,12)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l12,
    coalesce((lag(total_starts,13)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l13,
    coalesce((lag(total_starts,14)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l14,
    coalesce((lag(total_starts,15)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l15,
    coalesce((lag(total_starts,16)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l16,
    coalesce((lag(total_starts,17)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l17,
    coalesce((lag(total_starts,18)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l18,
    coalesce((lag(total_starts,19)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l19,
    coalesce((lag(total_starts,20)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l20,
    coalesce((lag(total_starts,21)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l21,
    coalesce((lag(total_starts,22)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l22,
    coalesce((lag(total_starts,23)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l23,
    coalesce((lag(total_starts,24)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l24,
    coalesce((lag(total_starts,25)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l25,
    coalesce((lag(total_starts,26)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l26,
    coalesce((lag(total_starts,27)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l27,
    coalesce((lag(total_starts,28)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l28,
    coalesce((lag(total_starts,29)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l29,
    coalesce((lag(total_starts,30)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l30,
    coalesce((lag(total_starts,31)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l31,
    coalesce((lag(total_starts,32)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l32,
    coalesce((lag(total_starts,33)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l33,
    coalesce((lag(total_starts,34)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l34,
    coalesce((lag(total_starts,35)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l35,
    coalesce((lag(total_starts,36)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l36,
    coalesce((lag(total_starts,37)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l37,
    coalesce((lag(total_starts,38)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l38,
    coalesce((lag(total_starts,39)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l39,
    coalesce((lag(total_starts,40)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l40,
    coalesce((lag(total_starts,41)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l41,
    coalesce((lag(total_starts,42)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l42,
    coalesce((lag(total_starts,43)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l43,
    coalesce((lag(total_starts,44)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l44,
    coalesce((lag(total_starts,45)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l45,
    coalesce((lag(total_starts,46)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l46,
    coalesce((lag(total_starts,47)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l47,
    coalesce((lag(total_starts,48)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l48,
    coalesce((lag(total_starts,49)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l49,
    coalesce((lag(total_starts,50)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l50,
    coalesce((lag(total_starts,51)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l51,
    coalesce((lag(total_starts,52)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l52,
    coalesce((lag(total_starts,53)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l53,
        coalesce((lag(total_starts,54)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l54,
        coalesce((lag(total_starts,55)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l55,
        coalesce((lag(total_starts,56)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l56,
        coalesce((lag(total_starts,57)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l57,
        coalesce((lag(total_starts,58)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l58,
        coalesce((lag(total_starts,59)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l59,
        coalesce((lag(total_starts,60)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l60,
        coalesce((lag(total_starts,61)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l61,
        coalesce((lag(total_starts,62)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l62,
        coalesce((lag(total_starts,63)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l63,
        coalesce((lag(total_starts,64)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l64,
        coalesce((lag(total_starts,65)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l65,
        coalesce((lag(total_starts,66)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l66,
            coalesce((lag(total_starts,67)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l67,
            coalesce((lag(total_starts,68)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l68,
            coalesce((lag(total_starts,69)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l69,
    coalesce((lag(total_starts,70)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l70,
    coalesce((lag(total_starts,71)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l71,
    coalesce((lag(total_starts,72)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l72,
    coalesce((lag(total_starts,73)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l73,
    
        coalesce((lag(total_starts,74)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l74,
        coalesce((lag(total_starts,75)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l75,
        coalesce((lag(total_starts,76)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l76,
        coalesce((lag(total_starts,77)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l77,
        coalesce((lag(total_starts,78)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l78,
        coalesce((lag(total_starts,79)
    OVER (PARTITION BY signup_plan,Trial_Period ORDER BY Signup_month ASC)),0) AS total_starts_l79
    
     from LTV_stg2
     order by 2,1
)

select 
signup_plan,Trial_Period,
sum( Subs_Retained_1) as  Subs_Retained_1,
sum( Subs_Retained_2) as  Subs_Retained_2,
sum( Subs_Retained_3) as  Subs_Retained_3,
sum( Subs_Retained_4) as  Subs_Retained_4,
sum( Subs_Retained_5) as  Subs_Retained_5,
sum( Subs_Retained_6) as  Subs_Retained_6,
sum( Subs_Retained_7) as  Subs_Retained_7,
sum( Subs_Retained_8) as  Subs_Retained_8,
sum( Subs_Retained_9) as  Subs_Retained_9,
sum( Subs_Retained_10) as  Subs_Retained_10,
sum( Subs_Retained_11) as  Subs_Retained_11,
sum( Subs_Retained_12) as  Subs_Retained_12,

sum( Subs_Retained_13) as  Subs_Retained_13,
sum( Subs_Retained_14) as  Subs_Retained_14,
sum( Subs_Retained_15) as  Subs_Retained_15,
sum( Subs_Retained_16) as  Subs_Retained_16,
sum( Subs_Retained_17) as  Subs_Retained_17,
sum( Subs_Retained_18) as  Subs_Retained_18,
sum( Subs_Retained_19) as  Subs_Retained_19,
sum( Subs_Retained_20) as  Subs_Retained_20,
sum( Subs_Retained_21) as  Subs_Retained_21,
sum( Subs_Retained_22) as  Subs_Retained_22,
sum( Subs_Retained_23) as  Subs_Retained_23,
sum( Subs_Retained_24) as  Subs_Retained_24,
sum( Subs_Retained_25) as  Subs_Retained_25,
sum( Subs_Retained_26) as  Subs_Retained_26,
sum( Subs_Retained_27) as  Subs_Retained_27,
sum( Subs_Retained_28) as  Subs_Retained_28,
sum( Subs_Retained_29) as  Subs_Retained_29,
sum( Subs_Retained_30) as  Subs_Retained_30,
sum( Subs_Retained_31) as  Subs_Retained_31,
sum( Subs_Retained_32) as  Subs_Retained_32,
sum( Subs_Retained_33) as  Subs_Retained_33,
sum( Subs_Retained_34) as  Subs_Retained_34,
sum( Subs_Retained_35) as  Subs_Retained_35,
sum( Subs_Retained_36) as  Subs_Retained_36,
sum( Subs_Retained_37) as  Subs_Retained_37,
sum( Subs_Retained_38) as  Subs_Retained_38,
sum( Subs_Retained_39) as  Subs_Retained_39,
sum( Subs_Retained_40) as  Subs_Retained_40,
sum( Subs_Retained_41) as  Subs_Retained_41,
sum( Subs_Retained_42) as  Subs_Retained_42,
sum( Subs_Retained_43) as  Subs_Retained_43,
sum( Subs_Retained_44) as  Subs_Retained_44,
sum( Subs_Retained_45) as  Subs_Retained_45,
sum( Subs_Retained_46) as  Subs_Retained_46,
sum( Subs_Retained_47) as  Subs_Retained_47,
sum( Subs_Retained_48) as  Subs_Retained_48,
sum( Subs_Retained_49) as  Subs_Retained_49,
sum( Subs_Retained_50) as  Subs_Retained_50,
sum( Subs_Retained_51) as  Subs_Retained_51,
sum( Subs_Retained_52) as  Subs_Retained_52,
sum( Subs_Retained_53) as  Subs_Retained_53,
sum( Subs_Retained_54) as  Subs_Retained_54,
sum( Subs_Retained_55) as  Subs_Retained_55,
sum( Subs_Retained_56) as  Subs_Retained_56,
sum( Subs_Retained_57) as  Subs_Retained_57,
sum( Subs_Retained_58) as  Subs_Retained_58,
sum( Subs_Retained_59) as  Subs_Retained_59,
sum( Subs_Retained_60) as  Subs_Retained_60,
sum( Subs_Retained_61) as  Subs_Retained_61,
sum( Subs_Retained_62) as  Subs_Retained_62,
sum( Subs_Retained_63) as  Subs_Retained_63,
sum( Subs_Retained_64) as  Subs_Retained_64,
sum( Subs_Retained_65) as  Subs_Retained_65,
sum( Subs_Retained_66) as  Subs_Retained_66,
sum( Subs_Retained_67) as  Subs_Retained_67,
sum( Subs_Retained_68) as  Subs_Retained_68,
sum( Subs_Retained_69) as  Subs_Retained_69,
sum( Subs_Retained_70) as  Subs_Retained_70,
sum( Subs_Retained_71) as  Subs_Retained_71,
sum( Subs_Retained_72) as  Subs_Retained_72,
sum( Subs_Retained_73) as  Subs_Retained_73,
sum( Subs_Retained_74) as  Subs_Retained_74,
sum( Subs_Retained_75) as  Subs_Retained_75,
sum( Subs_Retained_76) as  Subs_Retained_76,
sum( Subs_Retained_77) as  Subs_Retained_77,
sum( Subs_Retained_78) as  Subs_Retained_78,
sum( Subs_Retained_79) as  Subs_Retained_79
from LTV_stg2
group by 1,2
UNION ALL

select 
signup_plan,Trial_Period,
sum( total_starts) as  total_starts,
sum( total_starts_l1) as  total_starts_l1,
sum( total_starts_l2) as  total_starts_l2,
sum( total_starts_l3) as  total_starts_l3,
sum( total_starts_l4) as  total_starts_l4,
sum( total_starts_l5) as  total_starts_l5,
sum( total_starts_l6) as  total_starts_l6,
sum( total_starts_l7) as  total_starts_l7,
sum( total_starts_l8) as  total_starts_l8,
sum( total_starts_l9) as  total_starts_l9,
sum( total_starts_l10) as  total_starts_l10,
sum( total_starts_l11) as  total_starts_l11,

sum( total_starts_l12) as  total_starts_l12,
sum( total_starts_l13) as  total_starts_l13,
sum( total_starts_l14) as  total_starts_l14,
sum( total_starts_l15) as  total_starts_l15,
sum( total_starts_l16) as  total_starts_l16,
sum( total_starts_l17) as  total_starts_l17,
sum( total_starts_l18) as  total_starts_l18,
sum( total_starts_l19) as  total_starts_l19,
sum( total_starts_l20) as  total_starts_l20,
sum( total_starts_l21) as  total_starts_l21,
sum( total_starts_l22) as  total_starts_l22,
sum( total_starts_l23) as  total_starts_l23,
sum( total_starts_l24) as  total_starts_l24,
sum( total_starts_l25) as  total_starts_l25,
sum( total_starts_l26) as  total_starts_l26,
sum( total_starts_l27) as  total_starts_l27,
sum( total_starts_l28) as  total_starts_l28,
sum( total_starts_l29) as  total_starts_l29,
sum( total_starts_l30) as  total_starts_l30,
sum( total_starts_l31) as  total_starts_l31,
sum( total_starts_l32) as  total_starts_l32,
sum( total_starts_l33) as  total_starts_l33,
sum( total_starts_l34) as  total_starts_l34,
sum( total_starts_l35) as  total_starts_l35,
sum( total_starts_l36) as  total_starts_l36,
sum( total_starts_l37) as  total_starts_l37,
sum( total_starts_l38) as  total_starts_l38,
sum( total_starts_l39) as  total_starts_l39,
sum( total_starts_l40) as  total_starts_l40,
sum( total_starts_l41) as  total_starts_l41,
sum( total_starts_l42) as  total_starts_l42,
sum( total_starts_l43) as  total_starts_l43,
sum( total_starts_l44) as  total_starts_l44,
sum( total_starts_l45) as  total_starts_l45,
sum( total_starts_l46) as  total_starts_l46,
sum( total_starts_l47) as  total_starts_l47,
sum( total_starts_l48) as  total_starts_l48,
sum( total_starts_l49) as  total_starts_l49,
sum( total_starts_l50) as  total_starts_l50,
sum( total_starts_l51) as  total_starts_l51,
sum( total_starts_l52) as  total_starts_l52,
sum( total_starts_l53) as  total_starts_l53,
sum( total_starts_l54) as  total_starts_l54,
sum( total_starts_l55) as  total_starts_l55,
sum( total_starts_l56) as  total_starts_l56,
sum( total_starts_l57) as  total_starts_l57,
sum( total_starts_l58) as  total_starts_l58,
sum( total_starts_l59) as  total_starts_l59,
sum( total_starts_l60) as  total_starts_l60,
sum( total_starts_l61) as  total_starts_l61,
sum( total_starts_l62) as  total_starts_l62,
sum( total_starts_l63) as  total_starts_l63,
sum( total_starts_l64) as  total_starts_l64,
sum( total_starts_l65) as  total_starts_l65,
sum( total_starts_l66) as  total_starts_l66,
sum( total_starts_l67) as  total_starts_l67,
sum( total_starts_l68) as  total_starts_l68,
sum( total_starts_l69) as  total_starts_l69,
sum( total_starts_l70) as  total_starts_l70,
sum( total_starts_l71) as  total_starts_l71,
sum( total_starts_l72) as  total_starts_l72,
sum( total_starts_l73) as  total_starts_l73,
sum( total_starts_l74) as  total_starts_l74,
sum( total_starts_l75) as  total_starts_l75,
sum( total_starts_l76) as  total_starts_l76,
sum( total_starts_l77) as  total_starts_l77,
sum( total_starts_l78) as  total_starts_l78


from LTV_stg3
group by 1,2
order by 1,2,3
"""
    return query

def signup_mnth_with_free_trial():
    query=""" with AA_LTV_sub as  (
 select * from 
 (
Select A1.*,
activation_dt as start_dt,
EXTRACT(Month FROM activation_dt) AS Month_Start,
date_trunc( activation_dt, Month) as Signup_month,
cdm_udf.LTV_SIGNUP_PLAN_CALC(signup_plan_cd) as signup_plan,
case when subscription_platform_cd in ('RECURLY','PSP') then 'RECURLY' else subscription_platform_cd end as subscription_platform,
--- clean version of trial period offered
cdm_udf.LTV_TRIAL_PERIOD_CALC(signup_plan_cd, signup_trial_period_desc, sku_cd, trial_start_dt, paid_start_dt) as Trial_Period     
from   `i-dss-cdm-data-dev.ent_vw.subscription_fct` A1
where A1.src_system_id=@src_system_id 
and subscription_platform_cd not in ('Apple iOS', 'Apple TV')    
and activation_dt<@till_date
-- and lower(signup_plan_cd) not like '%annual%' 
)  
where Trial_Period is not null and Trial_Period  in ('1 Month Free','2 Month Free','3 Month Free')
)

, LTV_stg2 as (
select ov.Signup_month,
ov.Month_Start,
case when total_starts is null then 0 else total_starts end as total_starts,
Subs_Retained_1 ,
case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 77  MONTH)) then Subs_Retained_2 else 0 end as Subs_Retained_2,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 76  MONTH)) then Subs_Retained_3 else 0 end as Subs_Retained_3,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 75  MONTH)) then Subs_Retained_4 else 0 end as Subs_Retained_4,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 74  MONTH)) then Subs_Retained_5 else 0 end as Subs_Retained_5,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 73  MONTH)) then Subs_Retained_6 else 0 end as Subs_Retained_6,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 72  MONTH)) then Subs_Retained_7 else 0 end as Subs_Retained_7,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 71  MONTH)) then Subs_Retained_8 else 0 end as Subs_Retained_8,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 70  MONTH)) then Subs_Retained_9 else 0 end as Subs_Retained_9,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 69  MONTH)) then Subs_Retained_10 else 0 end as Subs_Retained_10,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 68  MONTH)) then Subs_Retained_11 else 0 end as Subs_Retained_11,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 67  MONTH)) then Subs_Retained_12 else 0 end as Subs_Retained_12,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 66  MONTH)) then Subs_Retained_13 else 0 end as Subs_Retained_13,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 65  MONTH)) then Subs_Retained_14 else 0 end as Subs_Retained_14,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 64  MONTH)) then Subs_Retained_15 else 0 end as Subs_Retained_15,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 63  MONTH)) then Subs_Retained_16 else 0 end as Subs_Retained_16,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 62  MONTH)) then Subs_Retained_17 else 0 end as Subs_Retained_17,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 61  MONTH)) then Subs_Retained_18 else 0 end as Subs_Retained_18,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 60  MONTH)) then Subs_Retained_19 else 0 end as Subs_Retained_19,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 59  MONTH)) then Subs_Retained_20 else 0 end as Subs_Retained_20,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 58  MONTH)) then Subs_Retained_21 else 0 end as Subs_Retained_21,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 57  MONTH)) then Subs_Retained_22 else 0 end as Subs_Retained_22,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 56  MONTH)) then Subs_Retained_23 else 0 end as Subs_Retained_23,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 55  MONTH)) then Subs_Retained_24 else 0 end as Subs_Retained_24,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 54  MONTH)) then Subs_Retained_25 else 0 end as Subs_Retained_25,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 53  MONTH)) then Subs_Retained_26 else 0 end as Subs_Retained_26,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 52  MONTH)) then Subs_Retained_27 else 0 end as Subs_Retained_27,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 51  MONTH)) then Subs_Retained_28 else 0 end as Subs_Retained_28,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 50  MONTH)) then Subs_Retained_29 else 0 end as Subs_Retained_29,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 49  MONTH)) then Subs_Retained_30 else 0 end as Subs_Retained_30,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 48  MONTH)) then Subs_Retained_31 else 0 end as Subs_Retained_31,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 47  MONTH)) then Subs_Retained_32 else 0 end as Subs_Retained_32,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 46  MONTH)) then Subs_Retained_33 else 0 end as Subs_Retained_33,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 45  MONTH)) then Subs_Retained_34 else 0 end as Subs_Retained_34,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 44  MONTH)) then Subs_Retained_35 else 0 end as Subs_Retained_35,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 43  MONTH)) then Subs_Retained_36 else 0 end as Subs_Retained_36,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 42  MONTH)) then Subs_Retained_37 else 0 end as Subs_Retained_37,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 41  MONTH)) then Subs_Retained_38 else 0 end as Subs_Retained_38,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 40  MONTH)) then Subs_Retained_39 else 0 end as Subs_Retained_39,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 39  MONTH)) then Subs_Retained_40 else 0 end as Subs_Retained_40,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 38  MONTH)) then Subs_Retained_41 else 0 end as Subs_Retained_41,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 37  MONTH)) then Subs_Retained_42 else 0 end as Subs_Retained_42,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 36  MONTH)) then Subs_Retained_43 else 0 end as Subs_Retained_43,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 35  MONTH)) then Subs_Retained_44 else 0 end as Subs_Retained_44,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 34  MONTH)) then Subs_Retained_45 else 0 end as Subs_Retained_45,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 33  MONTH)) then Subs_Retained_46 else 0 end as Subs_Retained_46,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 32  MONTH)) then Subs_Retained_47 else 0 end as Subs_Retained_47,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 31  MONTH)) then Subs_Retained_48 else 0 end as Subs_Retained_48,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 30  MONTH)) then Subs_Retained_49 else 0 end as Subs_Retained_49,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 29  MONTH)) then Subs_Retained_50 else 0 end as Subs_Retained_50,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 28  MONTH)) then Subs_Retained_51 else 0 end as Subs_Retained_51,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 27  MONTH)) then Subs_Retained_52 else 0 end as Subs_Retained_52,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 26  MONTH)) then Subs_Retained_53 else 0 end as Subs_Retained_53,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 25  MONTH)) then Subs_Retained_54 else 0 end as Subs_Retained_54,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 24  MONTH)) then Subs_Retained_55 else 0 end as Subs_Retained_55,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 23  MONTH)) then Subs_Retained_56 else 0 end as Subs_Retained_56,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 22  MONTH)) then Subs_Retained_57 else 0 end as Subs_Retained_57,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 21  MONTH)) then Subs_Retained_58 else 0 end as Subs_Retained_58,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 20  MONTH)) then Subs_Retained_59 else 0 end as Subs_Retained_59,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 19  MONTH)) then Subs_Retained_60 else 0 end as Subs_Retained_60,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 18  MONTH)) then Subs_Retained_61 else 0 end as Subs_Retained_61,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 17  MONTH)) then Subs_Retained_62 else 0 end as Subs_Retained_62,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 16  MONTH)) then Subs_Retained_63 else 0 end as Subs_Retained_63,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 15  MONTH)) then Subs_Retained_64 else 0 end as Subs_Retained_64,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 14  MONTH)) then Subs_Retained_65 else 0 end as Subs_Retained_65,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 13  MONTH)) then Subs_Retained_66 else 0 end as Subs_Retained_66,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 12  MONTH)) then Subs_Retained_67 else 0 end as Subs_Retained_67,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 11  MONTH)) then Subs_Retained_68 else 0 end as Subs_Retained_68,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 10  MONTH)) then Subs_Retained_69 else 0 end as Subs_Retained_69,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 9  MONTH)) then Subs_Retained_70 else 0 end as Subs_Retained_70,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 8  MONTH)) then Subs_Retained_71 else 0 end as Subs_Retained_71,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 7  MONTH)) then Subs_Retained_72 else 0 end as Subs_Retained_72,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 6  MONTH)) then Subs_Retained_73 else 0 end as Subs_Retained_73,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 5  MONTH)) then Subs_Retained_74 else 0 end as Subs_Retained_74,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 4  MONTH)) then Subs_Retained_75 else 0 end as Subs_Retained_75,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 3  MONTH)) then Subs_Retained_76 else 0 end as Subs_Retained_76,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL  2   MONTH)) then Subs_Retained_77 else 0 end as Subs_Retained_77,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL  1   MONTH)) then Subs_Retained_78 else 0 end as Subs_Retained_78,
    case when ov.Signup_month<@start_date then Subs_Retained_79 else 0 end as Subs_Retained_79

from 
(
select * from 

 (

 Select t.*,running_sum as Cum_cancels ,
 begin_paid_subs as starting_paid_subs
 from 

  (
 select Signup_month,
Month_Start,
 subsequent_month,
 from
       (
       select distinct t.Signup_month,Month_Start from (select r.* from AA_LTV_sub r)  dt 
       cross join
        (select DATE_TRUNC(day_dt,MONTH) as Signup_month from `i-dss-cdm-data-dev.dw_vw.days` where day_dt between  @start_date and @till_date) t -- Change the date range here 

      )
      cross join 
      (
      select s.* from 
      (select row_number() over (order by day_dt) subsequent_month from `i-dss-cdm-data-dev.dw_vw.days`) s where subsequent_month <= (SELECT DATE_DIFF(@end_date,@start_date,MONTH)+2)) sb -- change the subsequent months
      ) t

      left join (
       select Signup_Month,
    Month_Start,
            subsequent_month as mn,
            SUM (cancels) OVER (PARTITION BY  Signup_month,Month_Start ORDER BY subsequent_month) AS running_sum
            from (
             select Signup_month,
             Month_Start,
                case when (Days_actv/30)<=1 then 1 else CEILING(Days_actv/30) end as subsequent_month,
                sum(users) as cancels
                from (
             select Signup_month, 
             Month_Start,
                Days_actv,
                count(distinct subscription_guid ) as users
                from 
                               (
                               select t.*, 
                               DATE_DIFF(cast(case when expiration_dt is null OR expiration_dt>=@till_date then @till_date else expiration_dt end as date), cast( start_dt as Date),DAY) +1 as Days_actv
                                      from AA_LTV_sub t

                               ) a1 
                               where cast(expiration_dt as date)<=@till_date
                               group by Signup_month,Month_Start,
                               Days_actv) a2
            group by Signup_month,Month_Start,
            case when (Days_actv/30)<=1 then 1
       else CEILING(Days_actv/30) end
       ) a3 
       ) can
       on (t.Signup_month=can.Signup_month
       and t.subsequent_month=can.mn
       and t.Month_Start=can.Month_Start)    
        left join ( 
       select Signup_month,   
       Month_Start,
       count(distinct subscription_guid) as begin_paid_subs
                               from 
                               (
                               select t.*, 
                               DATE_DIFF(cast(case when expiration_dt is null OR expiration_dt>=@till_date then @till_date else expiration_dt end as date), cast( start_dt as Date),DAY) +1 as Days_actv
                                      from AA_LTV_sub t

                               ) a1 
                               group by Signup_month    ,Month_Start                   
                               ) a4
                               on (t.Signup_month=a4.Signup_month
                               and t.Month_Start=a4.Month_Start)
       ) a5
         PIVOT(SUM(starting_paid_subs-Cum_cancels) as subs_retained FOR subsequent_month in
        (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,
        21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,
        39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,
        57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,
        75,76,77,78,79))
       --group by Signup_month,Month_Start
     ) ov
left join ( 
       select Signup_month,   
       Month_Start,
       count(distinct subscription_guid ) as total_starts
                               from 
                               (
                               select t.*, 
                               DATE_DIFF(cast(case when expiration_dt is null OR expiration_dt>=@till_date then @till_date else expiration_dt end as date), cast( start_dt as Date),DAY) +1 as Days_actv
                                      from AA_LTV_sub t

                               ) a1 
                               group by Signup_month,Month_Start                
                               ) strt
                               on (ov.Signup_month=strt.Signup_month
                               and ov.Month_Start=strt.Month_Start)
order by 2,1

)
, LTV_stg3 as (
select Signup_month,Month_Start,
total_starts,
coalesce((lag(total_starts,1)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l1,
coalesce((lag(total_starts,2)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l2,
    coalesce((lag(total_starts,3)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l3,
    coalesce((lag(total_starts,4)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l4,
    coalesce((lag(total_starts,5)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l5,
    coalesce((lag(total_starts,6)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l6,
    coalesce((lag(total_starts,7)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l7,
    coalesce((lag(total_starts,8)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l8,
    coalesce((lag(total_starts,9)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l9,
    coalesce((lag(total_starts,10)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l10,
    coalesce((lag(total_starts,11)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l11,
    coalesce((lag(total_starts,12)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l12,
    coalesce((lag(total_starts,13)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l13,
    coalesce((lag(total_starts,14)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l14,
    coalesce((lag(total_starts,15)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l15,
    coalesce((lag(total_starts,16)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l16,
    coalesce((lag(total_starts,17)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l17,
    coalesce((lag(total_starts,18)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l18,
    coalesce((lag(total_starts,19)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l19,
    coalesce((lag(total_starts,20)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l20,
    coalesce((lag(total_starts,21)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l21,
    coalesce((lag(total_starts,22)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l22,
    coalesce((lag(total_starts,23)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l23,
    coalesce((lag(total_starts,24)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l24,
    coalesce((lag(total_starts,25)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l25,
    coalesce((lag(total_starts,26)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l26,
    coalesce((lag(total_starts,27)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l27,
    coalesce((lag(total_starts,28)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l28,
    coalesce((lag(total_starts,29)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l29,
    coalesce((lag(total_starts,30)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l30,
    coalesce((lag(total_starts,31)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l31,
    coalesce((lag(total_starts,32)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l32,
    coalesce((lag(total_starts,33)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l33,
    coalesce((lag(total_starts,34)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l34,
    coalesce((lag(total_starts,35)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l35,
    coalesce((lag(total_starts,36)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l36,
    coalesce((lag(total_starts,37)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l37,
    coalesce((lag(total_starts,38)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l38,
    coalesce((lag(total_starts,39)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l39,
    coalesce((lag(total_starts,40)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l40,
    coalesce((lag(total_starts,41)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l41,
    coalesce((lag(total_starts,42)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l42,
    coalesce((lag(total_starts,43)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l43,
    coalesce((lag(total_starts,44)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l44,
    coalesce((lag(total_starts,45)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l45,
    coalesce((lag(total_starts,46)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l46,
    coalesce((lag(total_starts,47)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l47,
    coalesce((lag(total_starts,48)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l48,
    coalesce((lag(total_starts,49)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l49,
    coalesce((lag(total_starts,50)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l50,
    coalesce((lag(total_starts,51)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l51,
    coalesce((lag(total_starts,52)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l52,
    coalesce((lag(total_starts,53)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l53,
        coalesce((lag(total_starts,54)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l54,
        coalesce((lag(total_starts,55)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l55,
        coalesce((lag(total_starts,56)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l56,
        coalesce((lag(total_starts,57)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l57,
        coalesce((lag(total_starts,58)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l58,
        coalesce((lag(total_starts,59)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l59,
        coalesce((lag(total_starts,60)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l60,
        coalesce((lag(total_starts,61)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l61,
        coalesce((lag(total_starts,62)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l62,
        coalesce((lag(total_starts,63)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l63,
        coalesce((lag(total_starts,64)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l64,
        coalesce((lag(total_starts,65)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l65,
        coalesce((lag(total_starts,66)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l66,
            coalesce((lag(total_starts,67)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l67,
            coalesce((lag(total_starts,68)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l68,
            coalesce((lag(total_starts,69)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l69,
            coalesce((lag(total_starts,70)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l70,
            coalesce((lag(total_starts,71)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l71,
            coalesce((lag(total_starts,72)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l72,
            coalesce((lag(total_starts,73)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l73,

                coalesce((lag(total_starts,74)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l74,
                coalesce((lag(total_starts,75)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l75,
                coalesce((lag(total_starts,76)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l76,
                coalesce((lag(total_starts,77)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l77,
                coalesce((lag(total_starts,78)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l78,
                coalesce((lag(total_starts,79)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l79

     from LTV_stg2
     order by 2,1
)
select 
Month_Start,
sum( Subs_Retained_1) as  Subs_Retained_1,
sum( Subs_Retained_2) as  Subs_Retained_2,
sum( Subs_Retained_3) as  Subs_Retained_3,
sum( Subs_Retained_4) as  Subs_Retained_4,
sum( Subs_Retained_5) as  Subs_Retained_5,
sum( Subs_Retained_6) as  Subs_Retained_6,
sum( Subs_Retained_7) as  Subs_Retained_7,
sum( Subs_Retained_8) as  Subs_Retained_8,
sum( Subs_Retained_9) as  Subs_Retained_9,
sum( Subs_Retained_10) as  Subs_Retained_10,
sum( Subs_Retained_11) as  Subs_Retained_11,
sum( Subs_Retained_12) as  Subs_Retained_12,

sum( Subs_Retained_13) as  Subs_Retained_13,
sum( Subs_Retained_14) as  Subs_Retained_14,
sum( Subs_Retained_15) as  Subs_Retained_15,
sum( Subs_Retained_16) as  Subs_Retained_16,
sum( Subs_Retained_17) as  Subs_Retained_17,
sum( Subs_Retained_18) as  Subs_Retained_18,
sum( Subs_Retained_19) as  Subs_Retained_19,
sum( Subs_Retained_20) as  Subs_Retained_20,
sum( Subs_Retained_21) as  Subs_Retained_21,
sum( Subs_Retained_22) as  Subs_Retained_22,
sum( Subs_Retained_23) as  Subs_Retained_23,
sum( Subs_Retained_24) as  Subs_Retained_24,
sum( Subs_Retained_25) as  Subs_Retained_25,
sum( Subs_Retained_26) as  Subs_Retained_26,
sum( Subs_Retained_27) as  Subs_Retained_27,
sum( Subs_Retained_28) as  Subs_Retained_28,
sum( Subs_Retained_29) as  Subs_Retained_29,
sum( Subs_Retained_30) as  Subs_Retained_30,
sum( Subs_Retained_31) as  Subs_Retained_31,
sum( Subs_Retained_32) as  Subs_Retained_32,
sum( Subs_Retained_33) as  Subs_Retained_33,
sum( Subs_Retained_34) as  Subs_Retained_34,
sum( Subs_Retained_35) as  Subs_Retained_35,
sum( Subs_Retained_36) as  Subs_Retained_36,
sum( Subs_Retained_37) as  Subs_Retained_37,
sum( Subs_Retained_38) as  Subs_Retained_38,
sum( Subs_Retained_39) as  Subs_Retained_39,
sum( Subs_Retained_40) as  Subs_Retained_40,
sum( Subs_Retained_41) as  Subs_Retained_41,
sum( Subs_Retained_42) as  Subs_Retained_42,
sum( Subs_Retained_43) as  Subs_Retained_43,
sum( Subs_Retained_44) as  Subs_Retained_44,
sum( Subs_Retained_45) as  Subs_Retained_45,
sum( Subs_Retained_46) as  Subs_Retained_46,
sum( Subs_Retained_47) as  Subs_Retained_47,
sum( Subs_Retained_48) as  Subs_Retained_48,
sum( Subs_Retained_49) as  Subs_Retained_49,
sum( Subs_Retained_50) as  Subs_Retained_50,
sum( Subs_Retained_51) as  Subs_Retained_51,
sum( Subs_Retained_52) as  Subs_Retained_52,
sum( Subs_Retained_53) as  Subs_Retained_53,
sum( Subs_Retained_54) as  Subs_Retained_54,
sum( Subs_Retained_55) as  Subs_Retained_55,
sum( Subs_Retained_56) as  Subs_Retained_56,
sum( Subs_Retained_57) as  Subs_Retained_57,
sum( Subs_Retained_58) as  Subs_Retained_58,
sum( Subs_Retained_59) as  Subs_Retained_59,
sum( Subs_Retained_60) as  Subs_Retained_60,
sum( Subs_Retained_61) as  Subs_Retained_61,
sum( Subs_Retained_62) as  Subs_Retained_62,
sum( Subs_Retained_63) as  Subs_Retained_63,
sum( Subs_Retained_64) as  Subs_Retained_64,
sum( Subs_Retained_65) as  Subs_Retained_65,
sum( Subs_Retained_66) as  Subs_Retained_66,
sum( Subs_Retained_67) as  Subs_Retained_67,
sum( Subs_Retained_68) as  Subs_Retained_68,
sum( Subs_Retained_69) as  Subs_Retained_69,
sum( Subs_Retained_70) as  Subs_Retained_70,
sum( Subs_Retained_71) as  Subs_Retained_71,
sum( Subs_Retained_72) as  Subs_Retained_72,
sum( Subs_Retained_73) as  Subs_Retained_73 ,
sum( Subs_Retained_74) as  Subs_Retained_74,
sum( Subs_Retained_75) as  Subs_Retained_75,
sum( Subs_Retained_76) as  Subs_Retained_76,
sum( Subs_Retained_77) as  Subs_Retained_77,
sum( Subs_Retained_78) as  Subs_Retained_78,
sum( Subs_Retained_79) as  Subs_Retained_79

from LTV_stg2
group by 1
UNION ALL

select 
Month_Start,
sum( total_starts) as  total_starts,
sum( total_starts_l1) as  total_starts_l1,
sum( total_starts_l2) as  total_starts_l2,
sum( total_starts_l3) as  total_starts_l3,
sum( total_starts_l4) as  total_starts_l4,
sum( total_starts_l5) as  total_starts_l5,
sum( total_starts_l6) as  total_starts_l6,
sum( total_starts_l7) as  total_starts_l7,
sum( total_starts_l8) as  total_starts_l8,
sum( total_starts_l9) as  total_starts_l9,
sum( total_starts_l10) as  total_starts_l10,
sum( total_starts_l11) as  total_starts_l11,

sum( total_starts_l12) as  total_starts_l12,
sum( total_starts_l13) as  total_starts_l13,
sum( total_starts_l14) as  total_starts_l14,
sum( total_starts_l15) as  total_starts_l15,
sum( total_starts_l16) as  total_starts_l16,
sum( total_starts_l17) as  total_starts_l17,
sum( total_starts_l18) as  total_starts_l18,
sum( total_starts_l19) as  total_starts_l19,
sum( total_starts_l20) as  total_starts_l20,
sum( total_starts_l21) as  total_starts_l21,
sum( total_starts_l22) as  total_starts_l22,
sum( total_starts_l23) as  total_starts_l23,
sum( total_starts_l24) as  total_starts_l24,
sum( total_starts_l25) as  total_starts_l25,
sum( total_starts_l26) as  total_starts_l26,
sum( total_starts_l27) as  total_starts_l27,
sum( total_starts_l28) as  total_starts_l28,
sum( total_starts_l29) as  total_starts_l29,
sum( total_starts_l30) as  total_starts_l30,
sum( total_starts_l31) as  total_starts_l31,
sum( total_starts_l32) as  total_starts_l32,
sum( total_starts_l33) as  total_starts_l33,
sum( total_starts_l34) as  total_starts_l34,
sum( total_starts_l35) as  total_starts_l35,
sum( total_starts_l36) as  total_starts_l36,
sum( total_starts_l37) as  total_starts_l37,
sum( total_starts_l38) as  total_starts_l38,
sum( total_starts_l39) as  total_starts_l39,
sum( total_starts_l40) as  total_starts_l40,
sum( total_starts_l41) as  total_starts_l41,
sum( total_starts_l42) as  total_starts_l42,
sum( total_starts_l43) as  total_starts_l43,
sum( total_starts_l44) as  total_starts_l44,
sum( total_starts_l45) as  total_starts_l45,
sum( total_starts_l46) as  total_starts_l46,
sum( total_starts_l47) as  total_starts_l47,
sum( total_starts_l48) as  total_starts_l48,
sum( total_starts_l49) as  total_starts_l49,
sum( total_starts_l50) as  total_starts_l50,
sum( total_starts_l51) as  total_starts_l51,
sum( total_starts_l52) as  total_starts_l52,
sum( total_starts_l53) as  total_starts_l53,
sum( total_starts_l54) as  total_starts_l54,
sum( total_starts_l55) as  total_starts_l55,
sum( total_starts_l56) as  total_starts_l56,
sum( total_starts_l57) as  total_starts_l57,
sum( total_starts_l58) as  total_starts_l58,
sum( total_starts_l59) as  total_starts_l59,
sum( total_starts_l60) as  total_starts_l60,
sum( total_starts_l61) as  total_starts_l61,
sum( total_starts_l62) as  total_starts_l62,
sum( total_starts_l63) as  total_starts_l63,
sum( total_starts_l64) as  total_starts_l64,
sum( total_starts_l65) as  total_starts_l65,
sum( total_starts_l66) as  total_starts_l66,
sum( total_starts_l67) as  total_starts_l67,
sum( total_starts_l68) as  total_starts_l68,
sum( total_starts_l69) as  total_starts_l69,
sum( total_starts_l70) as  total_starts_l70,
sum( total_starts_l71) as  total_starts_l71,
sum( total_starts_l72) as  total_starts_l72,
sum( total_starts_l73) as  total_starts_l73,
sum( total_starts_l74) as  total_starts_l74,
sum( total_starts_l75) as  total_starts_l75,
sum( total_starts_l76) as  total_starts_l76,
sum( total_starts_l77) as  total_starts_l77,
sum( total_starts_l78) as  total_starts_l78


from LTV_stg3
group by 1
order by 1,2,3
"""
    return query

def signup_mnth_without_free_trial():
    query=""" with AA_LTV_sub as  (
 select * from 
 (
Select A1.*,
activation_dt as start_dt,
EXTRACT(Month FROM activation_dt) AS Month_Start,
date_trunc( activation_dt, Month) as Signup_month,
cdm_udf.LTV_SIGNUP_PLAN_CALC(signup_plan_cd) as signup_plan,
case when subscription_platform_cd in ('RECURLY','PSP') then 'RECURLY' else subscription_platform_cd end as subscription_platform,
--- clean version of trial period offered
cdm_udf.LTV_TRIAL_PERIOD_CALC(signup_plan_cd, signup_trial_period_desc, sku_cd, trial_start_dt, paid_start_dt) as Trial_Period     
from   `i-dss-cdm-data-dev.ent_vw.subscription_fct` A1
where A1.src_system_id=@src_system_id 
and subscription_platform_cd not in ('Apple iOS', 'Apple TV')    
and activation_dt<@till_date
-- and lower(signup_plan_cd) not like '%annual%' 
)  
where Trial_Period is not null and Trial_Period  not in ('1 Month Free','2 Month Free','3 Month Free')
)

, LTV_stg2 as (
select ov.Signup_month,
ov.Month_Start,
case when total_starts is null then 0 else total_starts end as total_starts,
Subs_Retained_1 ,
case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 77  MONTH)) then Subs_Retained_2 else 0 end as Subs_Retained_2,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 76  MONTH)) then Subs_Retained_3 else 0 end as Subs_Retained_3,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 75  MONTH)) then Subs_Retained_4 else 0 end as Subs_Retained_4,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 74  MONTH)) then Subs_Retained_5 else 0 end as Subs_Retained_5,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 73  MONTH)) then Subs_Retained_6 else 0 end as Subs_Retained_6,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 72  MONTH)) then Subs_Retained_7 else 0 end as Subs_Retained_7,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 71  MONTH)) then Subs_Retained_8 else 0 end as Subs_Retained_8,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 70  MONTH)) then Subs_Retained_9 else 0 end as Subs_Retained_9,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 69  MONTH)) then Subs_Retained_10 else 0 end as Subs_Retained_10,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 68  MONTH)) then Subs_Retained_11 else 0 end as Subs_Retained_11,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 67  MONTH)) then Subs_Retained_12 else 0 end as Subs_Retained_12,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 66  MONTH)) then Subs_Retained_13 else 0 end as Subs_Retained_13,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 65  MONTH)) then Subs_Retained_14 else 0 end as Subs_Retained_14,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 64  MONTH)) then Subs_Retained_15 else 0 end as Subs_Retained_15,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 63  MONTH)) then Subs_Retained_16 else 0 end as Subs_Retained_16,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 62  MONTH)) then Subs_Retained_17 else 0 end as Subs_Retained_17,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 61  MONTH)) then Subs_Retained_18 else 0 end as Subs_Retained_18,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 60  MONTH)) then Subs_Retained_19 else 0 end as Subs_Retained_19,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 59  MONTH)) then Subs_Retained_20 else 0 end as Subs_Retained_20,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 58  MONTH)) then Subs_Retained_21 else 0 end as Subs_Retained_21,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 57  MONTH)) then Subs_Retained_22 else 0 end as Subs_Retained_22,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 56  MONTH)) then Subs_Retained_23 else 0 end as Subs_Retained_23,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 55  MONTH)) then Subs_Retained_24 else 0 end as Subs_Retained_24,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 54  MONTH)) then Subs_Retained_25 else 0 end as Subs_Retained_25,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 53  MONTH)) then Subs_Retained_26 else 0 end as Subs_Retained_26,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 52  MONTH)) then Subs_Retained_27 else 0 end as Subs_Retained_27,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 51  MONTH)) then Subs_Retained_28 else 0 end as Subs_Retained_28,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 50  MONTH)) then Subs_Retained_29 else 0 end as Subs_Retained_29,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 49  MONTH)) then Subs_Retained_30 else 0 end as Subs_Retained_30,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 48  MONTH)) then Subs_Retained_31 else 0 end as Subs_Retained_31,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 47  MONTH)) then Subs_Retained_32 else 0 end as Subs_Retained_32,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 46  MONTH)) then Subs_Retained_33 else 0 end as Subs_Retained_33,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 45  MONTH)) then Subs_Retained_34 else 0 end as Subs_Retained_34,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 44  MONTH)) then Subs_Retained_35 else 0 end as Subs_Retained_35,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 43  MONTH)) then Subs_Retained_36 else 0 end as Subs_Retained_36,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 42  MONTH)) then Subs_Retained_37 else 0 end as Subs_Retained_37,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 41  MONTH)) then Subs_Retained_38 else 0 end as Subs_Retained_38,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 40  MONTH)) then Subs_Retained_39 else 0 end as Subs_Retained_39,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 39  MONTH)) then Subs_Retained_40 else 0 end as Subs_Retained_40,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 38  MONTH)) then Subs_Retained_41 else 0 end as Subs_Retained_41,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 37  MONTH)) then Subs_Retained_42 else 0 end as Subs_Retained_42,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 36  MONTH)) then Subs_Retained_43 else 0 end as Subs_Retained_43,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 35  MONTH)) then Subs_Retained_44 else 0 end as Subs_Retained_44,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 34  MONTH)) then Subs_Retained_45 else 0 end as Subs_Retained_45,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 33  MONTH)) then Subs_Retained_46 else 0 end as Subs_Retained_46,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 32  MONTH)) then Subs_Retained_47 else 0 end as Subs_Retained_47,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 31  MONTH)) then Subs_Retained_48 else 0 end as Subs_Retained_48,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 30  MONTH)) then Subs_Retained_49 else 0 end as Subs_Retained_49,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 29  MONTH)) then Subs_Retained_50 else 0 end as Subs_Retained_50,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 28  MONTH)) then Subs_Retained_51 else 0 end as Subs_Retained_51,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 27  MONTH)) then Subs_Retained_52 else 0 end as Subs_Retained_52,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 26  MONTH)) then Subs_Retained_53 else 0 end as Subs_Retained_53,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 25  MONTH)) then Subs_Retained_54 else 0 end as Subs_Retained_54,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 24  MONTH)) then Subs_Retained_55 else 0 end as Subs_Retained_55,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 23  MONTH)) then Subs_Retained_56 else 0 end as Subs_Retained_56,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 22  MONTH)) then Subs_Retained_57 else 0 end as Subs_Retained_57,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 21  MONTH)) then Subs_Retained_58 else 0 end as Subs_Retained_58,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 20  MONTH)) then Subs_Retained_59 else 0 end as Subs_Retained_59,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 19  MONTH)) then Subs_Retained_60 else 0 end as Subs_Retained_60,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 18  MONTH)) then Subs_Retained_61 else 0 end as Subs_Retained_61,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 17  MONTH)) then Subs_Retained_62 else 0 end as Subs_Retained_62,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 16  MONTH)) then Subs_Retained_63 else 0 end as Subs_Retained_63,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 15  MONTH)) then Subs_Retained_64 else 0 end as Subs_Retained_64,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 14  MONTH)) then Subs_Retained_65 else 0 end as Subs_Retained_65,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 13  MONTH)) then Subs_Retained_66 else 0 end as Subs_Retained_66,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 12  MONTH)) then Subs_Retained_67 else 0 end as Subs_Retained_67,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 11  MONTH)) then Subs_Retained_68 else 0 end as Subs_Retained_68,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 10  MONTH)) then Subs_Retained_69 else 0 end as Subs_Retained_69,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 9  MONTH)) then Subs_Retained_70 else 0 end as Subs_Retained_70,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 8  MONTH)) then Subs_Retained_71 else 0 end as Subs_Retained_71,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 7  MONTH)) then Subs_Retained_72 else 0 end as Subs_Retained_72,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 6  MONTH)) then Subs_Retained_73 else 0 end as Subs_Retained_73,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 5  MONTH)) then Subs_Retained_74 else 0 end as Subs_Retained_74,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 4  MONTH)) then Subs_Retained_75 else 0 end as Subs_Retained_75,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 3  MONTH)) then Subs_Retained_76 else 0 end as Subs_Retained_76,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL  2   MONTH)) then Subs_Retained_77 else 0 end as Subs_Retained_77,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL  1   MONTH)) then Subs_Retained_78 else 0 end as Subs_Retained_78,
    case when ov.Signup_month<@start_date then Subs_Retained_79 else 0 end as Subs_Retained_79

from 
(
select * from 

 (

 Select t.*,running_sum as Cum_cancels ,
 begin_paid_subs as starting_paid_subs
 from 

  (
 select Signup_month,
Month_Start,
 subsequent_month,
 from
       (
       select distinct t.Signup_month,Month_Start, from (select r.* from AA_LTV_sub r)  dt 
       cross join
        (select DATE_TRUNC(day_dt,MONTH) as Signup_month from `i-dss-cdm-data-dev.dw_vw.days` where day_dt between  @start_date and @till_date) t -- Change the date range here 

      )
      cross join 
      (
      select s.* from 
      (select row_number() over (order by day_dt) subsequent_month from `i-dss-cdm-data-dev.dw_vw.days`) s where subsequent_month <= (SELECT DATE_DIFF(@end_date,@start_date,MONTH)+2)) sb -- change the subsequent months
      ) t

      left join (
       select Signup_Month,
    Month_Start,
            subsequent_month as mn,
            SUM (cancels) OVER (PARTITION BY  Signup_month,Month_Start ORDER BY subsequent_month) AS running_sum
            from (
             select Signup_month,
             Month_Start,
                case when (Days_actv/30)<=1 then 1 else CEILING(Days_actv/30) end as subsequent_month,
                sum(users) as cancels
                from (
             select Signup_month, 
             Month_Start,
                Days_actv,
                count(distinct subscription_guid ) as users
                from 
                               (
                               select t.*, 
                               DATE_DIFF(cast(case when expiration_dt is null OR expiration_dt>=@till_date then @till_date else expiration_dt end as date), cast( start_dt as Date),DAY) +1 as Days_actv
                                      from AA_LTV_sub t

                               ) a1 
                               where cast(expiration_dt as date)<=@till_date
                               group by Signup_month,Month_Start,
                               Days_actv) a2
            group by Signup_month,Month_Start,
            case when (Days_actv/30)<=1 then 1
       else CEILING(Days_actv/30) end
       ) a3 
       ) can
       on (t.Signup_month=can.Signup_month
       and t.subsequent_month=can.mn
       and t.Month_Start=can.Month_Start)    
        left join ( 
       select Signup_month,   
       Month_Start,
       count(distinct subscription_guid) as begin_paid_subs
                               from 
                               (
                               select t.*, 
                               DATE_DIFF(cast(case when expiration_dt is null OR expiration_dt>=@till_date then @till_date else expiration_dt end as date), cast( start_dt as Date),DAY) +1 as Days_actv
                                      from AA_LTV_sub t

                               ) a1 
                               group by Signup_month    ,Month_Start                 
                               ) a4
                               on (t.Signup_month=a4.Signup_month
                               and t.Month_Start=a4.Month_Start)
       ) a5
         PIVOT(SUM(starting_paid_subs-Cum_cancels) as subs_retained FOR subsequent_month in
        (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,
        21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,
        39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,
        57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,
        75,76,77,78,79))
       --group by Signup_month,Month_Start
     ) ov
left join ( 
       select Signup_month,   
       Month_Start,
       count(distinct subscription_guid ) as total_starts
                               from 
                               (
                               select t.*, 
                               DATE_DIFF(cast(case when expiration_dt is null OR expiration_dt>=@till_date then @till_date else expiration_dt end as date), cast( start_dt as Date),DAY) +1 as Days_actv
                                      from AA_LTV_sub t

                               ) a1 
                               group by Signup_month,Month_Start                
                               ) strt
                               on (ov.Signup_month=strt.Signup_month
                               and ov.Month_Start=strt.Month_Start)
order by 2,1

)
, LTV_stg3 as (
select Signup_month,Month_Start,
total_starts,
coalesce((lag(total_starts,1)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l1,
coalesce((lag(total_starts,2)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l2,
    coalesce((lag(total_starts,3)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l3,
    coalesce((lag(total_starts,4)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l4,
    coalesce((lag(total_starts,5)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l5,
    coalesce((lag(total_starts,6)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l6,
    coalesce((lag(total_starts,7)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l7,
    coalesce((lag(total_starts,8)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l8,
    coalesce((lag(total_starts,9)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l9,
    coalesce((lag(total_starts,10)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l10,
    coalesce((lag(total_starts,11)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l11,
    coalesce((lag(total_starts,12)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l12,
    coalesce((lag(total_starts,13)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l13,
    coalesce((lag(total_starts,14)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l14,
    coalesce((lag(total_starts,15)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l15,
    coalesce((lag(total_starts,16)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l16,
    coalesce((lag(total_starts,17)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l17,
    coalesce((lag(total_starts,18)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l18,
    coalesce((lag(total_starts,19)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l19,
    coalesce((lag(total_starts,20)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l20,
    coalesce((lag(total_starts,21)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l21,
    coalesce((lag(total_starts,22)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l22,
    coalesce((lag(total_starts,23)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l23,
    coalesce((lag(total_starts,24)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l24,
    coalesce((lag(total_starts,25)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l25,
    coalesce((lag(total_starts,26)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l26,
    coalesce((lag(total_starts,27)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l27,
    coalesce((lag(total_starts,28)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l28,
    coalesce((lag(total_starts,29)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l29,
    coalesce((lag(total_starts,30)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l30,
    coalesce((lag(total_starts,31)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l31,
    coalesce((lag(total_starts,32)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l32,
    coalesce((lag(total_starts,33)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l33,
    coalesce((lag(total_starts,34)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l34,
    coalesce((lag(total_starts,35)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l35,
    coalesce((lag(total_starts,36)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l36,
    coalesce((lag(total_starts,37)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l37,
    coalesce((lag(total_starts,38)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l38,
    coalesce((lag(total_starts,39)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l39,
    coalesce((lag(total_starts,40)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l40,
    coalesce((lag(total_starts,41)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l41,
    coalesce((lag(total_starts,42)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l42,
    coalesce((lag(total_starts,43)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l43,
    coalesce((lag(total_starts,44)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l44,
    coalesce((lag(total_starts,45)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l45,
    coalesce((lag(total_starts,46)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l46,
    coalesce((lag(total_starts,47)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l47,
    coalesce((lag(total_starts,48)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l48,
    coalesce((lag(total_starts,49)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l49,
    coalesce((lag(total_starts,50)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l50,
    coalesce((lag(total_starts,51)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l51,
    coalesce((lag(total_starts,52)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l52,
    coalesce((lag(total_starts,53)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l53,
        coalesce((lag(total_starts,54)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l54,
        coalesce((lag(total_starts,55)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l55,
        coalesce((lag(total_starts,56)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l56,
        coalesce((lag(total_starts,57)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l57,
        coalesce((lag(total_starts,58)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l58,
        coalesce((lag(total_starts,59)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l59,
        coalesce((lag(total_starts,60)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l60,
        coalesce((lag(total_starts,61)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l61,
        coalesce((lag(total_starts,62)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l62,
        coalesce((lag(total_starts,63)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l63,
        coalesce((lag(total_starts,64)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l64,
        coalesce((lag(total_starts,65)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l65,
        coalesce((lag(total_starts,66)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l66,
            coalesce((lag(total_starts,67)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l67,
            coalesce((lag(total_starts,68)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l68,
            coalesce((lag(total_starts,69)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l69,
            coalesce((lag(total_starts,70)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l70,
            coalesce((lag(total_starts,71)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l71,
            coalesce((lag(total_starts,72)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l72,
            coalesce((lag(total_starts,73)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l73,

                coalesce((lag(total_starts,74)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l74,
                coalesce((lag(total_starts,75)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l75,
                coalesce((lag(total_starts,76)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l76,
                coalesce((lag(total_starts,77)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l77,
                coalesce((lag(total_starts,78)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l78,
                coalesce((lag(total_starts,79)
    OVER (PARTITION BY Month_Start ORDER BY Signup_month ASC)),0) AS total_starts_l79

     from LTV_stg2
     order by 2,1
)
select 
Month_Start,
sum( Subs_Retained_1) as  Subs_Retained_1,
sum( Subs_Retained_2) as  Subs_Retained_2,
sum( Subs_Retained_3) as  Subs_Retained_3,
sum( Subs_Retained_4) as  Subs_Retained_4,
sum( Subs_Retained_5) as  Subs_Retained_5,
sum( Subs_Retained_6) as  Subs_Retained_6,
sum( Subs_Retained_7) as  Subs_Retained_7,
sum( Subs_Retained_8) as  Subs_Retained_8,
sum( Subs_Retained_9) as  Subs_Retained_9,
sum( Subs_Retained_10) as  Subs_Retained_10,
sum( Subs_Retained_11) as  Subs_Retained_11,
sum( Subs_Retained_12) as  Subs_Retained_12,

sum( Subs_Retained_13) as  Subs_Retained_13,
sum( Subs_Retained_14) as  Subs_Retained_14,
sum( Subs_Retained_15) as  Subs_Retained_15,
sum( Subs_Retained_16) as  Subs_Retained_16,
sum( Subs_Retained_17) as  Subs_Retained_17,
sum( Subs_Retained_18) as  Subs_Retained_18,
sum( Subs_Retained_19) as  Subs_Retained_19,
sum( Subs_Retained_20) as  Subs_Retained_20,
sum( Subs_Retained_21) as  Subs_Retained_21,
sum( Subs_Retained_22) as  Subs_Retained_22,
sum( Subs_Retained_23) as  Subs_Retained_23,
sum( Subs_Retained_24) as  Subs_Retained_24,
sum( Subs_Retained_25) as  Subs_Retained_25,
sum( Subs_Retained_26) as  Subs_Retained_26,
sum( Subs_Retained_27) as  Subs_Retained_27,
sum( Subs_Retained_28) as  Subs_Retained_28,
sum( Subs_Retained_29) as  Subs_Retained_29,
sum( Subs_Retained_30) as  Subs_Retained_30,
sum( Subs_Retained_31) as  Subs_Retained_31,
sum( Subs_Retained_32) as  Subs_Retained_32,
sum( Subs_Retained_33) as  Subs_Retained_33,
sum( Subs_Retained_34) as  Subs_Retained_34,
sum( Subs_Retained_35) as  Subs_Retained_35,
sum( Subs_Retained_36) as  Subs_Retained_36,
sum( Subs_Retained_37) as  Subs_Retained_37,
sum( Subs_Retained_38) as  Subs_Retained_38,
sum( Subs_Retained_39) as  Subs_Retained_39,
sum( Subs_Retained_40) as  Subs_Retained_40,
sum( Subs_Retained_41) as  Subs_Retained_41,
sum( Subs_Retained_42) as  Subs_Retained_42,
sum( Subs_Retained_43) as  Subs_Retained_43,
sum( Subs_Retained_44) as  Subs_Retained_44,
sum( Subs_Retained_45) as  Subs_Retained_45,
sum( Subs_Retained_46) as  Subs_Retained_46,
sum( Subs_Retained_47) as  Subs_Retained_47,
sum( Subs_Retained_48) as  Subs_Retained_48,
sum( Subs_Retained_49) as  Subs_Retained_49,
sum( Subs_Retained_50) as  Subs_Retained_50,
sum( Subs_Retained_51) as  Subs_Retained_51,
sum( Subs_Retained_52) as  Subs_Retained_52,
sum( Subs_Retained_53) as  Subs_Retained_53,
sum( Subs_Retained_54) as  Subs_Retained_54,
sum( Subs_Retained_55) as  Subs_Retained_55,
sum( Subs_Retained_56) as  Subs_Retained_56,
sum( Subs_Retained_57) as  Subs_Retained_57,
sum( Subs_Retained_58) as  Subs_Retained_58,
sum( Subs_Retained_59) as  Subs_Retained_59,
sum( Subs_Retained_60) as  Subs_Retained_60,
sum( Subs_Retained_61) as  Subs_Retained_61,
sum( Subs_Retained_62) as  Subs_Retained_62,
sum( Subs_Retained_63) as  Subs_Retained_63,
sum( Subs_Retained_64) as  Subs_Retained_64,
sum( Subs_Retained_65) as  Subs_Retained_65,
sum( Subs_Retained_66) as  Subs_Retained_66,
sum( Subs_Retained_67) as  Subs_Retained_67,
sum( Subs_Retained_68) as  Subs_Retained_68,
sum( Subs_Retained_69) as  Subs_Retained_69,
sum( Subs_Retained_70) as  Subs_Retained_70,
sum( Subs_Retained_71) as  Subs_Retained_71,
sum( Subs_Retained_72) as  Subs_Retained_72,
sum( Subs_Retained_73) as  Subs_Retained_73 ,
sum( Subs_Retained_74) as  Subs_Retained_74,
sum( Subs_Retained_75) as  Subs_Retained_75,
sum( Subs_Retained_76) as  Subs_Retained_76,
sum( Subs_Retained_77) as  Subs_Retained_77,
sum( Subs_Retained_78) as  Subs_Retained_78,
sum( Subs_Retained_79) as  Subs_Retained_79

from LTV_stg2
group by 1
UNION ALL

select 
Month_Start,
sum( total_starts) as  total_starts,
sum( total_starts_l1) as  total_starts_l1,
sum( total_starts_l2) as  total_starts_l2,
sum( total_starts_l3) as  total_starts_l3,
sum( total_starts_l4) as  total_starts_l4,
sum( total_starts_l5) as  total_starts_l5,
sum( total_starts_l6) as  total_starts_l6,
sum( total_starts_l7) as  total_starts_l7,
sum( total_starts_l8) as  total_starts_l8,
sum( total_starts_l9) as  total_starts_l9,
sum( total_starts_l10) as  total_starts_l10,
sum( total_starts_l11) as  total_starts_l11,

sum( total_starts_l12) as  total_starts_l12,
sum( total_starts_l13) as  total_starts_l13,
sum( total_starts_l14) as  total_starts_l14,
sum( total_starts_l15) as  total_starts_l15,
sum( total_starts_l16) as  total_starts_l16,
sum( total_starts_l17) as  total_starts_l17,
sum( total_starts_l18) as  total_starts_l18,
sum( total_starts_l19) as  total_starts_l19,
sum( total_starts_l20) as  total_starts_l20,
sum( total_starts_l21) as  total_starts_l21,
sum( total_starts_l22) as  total_starts_l22,
sum( total_starts_l23) as  total_starts_l23,
sum( total_starts_l24) as  total_starts_l24,
sum( total_starts_l25) as  total_starts_l25,
sum( total_starts_l26) as  total_starts_l26,
sum( total_starts_l27) as  total_starts_l27,
sum( total_starts_l28) as  total_starts_l28,
sum( total_starts_l29) as  total_starts_l29,
sum( total_starts_l30) as  total_starts_l30,
sum( total_starts_l31) as  total_starts_l31,
sum( total_starts_l32) as  total_starts_l32,
sum( total_starts_l33) as  total_starts_l33,
sum( total_starts_l34) as  total_starts_l34,
sum( total_starts_l35) as  total_starts_l35,
sum( total_starts_l36) as  total_starts_l36,
sum( total_starts_l37) as  total_starts_l37,
sum( total_starts_l38) as  total_starts_l38,
sum( total_starts_l39) as  total_starts_l39,
sum( total_starts_l40) as  total_starts_l40,
sum( total_starts_l41) as  total_starts_l41,
sum( total_starts_l42) as  total_starts_l42,
sum( total_starts_l43) as  total_starts_l43,
sum( total_starts_l44) as  total_starts_l44,
sum( total_starts_l45) as  total_starts_l45,
sum( total_starts_l46) as  total_starts_l46,
sum( total_starts_l47) as  total_starts_l47,
sum( total_starts_l48) as  total_starts_l48,
sum( total_starts_l49) as  total_starts_l49,
sum( total_starts_l50) as  total_starts_l50,
sum( total_starts_l51) as  total_starts_l51,
sum( total_starts_l52) as  total_starts_l52,
sum( total_starts_l53) as  total_starts_l53,
sum( total_starts_l54) as  total_starts_l54,
sum( total_starts_l55) as  total_starts_l55,
sum( total_starts_l56) as  total_starts_l56,
sum( total_starts_l57) as  total_starts_l57,
sum( total_starts_l58) as  total_starts_l58,
sum( total_starts_l59) as  total_starts_l59,
sum( total_starts_l60) as  total_starts_l60,
sum( total_starts_l61) as  total_starts_l61,
sum( total_starts_l62) as  total_starts_l62,
sum( total_starts_l63) as  total_starts_l63,
sum( total_starts_l64) as  total_starts_l64,
sum( total_starts_l65) as  total_starts_l65,
sum( total_starts_l66) as  total_starts_l66,
sum( total_starts_l67) as  total_starts_l67,
sum( total_starts_l68) as  total_starts_l68,
sum( total_starts_l69) as  total_starts_l69,
sum( total_starts_l70) as  total_starts_l70,
sum( total_starts_l71) as  total_starts_l71,
sum( total_starts_l72) as  total_starts_l72,
sum( total_starts_l73) as  total_starts_l73,
sum( total_starts_l74) as  total_starts_l74,
sum( total_starts_l75) as  total_starts_l75,
sum( total_starts_l76) as  total_starts_l76,
sum( total_starts_l77) as  total_starts_l77,
sum( total_starts_l78) as  total_starts_l78


from LTV_stg3
group by 1
order by 1,2,3
"""
    return query

def billing_partner_without_free_trial():
    query=""" with AA_LTV_sub as 
     (
     
     select * from 
     (
    Select A1.*,
    activation_dt as start_dt,
    date_trunc( activation_dt, Month) as Signup_month,
    cdm_udf.LTV_SIGNUP_PLAN_CALC(signup_plan_cd) as signup_plan_cd1,
    
    case when subscription_platform_cd in ('RECURLY','PSP') then 'RECURLY' else subscription_platform_cd end as subscription_platform,
    
    --- clean version of trial period offered
  cdm_udf.LTV_TRIAL_PERIOD_CALC(signup_plan_cd, signup_trial_period_desc, sku_cd, trial_start_dt, paid_start_dt) as Trial_Period

         
    from 
      `i-dss-cdm-data-dev.ent_vw.subscription_fct` A1
    where 
      src_system_id=@src_system_id
      and subscription_platform_cd not in ('Apple iOS', 'Apple TV')    
      and activation_dt<@till_date
    )
    where Trial_Period not in ('1 Month Free', '2 Month Free', '3 Month Free')
    and lower(signup_plan_cd1) not like '%annual%'
    )
    
    , LTV_stg2 as (
    select ov.Signup_month,
    ov.subscription_platform,
    case when total_starts is null then 0 else total_starts end as total_starts,
    Subs_Retained_1 ,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 77  MONTH)) then Subs_Retained_2 else 0 end as Subs_Retained_2,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 76  MONTH)) then Subs_Retained_3 else 0 end as Subs_Retained_3,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 75  MONTH)) then Subs_Retained_4 else 0 end as Subs_Retained_4,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 74  MONTH)) then Subs_Retained_5 else 0 end as Subs_Retained_5,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 73  MONTH)) then Subs_Retained_6 else 0 end as Subs_Retained_6,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 72  MONTH)) then Subs_Retained_7 else 0 end as Subs_Retained_7,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 71  MONTH)) then Subs_Retained_8 else 0 end as Subs_Retained_8,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 70  MONTH)) then Subs_Retained_9 else 0 end as Subs_Retained_9,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 69  MONTH)) then Subs_Retained_10 else 0 end as Subs_Retained_10,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 68  MONTH)) then Subs_Retained_11 else 0 end as Subs_Retained_11,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 67  MONTH)) then Subs_Retained_12 else 0 end as Subs_Retained_12,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 66  MONTH)) then Subs_Retained_13 else 0 end as Subs_Retained_13,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 65  MONTH)) then Subs_Retained_14 else 0 end as Subs_Retained_14,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 64  MONTH)) then Subs_Retained_15 else 0 end as Subs_Retained_15,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 63  MONTH)) then Subs_Retained_16 else 0 end as Subs_Retained_16,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 62  MONTH)) then Subs_Retained_17 else 0 end as Subs_Retained_17,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 61  MONTH)) then Subs_Retained_18 else 0 end as Subs_Retained_18,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 60  MONTH)) then Subs_Retained_19 else 0 end as Subs_Retained_19,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 59  MONTH)) then Subs_Retained_20 else 0 end as Subs_Retained_20,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 58  MONTH)) then Subs_Retained_21 else 0 end as Subs_Retained_21,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 57  MONTH)) then Subs_Retained_22 else 0 end as Subs_Retained_22,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 56  MONTH)) then Subs_Retained_23 else 0 end as Subs_Retained_23,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 55  MONTH)) then Subs_Retained_24 else 0 end as Subs_Retained_24,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 54  MONTH)) then Subs_Retained_25 else 0 end as Subs_Retained_25,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 53  MONTH)) then Subs_Retained_26 else 0 end as Subs_Retained_26,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 52  MONTH)) then Subs_Retained_27 else 0 end as Subs_Retained_27,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 51  MONTH)) then Subs_Retained_28 else 0 end as Subs_Retained_28,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 50  MONTH)) then Subs_Retained_29 else 0 end as Subs_Retained_29,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 49  MONTH)) then Subs_Retained_30 else 0 end as Subs_Retained_30,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 48  MONTH)) then Subs_Retained_31 else 0 end as Subs_Retained_31,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 47  MONTH)) then Subs_Retained_32 else 0 end as Subs_Retained_32,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 46  MONTH)) then Subs_Retained_33 else 0 end as Subs_Retained_33,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 45  MONTH)) then Subs_Retained_34 else 0 end as Subs_Retained_34,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 44  MONTH)) then Subs_Retained_35 else 0 end as Subs_Retained_35,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 43  MONTH)) then Subs_Retained_36 else 0 end as Subs_Retained_36,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 42  MONTH)) then Subs_Retained_37 else 0 end as Subs_Retained_37,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 41  MONTH)) then Subs_Retained_38 else 0 end as Subs_Retained_38,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 40  MONTH)) then Subs_Retained_39 else 0 end as Subs_Retained_39,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 39  MONTH)) then Subs_Retained_40 else 0 end as Subs_Retained_40,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 38  MONTH)) then Subs_Retained_41 else 0 end as Subs_Retained_41,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 37  MONTH)) then Subs_Retained_42 else 0 end as Subs_Retained_42,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 36  MONTH)) then Subs_Retained_43 else 0 end as Subs_Retained_43,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 35  MONTH)) then Subs_Retained_44 else 0 end as Subs_Retained_44,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 34  MONTH)) then Subs_Retained_45 else 0 end as Subs_Retained_45,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 33  MONTH)) then Subs_Retained_46 else 0 end as Subs_Retained_46,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 32  MONTH)) then Subs_Retained_47 else 0 end as Subs_Retained_47,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 31  MONTH)) then Subs_Retained_48 else 0 end as Subs_Retained_48,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 30  MONTH)) then Subs_Retained_49 else 0 end as Subs_Retained_49,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 29  MONTH)) then Subs_Retained_50 else 0 end as Subs_Retained_50,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 28  MONTH)) then Subs_Retained_51 else 0 end as Subs_Retained_51,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 27  MONTH)) then Subs_Retained_52 else 0 end as Subs_Retained_52,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 26  MONTH)) then Subs_Retained_53 else 0 end as Subs_Retained_53,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 25  MONTH)) then Subs_Retained_54 else 0 end as Subs_Retained_54,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 24  MONTH)) then Subs_Retained_55 else 0 end as Subs_Retained_55,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 23  MONTH)) then Subs_Retained_56 else 0 end as Subs_Retained_56,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 22  MONTH)) then Subs_Retained_57 else 0 end as Subs_Retained_57,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 21  MONTH)) then Subs_Retained_58 else 0 end as Subs_Retained_58,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 20  MONTH)) then Subs_Retained_59 else 0 end as Subs_Retained_59,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 19  MONTH)) then Subs_Retained_60 else 0 end as Subs_Retained_60,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 18  MONTH)) then Subs_Retained_61 else 0 end as Subs_Retained_61,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 17  MONTH)) then Subs_Retained_62 else 0 end as Subs_Retained_62,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 16  MONTH)) then Subs_Retained_63 else 0 end as Subs_Retained_63,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 15  MONTH)) then Subs_Retained_64 else 0 end as Subs_Retained_64,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 14  MONTH)) then Subs_Retained_65 else 0 end as Subs_Retained_65,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 13  MONTH)) then Subs_Retained_66 else 0 end as Subs_Retained_66,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 12  MONTH)) then Subs_Retained_67 else 0 end as Subs_Retained_67,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 11  MONTH)) then Subs_Retained_68 else 0 end as Subs_Retained_68,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 10  MONTH)) then Subs_Retained_69 else 0 end as Subs_Retained_69,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 9  MONTH)) then Subs_Retained_70 else 0 end as Subs_Retained_70,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 8  MONTH)) then Subs_Retained_71 else 0 end as Subs_Retained_71,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 7  MONTH)) then Subs_Retained_72 else 0 end as Subs_Retained_72,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 6  MONTH)) then Subs_Retained_73 else 0 end as Subs_Retained_73,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 5  MONTH)) then Subs_Retained_74 else 0 end as Subs_Retained_74,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 4  MONTH)) then Subs_Retained_75 else 0 end as Subs_Retained_75,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 3  MONTH)) then Subs_Retained_76 else 0 end as Subs_Retained_76,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL  2   MONTH)) then Subs_Retained_77 else 0 end as Subs_Retained_77,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL  1   MONTH)) then Subs_Retained_78 else 0 end as Subs_Retained_78,
    case when ov.Signup_month<@start_date then Subs_Retained_79 else 0 end as Subs_Retained_79
    
    
    from 
    (
    
    /* Part 6: Comes after Part-5. Since the values created in Part-5 had multiple records, needs to be summeirzed into one record for each month & platform */
    
    select * from
     (
     
     Select t.*,running_sum as Cum_cancels ,
     begin_paid_subs as starting_paid_subs
     from 
    
    /* Part-1: Create a skeleton Table with sign-up month, subscription platform and subsequent months. Created by using day_dt table  */
      (
     select Signup_month,
    subscription_platform,
     subsequent_month
     from
           (
           select distinct t.Signup_month,subscription_platform from (select r.* from AA_LTV_sub r)  dt 
           cross join
           (select DATE_TRUNC(day_dt,MONTH) as Signup_month from `i-dss-cdm-data-dev.dw_vw.days` where day_dt between @start_date and @end_date) t -- Change the date range here 
     
          )
          cross join 
          (
          select s.* from 
          (select row_number() over (order by day_dt) subsequent_month from `i-dss-cdm-data-dev.dw_vw.days`) s where subsequent_month <= (SELECT DATE_DIFF(@end_date,@start_date,MONTH)+2)) sb -- change the subsequent months
          ) t
        
    /*Part-2: For subscribers who expired, get the running SUM of those by Month, platform and subsequent month. Use expired subscribers ONLY. subsequent month is created by using (exp.date - act. date)/30 
    Celing function is used for rounding off the subscribers
    */    
    
          left join (
          select Signup_Month,
        subscription_platform,
                subsequent_month as mn,
                SUM (cancels) OVER (PARTITION BY  Signup_month,subscription_platform ORDER BY subsequent_month) AS running_sum
                from (
                 select Signup_month,
                 subscription_platform,
                 case when (Days_actv/30)<=1 then 1 else CEILING(Days_actv/30) end as subsequent_month,
                 sum(users) as cancels
                 from (
                 select Signup_month, 
                 subscription_platform,
                 Days_actv,
                 count(distinct subscription_guid ) as users
                 from 
                                   (
                                   select t.*, 
                                   DATE_DIFF(cast(case when expiration_dt is null OR expiration_dt>=@till_date then @till_date else expiration_dt end as date), cast( start_dt as Date),DAY) +1 as Days_actv
                                   from AA_LTV_sub t
     
                                   ) a1 
                                   where cast(expiration_dt as date)<=@till_date
                                   group by Signup_month, subscription_platform,
                                   Days_actv) a2
                group by Signup_month,subscription_platform,
                case when (Days_actv/30)<=1 then 1
           else CEILING(Days_actv/30) end
           ) a3 
           ) can
           on (t.Signup_month=can.Signup_month
           and t.subsequent_month=can.mn
           and t.subscription_platform=can.subscription_platform)  
    
    /*Part-3: Calculate the total starts by month, platform. Use all subscribers (unlike part-2 where only expired subs were used). */
    
            left join ( 
           select Signup_month,   
           subscription_platform,
           count(distinct subscription_guid) as begin_paid_subs
                                   from 
                                   (
                                   select t.*, 
                                   DATE_DIFF(cast(case when expiration_dt is null OR expiration_dt>=@till_date then @till_date else expiration_dt end as date), cast( start_dt as Date),DAY) +1 as Days_actv
                                   from AA_LTV_sub t
     
                                   ) a1 
                                   group by Signup_month    ,subscription_platform                      
                                   ) a4
                                   on (t.Signup_month=a4.Signup_month
                                   and t.subscription_platform=a4.subscription_platform)
           ) 
            PIVOT(SUM(starting_paid_subs-Cum_cancels) as subs_retained FOR subsequent_month in
        (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,
        21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,
        39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,
        57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,
        75,76,77,78,79))
         --  ) a6
         -- group by Signup_month,subscription_platform
    ) ov
    
    
    /*Part-4: SAME as part-3 (total starts). This will be used in the next part of the SQL, to join with LAG function (so that each month's starts will be joined to next month */
    
    left join ( 
           select Signup_month,   
           subscription_platform,
           count(distinct subscription_guid ) as total_starts
                                   from 
                                   (
                                   select t.*, 
                                   DATE_DIFF(cast(case when expiration_dt is null OR expiration_dt>=@till_date then @till_date else expiration_dt end as date), cast( start_dt as Date),DAY) +1 as Days_actv
                                   from AA_LTV_sub t
     
                                   ) a1 
                                   group by Signup_month,subscription_platform                             
                                   ) strt
                                   on (ov.Signup_month=strt.Signup_month
                                   and ov.subscription_platform=strt.subscription_platform)
    order by 2,1
    
    )
    
    
    /*Stage3- Create a 'total starts' table, where for each subsequent month, the starts are shifted down by 1-month. Because we only need Starts for those months where there is 30 days, 60 days retention*/
    
    , LTV_stg3 as (
    select Signup_month,subscription_platform,
    total_starts,
    coalesce((lag(total_starts,1)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l1,
    coalesce((lag(total_starts,2)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l2,
        coalesce((lag(total_starts,3)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l3,
        coalesce((lag(total_starts,4)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l4,
        coalesce((lag(total_starts,5)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l5,
        coalesce((lag(total_starts,6)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l6,
        coalesce((lag(total_starts,7)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l7,
        coalesce((lag(total_starts,8)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l8,
        coalesce((lag(total_starts,9)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l9,
        coalesce((lag(total_starts,10)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l10,
        coalesce((lag(total_starts,11)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l11,
        coalesce((lag(total_starts,12)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l12,
        coalesce((lag(total_starts,13)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l13,
        coalesce((lag(total_starts,14)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l14,
        coalesce((lag(total_starts,15)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l15,
        coalesce((lag(total_starts,16)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l16,
        coalesce((lag(total_starts,17)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l17,
        coalesce((lag(total_starts,18)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l18,
        coalesce((lag(total_starts,19)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l19,
        coalesce((lag(total_starts,20)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l20,
        coalesce((lag(total_starts,21)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l21,
        coalesce((lag(total_starts,22)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l22,
        coalesce((lag(total_starts,23)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l23,
        coalesce((lag(total_starts,24)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l24,
        coalesce((lag(total_starts,25)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l25,
        coalesce((lag(total_starts,26)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l26,
        coalesce((lag(total_starts,27)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l27,
        coalesce((lag(total_starts,28)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l28,
        coalesce((lag(total_starts,29)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l29,
        coalesce((lag(total_starts,30)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l30,
        coalesce((lag(total_starts,31)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l31,
        coalesce((lag(total_starts,32)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l32,
        coalesce((lag(total_starts,33)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l33,
        coalesce((lag(total_starts,34)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l34,
        coalesce((lag(total_starts,35)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l35,
        coalesce((lag(total_starts,36)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l36,
        coalesce((lag(total_starts,37)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l37,
        coalesce((lag(total_starts,38)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l38,
        coalesce((lag(total_starts,39)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l39,
        coalesce((lag(total_starts,40)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l40,
        coalesce((lag(total_starts,41)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l41,
        coalesce((lag(total_starts,42)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l42,
        coalesce((lag(total_starts,43)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l43,
        coalesce((lag(total_starts,44)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l44,
        coalesce((lag(total_starts,45)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l45,
        coalesce((lag(total_starts,46)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l46,
        coalesce((lag(total_starts,47)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l47,
        coalesce((lag(total_starts,48)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l48,
        coalesce((lag(total_starts,49)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l49,
        coalesce((lag(total_starts,50)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l50,
        coalesce((lag(total_starts,51)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l51,
        coalesce((lag(total_starts,52)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l52,
        coalesce((lag(total_starts,53)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l53,
            coalesce((lag(total_starts,54)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l54,
            coalesce((lag(total_starts,55)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l55,
            coalesce((lag(total_starts,56)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l56,
            coalesce((lag(total_starts,57)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l57,
            coalesce((lag(total_starts,58)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l58,
            coalesce((lag(total_starts,59)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l59,
            coalesce((lag(total_starts,60)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l60,
            coalesce((lag(total_starts,61)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l61,
            coalesce((lag(total_starts,62)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l62,
            coalesce((lag(total_starts,63)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l63,
            coalesce((lag(total_starts,64)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l64,
            coalesce((lag(total_starts,65)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l65,
            coalesce((lag(total_starts,66)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l66,
                coalesce((lag(total_starts,67)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l67,
                coalesce((lag(total_starts,68)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l68,
                coalesce((lag(total_starts,69)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l69,
                    coalesce((lag(total_starts,70)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l70,
                    coalesce((lag(total_starts,71)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l71,
                    coalesce((lag(total_starts,72)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l72,
                    coalesce((lag(total_starts,73)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l73, 
        
                        coalesce((lag(total_starts,74)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l74, 
                        coalesce((lag(total_starts,75)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l75, 
                        coalesce((lag(total_starts,76)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l76, 
                        coalesce((lag(total_starts,77)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l77, 
                        coalesce((lag(total_starts,78)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l78, 
                        coalesce((lag(total_starts,79)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l79, 
        
         from LTV_stg2
         order by 2,1
    )
    
    select 
    subscription_platform,
    sum( Subs_Retained_1) as  Subs_Retained_1,
    sum( Subs_Retained_2) as  Subs_Retained_2,
    sum( Subs_Retained_3) as  Subs_Retained_3,
    sum( Subs_Retained_4) as  Subs_Retained_4,
    sum( Subs_Retained_5) as  Subs_Retained_5,
    sum( Subs_Retained_6) as  Subs_Retained_6,
    sum( Subs_Retained_7) as  Subs_Retained_7,
    sum( Subs_Retained_8) as  Subs_Retained_8,
    sum( Subs_Retained_9) as  Subs_Retained_9,
    sum( Subs_Retained_10) as  Subs_Retained_10,
    sum( Subs_Retained_11) as  Subs_Retained_11,
    sum( Subs_Retained_12) as  Subs_Retained_12,
    
    sum( Subs_Retained_13) as  Subs_Retained_13,
    sum( Subs_Retained_14) as  Subs_Retained_14,
    sum( Subs_Retained_15) as  Subs_Retained_15,
    sum( Subs_Retained_16) as  Subs_Retained_16,
    sum( Subs_Retained_17) as  Subs_Retained_17,
    sum( Subs_Retained_18) as  Subs_Retained_18,
    sum( Subs_Retained_19) as  Subs_Retained_19,
    sum( Subs_Retained_20) as  Subs_Retained_20,
    sum( Subs_Retained_21) as  Subs_Retained_21,
    sum( Subs_Retained_22) as  Subs_Retained_22,
    sum( Subs_Retained_23) as  Subs_Retained_23,
    sum( Subs_Retained_24) as  Subs_Retained_24,
    sum( Subs_Retained_25) as  Subs_Retained_25,
    sum( Subs_Retained_26) as  Subs_Retained_26,
    sum( Subs_Retained_27) as  Subs_Retained_27,
    sum( Subs_Retained_28) as  Subs_Retained_28,
    sum( Subs_Retained_29) as  Subs_Retained_29,
    sum( Subs_Retained_30) as  Subs_Retained_30,
    sum( Subs_Retained_31) as  Subs_Retained_31,
    sum( Subs_Retained_32) as  Subs_Retained_32,
    sum( Subs_Retained_33) as  Subs_Retained_33,
    sum( Subs_Retained_34) as  Subs_Retained_34,
    sum( Subs_Retained_35) as  Subs_Retained_35,
    sum( Subs_Retained_36) as  Subs_Retained_36,
    sum( Subs_Retained_37) as  Subs_Retained_37,
    sum( Subs_Retained_38) as  Subs_Retained_38,
    sum( Subs_Retained_39) as  Subs_Retained_39,
    sum( Subs_Retained_40) as  Subs_Retained_40,
    sum( Subs_Retained_41) as  Subs_Retained_41,
    sum( Subs_Retained_42) as  Subs_Retained_42,
    sum( Subs_Retained_43) as  Subs_Retained_43,
    sum( Subs_Retained_44) as  Subs_Retained_44,
    sum( Subs_Retained_45) as  Subs_Retained_45,
    sum( Subs_Retained_46) as  Subs_Retained_46,
    sum( Subs_Retained_47) as  Subs_Retained_47,
    sum( Subs_Retained_48) as  Subs_Retained_48,
    sum( Subs_Retained_49) as  Subs_Retained_49,
    sum( Subs_Retained_50) as  Subs_Retained_50,
    sum( Subs_Retained_51) as  Subs_Retained_51,
    sum( Subs_Retained_52) as  Subs_Retained_52,
    sum( Subs_Retained_53) as  Subs_Retained_53,
    sum( Subs_Retained_54) as  Subs_Retained_54,
    sum( Subs_Retained_55) as  Subs_Retained_55,
    sum( Subs_Retained_56) as  Subs_Retained_56,
    sum( Subs_Retained_57) as  Subs_Retained_57,
    sum( Subs_Retained_58) as  Subs_Retained_58,
    sum( Subs_Retained_59) as  Subs_Retained_59,
    sum( Subs_Retained_60) as  Subs_Retained_60,
    sum( Subs_Retained_61) as  Subs_Retained_61,
    sum( Subs_Retained_62) as  Subs_Retained_62,
    sum( Subs_Retained_63) as  Subs_Retained_63,
    sum( Subs_Retained_64) as  Subs_Retained_64,
    sum( Subs_Retained_65) as  Subs_Retained_65,
    sum( Subs_Retained_66) as  Subs_Retained_66,
    sum( Subs_Retained_67) as  Subs_Retained_67,
    sum( Subs_Retained_68) as  Subs_Retained_68,
    sum( Subs_Retained_69) as  Subs_Retained_69,
    sum( Subs_Retained_70) as  Subs_Retained_70,
    sum( Subs_Retained_71) as  Subs_Retained_71,
    sum( Subs_Retained_72) as  Subs_Retained_72,
    sum( Subs_Retained_73) as  Subs_Retained_73 ,
    
    sum( Subs_Retained_74) as  Subs_Retained_74,
    sum( Subs_Retained_75) as  Subs_Retained_75,
    sum( Subs_Retained_76) as  Subs_Retained_76,
    sum( Subs_Retained_77) as  Subs_Retained_77,
    sum( Subs_Retained_78) as  Subs_Retained_78,
    sum( Subs_Retained_79) as  Subs_Retained_79
    
    from LTV_stg2
    group by 1
    UNION ALL
    
    select 
    CONCAT(subscription_platform,' - Total Starts'),
    sum( total_starts) as  total_starts,
    sum( total_starts_l1) as  total_starts_l1,
    sum( total_starts_l2) as  total_starts_l2,
    sum( total_starts_l3) as  total_starts_l3,
    sum( total_starts_l4) as  total_starts_l4,
    sum( total_starts_l5) as  total_starts_l5,
    sum( total_starts_l6) as  total_starts_l6,
    sum( total_starts_l7) as  total_starts_l7,
    sum( total_starts_l8) as  total_starts_l8,
    sum( total_starts_l9) as  total_starts_l9,
    sum( total_starts_l10) as  total_starts_l10,
    sum( total_starts_l11) as  total_starts_l11,
    
    sum( total_starts_l12) as  total_starts_l12,
    sum( total_starts_l13) as  total_starts_l13,
    sum( total_starts_l14) as  total_starts_l14,
    sum( total_starts_l15) as  total_starts_l15,
    sum( total_starts_l16) as  total_starts_l16,
    sum( total_starts_l17) as  total_starts_l17,
    sum( total_starts_l18) as  total_starts_l18,
    sum( total_starts_l19) as  total_starts_l19,
    sum( total_starts_l20) as  total_starts_l20,
    sum( total_starts_l21) as  total_starts_l21,
    sum( total_starts_l22) as  total_starts_l22,
    sum( total_starts_l23) as  total_starts_l23,
    sum( total_starts_l24) as  total_starts_l24,
    sum( total_starts_l25) as  total_starts_l25,
    sum( total_starts_l26) as  total_starts_l26,
    sum( total_starts_l27) as  total_starts_l27,
    sum( total_starts_l28) as  total_starts_l28,
    sum( total_starts_l29) as  total_starts_l29,
    sum( total_starts_l30) as  total_starts_l30,
    sum( total_starts_l31) as  total_starts_l31,
    sum( total_starts_l32) as  total_starts_l32,
    sum( total_starts_l33) as  total_starts_l33,
    sum( total_starts_l34) as  total_starts_l34,
    sum( total_starts_l35) as  total_starts_l35,
    sum( total_starts_l36) as  total_starts_l36,
    sum( total_starts_l37) as  total_starts_l37,
    sum( total_starts_l38) as  total_starts_l38,
    sum( total_starts_l39) as  total_starts_l39,
    sum( total_starts_l40) as  total_starts_l40,
    sum( total_starts_l41) as  total_starts_l41,
    sum( total_starts_l42) as  total_starts_l42,
    sum( total_starts_l43) as  total_starts_l43,
    sum( total_starts_l44) as  total_starts_l44,
    sum( total_starts_l45) as  total_starts_l45,
    sum( total_starts_l46) as  total_starts_l46,
    sum( total_starts_l47) as  total_starts_l47,
    sum( total_starts_l48) as  total_starts_l48,
    sum( total_starts_l49) as  total_starts_l49,
    sum( total_starts_l50) as  total_starts_l50,
    sum( total_starts_l51) as  total_starts_l51,
    sum( total_starts_l52) as  total_starts_l52,
    sum( total_starts_l53) as  total_starts_l53,
    sum( total_starts_l54) as  total_starts_l54,
    sum( total_starts_l55) as  total_starts_l55,
    sum( total_starts_l56) as  total_starts_l56,
    sum( total_starts_l57) as  total_starts_l57,
    sum( total_starts_l58) as  total_starts_l58,
    sum( total_starts_l59) as  total_starts_l59,
    sum( total_starts_l60) as  total_starts_l60,
    sum( total_starts_l61) as  total_starts_l61,
    sum( total_starts_l62) as  total_starts_l62,
    sum( total_starts_l63) as  total_starts_l63,
    sum( total_starts_l64) as  total_starts_l64,
    sum( total_starts_l65) as  total_starts_l65,
    sum( total_starts_l66) as  total_starts_l66,
    sum( total_starts_l67) as  total_starts_l67,
    sum( total_starts_l68) as  total_starts_l68,
    sum( total_starts_l69) as  total_starts_l69,
    sum( total_starts_l70) as  total_starts_l70,
    sum( total_starts_l71) as  total_starts_l71,
    sum( total_starts_l72) as  total_starts_l72,
    sum( total_starts_l73) as  total_starts_l73,
    sum( total_starts_l74) as  total_starts_l74,
    sum( total_starts_l75) as  total_starts_l75,
    sum( total_starts_l76) as  total_starts_l76,
    sum( total_starts_l77) as  total_starts_l77,
    sum( total_starts_l78) as  total_starts_l78,
    -- sum( total_starts_l73) as  total_starts_l73
    -- sum( total_starts_l66) as  total_starts_l66,
    
    from LTV_stg3
    group by 1
    order by 1,2
    """
    return query

def billing_partner_with_free_trial():
    query=""" with AA_LTV_sub as 
     (
     
     select * from 
     (
    Select A1.*,
    activation_dt as start_dt,
    date_trunc( activation_dt, Month) as Signup_month,
    cdm_udf.LTV_SIGNUP_PLAN_CALC(signup_plan_cd) as signup_plan_cd1,
    
    case when subscription_platform_cd in ('RECURLY','PSP') then 'RECURLY' else subscription_platform_cd end as subscription_platform,
    
    --- clean version of trial period offered
  cdm_udf.LTV_TRIAL_PERIOD_CALC(signup_plan_cd, signup_trial_period_desc, sku_cd, trial_start_dt, paid_start_dt) as Trial_Period

         
    from 
      `i-dss-cdm-data-dev.ent_vw.subscription_fct` A1
    where 
      src_system_id=@src_system_id 
      and subscription_platform_cd not in ('Apple iOS', 'Apple TV')    
      and activation_dt<@till_date
    )
    where Trial_Period  in ('1 Month Free', '2 Month Free', '3 Month Free')
    and lower(signup_plan_cd1) not like '%annual%'
    )
    
    , LTV_stg2 as (
    select ov.Signup_month,
    ov.subscription_platform,
    case when total_starts is null then 0 else total_starts end as total_starts,
    Subs_Retained_1 ,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 77  MONTH)) then Subs_Retained_2 else 0 end as Subs_Retained_2,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 76  MONTH)) then Subs_Retained_3 else 0 end as Subs_Retained_3,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 75  MONTH)) then Subs_Retained_4 else 0 end as Subs_Retained_4,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 74  MONTH)) then Subs_Retained_5 else 0 end as Subs_Retained_5,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 73  MONTH)) then Subs_Retained_6 else 0 end as Subs_Retained_6,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 72  MONTH)) then Subs_Retained_7 else 0 end as Subs_Retained_7,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 71  MONTH)) then Subs_Retained_8 else 0 end as Subs_Retained_8,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 70  MONTH)) then Subs_Retained_9 else 0 end as Subs_Retained_9,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 69  MONTH)) then Subs_Retained_10 else 0 end as Subs_Retained_10,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 68  MONTH)) then Subs_Retained_11 else 0 end as Subs_Retained_11,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 67  MONTH)) then Subs_Retained_12 else 0 end as Subs_Retained_12,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 66  MONTH)) then Subs_Retained_13 else 0 end as Subs_Retained_13,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 65  MONTH)) then Subs_Retained_14 else 0 end as Subs_Retained_14,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 64  MONTH)) then Subs_Retained_15 else 0 end as Subs_Retained_15,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 63  MONTH)) then Subs_Retained_16 else 0 end as Subs_Retained_16,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 62  MONTH)) then Subs_Retained_17 else 0 end as Subs_Retained_17,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 61  MONTH)) then Subs_Retained_18 else 0 end as Subs_Retained_18,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 60  MONTH)) then Subs_Retained_19 else 0 end as Subs_Retained_19,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 59  MONTH)) then Subs_Retained_20 else 0 end as Subs_Retained_20,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 58  MONTH)) then Subs_Retained_21 else 0 end as Subs_Retained_21,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 57  MONTH)) then Subs_Retained_22 else 0 end as Subs_Retained_22,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 56  MONTH)) then Subs_Retained_23 else 0 end as Subs_Retained_23,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 55  MONTH)) then Subs_Retained_24 else 0 end as Subs_Retained_24,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 54  MONTH)) then Subs_Retained_25 else 0 end as Subs_Retained_25,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 53  MONTH)) then Subs_Retained_26 else 0 end as Subs_Retained_26,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 52  MONTH)) then Subs_Retained_27 else 0 end as Subs_Retained_27,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 51  MONTH)) then Subs_Retained_28 else 0 end as Subs_Retained_28,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 50  MONTH)) then Subs_Retained_29 else 0 end as Subs_Retained_29,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 49  MONTH)) then Subs_Retained_30 else 0 end as Subs_Retained_30,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 48  MONTH)) then Subs_Retained_31 else 0 end as Subs_Retained_31,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 47  MONTH)) then Subs_Retained_32 else 0 end as Subs_Retained_32,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 46  MONTH)) then Subs_Retained_33 else 0 end as Subs_Retained_33,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 45  MONTH)) then Subs_Retained_34 else 0 end as Subs_Retained_34,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 44  MONTH)) then Subs_Retained_35 else 0 end as Subs_Retained_35,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 43  MONTH)) then Subs_Retained_36 else 0 end as Subs_Retained_36,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 42  MONTH)) then Subs_Retained_37 else 0 end as Subs_Retained_37,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 41  MONTH)) then Subs_Retained_38 else 0 end as Subs_Retained_38,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 40  MONTH)) then Subs_Retained_39 else 0 end as Subs_Retained_39,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 39  MONTH)) then Subs_Retained_40 else 0 end as Subs_Retained_40,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 38  MONTH)) then Subs_Retained_41 else 0 end as Subs_Retained_41,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 37  MONTH)) then Subs_Retained_42 else 0 end as Subs_Retained_42,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 36  MONTH)) then Subs_Retained_43 else 0 end as Subs_Retained_43,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 35  MONTH)) then Subs_Retained_44 else 0 end as Subs_Retained_44,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 34  MONTH)) then Subs_Retained_45 else 0 end as Subs_Retained_45,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 33  MONTH)) then Subs_Retained_46 else 0 end as Subs_Retained_46,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 32  MONTH)) then Subs_Retained_47 else 0 end as Subs_Retained_47,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 31  MONTH)) then Subs_Retained_48 else 0 end as Subs_Retained_48,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 30  MONTH)) then Subs_Retained_49 else 0 end as Subs_Retained_49,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 29  MONTH)) then Subs_Retained_50 else 0 end as Subs_Retained_50,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 28  MONTH)) then Subs_Retained_51 else 0 end as Subs_Retained_51,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 27  MONTH)) then Subs_Retained_52 else 0 end as Subs_Retained_52,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 26  MONTH)) then Subs_Retained_53 else 0 end as Subs_Retained_53,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 25  MONTH)) then Subs_Retained_54 else 0 end as Subs_Retained_54,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 24  MONTH)) then Subs_Retained_55 else 0 end as Subs_Retained_55,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 23  MONTH)) then Subs_Retained_56 else 0 end as Subs_Retained_56,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 22  MONTH)) then Subs_Retained_57 else 0 end as Subs_Retained_57,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 21  MONTH)) then Subs_Retained_58 else 0 end as Subs_Retained_58,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 20  MONTH)) then Subs_Retained_59 else 0 end as Subs_Retained_59,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 19  MONTH)) then Subs_Retained_60 else 0 end as Subs_Retained_60,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 18  MONTH)) then Subs_Retained_61 else 0 end as Subs_Retained_61,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 17  MONTH)) then Subs_Retained_62 else 0 end as Subs_Retained_62,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 16  MONTH)) then Subs_Retained_63 else 0 end as Subs_Retained_63,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 15  MONTH)) then Subs_Retained_64 else 0 end as Subs_Retained_64,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 14  MONTH)) then Subs_Retained_65 else 0 end as Subs_Retained_65,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 13  MONTH)) then Subs_Retained_66 else 0 end as Subs_Retained_66,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 12  MONTH)) then Subs_Retained_67 else 0 end as Subs_Retained_67,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 11  MONTH)) then Subs_Retained_68 else 0 end as Subs_Retained_68,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 10  MONTH)) then Subs_Retained_69 else 0 end as Subs_Retained_69,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 9  MONTH)) then Subs_Retained_70 else 0 end as Subs_Retained_70,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 8  MONTH)) then Subs_Retained_71 else 0 end as Subs_Retained_71,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 7  MONTH)) then Subs_Retained_72 else 0 end as Subs_Retained_72,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 6  MONTH)) then Subs_Retained_73 else 0 end as Subs_Retained_73,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 5  MONTH)) then Subs_Retained_74 else 0 end as Subs_Retained_74,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 4  MONTH)) then Subs_Retained_75 else 0 end as Subs_Retained_75,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 3  MONTH)) then Subs_Retained_76 else 0 end as Subs_Retained_76,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL  2   MONTH)) then Subs_Retained_77 else 0 end as Subs_Retained_77,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL  1   MONTH)) then Subs_Retained_78 else 0 end as Subs_Retained_78,
    case when ov.Signup_month<@start_date then Subs_Retained_79 else 0 end as Subs_Retained_79
    
    
    from 
    (
    
    /* Part 6: Comes after Part-5. Since the values created in Part-5 had multiple records, needs to be summeirzed into one record for each month & platform */
    
    select * from
     (
     
     Select t.*,running_sum as Cum_cancels ,
     begin_paid_subs as starting_paid_subs
     from 
    
    /* Part-1: Create a skeleton Table with sign-up month, subscription platform and subsequent months. Created by using day_dt table  */
      (
     select Signup_month,
    subscription_platform,
     subsequent_month
     from
           (
           select distinct t.Signup_month,subscription_platform from (select r.* from AA_LTV_sub r)  dt 
           cross join
           (select DATE_TRUNC(day_dt,MONTH) as Signup_month from `i-dss-cdm-data-dev.dw_vw.days` where day_dt between @start_date and @end_date) t -- Change the date range here 
     
          )
          cross join 
          (
          select s.* from 
          (select row_number() over (order by day_dt) subsequent_month from `i-dss-cdm-data-dev.dw_vw.days`) s where subsequent_month <= (SELECT DATE_DIFF(@end_date,@start_date,MONTH)+2)) sb -- change the subsequent months
          ) t
        
    /*Part-2: For subscribers who expired, get the running SUM of those by Month, platform and subsequent month. Use expired subscribers ONLY. subsequent month is created by using (exp.date - act. date)/30 
    Celing function is used for rounding off the subscribers
    */    
    
          left join (
          select Signup_Month,
        subscription_platform,
                subsequent_month as mn,
                SUM (cancels) OVER (PARTITION BY  Signup_month,subscription_platform ORDER BY subsequent_month) AS running_sum
                from (
                 select Signup_month,
                 subscription_platform,
                 case when (Days_actv/30)<=1 then 1 else CEILING(Days_actv/30) end as subsequent_month,
                 sum(users) as cancels
                 from (
                 select Signup_month, 
                 subscription_platform,
                 Days_actv,
                 count(distinct subscription_guid ) as users
                 from 
                                   (
                                   select t.*, 
                                   DATE_DIFF(cast(case when expiration_dt is null OR expiration_dt>=@till_date then @till_date else expiration_dt end as date), cast( start_dt as Date),DAY) +1 as Days_actv
                                   from AA_LTV_sub t
     
                                   ) a1 
                                   where cast(expiration_dt as date)<=@till_date
                                   group by Signup_month, subscription_platform,
                                   Days_actv) a2
                group by Signup_month,subscription_platform,
                case when (Days_actv/30)<=1 then 1
           else CEILING(Days_actv/30) end
           ) a3 
           ) can
           on (t.Signup_month=can.Signup_month
           and t.subsequent_month=can.mn
           and t.subscription_platform=can.subscription_platform)  
    
    /*Part-3: Calculate the total starts by month, platform. Use all subscribers (unlike part-2 where only expired subs were used). */
    
            left join ( 
           select Signup_month,   
           subscription_platform,
           count(distinct subscription_guid) as begin_paid_subs
                                   from 
                                   (
                                   select t.*, 
                                   DATE_DIFF(cast(case when expiration_dt is null OR expiration_dt>=@till_date then @till_date else expiration_dt end as date), cast( start_dt as Date),DAY) +1 as Days_actv
                                   from AA_LTV_sub t
     
                                   ) a1 
                                   group by Signup_month    ,subscription_platform                      
                                   ) a4
                                   on (t.Signup_month=a4.Signup_month
                                   and t.subscription_platform=a4.subscription_platform)
           ) 
            PIVOT(SUM(starting_paid_subs-Cum_cancels) as subs_retained FOR subsequent_month in
        (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,
        21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,
        39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,
        57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,
        75,76,77,78,79))
         --  ) a6
         -- group by Signup_month,subscription_platform
    ) ov
    
    
    /*Part-4: SAME as part-3 (total starts). This will be used in the next part of the SQL, to join with LAG function (so that each month's starts will be joined to next month */
    
    left join ( 
           select Signup_month,   
           subscription_platform,
           count(distinct subscription_guid ) as total_starts
                                   from 
                                   (
                                   select t.*, 
                                   DATE_DIFF(cast(case when expiration_dt is null OR expiration_dt>=@till_date then @till_date else expiration_dt end as date), cast( start_dt as Date),DAY) +1 as Days_actv
                                   from AA_LTV_sub t
     
                                   ) a1 
                                   group by Signup_month,subscription_platform                             
                                   ) strt
                                   on (ov.Signup_month=strt.Signup_month
                                   and ov.subscription_platform=strt.subscription_platform)
    order by 2,1
    
    )
    
    
    /*Stage3- Create a 'total starts' table, where for each subsequent month, the starts are shifted down by 1-month. Because we only need Starts for those months where there is 30 days, 60 days retention*/
    
    , LTV_stg3 as (
    select Signup_month,subscription_platform,
    total_starts,
    coalesce((lag(total_starts,1)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l1,
    coalesce((lag(total_starts,2)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l2,
        coalesce((lag(total_starts,3)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l3,
        coalesce((lag(total_starts,4)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l4,
        coalesce((lag(total_starts,5)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l5,
        coalesce((lag(total_starts,6)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l6,
        coalesce((lag(total_starts,7)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l7,
        coalesce((lag(total_starts,8)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l8,
        coalesce((lag(total_starts,9)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l9,
        coalesce((lag(total_starts,10)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l10,
        coalesce((lag(total_starts,11)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l11,
        coalesce((lag(total_starts,12)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l12,
        coalesce((lag(total_starts,13)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l13,
        coalesce((lag(total_starts,14)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l14,
        coalesce((lag(total_starts,15)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l15,
        coalesce((lag(total_starts,16)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l16,
        coalesce((lag(total_starts,17)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l17,
        coalesce((lag(total_starts,18)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l18,
        coalesce((lag(total_starts,19)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l19,
        coalesce((lag(total_starts,20)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l20,
        coalesce((lag(total_starts,21)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l21,
        coalesce((lag(total_starts,22)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l22,
        coalesce((lag(total_starts,23)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l23,
        coalesce((lag(total_starts,24)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l24,
        coalesce((lag(total_starts,25)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l25,
        coalesce((lag(total_starts,26)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l26,
        coalesce((lag(total_starts,27)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l27,
        coalesce((lag(total_starts,28)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l28,
        coalesce((lag(total_starts,29)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l29,
        coalesce((lag(total_starts,30)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l30,
        coalesce((lag(total_starts,31)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l31,
        coalesce((lag(total_starts,32)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l32,
        coalesce((lag(total_starts,33)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l33,
        coalesce((lag(total_starts,34)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l34,
        coalesce((lag(total_starts,35)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l35,
        coalesce((lag(total_starts,36)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l36,
        coalesce((lag(total_starts,37)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l37,
        coalesce((lag(total_starts,38)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l38,
        coalesce((lag(total_starts,39)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l39,
        coalesce((lag(total_starts,40)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l40,
        coalesce((lag(total_starts,41)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l41,
        coalesce((lag(total_starts,42)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l42,
        coalesce((lag(total_starts,43)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l43,
        coalesce((lag(total_starts,44)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l44,
        coalesce((lag(total_starts,45)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l45,
        coalesce((lag(total_starts,46)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l46,
        coalesce((lag(total_starts,47)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l47,
        coalesce((lag(total_starts,48)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l48,
        coalesce((lag(total_starts,49)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l49,
        coalesce((lag(total_starts,50)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l50,
        coalesce((lag(total_starts,51)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l51,
        coalesce((lag(total_starts,52)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l52,
        coalesce((lag(total_starts,53)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l53,
            coalesce((lag(total_starts,54)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l54,
            coalesce((lag(total_starts,55)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l55,
            coalesce((lag(total_starts,56)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l56,
            coalesce((lag(total_starts,57)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l57,
            coalesce((lag(total_starts,58)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l58,
            coalesce((lag(total_starts,59)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l59,
            coalesce((lag(total_starts,60)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l60,
            coalesce((lag(total_starts,61)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l61,
            coalesce((lag(total_starts,62)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l62,
            coalesce((lag(total_starts,63)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l63,
            coalesce((lag(total_starts,64)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l64,
            coalesce((lag(total_starts,65)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l65,
            coalesce((lag(total_starts,66)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l66,
                coalesce((lag(total_starts,67)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l67,
                coalesce((lag(total_starts,68)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l68,
                coalesce((lag(total_starts,69)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l69,
                    coalesce((lag(total_starts,70)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l70,
                    coalesce((lag(total_starts,71)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l71,
                    coalesce((lag(total_starts,72)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l72,
                    coalesce((lag(total_starts,73)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l73, 
        
                        coalesce((lag(total_starts,74)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l74, 
                        coalesce((lag(total_starts,75)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l75, 
                        coalesce((lag(total_starts,76)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l76, 
                        coalesce((lag(total_starts,77)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l77, 
                        coalesce((lag(total_starts,78)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l78, 
                        coalesce((lag(total_starts,79)
        OVER (PARTITION BY subscription_platform ORDER BY Signup_month ASC)),0) AS total_starts_l79, 
        
         from LTV_stg2
         order by 2,1
    )
    
    select 
    subscription_platform,
    sum( Subs_Retained_1) as  Subs_Retained_1,
    sum( Subs_Retained_2) as  Subs_Retained_2,
    sum( Subs_Retained_3) as  Subs_Retained_3,
    sum( Subs_Retained_4) as  Subs_Retained_4,
    sum( Subs_Retained_5) as  Subs_Retained_5,
    sum( Subs_Retained_6) as  Subs_Retained_6,
    sum( Subs_Retained_7) as  Subs_Retained_7,
    sum( Subs_Retained_8) as  Subs_Retained_8,
    sum( Subs_Retained_9) as  Subs_Retained_9,
    sum( Subs_Retained_10) as  Subs_Retained_10,
    sum( Subs_Retained_11) as  Subs_Retained_11,
    sum( Subs_Retained_12) as  Subs_Retained_12,
    
    sum( Subs_Retained_13) as  Subs_Retained_13,
    sum( Subs_Retained_14) as  Subs_Retained_14,
    sum( Subs_Retained_15) as  Subs_Retained_15,
    sum( Subs_Retained_16) as  Subs_Retained_16,
    sum( Subs_Retained_17) as  Subs_Retained_17,
    sum( Subs_Retained_18) as  Subs_Retained_18,
    sum( Subs_Retained_19) as  Subs_Retained_19,
    sum( Subs_Retained_20) as  Subs_Retained_20,
    sum( Subs_Retained_21) as  Subs_Retained_21,
    sum( Subs_Retained_22) as  Subs_Retained_22,
    sum( Subs_Retained_23) as  Subs_Retained_23,
    sum( Subs_Retained_24) as  Subs_Retained_24,
    sum( Subs_Retained_25) as  Subs_Retained_25,
    sum( Subs_Retained_26) as  Subs_Retained_26,
    sum( Subs_Retained_27) as  Subs_Retained_27,
    sum( Subs_Retained_28) as  Subs_Retained_28,
    sum( Subs_Retained_29) as  Subs_Retained_29,
    sum( Subs_Retained_30) as  Subs_Retained_30,
    sum( Subs_Retained_31) as  Subs_Retained_31,
    sum( Subs_Retained_32) as  Subs_Retained_32,
    sum( Subs_Retained_33) as  Subs_Retained_33,
    sum( Subs_Retained_34) as  Subs_Retained_34,
    sum( Subs_Retained_35) as  Subs_Retained_35,
    sum( Subs_Retained_36) as  Subs_Retained_36,
    sum( Subs_Retained_37) as  Subs_Retained_37,
    sum( Subs_Retained_38) as  Subs_Retained_38,
    sum( Subs_Retained_39) as  Subs_Retained_39,
    sum( Subs_Retained_40) as  Subs_Retained_40,
    sum( Subs_Retained_41) as  Subs_Retained_41,
    sum( Subs_Retained_42) as  Subs_Retained_42,
    sum( Subs_Retained_43) as  Subs_Retained_43,
    sum( Subs_Retained_44) as  Subs_Retained_44,
    sum( Subs_Retained_45) as  Subs_Retained_45,
    sum( Subs_Retained_46) as  Subs_Retained_46,
    sum( Subs_Retained_47) as  Subs_Retained_47,
    sum( Subs_Retained_48) as  Subs_Retained_48,
    sum( Subs_Retained_49) as  Subs_Retained_49,
    sum( Subs_Retained_50) as  Subs_Retained_50,
    sum( Subs_Retained_51) as  Subs_Retained_51,
    sum( Subs_Retained_52) as  Subs_Retained_52,
    sum( Subs_Retained_53) as  Subs_Retained_53,
    sum( Subs_Retained_54) as  Subs_Retained_54,
    sum( Subs_Retained_55) as  Subs_Retained_55,
    sum( Subs_Retained_56) as  Subs_Retained_56,
    sum( Subs_Retained_57) as  Subs_Retained_57,
    sum( Subs_Retained_58) as  Subs_Retained_58,
    sum( Subs_Retained_59) as  Subs_Retained_59,
    sum( Subs_Retained_60) as  Subs_Retained_60,
    sum( Subs_Retained_61) as  Subs_Retained_61,
    sum( Subs_Retained_62) as  Subs_Retained_62,
    sum( Subs_Retained_63) as  Subs_Retained_63,
    sum( Subs_Retained_64) as  Subs_Retained_64,
    sum( Subs_Retained_65) as  Subs_Retained_65,
    sum( Subs_Retained_66) as  Subs_Retained_66,
    sum( Subs_Retained_67) as  Subs_Retained_67,
    sum( Subs_Retained_68) as  Subs_Retained_68,
    sum( Subs_Retained_69) as  Subs_Retained_69,
    sum( Subs_Retained_70) as  Subs_Retained_70,
    sum( Subs_Retained_71) as  Subs_Retained_71,
    sum( Subs_Retained_72) as  Subs_Retained_72,
    sum( Subs_Retained_73) as  Subs_Retained_73 ,
    
    sum( Subs_Retained_74) as  Subs_Retained_74,
    sum( Subs_Retained_75) as  Subs_Retained_75,
    sum( Subs_Retained_76) as  Subs_Retained_76,
    sum( Subs_Retained_77) as  Subs_Retained_77,
    sum( Subs_Retained_78) as  Subs_Retained_78,
    sum( Subs_Retained_79) as  Subs_Retained_79
    
    from LTV_stg2
    group by 1
    UNION ALL
    
    select 
    CONCAT(subscription_platform,' - Total Starts'),
    sum( total_starts) as  total_starts,
    sum( total_starts_l1) as  total_starts_l1,
    sum( total_starts_l2) as  total_starts_l2,
    sum( total_starts_l3) as  total_starts_l3,
    sum( total_starts_l4) as  total_starts_l4,
    sum( total_starts_l5) as  total_starts_l5,
    sum( total_starts_l6) as  total_starts_l6,
    sum( total_starts_l7) as  total_starts_l7,
    sum( total_starts_l8) as  total_starts_l8,
    sum( total_starts_l9) as  total_starts_l9,
    sum( total_starts_l10) as  total_starts_l10,
    sum( total_starts_l11) as  total_starts_l11,
    
    sum( total_starts_l12) as  total_starts_l12,
    sum( total_starts_l13) as  total_starts_l13,
    sum( total_starts_l14) as  total_starts_l14,
    sum( total_starts_l15) as  total_starts_l15,
    sum( total_starts_l16) as  total_starts_l16,
    sum( total_starts_l17) as  total_starts_l17,
    sum( total_starts_l18) as  total_starts_l18,
    sum( total_starts_l19) as  total_starts_l19,
    sum( total_starts_l20) as  total_starts_l20,
    sum( total_starts_l21) as  total_starts_l21,
    sum( total_starts_l22) as  total_starts_l22,
    sum( total_starts_l23) as  total_starts_l23,
    sum( total_starts_l24) as  total_starts_l24,
    sum( total_starts_l25) as  total_starts_l25,
    sum( total_starts_l26) as  total_starts_l26,
    sum( total_starts_l27) as  total_starts_l27,
    sum( total_starts_l28) as  total_starts_l28,
    sum( total_starts_l29) as  total_starts_l29,
    sum( total_starts_l30) as  total_starts_l30,
    sum( total_starts_l31) as  total_starts_l31,
    sum( total_starts_l32) as  total_starts_l32,
    sum( total_starts_l33) as  total_starts_l33,
    sum( total_starts_l34) as  total_starts_l34,
    sum( total_starts_l35) as  total_starts_l35,
    sum( total_starts_l36) as  total_starts_l36,
    sum( total_starts_l37) as  total_starts_l37,
    sum( total_starts_l38) as  total_starts_l38,
    sum( total_starts_l39) as  total_starts_l39,
    sum( total_starts_l40) as  total_starts_l40,
    sum( total_starts_l41) as  total_starts_l41,
    sum( total_starts_l42) as  total_starts_l42,
    sum( total_starts_l43) as  total_starts_l43,
    sum( total_starts_l44) as  total_starts_l44,
    sum( total_starts_l45) as  total_starts_l45,
    sum( total_starts_l46) as  total_starts_l46,
    sum( total_starts_l47) as  total_starts_l47,
    sum( total_starts_l48) as  total_starts_l48,
    sum( total_starts_l49) as  total_starts_l49,
    sum( total_starts_l50) as  total_starts_l50,
    sum( total_starts_l51) as  total_starts_l51,
    sum( total_starts_l52) as  total_starts_l52,
    sum( total_starts_l53) as  total_starts_l53,
    sum( total_starts_l54) as  total_starts_l54,
    sum( total_starts_l55) as  total_starts_l55,
    sum( total_starts_l56) as  total_starts_l56,
    sum( total_starts_l57) as  total_starts_l57,
    sum( total_starts_l58) as  total_starts_l58,
    sum( total_starts_l59) as  total_starts_l59,
    sum( total_starts_l60) as  total_starts_l60,
    sum( total_starts_l61) as  total_starts_l61,
    sum( total_starts_l62) as  total_starts_l62,
    sum( total_starts_l63) as  total_starts_l63,
    sum( total_starts_l64) as  total_starts_l64,
    sum( total_starts_l65) as  total_starts_l65,
    sum( total_starts_l66) as  total_starts_l66,
    sum( total_starts_l67) as  total_starts_l67,
    sum( total_starts_l68) as  total_starts_l68,
    sum( total_starts_l69) as  total_starts_l69,
    sum( total_starts_l70) as  total_starts_l70,
    sum( total_starts_l71) as  total_starts_l71,
    sum( total_starts_l72) as  total_starts_l72,
    sum( total_starts_l73) as  total_starts_l73,
    sum( total_starts_l74) as  total_starts_l74,
    sum( total_starts_l75) as  total_starts_l75,
    sum( total_starts_l76) as  total_starts_l76,
    sum( total_starts_l77) as  total_starts_l77,
    sum( total_starts_l78) as  total_starts_l78,
    -- sum( total_starts_l73) as  total_starts_l73
    -- sum( total_starts_l66) as  total_starts_l66,
    
    from LTV_stg3
    group by 1
    order by 1,2
    """
    return query

def billing_partner_signup_plan_without_free_trial():
    query=""" with AA_LTV_sub as  (
 select * from
 (
Select A1.*,
activation_dt as start_dt,
date_trunc( activation_dt, Month) as Signup_month,
cdm_udf.LTV_SIGNUP_PLAN_CALC(signup_plan_cd) as signup_plan,

case when subscription_platform_cd in ('RECURLY','PSP') then 'RECURLY' else subscription_platform_cd end as subscription_platform,

--- clean version of trial period offered
cdm_udf.LTV_TRIAL_PERIOD_CALC(signup_plan_cd, signup_trial_period_desc, sku_cd, trial_start_dt, paid_start_dt) as Trial_Period     


 from `i-dss-cdm-data-dev.ent_vw.subscription_fct` A1
where A1.src_system_id=@src_system_id
and subscription_platform_cd not in ('Apple iOS', 'Apple TV')
and activation_dt<@till_date
-- and lower(signup_plan_cd) not like '%annual%'
)
where Trial_Period NOT in ('1 Month Free','2 Month Free','3 Month Free')
)


, LTV_stg2 as (
select ov.Signup_month,
ov.subscription_platform,
ov.signup_plan,
case when total_starts is null then 0 else total_starts end as total_starts,
Subs_Retained_1 ,
case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 77  MONTH)) then Subs_Retained_2 else 0 end as Subs_Retained_2,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 76  MONTH)) then Subs_Retained_3 else 0 end as Subs_Retained_3,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 75  MONTH)) then Subs_Retained_4 else 0 end as Subs_Retained_4,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 74  MONTH)) then Subs_Retained_5 else 0 end as Subs_Retained_5,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 73  MONTH)) then Subs_Retained_6 else 0 end as Subs_Retained_6,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 72  MONTH)) then Subs_Retained_7 else 0 end as Subs_Retained_7,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 71  MONTH)) then Subs_Retained_8 else 0 end as Subs_Retained_8,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 70  MONTH)) then Subs_Retained_9 else 0 end as Subs_Retained_9,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 69  MONTH)) then Subs_Retained_10 else 0 end as Subs_Retained_10,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 68  MONTH)) then Subs_Retained_11 else 0 end as Subs_Retained_11,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 67  MONTH)) then Subs_Retained_12 else 0 end as Subs_Retained_12,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 66  MONTH)) then Subs_Retained_13 else 0 end as Subs_Retained_13,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 65  MONTH)) then Subs_Retained_14 else 0 end as Subs_Retained_14,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 64  MONTH)) then Subs_Retained_15 else 0 end as Subs_Retained_15,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 63  MONTH)) then Subs_Retained_16 else 0 end as Subs_Retained_16,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 62  MONTH)) then Subs_Retained_17 else 0 end as Subs_Retained_17,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 61  MONTH)) then Subs_Retained_18 else 0 end as Subs_Retained_18,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 60  MONTH)) then Subs_Retained_19 else 0 end as Subs_Retained_19,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 59  MONTH)) then Subs_Retained_20 else 0 end as Subs_Retained_20,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 58  MONTH)) then Subs_Retained_21 else 0 end as Subs_Retained_21,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 57  MONTH)) then Subs_Retained_22 else 0 end as Subs_Retained_22,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 56  MONTH)) then Subs_Retained_23 else 0 end as Subs_Retained_23,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 55  MONTH)) then Subs_Retained_24 else 0 end as Subs_Retained_24,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 54  MONTH)) then Subs_Retained_25 else 0 end as Subs_Retained_25,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 53  MONTH)) then Subs_Retained_26 else 0 end as Subs_Retained_26,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 52  MONTH)) then Subs_Retained_27 else 0 end as Subs_Retained_27,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 51  MONTH)) then Subs_Retained_28 else 0 end as Subs_Retained_28,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 50  MONTH)) then Subs_Retained_29 else 0 end as Subs_Retained_29,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 49  MONTH)) then Subs_Retained_30 else 0 end as Subs_Retained_30,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 48  MONTH)) then Subs_Retained_31 else 0 end as Subs_Retained_31,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 47  MONTH)) then Subs_Retained_32 else 0 end as Subs_Retained_32,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 46  MONTH)) then Subs_Retained_33 else 0 end as Subs_Retained_33,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 45  MONTH)) then Subs_Retained_34 else 0 end as Subs_Retained_34,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 44  MONTH)) then Subs_Retained_35 else 0 end as Subs_Retained_35,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 43  MONTH)) then Subs_Retained_36 else 0 end as Subs_Retained_36,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 42  MONTH)) then Subs_Retained_37 else 0 end as Subs_Retained_37,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 41  MONTH)) then Subs_Retained_38 else 0 end as Subs_Retained_38,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 40  MONTH)) then Subs_Retained_39 else 0 end as Subs_Retained_39,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 39  MONTH)) then Subs_Retained_40 else 0 end as Subs_Retained_40,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 38  MONTH)) then Subs_Retained_41 else 0 end as Subs_Retained_41,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 37  MONTH)) then Subs_Retained_42 else 0 end as Subs_Retained_42,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 36  MONTH)) then Subs_Retained_43 else 0 end as Subs_Retained_43,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 35  MONTH)) then Subs_Retained_44 else 0 end as Subs_Retained_44,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 34  MONTH)) then Subs_Retained_45 else 0 end as Subs_Retained_45,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 33  MONTH)) then Subs_Retained_46 else 0 end as Subs_Retained_46,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 32  MONTH)) then Subs_Retained_47 else 0 end as Subs_Retained_47,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 31  MONTH)) then Subs_Retained_48 else 0 end as Subs_Retained_48,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 30  MONTH)) then Subs_Retained_49 else 0 end as Subs_Retained_49,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 29  MONTH)) then Subs_Retained_50 else 0 end as Subs_Retained_50,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 28  MONTH)) then Subs_Retained_51 else 0 end as Subs_Retained_51,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 27  MONTH)) then Subs_Retained_52 else 0 end as Subs_Retained_52,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 26  MONTH)) then Subs_Retained_53 else 0 end as Subs_Retained_53,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 25  MONTH)) then Subs_Retained_54 else 0 end as Subs_Retained_54,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 24  MONTH)) then Subs_Retained_55 else 0 end as Subs_Retained_55,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 23  MONTH)) then Subs_Retained_56 else 0 end as Subs_Retained_56,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 22  MONTH)) then Subs_Retained_57 else 0 end as Subs_Retained_57,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 21  MONTH)) then Subs_Retained_58 else 0 end as Subs_Retained_58,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 20  MONTH)) then Subs_Retained_59 else 0 end as Subs_Retained_59,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 19  MONTH)) then Subs_Retained_60 else 0 end as Subs_Retained_60,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 18  MONTH)) then Subs_Retained_61 else 0 end as Subs_Retained_61,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 17  MONTH)) then Subs_Retained_62 else 0 end as Subs_Retained_62,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 16  MONTH)) then Subs_Retained_63 else 0 end as Subs_Retained_63,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 15  MONTH)) then Subs_Retained_64 else 0 end as Subs_Retained_64,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 14  MONTH)) then Subs_Retained_65 else 0 end as Subs_Retained_65,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 13  MONTH)) then Subs_Retained_66 else 0 end as Subs_Retained_66,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 12  MONTH)) then Subs_Retained_67 else 0 end as Subs_Retained_67,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 11  MONTH)) then Subs_Retained_68 else 0 end as Subs_Retained_68,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 10  MONTH)) then Subs_Retained_69 else 0 end as Subs_Retained_69,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 9  MONTH)) then Subs_Retained_70 else 0 end as Subs_Retained_70,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 8  MONTH)) then Subs_Retained_71 else 0 end as Subs_Retained_71,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 7  MONTH)) then Subs_Retained_72 else 0 end as Subs_Retained_72,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 6  MONTH)) then Subs_Retained_73 else 0 end as Subs_Retained_73,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 5  MONTH)) then Subs_Retained_74 else 0 end as Subs_Retained_74,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 4  MONTH)) then Subs_Retained_75 else 0 end as Subs_Retained_75,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 3  MONTH)) then Subs_Retained_76 else 0 end as Subs_Retained_76,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL  2   MONTH)) then Subs_Retained_77 else 0 end as Subs_Retained_77,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL  1   MONTH)) then Subs_Retained_78 else 0 end as Subs_Retained_78,
    case when ov.Signup_month<@start_date then Subs_Retained_79 else 0 end as Subs_Retained_79


from
(

select * from
 (

 Select t.*,running_sum as Cum_cancels ,
 begin_paid_subs as starting_paid_subs
 from

  (
 select Signup_month,
subscription_platform,
signup_plan,
 subsequent_month
 from
       (
       select distinct t.Signup_month,subscription_platform,signup_plan from (select r.* from AA_LTV_sub r)  dt
       cross join
	   (select DATE_TRUNC(day_dt,MONTH) as Signup_month from `i-dss-cdm-data-dev.dw_vw.days` where day_dt between @start_date and @end_date) t -- Change the date range here

      )
      cross join
      (
      select s.* from
      (select row_number() over (order by day_dt) subsequent_month from `i-dss-cdm-data-dev.dw_vw.days`) s where subsequent_month <=  (SELECT DATE_DIFF(@end_date,@start_date,MONTH)+2)) sb -- change the subsequent months
      ) t

      left join (
	  select Signup_Month,
    subscription_platform,
    signup_plan,
            subsequent_month as mn,
            SUM (cancels) OVER (PARTITION BY  Signup_month,subscription_platform,signup_plan ORDER BY subsequent_month) AS running_sum
            from (
             select Signup_month,
             subscription_platform,
             signup_plan,
			 case when (Days_actv/30)<=1 then 1 else CEILING(Days_actv/30) end as subsequent_month,
			 sum(users) as cancels
			 from (
             select Signup_month,
             subscription_platform,
             signup_plan,
			 Days_actv,
			 count(distinct subscription_guid ) as users
			 from
                               (
                               select t.*,
                               DATE_DIFF(cast(case when expiration_dt is null OR expiration_dt>=@till_date then @till_date else expiration_dt end as date), cast( start_dt as Date),DAY) +1 as Days_actv
							   from AA_LTV_sub t

                               ) a1
                               where cast(expiration_dt as date)<=@till_date
                               group by Signup_month, subscription_platform,signup_plan,
                               Days_actv) a2
            group by Signup_month,subscription_platform,signup_plan,
            case when (Days_actv/30)<=1 then 1
       else CEILING(Days_actv/30) end
       ) a3
       ) can
       on (t.Signup_month=can.Signup_month
       and t.subsequent_month=can.mn
       and t.subscription_platform=can.subscription_platform
       and t.signup_plan=can.signup_plan)
        left join (
       select Signup_month,
       subscription_platform,signup_plan,
       count(distinct subscription_guid) as begin_paid_subs
                               from
                               (
                               select t.*,
                               DATE_DIFF(cast(case when expiration_dt is null OR expiration_dt>=@till_date then @till_date else expiration_dt end as date), cast( start_dt as Date),DAY) +1 as Days_actv
							   from AA_LTV_sub t

                               ) a1
                               group by Signup_month    ,subscription_platform   ,signup_plan
                               ) a4
                               on (t.Signup_month=a4.Signup_month
                               and t.subscription_platform=a4.subscription_platform
                               and t.signup_plan=a4.signup_plan)
       ) a5
        PIVOT(SUM(starting_paid_subs-Cum_cancels) as subs_retained FOR subsequent_month in
        (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,
        21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,
        39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,
        57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,
        75,76,77,78,79))
       --) a6
       --group by Signup_month,subscription_platform,signup_plan
) ov
left join (
       select Signup_month,
       subscription_platform,
       signup_plan,
       count(distinct subscription_guid ) as total_starts
                               from
                               (
                               select t.*,
                               DATE_DIFF(cast(case when expiration_dt is null OR expiration_dt>=@till_date then @till_date else expiration_dt end as date), cast( start_dt as Date),DAY) +1 as Days_actv
							   from AA_LTV_sub t

                               ) a1
                               group by Signup_month,subscription_platform ,signup_plan
                               ) strt
                               on (ov.Signup_month=strt.Signup_month
                               and ov.subscription_platform=strt.subscription_platform
                               and ov.signup_plan=strt.signup_plan)
order by 2,1

)

-- select * from LTV_stg2
, LTV_stg3 as (
select Signup_month,subscription_platform,
signup_plan,
total_starts,
coalesce((lag(total_starts,1)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l1,
coalesce((lag(total_starts,2)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l2,
    coalesce((lag(total_starts,3)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l3,
    coalesce((lag(total_starts,4)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l4,
    coalesce((lag(total_starts,5)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l5,
    coalesce((lag(total_starts,6)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l6,
    coalesce((lag(total_starts,7)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l7,
    coalesce((lag(total_starts,8)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l8,
    coalesce((lag(total_starts,9)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l9,
    coalesce((lag(total_starts,10)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l10,
    coalesce((lag(total_starts,11)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l11,
    coalesce((lag(total_starts,12)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l12,
    coalesce((lag(total_starts,13)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l13,
    coalesce((lag(total_starts,14)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l14,
    coalesce((lag(total_starts,15)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l15,
    coalesce((lag(total_starts,16)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l16,
    coalesce((lag(total_starts,17)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l17,
    coalesce((lag(total_starts,18)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l18,
    coalesce((lag(total_starts,19)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l19,
    coalesce((lag(total_starts,20)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l20,
    coalesce((lag(total_starts,21)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l21,
    coalesce((lag(total_starts,22)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l22,
    coalesce((lag(total_starts,23)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l23,
    coalesce((lag(total_starts,24)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l24,
    coalesce((lag(total_starts,25)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l25,
    coalesce((lag(total_starts,26)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l26,
    coalesce((lag(total_starts,27)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l27,
    coalesce((lag(total_starts,28)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l28,
    coalesce((lag(total_starts,29)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l29,
    coalesce((lag(total_starts,30)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l30,
    coalesce((lag(total_starts,31)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l31,
    coalesce((lag(total_starts,32)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l32,
    coalesce((lag(total_starts,33)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l33,
    coalesce((lag(total_starts,34)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l34,
    coalesce((lag(total_starts,35)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l35,
    coalesce((lag(total_starts,36)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l36,
    coalesce((lag(total_starts,37)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l37,
    coalesce((lag(total_starts,38)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l38,
    coalesce((lag(total_starts,39)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l39,
    coalesce((lag(total_starts,40)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l40,
    coalesce((lag(total_starts,41)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l41,
    coalesce((lag(total_starts,42)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l42,
    coalesce((lag(total_starts,43)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l43,
    coalesce((lag(total_starts,44)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l44,
    coalesce((lag(total_starts,45)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l45,
    coalesce((lag(total_starts,46)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l46,
    coalesce((lag(total_starts,47)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l47,
    coalesce((lag(total_starts,48)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l48,
    coalesce((lag(total_starts,49)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l49,
    coalesce((lag(total_starts,50)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l50,
    coalesce((lag(total_starts,51)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l51,
    coalesce((lag(total_starts,52)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l52,
    coalesce((lag(total_starts,53)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l53,
        coalesce((lag(total_starts,54)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l54,
        coalesce((lag(total_starts,55)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l55,
        coalesce((lag(total_starts,56)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l56,
        coalesce((lag(total_starts,57)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l57,
        coalesce((lag(total_starts,58)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l58,
        coalesce((lag(total_starts,59)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l59,
        coalesce((lag(total_starts,60)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l60,
        coalesce((lag(total_starts,61)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l61,
        coalesce((lag(total_starts,62)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l62,
        coalesce((lag(total_starts,63)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l63,
        coalesce((lag(total_starts,64)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l64,
        coalesce((lag(total_starts,65)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l65,
        coalesce((lag(total_starts,66)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l66,

        coalesce((lag(total_starts,67)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l67,

        coalesce((lag(total_starts,68)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l68,

        coalesce((lag(total_starts,69)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l69,

       coalesce((lag(total_starts,70)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l70,

       coalesce((lag(total_starts,71)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l71,

       coalesce((lag(total_starts,72)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l72,

       coalesce((lag(total_starts,73)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l73,

                       coalesce((lag(total_starts,74)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l74,
                    coalesce((lag(total_starts,75)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l75,
                    coalesce((lag(total_starts,76)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l76,
                    coalesce((lag(total_starts,77)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l77,
                    coalesce((lag(total_starts,78)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l78,
                    coalesce((lag(total_starts,79)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l79,

     from LTV_stg2
     order by 2,1
),

stagex as (
select
subscription_platform,
signup_plan,
sum( Subs_Retained_1) as  Subs_Retained_1,
sum( Subs_Retained_2) as  Subs_Retained_2,
sum( Subs_Retained_3) as  Subs_Retained_3,
sum( Subs_Retained_4) as  Subs_Retained_4,
sum( Subs_Retained_5) as  Subs_Retained_5,
sum( Subs_Retained_6) as  Subs_Retained_6,
sum( Subs_Retained_7) as  Subs_Retained_7,
sum( Subs_Retained_8) as  Subs_Retained_8,
sum( Subs_Retained_9) as  Subs_Retained_9,
sum( Subs_Retained_10) as  Subs_Retained_10,
sum( Subs_Retained_11) as  Subs_Retained_11,
sum( Subs_Retained_12) as  Subs_Retained_12,

sum( Subs_Retained_13) as  Subs_Retained_13,
sum( Subs_Retained_14) as  Subs_Retained_14,
sum( Subs_Retained_15) as  Subs_Retained_15,
sum( Subs_Retained_16) as  Subs_Retained_16,
sum( Subs_Retained_17) as  Subs_Retained_17,
sum( Subs_Retained_18) as  Subs_Retained_18,
sum( Subs_Retained_19) as  Subs_Retained_19,
sum( Subs_Retained_20) as  Subs_Retained_20,
sum( Subs_Retained_21) as  Subs_Retained_21,
sum( Subs_Retained_22) as  Subs_Retained_22,
sum( Subs_Retained_23) as  Subs_Retained_23,
sum( Subs_Retained_24) as  Subs_Retained_24,
sum( Subs_Retained_25) as  Subs_Retained_25,
sum( Subs_Retained_26) as  Subs_Retained_26,
sum( Subs_Retained_27) as  Subs_Retained_27,
sum( Subs_Retained_28) as  Subs_Retained_28,
sum( Subs_Retained_29) as  Subs_Retained_29,
sum( Subs_Retained_30) as  Subs_Retained_30,
sum( Subs_Retained_31) as  Subs_Retained_31,
sum( Subs_Retained_32) as  Subs_Retained_32,
sum( Subs_Retained_33) as  Subs_Retained_33,
sum( Subs_Retained_34) as  Subs_Retained_34,
sum( Subs_Retained_35) as  Subs_Retained_35,
sum( Subs_Retained_36) as  Subs_Retained_36,
sum( Subs_Retained_37) as  Subs_Retained_37,
sum( Subs_Retained_38) as  Subs_Retained_38,
sum( Subs_Retained_39) as  Subs_Retained_39,
sum( Subs_Retained_40) as  Subs_Retained_40,
sum( Subs_Retained_41) as  Subs_Retained_41,
sum( Subs_Retained_42) as  Subs_Retained_42,
sum( Subs_Retained_43) as  Subs_Retained_43,
sum( Subs_Retained_44) as  Subs_Retained_44,
sum( Subs_Retained_45) as  Subs_Retained_45,
sum( Subs_Retained_46) as  Subs_Retained_46,
sum( Subs_Retained_47) as  Subs_Retained_47,
sum( Subs_Retained_48) as  Subs_Retained_48,
sum( Subs_Retained_49) as  Subs_Retained_49,
sum( Subs_Retained_50) as  Subs_Retained_50,
sum( Subs_Retained_51) as  Subs_Retained_51,
sum( Subs_Retained_52) as  Subs_Retained_52,
sum( Subs_Retained_53) as  Subs_Retained_53,
sum( Subs_Retained_54) as  Subs_Retained_54,
sum( Subs_Retained_55) as  Subs_Retained_55,
sum( Subs_Retained_56) as  Subs_Retained_56,
sum( Subs_Retained_57) as  Subs_Retained_57,
sum( Subs_Retained_58) as  Subs_Retained_58,
sum( Subs_Retained_59) as  Subs_Retained_59,
sum( Subs_Retained_60) as  Subs_Retained_60,
sum( Subs_Retained_61) as  Subs_Retained_61,
sum( Subs_Retained_62) as  Subs_Retained_62,
sum( Subs_Retained_63) as  Subs_Retained_63,
sum( Subs_Retained_64) as  Subs_Retained_64,
sum( Subs_Retained_65) as  Subs_Retained_65,
sum( Subs_Retained_66) as  Subs_Retained_66,
sum( Subs_Retained_67) as  Subs_Retained_67,
sum( Subs_Retained_68) as  Subs_Retained_68,
sum( Subs_Retained_69) as  Subs_Retained_69,
sum( Subs_Retained_70) as  Subs_Retained_70,
sum( Subs_Retained_71) as  Subs_Retained_71,
sum( Subs_Retained_72) as  Subs_Retained_72,
sum( Subs_Retained_73) as  Subs_Retained_73 ,
sum( Subs_Retained_74) as  Subs_Retained_74,
sum( Subs_Retained_75) as  Subs_Retained_75,
sum( Subs_Retained_76) as  Subs_Retained_76,
sum( Subs_Retained_77) as  Subs_Retained_77,
sum( Subs_Retained_78) as  Subs_Retained_78,
sum( Subs_Retained_79) as  Subs_Retained_79

from LTV_stg2
group by 1,2
UNION ALL

select
--CONCAT(subscription_platform,' - Total Starts'),
subscription_platform,
signup_plan,
sum( total_starts) as  total_starts,
sum( total_starts_l1) as  total_starts_l1,
sum( total_starts_l2) as  total_starts_l2,
sum( total_starts_l3) as  total_starts_l3,
sum( total_starts_l4) as  total_starts_l4,
sum( total_starts_l5) as  total_starts_l5,
sum( total_starts_l6) as  total_starts_l6,
sum( total_starts_l7) as  total_starts_l7,
sum( total_starts_l8) as  total_starts_l8,
sum( total_starts_l9) as  total_starts_l9,
sum( total_starts_l10) as  total_starts_l10,
sum( total_starts_l11) as  total_starts_l11,

sum( total_starts_l12) as  total_starts_l12,
sum( total_starts_l13) as  total_starts_l13,
sum( total_starts_l14) as  total_starts_l14,
sum( total_starts_l15) as  total_starts_l15,
sum( total_starts_l16) as  total_starts_l16,
sum( total_starts_l17) as  total_starts_l17,
sum( total_starts_l18) as  total_starts_l18,
sum( total_starts_l19) as  total_starts_l19,
sum( total_starts_l20) as  total_starts_l20,
sum( total_starts_l21) as  total_starts_l21,
sum( total_starts_l22) as  total_starts_l22,
sum( total_starts_l23) as  total_starts_l23,
sum( total_starts_l24) as  total_starts_l24,
sum( total_starts_l25) as  total_starts_l25,
sum( total_starts_l26) as  total_starts_l26,
sum( total_starts_l27) as  total_starts_l27,
sum( total_starts_l28) as  total_starts_l28,
sum( total_starts_l29) as  total_starts_l29,
sum( total_starts_l30) as  total_starts_l30,
sum( total_starts_l31) as  total_starts_l31,
sum( total_starts_l32) as  total_starts_l32,
sum( total_starts_l33) as  total_starts_l33,
sum( total_starts_l34) as  total_starts_l34,
sum( total_starts_l35) as  total_starts_l35,
sum( total_starts_l36) as  total_starts_l36,
sum( total_starts_l37) as  total_starts_l37,
sum( total_starts_l38) as  total_starts_l38,
sum( total_starts_l39) as  total_starts_l39,
sum( total_starts_l40) as  total_starts_l40,
sum( total_starts_l41) as  total_starts_l41,
sum( total_starts_l42) as  total_starts_l42,
sum( total_starts_l43) as  total_starts_l43,
sum( total_starts_l44) as  total_starts_l44,
sum( total_starts_l45) as  total_starts_l45,
sum( total_starts_l46) as  total_starts_l46,
sum( total_starts_l47) as  total_starts_l47,
sum( total_starts_l48) as  total_starts_l48,
sum( total_starts_l49) as  total_starts_l49,
sum( total_starts_l50) as  total_starts_l50,
sum( total_starts_l51) as  total_starts_l51,
sum( total_starts_l52) as  total_starts_l52,
sum( total_starts_l53) as  total_starts_l53,
sum( total_starts_l54) as  total_starts_l54,
sum( total_starts_l55) as  total_starts_l55,
sum( total_starts_l56) as  total_starts_l56,
sum( total_starts_l57) as  total_starts_l57,
sum( total_starts_l58) as  total_starts_l58,
sum( total_starts_l59) as  total_starts_l59,
sum( total_starts_l60) as  total_starts_l60,
sum( total_starts_l61) as  total_starts_l61,
sum( total_starts_l62) as  total_starts_l62,
sum( total_starts_l63) as  total_starts_l63,
sum( total_starts_l64) as  total_starts_l64,
sum( total_starts_l65) as  total_starts_l65,
sum( total_starts_l66) as  total_starts_l66,
sum( total_starts_l67) as  total_starts_l67,
sum( total_starts_l68) as  total_starts_l68,
sum( total_starts_l69) as  total_starts_l69,
sum( total_starts_l70) as  total_starts_l70,
sum( total_starts_l71) as  total_starts_l71,
sum( total_starts_l72) as  total_starts_l72,
sum( total_starts_l73) as  total_starts_l73,
sum( total_starts_l74) as  total_starts_l74,
sum( total_starts_l75) as  total_starts_l75,
sum( total_starts_l76) as  total_starts_l76,
sum( total_starts_l77) as  total_starts_l77,
sum( total_starts_l78) as  total_starts_l78


from LTV_stg3
group by 1,2
order by 1,2,3
)

select * from stagex
where lower(signup_plan) not like '%annual%'"""
    return query

def billing_partner_signup_plan_with_free_trial():
    query=""" with AA_LTV_sub as  (
 select * from
 (
Select A1.*,
activation_dt as start_dt,
date_trunc( activation_dt, Month) as Signup_month,
cdm_udf.LTV_SIGNUP_PLAN_CALC(signup_plan_cd) as signup_plan,

case when subscription_platform_cd in ('RECURLY','PSP') then 'RECURLY' else subscription_platform_cd end as subscription_platform,

--- clean version of trial period offered
cdm_udf.LTV_TRIAL_PERIOD_CALC(signup_plan_cd, signup_trial_period_desc, sku_cd, trial_start_dt, paid_start_dt) as Trial_Period     


 from `i-dss-cdm-data-dev.ent_vw.subscription_fct` A1
where A1.src_system_id=@src_system_id
and subscription_platform_cd not in ('Apple iOS', 'Apple TV')
and activation_dt<@till_date
-- and lower(signup_plan_cd) not like '%annual%'
)
where Trial_Period  in ('1 Month Free','2 Month Free','3 Month Free')
)


, LTV_stg2 as (
select ov.Signup_month,
ov.subscription_platform,
ov.signup_plan,
case when total_starts is null then 0 else total_starts end as total_starts,
Subs_Retained_1 ,
case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 77  MONTH)) then Subs_Retained_2 else 0 end as Subs_Retained_2,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 76  MONTH)) then Subs_Retained_3 else 0 end as Subs_Retained_3,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 75  MONTH)) then Subs_Retained_4 else 0 end as Subs_Retained_4,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 74  MONTH)) then Subs_Retained_5 else 0 end as Subs_Retained_5,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 73  MONTH)) then Subs_Retained_6 else 0 end as Subs_Retained_6,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 72  MONTH)) then Subs_Retained_7 else 0 end as Subs_Retained_7,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 71  MONTH)) then Subs_Retained_8 else 0 end as Subs_Retained_8,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 70  MONTH)) then Subs_Retained_9 else 0 end as Subs_Retained_9,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 69  MONTH)) then Subs_Retained_10 else 0 end as Subs_Retained_10,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 68  MONTH)) then Subs_Retained_11 else 0 end as Subs_Retained_11,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 67  MONTH)) then Subs_Retained_12 else 0 end as Subs_Retained_12,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 66  MONTH)) then Subs_Retained_13 else 0 end as Subs_Retained_13,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 65  MONTH)) then Subs_Retained_14 else 0 end as Subs_Retained_14,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 64  MONTH)) then Subs_Retained_15 else 0 end as Subs_Retained_15,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 63  MONTH)) then Subs_Retained_16 else 0 end as Subs_Retained_16,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 62  MONTH)) then Subs_Retained_17 else 0 end as Subs_Retained_17,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 61  MONTH)) then Subs_Retained_18 else 0 end as Subs_Retained_18,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 60  MONTH)) then Subs_Retained_19 else 0 end as Subs_Retained_19,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 59  MONTH)) then Subs_Retained_20 else 0 end as Subs_Retained_20,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 58  MONTH)) then Subs_Retained_21 else 0 end as Subs_Retained_21,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 57  MONTH)) then Subs_Retained_22 else 0 end as Subs_Retained_22,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 56  MONTH)) then Subs_Retained_23 else 0 end as Subs_Retained_23,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 55  MONTH)) then Subs_Retained_24 else 0 end as Subs_Retained_24,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 54  MONTH)) then Subs_Retained_25 else 0 end as Subs_Retained_25,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 53  MONTH)) then Subs_Retained_26 else 0 end as Subs_Retained_26,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 52  MONTH)) then Subs_Retained_27 else 0 end as Subs_Retained_27,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 51  MONTH)) then Subs_Retained_28 else 0 end as Subs_Retained_28,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 50  MONTH)) then Subs_Retained_29 else 0 end as Subs_Retained_29,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 49  MONTH)) then Subs_Retained_30 else 0 end as Subs_Retained_30,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 48  MONTH)) then Subs_Retained_31 else 0 end as Subs_Retained_31,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 47  MONTH)) then Subs_Retained_32 else 0 end as Subs_Retained_32,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 46  MONTH)) then Subs_Retained_33 else 0 end as Subs_Retained_33,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 45  MONTH)) then Subs_Retained_34 else 0 end as Subs_Retained_34,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 44  MONTH)) then Subs_Retained_35 else 0 end as Subs_Retained_35,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 43  MONTH)) then Subs_Retained_36 else 0 end as Subs_Retained_36,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 42  MONTH)) then Subs_Retained_37 else 0 end as Subs_Retained_37,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 41  MONTH)) then Subs_Retained_38 else 0 end as Subs_Retained_38,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 40  MONTH)) then Subs_Retained_39 else 0 end as Subs_Retained_39,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 39  MONTH)) then Subs_Retained_40 else 0 end as Subs_Retained_40,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 38  MONTH)) then Subs_Retained_41 else 0 end as Subs_Retained_41,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 37  MONTH)) then Subs_Retained_42 else 0 end as Subs_Retained_42,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 36  MONTH)) then Subs_Retained_43 else 0 end as Subs_Retained_43,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 35  MONTH)) then Subs_Retained_44 else 0 end as Subs_Retained_44,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 34  MONTH)) then Subs_Retained_45 else 0 end as Subs_Retained_45,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 33  MONTH)) then Subs_Retained_46 else 0 end as Subs_Retained_46,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 32  MONTH)) then Subs_Retained_47 else 0 end as Subs_Retained_47,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 31  MONTH)) then Subs_Retained_48 else 0 end as Subs_Retained_48,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 30  MONTH)) then Subs_Retained_49 else 0 end as Subs_Retained_49,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 29  MONTH)) then Subs_Retained_50 else 0 end as Subs_Retained_50,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 28  MONTH)) then Subs_Retained_51 else 0 end as Subs_Retained_51,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 27  MONTH)) then Subs_Retained_52 else 0 end as Subs_Retained_52,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 26  MONTH)) then Subs_Retained_53 else 0 end as Subs_Retained_53,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 25  MONTH)) then Subs_Retained_54 else 0 end as Subs_Retained_54,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 24  MONTH)) then Subs_Retained_55 else 0 end as Subs_Retained_55,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 23  MONTH)) then Subs_Retained_56 else 0 end as Subs_Retained_56,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 22  MONTH)) then Subs_Retained_57 else 0 end as Subs_Retained_57,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 21  MONTH)) then Subs_Retained_58 else 0 end as Subs_Retained_58,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 20  MONTH)) then Subs_Retained_59 else 0 end as Subs_Retained_59,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 19  MONTH)) then Subs_Retained_60 else 0 end as Subs_Retained_60,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 18  MONTH)) then Subs_Retained_61 else 0 end as Subs_Retained_61,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 17  MONTH)) then Subs_Retained_62 else 0 end as Subs_Retained_62,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 16  MONTH)) then Subs_Retained_63 else 0 end as Subs_Retained_63,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 15  MONTH)) then Subs_Retained_64 else 0 end as Subs_Retained_64,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 14  MONTH)) then Subs_Retained_65 else 0 end as Subs_Retained_65,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 13  MONTH)) then Subs_Retained_66 else 0 end as Subs_Retained_66,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 12  MONTH)) then Subs_Retained_67 else 0 end as Subs_Retained_67,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 11  MONTH)) then Subs_Retained_68 else 0 end as Subs_Retained_68,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 10  MONTH)) then Subs_Retained_69 else 0 end as Subs_Retained_69,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 9  MONTH)) then Subs_Retained_70 else 0 end as Subs_Retained_70,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 8  MONTH)) then Subs_Retained_71 else 0 end as Subs_Retained_71,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 7  MONTH)) then Subs_Retained_72 else 0 end as Subs_Retained_72,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 6  MONTH)) then Subs_Retained_73 else 0 end as Subs_Retained_73,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 5  MONTH)) then Subs_Retained_74 else 0 end as Subs_Retained_74,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 4  MONTH)) then Subs_Retained_75 else 0 end as Subs_Retained_75,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL 3  MONTH)) then Subs_Retained_76 else 0 end as Subs_Retained_76,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL  2   MONTH)) then Subs_Retained_77 else 0 end as Subs_Retained_77,
    case when ov.Signup_month< (SELECT DATE_ADD(@start_date, INTERVAL  1   MONTH)) then Subs_Retained_78 else 0 end as Subs_Retained_78,
    case when ov.Signup_month<@start_date then Subs_Retained_79 else 0 end as Subs_Retained_79


from
(

select * from
 (

 Select t.*,running_sum as Cum_cancels ,
 begin_paid_subs as starting_paid_subs
 from

  (
 select Signup_month,
subscription_platform,
signup_plan,
 subsequent_month
 from
       (
       select distinct t.Signup_month,subscription_platform,signup_plan from (select r.* from AA_LTV_sub r)  dt
       cross join
	   (select DATE_TRUNC(day_dt,MONTH) as Signup_month from `i-dss-cdm-data-dev.dw_vw.days` where day_dt between @start_date and @end_date) t -- Change the date range here

      )
      cross join
      (
      select s.* from
      (select row_number() over (order by day_dt) subsequent_month from `i-dss-cdm-data-dev.dw_vw.days`) s where subsequent_month <=  (SELECT DATE_DIFF(@end_date,@start_date,MONTH)+2)) sb -- change the subsequent months
      ) t

      left join (
	  select Signup_Month,
    subscription_platform,
    signup_plan,
            subsequent_month as mn,
            SUM (cancels) OVER (PARTITION BY  Signup_month,subscription_platform,signup_plan ORDER BY subsequent_month) AS running_sum
            from (
             select Signup_month,
             subscription_platform,
             signup_plan,
			 case when (Days_actv/30)<=1 then 1 else CEILING(Days_actv/30) end as subsequent_month,
			 sum(users) as cancels
			 from (
             select Signup_month,
             subscription_platform,
             signup_plan,
			 Days_actv,
			 count(distinct subscription_guid ) as users
			 from
                               (
                               select t.*,
                               DATE_DIFF(cast(case when expiration_dt is null OR expiration_dt>=@till_date then @till_date else expiration_dt end as date), cast( start_dt as Date),DAY) +1 as Days_actv
							   from AA_LTV_sub t

                               ) a1
                               where cast(expiration_dt as date)<=@till_date
                               group by Signup_month, subscription_platform,signup_plan,
                               Days_actv) a2
            group by Signup_month,subscription_platform,signup_plan,
            case when (Days_actv/30)<=1 then 1
       else CEILING(Days_actv/30) end
       ) a3
       ) can
       on (t.Signup_month=can.Signup_month
       and t.subsequent_month=can.mn
       and t.subscription_platform=can.subscription_platform
       and t.signup_plan=can.signup_plan)
        left join (
       select Signup_month,
       subscription_platform,signup_plan,
       count(distinct subscription_guid) as begin_paid_subs
                               from
                               (
                               select t.*,
                               DATE_DIFF(cast(case when expiration_dt is null OR expiration_dt>=@till_date then @till_date else expiration_dt end as date), cast( start_dt as Date),DAY) +1 as Days_actv
							   from AA_LTV_sub t

                               ) a1
                               group by Signup_month    ,subscription_platform   ,signup_plan
                               ) a4
                               on (t.Signup_month=a4.Signup_month
                               and t.subscription_platform=a4.subscription_platform
                               and t.signup_plan=a4.signup_plan)
       ) a5
        PIVOT(SUM(starting_paid_subs-Cum_cancels) as subs_retained FOR subsequent_month in
        (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,
        21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,
        39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,
        57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,
        75,76,77,78,79))
       --) a6
       --group by Signup_month,subscription_platform,signup_plan
) ov
left join (
       select Signup_month,
       subscription_platform,
       signup_plan,
       count(distinct subscription_guid ) as total_starts
                               from
                               (
                               select t.*,
                               DATE_DIFF(cast(case when expiration_dt is null OR expiration_dt>=@till_date then @till_date else expiration_dt end as date), cast( start_dt as Date),DAY) +1 as Days_actv
							   from AA_LTV_sub t

                               ) a1
                               group by Signup_month,subscription_platform ,signup_plan
                               ) strt
                               on (ov.Signup_month=strt.Signup_month
                               and ov.subscription_platform=strt.subscription_platform
                               and ov.signup_plan=strt.signup_plan)
order by 2,1

)

-- select * from LTV_stg2
, LTV_stg3 as (
select Signup_month,subscription_platform,
signup_plan,
total_starts,
coalesce((lag(total_starts,1)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l1,
coalesce((lag(total_starts,2)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l2,
    coalesce((lag(total_starts,3)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l3,
    coalesce((lag(total_starts,4)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l4,
    coalesce((lag(total_starts,5)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l5,
    coalesce((lag(total_starts,6)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l6,
    coalesce((lag(total_starts,7)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l7,
    coalesce((lag(total_starts,8)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l8,
    coalesce((lag(total_starts,9)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l9,
    coalesce((lag(total_starts,10)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l10,
    coalesce((lag(total_starts,11)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l11,
    coalesce((lag(total_starts,12)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l12,
    coalesce((lag(total_starts,13)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l13,
    coalesce((lag(total_starts,14)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l14,
    coalesce((lag(total_starts,15)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l15,
    coalesce((lag(total_starts,16)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l16,
    coalesce((lag(total_starts,17)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l17,
    coalesce((lag(total_starts,18)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l18,
    coalesce((lag(total_starts,19)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l19,
    coalesce((lag(total_starts,20)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l20,
    coalesce((lag(total_starts,21)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l21,
    coalesce((lag(total_starts,22)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l22,
    coalesce((lag(total_starts,23)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l23,
    coalesce((lag(total_starts,24)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l24,
    coalesce((lag(total_starts,25)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l25,
    coalesce((lag(total_starts,26)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l26,
    coalesce((lag(total_starts,27)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l27,
    coalesce((lag(total_starts,28)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l28,
    coalesce((lag(total_starts,29)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l29,
    coalesce((lag(total_starts,30)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l30,
    coalesce((lag(total_starts,31)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l31,
    coalesce((lag(total_starts,32)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l32,
    coalesce((lag(total_starts,33)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l33,
    coalesce((lag(total_starts,34)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l34,
    coalesce((lag(total_starts,35)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l35,
    coalesce((lag(total_starts,36)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l36,
    coalesce((lag(total_starts,37)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l37,
    coalesce((lag(total_starts,38)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l38,
    coalesce((lag(total_starts,39)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l39,
    coalesce((lag(total_starts,40)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l40,
    coalesce((lag(total_starts,41)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l41,
    coalesce((lag(total_starts,42)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l42,
    coalesce((lag(total_starts,43)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l43,
    coalesce((lag(total_starts,44)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l44,
    coalesce((lag(total_starts,45)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l45,
    coalesce((lag(total_starts,46)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l46,
    coalesce((lag(total_starts,47)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l47,
    coalesce((lag(total_starts,48)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l48,
    coalesce((lag(total_starts,49)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l49,
    coalesce((lag(total_starts,50)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l50,
    coalesce((lag(total_starts,51)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l51,
    coalesce((lag(total_starts,52)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l52,
    coalesce((lag(total_starts,53)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l53,
        coalesce((lag(total_starts,54)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l54,
        coalesce((lag(total_starts,55)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l55,
        coalesce((lag(total_starts,56)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l56,
        coalesce((lag(total_starts,57)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l57,
        coalesce((lag(total_starts,58)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l58,
        coalesce((lag(total_starts,59)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l59,
        coalesce((lag(total_starts,60)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l60,
        coalesce((lag(total_starts,61)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l61,
        coalesce((lag(total_starts,62)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l62,
        coalesce((lag(total_starts,63)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l63,
        coalesce((lag(total_starts,64)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l64,
        coalesce((lag(total_starts,65)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l65,
        coalesce((lag(total_starts,66)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l66,

        coalesce((lag(total_starts,67)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l67,

        coalesce((lag(total_starts,68)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l68,

        coalesce((lag(total_starts,69)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l69,

       coalesce((lag(total_starts,70)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l70,

       coalesce((lag(total_starts,71)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l71,

       coalesce((lag(total_starts,72)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l72,

       coalesce((lag(total_starts,73)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l73,

                       coalesce((lag(total_starts,74)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l74,
                    coalesce((lag(total_starts,75)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l75,
                    coalesce((lag(total_starts,76)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l76,
                    coalesce((lag(total_starts,77)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l77,
                    coalesce((lag(total_starts,78)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l78,
                    coalesce((lag(total_starts,79)
    OVER (PARTITION BY subscription_platform,signup_plan ORDER BY Signup_month ASC)),0) AS total_starts_l79,

     from LTV_stg2
     order by 2,1
),

stagex as (
select
subscription_platform,
signup_plan,
sum( Subs_Retained_1) as  Subs_Retained_1,
sum( Subs_Retained_2) as  Subs_Retained_2,
sum( Subs_Retained_3) as  Subs_Retained_3,
sum( Subs_Retained_4) as  Subs_Retained_4,
sum( Subs_Retained_5) as  Subs_Retained_5,
sum( Subs_Retained_6) as  Subs_Retained_6,
sum( Subs_Retained_7) as  Subs_Retained_7,
sum( Subs_Retained_8) as  Subs_Retained_8,
sum( Subs_Retained_9) as  Subs_Retained_9,
sum( Subs_Retained_10) as  Subs_Retained_10,
sum( Subs_Retained_11) as  Subs_Retained_11,
sum( Subs_Retained_12) as  Subs_Retained_12,

sum( Subs_Retained_13) as  Subs_Retained_13,
sum( Subs_Retained_14) as  Subs_Retained_14,
sum( Subs_Retained_15) as  Subs_Retained_15,
sum( Subs_Retained_16) as  Subs_Retained_16,
sum( Subs_Retained_17) as  Subs_Retained_17,
sum( Subs_Retained_18) as  Subs_Retained_18,
sum( Subs_Retained_19) as  Subs_Retained_19,
sum( Subs_Retained_20) as  Subs_Retained_20,
sum( Subs_Retained_21) as  Subs_Retained_21,
sum( Subs_Retained_22) as  Subs_Retained_22,
sum( Subs_Retained_23) as  Subs_Retained_23,
sum( Subs_Retained_24) as  Subs_Retained_24,
sum( Subs_Retained_25) as  Subs_Retained_25,
sum( Subs_Retained_26) as  Subs_Retained_26,
sum( Subs_Retained_27) as  Subs_Retained_27,
sum( Subs_Retained_28) as  Subs_Retained_28,
sum( Subs_Retained_29) as  Subs_Retained_29,
sum( Subs_Retained_30) as  Subs_Retained_30,
sum( Subs_Retained_31) as  Subs_Retained_31,
sum( Subs_Retained_32) as  Subs_Retained_32,
sum( Subs_Retained_33) as  Subs_Retained_33,
sum( Subs_Retained_34) as  Subs_Retained_34,
sum( Subs_Retained_35) as  Subs_Retained_35,
sum( Subs_Retained_36) as  Subs_Retained_36,
sum( Subs_Retained_37) as  Subs_Retained_37,
sum( Subs_Retained_38) as  Subs_Retained_38,
sum( Subs_Retained_39) as  Subs_Retained_39,
sum( Subs_Retained_40) as  Subs_Retained_40,
sum( Subs_Retained_41) as  Subs_Retained_41,
sum( Subs_Retained_42) as  Subs_Retained_42,
sum( Subs_Retained_43) as  Subs_Retained_43,
sum( Subs_Retained_44) as  Subs_Retained_44,
sum( Subs_Retained_45) as  Subs_Retained_45,
sum( Subs_Retained_46) as  Subs_Retained_46,
sum( Subs_Retained_47) as  Subs_Retained_47,
sum( Subs_Retained_48) as  Subs_Retained_48,
sum( Subs_Retained_49) as  Subs_Retained_49,
sum( Subs_Retained_50) as  Subs_Retained_50,
sum( Subs_Retained_51) as  Subs_Retained_51,
sum( Subs_Retained_52) as  Subs_Retained_52,
sum( Subs_Retained_53) as  Subs_Retained_53,
sum( Subs_Retained_54) as  Subs_Retained_54,
sum( Subs_Retained_55) as  Subs_Retained_55,
sum( Subs_Retained_56) as  Subs_Retained_56,
sum( Subs_Retained_57) as  Subs_Retained_57,
sum( Subs_Retained_58) as  Subs_Retained_58,
sum( Subs_Retained_59) as  Subs_Retained_59,
sum( Subs_Retained_60) as  Subs_Retained_60,
sum( Subs_Retained_61) as  Subs_Retained_61,
sum( Subs_Retained_62) as  Subs_Retained_62,
sum( Subs_Retained_63) as  Subs_Retained_63,
sum( Subs_Retained_64) as  Subs_Retained_64,
sum( Subs_Retained_65) as  Subs_Retained_65,
sum( Subs_Retained_66) as  Subs_Retained_66,
sum( Subs_Retained_67) as  Subs_Retained_67,
sum( Subs_Retained_68) as  Subs_Retained_68,
sum( Subs_Retained_69) as  Subs_Retained_69,
sum( Subs_Retained_70) as  Subs_Retained_70,
sum( Subs_Retained_71) as  Subs_Retained_71,
sum( Subs_Retained_72) as  Subs_Retained_72,
sum( Subs_Retained_73) as  Subs_Retained_73 ,
sum( Subs_Retained_74) as  Subs_Retained_74,
sum( Subs_Retained_75) as  Subs_Retained_75,
sum( Subs_Retained_76) as  Subs_Retained_76,
sum( Subs_Retained_77) as  Subs_Retained_77,
sum( Subs_Retained_78) as  Subs_Retained_78,
sum( Subs_Retained_79) as  Subs_Retained_79

from LTV_stg2
group by 1,2
UNION ALL

select
--CONCAT(subscription_platform,' - Total Starts'),
subscription_platform,
signup_plan,
sum( total_starts) as  total_starts,
sum( total_starts_l1) as  total_starts_l1,
sum( total_starts_l2) as  total_starts_l2,
sum( total_starts_l3) as  total_starts_l3,
sum( total_starts_l4) as  total_starts_l4,
sum( total_starts_l5) as  total_starts_l5,
sum( total_starts_l6) as  total_starts_l6,
sum( total_starts_l7) as  total_starts_l7,
sum( total_starts_l8) as  total_starts_l8,
sum( total_starts_l9) as  total_starts_l9,
sum( total_starts_l10) as  total_starts_l10,
sum( total_starts_l11) as  total_starts_l11,

sum( total_starts_l12) as  total_starts_l12,
sum( total_starts_l13) as  total_starts_l13,
sum( total_starts_l14) as  total_starts_l14,
sum( total_starts_l15) as  total_starts_l15,
sum( total_starts_l16) as  total_starts_l16,
sum( total_starts_l17) as  total_starts_l17,
sum( total_starts_l18) as  total_starts_l18,
sum( total_starts_l19) as  total_starts_l19,
sum( total_starts_l20) as  total_starts_l20,
sum( total_starts_l21) as  total_starts_l21,
sum( total_starts_l22) as  total_starts_l22,
sum( total_starts_l23) as  total_starts_l23,
sum( total_starts_l24) as  total_starts_l24,
sum( total_starts_l25) as  total_starts_l25,
sum( total_starts_l26) as  total_starts_l26,
sum( total_starts_l27) as  total_starts_l27,
sum( total_starts_l28) as  total_starts_l28,
sum( total_starts_l29) as  total_starts_l29,
sum( total_starts_l30) as  total_starts_l30,
sum( total_starts_l31) as  total_starts_l31,
sum( total_starts_l32) as  total_starts_l32,
sum( total_starts_l33) as  total_starts_l33,
sum( total_starts_l34) as  total_starts_l34,
sum( total_starts_l35) as  total_starts_l35,
sum( total_starts_l36) as  total_starts_l36,
sum( total_starts_l37) as  total_starts_l37,
sum( total_starts_l38) as  total_starts_l38,
sum( total_starts_l39) as  total_starts_l39,
sum( total_starts_l40) as  total_starts_l40,
sum( total_starts_l41) as  total_starts_l41,
sum( total_starts_l42) as  total_starts_l42,
sum( total_starts_l43) as  total_starts_l43,
sum( total_starts_l44) as  total_starts_l44,
sum( total_starts_l45) as  total_starts_l45,
sum( total_starts_l46) as  total_starts_l46,
sum( total_starts_l47) as  total_starts_l47,
sum( total_starts_l48) as  total_starts_l48,
sum( total_starts_l49) as  total_starts_l49,
sum( total_starts_l50) as  total_starts_l50,
sum( total_starts_l51) as  total_starts_l51,
sum( total_starts_l52) as  total_starts_l52,
sum( total_starts_l53) as  total_starts_l53,
sum( total_starts_l54) as  total_starts_l54,
sum( total_starts_l55) as  total_starts_l55,
sum( total_starts_l56) as  total_starts_l56,
sum( total_starts_l57) as  total_starts_l57,
sum( total_starts_l58) as  total_starts_l58,
sum( total_starts_l59) as  total_starts_l59,
sum( total_starts_l60) as  total_starts_l60,
sum( total_starts_l61) as  total_starts_l61,
sum( total_starts_l62) as  total_starts_l62,
sum( total_starts_l63) as  total_starts_l63,
sum( total_starts_l64) as  total_starts_l64,
sum( total_starts_l65) as  total_starts_l65,
sum( total_starts_l66) as  total_starts_l66,
sum( total_starts_l67) as  total_starts_l67,
sum( total_starts_l68) as  total_starts_l68,
sum( total_starts_l69) as  total_starts_l69,
sum( total_starts_l70) as  total_starts_l70,
sum( total_starts_l71) as  total_starts_l71,
sum( total_starts_l72) as  total_starts_l72,
sum( total_starts_l73) as  total_starts_l73,
sum( total_starts_l74) as  total_starts_l74,
sum( total_starts_l75) as  total_starts_l75,
sum( total_starts_l76) as  total_starts_l76,
sum( total_starts_l77) as  total_starts_l77,
sum( total_starts_l78) as  total_starts_l78


from LTV_stg3
group by 1,2
order by 1,2,3
)

select * from stagex
where lower(signup_plan) not like '%annual%'"""
    return query

def annual_plan():
    query="""
    with Annual_pla_subs as  
    (
    select A1.*,
    --coalesce(Num_Paid_transaction1,0) as Num_Paid_transaction_sprt,
    --coalesce(Num_Paid_Refund_transaction,0) as Num_Paid_Refund_transaction,
    coalesce(Num_Paid_transaction1,0) as Num_Paid_transaction,
    coalesce(Amount_paid,0) as Amount_paid
    
    from `i-dss-cdm-data-dev.ent_vw.subscription_fct` A1
    left join (
    
    select 
    subscription_guid, 
    --count(distinct case when lower( invoice_type_desc ) like '%renewal%' and invoice_status_desc<>'failed' then invoice_guid else null end) as Num_Paid_transaction,
    count(distinct case when lower(invoice_status_desc)  in ('paid','closed')  then invoice_guid else null end) as Num_Paid_transaction1,
    --count(distinct case when lower(invoice_type_desc) like '%refund%' then invoice_guid else null end) as Num_Paid_Refund_transaction,
    sum(case when invoice_status_desc<>'failed' then after_discount_amt else 0 end ) as Amount_paid
    from `i-dss-cdm-data-dev.ent_vw.subscription_invoice_fct`
    where src_system_id=@src_system_id
    group by subscription_guid
    ) A2
    on A1.subscription_guid=A2.subscription_guid
    --left join `ent_summary.att_20160201_present` A3
    --on A1.subscription_guid=A3.subscription_guid
    where A1.src_system_id=115
    and A1.activation_dt  <= '2021-3-31' -- activation date to include the paid subs
    and lower(A1.signup_plan_cd) like '%annual%'
    and A1.subscription_platform_cd in  ('RECURLY','PSP')
    --and A1.signup_coupon_cd not in ('paramountplus','year')
    )
    
    
    select a1.*,
    coalesce(Paid_subs_1Invoice,0) as Paid_subs_1Invoice,
    coalesce(Paid_subs_2Invoice,0) as Paid_subs_2Invoice,
    coalesce(Paid_subs_3Invoice,0) as Paid_subs_3Invoice,
    coalesce(Paid_subs_4Invoice,0) as Paid_subs_4Invoice
    from
    (select 
    subscription_platform_cd,
    date_trunc(activation_dt, Month) as signup_month,
    signup_plan_cd,
    count(distinct subscription_guid) as ttl_trial_subs
    from Annual_pla_subs
    group by 1,2,3) a1
    
    left join (
    select 
    subscription_platform_cd,
    date_trunc(activation_dt, Month) as signup_month,
    signup_plan_cd,
    count(distinct subscription_guid) as Paid_subs_1Invoice
    from Annual_pla_subs
    where Amount_paid>40 ------- This was additional filtering using the price point- making sure only Annual Plan starts are considered; Might have to make adjustments for people who came with 50% discount or more as they would have lower price point that this
    and Num_Paid_transaction>=1
    group by 1,2,3) a2
    on a1.signup_month=a2.signup_month
    and a1.signup_plan_cd=a2.signup_plan_cd
    and a1.subscription_platform_cd= a2.subscription_platform_cd
    
    left join (
    select 
    subscription_platform_cd,
    date_trunc(activation_dt, Month) as signup_month,
    signup_plan_cd,
    SUM(case when Num_Paid_transaction=2 then Amount_paid else 0 end) Amount_paid_2,
    count(distinct subscription_guid) as Paid_subs_2Invoice
    from Annual_pla_subs
    where Amount_paid>40 
    and Num_Paid_transaction>=2
    group by 1,2,3) a3
    on a1.signup_month=a3.signup_month
    and a1.signup_plan_cd=a3.signup_plan_cd
    and a1.subscription_platform_cd= a3.subscription_platform_cd
    
    left join (
    select 
    subscription_platform_cd,
    date_trunc(activation_dt, Month) as signup_month,
    signup_plan_cd,
    count(distinct subscription_guid) as Paid_subs_3Invoice
    from Annual_pla_subs
    where Amount_paid>40 
    and Num_Paid_transaction>=3
    group by 1,2,3) a4
    on a1.signup_month=a4.signup_month
    and a1.signup_plan_cd=a4.signup_plan_cd
    and a1.subscription_platform_cd=a4.subscription_platform_cd
    
    left join (
    select 
    subscription_platform_cd,
    date_trunc(activation_dt, Month) as signup_month,
    signup_plan_cd,
    count(distinct subscription_guid) as Paid_subs_4Invoice
    from Annual_pla_subs
    where Amount_paid>40 
    and Num_Paid_transaction>=4
    group by 1,2,3)
    a5
    on a1.signup_month=a5.signup_month
    and a1.signup_plan_cd=a5.signup_plan_cd
    and a1.subscription_platform_cd = a5.subscription_platform_cd
    order by 1,3,2
    """
    return query