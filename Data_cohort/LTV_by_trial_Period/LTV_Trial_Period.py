from google.cloud import bigquery
import warnings
import pandas as pd
import numpy as np
from SQL import sql_queries as sq
warnings.filterwarnings("ignore")

def get_data_tp(client,src_system_id,till_date='2021-04-01', start_date='2014-10-01', end_date='2021-3-01'):
    sql = sq.trial_period_sql()
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("src_system_id", "NUMERIC", src_system_id),
            bigquery.ScalarQueryParameter("till_date", "DATE", till_date),
            bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
            bigquery.ScalarQueryParameter("end_date", "DATE", end_date)
        ]
    )
    df = client.query(sql, job_config=job_config).to_dataframe()
    return df

def renaming_columns_tp(df):
    for i in df.index:
        if i%2!=0:
            df['Trial_Period'][i]=df['Trial_Period'][i]+'- Total Starts'
    return df

def cal_overall_df_tp(df):
    trial_period=['1 Month Free','1 Week Free','3 Month Free', '50% off 1 Month','50% off 3 Month','Direct-to-Paid']
    temp=pd.DataFrame()
    for i in range(len(trial_period)):
        filter_df=df[df['Trial_Period'].str.endswith(trial_period[i])].reset_index(drop=True)
        total = filter_df.apply(np.sum)
        total['signup_plan'] = 'OVERALL'
        filter_df=filter_df.append(pd.DataFrame(total.values, index=total.keys()).T, ignore_index=True)
        filter_df['Trial_Period'][2]=filter_df['Trial_Period'][2][0:int(len(filter_df['Trial_Period'][2])/2)]

        total_starts_df=df[df['Trial_Period'].str.endswith(trial_period[i]+'- Total Starts')].reset_index(drop=True)
        total = total_starts_df.apply(np.sum)
        total['signup_plan'] = 'OVERALL'
        total_starts_df=total_starts_df.append(pd.DataFrame(total.values, index=total.keys()).T, ignore_index=True)
        total_starts_df['Trial_Period'][2]=total_starts_df['Trial_Period'][2][0:int(len(total_starts_df['Trial_Period'][2])/2)]

        df1=df[df['Trial_Period'].str.endswith(trial_period[i])]
        df2=df[df['Trial_Period'].str.endswith(trial_period[i]+'- Total Starts')]
        df3=filter_df[filter_df['signup_plan'].str.endswith('OVERALL')]
        df4=total_starts_df[total_starts_df['signup_plan'].str.endswith('OVERALL')]
        new_df=pd.concat([df1, df2,df3,df4]).reset_index(drop=True)
        temp=pd.concat([temp,new_df]).reset_index(drop=True)
    add_remaining=df[df['Trial_Period'].str.contains('2-Day Free') | df['Trial_Period'].str.contains('3-Day Free') |df['Trial_Period'].str.contains('2 Month Free') ]
    new_df=pd.concat([temp,add_remaining]).reset_index(drop=True)
    return new_df

def calc_weighted_avg_retention_rate_tp(df):
    weighted_avg_retention_Rate = []
    for i in range(1, 67):
        if (df['Subs_Retained_' + str(i)][0] != 0 and df['Subs_Retained_' + str(i)][1] != 0):
            weighted_avg_retention_Rate.append(round(df['Subs_Retained_' + str(i)][0] / df['Subs_Retained_' + str(i)][1] * 100, 2))
        else:
            weighted_avg_retention_Rate.append(0)
    return weighted_avg_retention_Rate;

def calc_retained_from_previous_month_tp(df):
    calc_wt_avg_ret_rt = calc_weighted_avg_retention_rate_tp(df)
    retained_from_previous_month = []
    for i in range(0, 65):
        if (calc_wt_avg_ret_rt[i + 1]!=0 and calc_wt_avg_ret_rt[i]!=0 and calc_wt_avg_ret_rt[i + 1] / calc_wt_avg_ret_rt[i] < 1):
            retained_from_previous_month.append(round(calc_wt_avg_ret_rt[i + 1] / calc_wt_avg_ret_rt[i] * 100, 2))

        elif (calc_wt_avg_ret_rt[i + 1]!=0 and calc_wt_avg_ret_rt[i] and calc_wt_avg_ret_rt[i + 1] / calc_wt_avg_ret_rt[i] >= 1):
            if i!=0:
                retained_from_previous_month.append(retained_from_previous_month[i - 1])
            else:
                retained_from_previous_month.append(0)
        else:
            retained_from_previous_month.append(0)

    return retained_from_previous_month

def calc_avg_retain_first_12_month_tp(df):
    ret_pre_month = calc_retained_from_previous_month_tp(df)
    return round(np.average(ret_pre_month[0:12]), 2)

def calc_conf_rev_CF():
    return 0.71*9.99

def calc_conf_rev(plan_cd):
    if plan_cd=='LC':
        return 0.71*5.99
    elif plan_cd=='OVERALL':
        return 0.71*7.03

def calc_conf_w_ad_rev(plan_cd):
     if plan_cd=='LC':
        return 1.95
     elif plan_cd=='OVERALL':
        return 0.734*1.95

def calc_projected_retenion_month_tp(df):
    projected_retenion_month = []
    wt_avg_ret_rt = calc_weighted_avg_retention_rate_tp(df)
    avg_ret_12_mths = calc_avg_retain_first_12_month_tp(df)
    for i in range(0, 60):
        if  wt_avg_ret_rt[i] == 0:
            projected_retenion_month.append(projected_retenion_month[i - 1] * avg_ret_12_mths/100)
        elif i == 0 and wt_avg_ret_rt[i] == 0:  # added an extra check seems broken in excel
            projected_retenion_month.append(avg_ret_12_mths)
        else:
            projected_retenion_month.append(wt_avg_ret_rt[i])
    return projected_retenion_month

def calc_exp_dur_subs_mnths_and_LTV_CF_tp(df,given_inp=False):
    projected_retenion_month = calc_projected_retenion_month_tp(df)
    duration = 0
    if given_inp == True:
        print('options 3M, 6M, 9M, 1Y, 2Y, 3Y, 4Y, 5Y, ALL')
        duration = input('Enter the Duration from above options')

    if duration == '3M':
        exp_dur_subs_mnths = round(sum(projected_retenion_month[0:3]) / 100, 1)
        LTV_wo_ad_Rev = round((sum(projected_retenion_month[0:3]) / 100) * calc_conf_rev_CF(), 2)
        return exp_dur_subs_mnths, LTV_wo_ad_Rev

    elif duration == '6M':
        exp_dur_subs_mnths = round(sum(projected_retenion_month[0:6]) / 100, 1)
        LTV_wo_ad_Rev = round((sum(projected_retenion_month[0:6]) / 100) * calc_conf_rev_CF(), 2)
        return exp_dur_subs_mnths, LTV_wo_ad_Rev

    elif duration == '9M':
        exp_dur_subs_mnths = round(sum(projected_retenion_month[0:9]) / 100, 1)
        LTV_wo_ad_Rev = round((sum(projected_retenion_month[0:9]) / 100) * calc_conf_rev_CF(), 2)
        return exp_dur_subs_mnths, LTV_wo_ad_Rev

    elif duration == '1Y':
        exp_dur_subs_mnths = round(sum(projected_retenion_month[0:12]) / 100, 1)
        LTV_wo_ad_Rev = round((sum(projected_retenion_month[0:12]) / 100) * calc_conf_rev_CF(), 2)
        return exp_dur_subs_mnths, LTV_wo_ad_Rev

    elif duration == '2Y':
        exp_dur_subs_mnths = round(sum(projected_retenion_month[0:24]) / 100, 1)
        LTV_wo_ad_Rev = round((sum(projected_retenion_month[0:24]) / 100) * calc_conf_rev_CF(), 2)
        return exp_dur_subs_mnths, LTV_wo_ad_Rev

    elif duration == '3Y':
        exp_dur_subs_mnths = round(sum(projected_retenion_month[0:36]) / 100, 1)
        LTV_wo_ad_Rev = round((sum(projected_retenion_month[0:36]) / 100) * calc_conf_rev_CF(), 2)
        return exp_dur_subs_mnths, LTV_wo_ad_Rev

    elif duration == '4Y':
        exp_dur_subs_mnths = round(sum(projected_retenion_month[0:48]) / 100, 1)
        LTV_wo_ad_Rev = round((sum(projected_retenion_month[0:48]) / 100) * calc_conf_rev_CF(), 2)
        return exp_dur_subs_mnths, LTV_wo_ad_Rev

    elif duration == '5Y':
        exp_dur_subs_mnths = round(sum(projected_retenion_month[0:60]) / 100, 1)
        LTV_wo_ad_Rev = round((sum(projected_retenion_month[0:60]) / 100) * calc_conf_rev_CF(), 2)
        return exp_dur_subs_mnths, LTV_wo_ad_Rev

    elif duration == 'ALL' or duration == 0:
        exp_dur_subs_mnths = []
        exp_dur_subs_mnths.append(round(sum(projected_retenion_month[0:3]) / 100, 1))
        exp_dur_subs_mnths.append(round(sum(projected_retenion_month[0:6]) / 100, 1))
        exp_dur_subs_mnths.append(round(sum(projected_retenion_month[0:9]) / 100, 1))
        exp_dur_subs_mnths.append(round(sum(projected_retenion_month[0:12]) / 100, 1))
        exp_dur_subs_mnths.append(round(sum(projected_retenion_month[0:24]) / 100, 1))
        exp_dur_subs_mnths.append(round(sum(projected_retenion_month[0:36]) / 100, 1))
        exp_dur_subs_mnths.append(round(sum(projected_retenion_month[0:48]) / 100, 1))
        exp_dur_subs_mnths.append(round(sum(projected_retenion_month[0:60]) / 100, 1))
        LTV_wo_ad_Rev = []
        LTV_wo_ad_Rev.append(round((sum(projected_retenion_month[0:3]) / 100) * calc_conf_rev_CF(), 2))
        LTV_wo_ad_Rev.append(round((sum(projected_retenion_month[0:6]) / 100) * calc_conf_rev_CF(), 2))
        LTV_wo_ad_Rev.append(round((sum(projected_retenion_month[0:9]) / 100) * calc_conf_rev_CF(), 2))
        LTV_wo_ad_Rev.append(round((sum(projected_retenion_month[0:12]) / 100) * calc_conf_rev_CF(), 2))
        LTV_wo_ad_Rev.append(round((sum(projected_retenion_month[0:24]) / 100) * calc_conf_rev_CF(), 2))
        LTV_wo_ad_Rev.append(round((sum(projected_retenion_month[0:36]) / 100) * calc_conf_rev_CF(), 2))
        LTV_wo_ad_Rev.append(round((sum(projected_retenion_month[0:48]) / 100) * calc_conf_rev_CF(), 2))
        LTV_wo_ad_Rev.append(round((sum(projected_retenion_month[0:60]) / 100) * calc_conf_rev_CF(), 2))
        return exp_dur_subs_mnths, LTV_wo_ad_Rev

    else:
        print("Please select appropiate option")
        return calc_exp_dur_subs_mnths_and_LTV_CF_tp(df)

def calc_exp_dur_subs_mnths_and_LTV_LC_tp(df,plan_cd, given_inp=False):
    projected_retenion_month = calc_projected_retenion_month_tp(df)
    duration = 0
    if given_inp == True:
        print('options 3M, 6M, 9M, 1Y, 2Y, 3Y, 4Y, 5Y, ALL')
        duration = input('Enter the Duration from above options')

    if duration == '3M':
        exp_dur_subs_mnths = round(sum(projected_retenion_month[0:3]) / 100, 1)
        LTV_wo_ad_Rev = round((sum(projected_retenion_month[0:3]) / 100) * calc_conf_rev(plan_cd), 2)
        LTV_w_ad_Rev = round(LTV_wo_ad_Rev + (sum(projected_retenion_month[0:3]) / 100) * calc_conf_w_ad_rev(plan_cd), 2)
        return exp_dur_subs_mnths, LTV_wo_ad_Rev, LTV_w_ad_Rev

    elif duration == '6M':
        exp_dur_subs_mnths = round(sum(projected_retenion_month[0:6]) / 100, 1)
        LTV_wo_ad_Rev = round((sum(projected_retenion_month[0:6]) / 100) * calc_conf_rev(plan_cd), 2)
        LTV_w_ad_Rev = round(LTV_wo_ad_Rev + (sum(projected_retenion_month[0:6]) / 100) * calc_conf_w_ad_rev(plan_cd), 2)
        return exp_dur_subs_mnths, LTV_wo_ad_Rev, LTV_w_ad_Rev

    elif duration == '9M':
        exp_dur_subs_mnths = round(sum(projected_retenion_month[0:9]) / 100, 1)
        LTV_wo_ad_Rev = round((sum(projected_retenion_month[0:9]) / 100) * calc_conf_rev(plan_cd), 2)
        LTV_w_ad_Rev = round(LTV_wo_ad_Rev + (sum(projected_retenion_month[0:9]) / 100) * calc_conf_w_ad_rev(plan_cd), 2)
        return exp_dur_subs_mnths, LTV_wo_ad_Rev, LTV_w_ad_Rev

    elif duration == '1Y':
        exp_dur_subs_mnths = round(sum(projected_retenion_month[0:12]) / 100, 1)
        LTV_wo_ad_Rev = round((sum(projected_retenion_month[0:12]) / 100) * calc_conf_rev(plan_cd), 2)
        LTV_w_ad_Rev = round(LTV_wo_ad_Rev[3] + (sum(projected_retenion_month[0:12]) / 100) * calc_conf_w_ad_rev(plan_cd), 2)
        return exp_dur_subs_mnths, LTV_wo_ad_Rev, LTV_w_ad_Rev

    elif duration == '2Y':
        exp_dur_subs_mnths = round(sum(projected_retenion_month[0:24]) / 100, 1)
        LTV_wo_ad_Rev = round((sum(projected_retenion_month[0:24]) / 100) * calc_conf_rev(plan_cd), 2)
        LTV_w_ad_Rev = round(LTV_wo_ad_Rev[4] + (sum(projected_retenion_month[0:24]) / 100) * calc_conf_w_ad_rev(plan_cd), 2)
        return exp_dur_subs_mnths, LTV_wo_ad_Rev, LTV_w_ad_Rev

    elif duration == '3Y':
        exp_dur_subs_mnths = round(sum(projected_retenion_month[0:36]) / 100, 1)
        LTV_wo_ad_Rev = round((sum(projected_retenion_month[0:36]) / 100) * calc_conf_rev(plan_cd), 2)
        LTV_w_ad_Rev = round(LTV_wo_ad_Rev[5] + (sum(projected_retenion_month[0:36]) / 100) * calc_conf_w_ad_rev(plan_cd), 2)
        return exp_dur_subs_mnths, LTV_wo_ad_Rev, LTV_w_ad_Rev

    elif duration == '4Y':
        exp_dur_subs_mnths = round(sum(projected_retenion_month[0:48]) / 100, 1)
        LTV_wo_ad_Rev = round((sum(projected_retenion_month[0:48]) / 100) * calc_conf_rev(plan_cd), 2)
        LTV_w_ad_Rev = round(LTV_wo_ad_Rev[6] + (sum(projected_retenion_month[0:48]) / 100) * calc_conf_w_ad_rev(plan_cd), 2)
        return exp_dur_subs_mnths, LTV_wo_ad_Rev, LTV_w_ad_Rev

    elif duration == '5Y':
        exp_dur_subs_mnths = round(sum(projected_retenion_month[0:60]) / 100, 1)
        LTV_wo_ad_Rev = round((sum(projected_retenion_month[0:60]) / 100) * calc_conf_rev(plan_cd), 2)
        LTV_w_ad_Rev = round(LTV_wo_ad_Rev[7] + (sum(projected_retenion_month[0:60]) / 100) * calc_conf_w_ad_rev(plan_cd), 2)
        return exp_dur_subs_mnths, LTV_wo_ad_Rev, LTV_w_ad_Rev

    elif duration == 'ALL' or duration == 0:
        exp_dur_subs_mnths = []
        exp_dur_subs_mnths.append(round(sum(projected_retenion_month[0:3]) / 100, 1))
        exp_dur_subs_mnths.append(round(sum(projected_retenion_month[0:6]) / 100, 1))
        exp_dur_subs_mnths.append(round(sum(projected_retenion_month[0:9]) / 100, 1))
        exp_dur_subs_mnths.append(round(sum(projected_retenion_month[0:12]) / 100, 1))
        exp_dur_subs_mnths.append(round(sum(projected_retenion_month[0:24]) / 100, 1))
        exp_dur_subs_mnths.append(round(sum(projected_retenion_month[0:36]) / 100, 1))
        exp_dur_subs_mnths.append(round(sum(projected_retenion_month[0:48]) / 100, 1))
        exp_dur_subs_mnths.append(round(sum(projected_retenion_month[0:60]) / 100, 1))
        LTV_wo_ad_Rev = []
        LTV_wo_ad_Rev.append(round((sum(projected_retenion_month[0:3]) / 100) * calc_conf_rev(plan_cd), 2))
        LTV_wo_ad_Rev.append(round((sum(projected_retenion_month[0:6]) / 100) * calc_conf_rev(plan_cd), 2))
        LTV_wo_ad_Rev.append(round((sum(projected_retenion_month[0:9]) / 100) * calc_conf_rev(plan_cd), 2))
        LTV_wo_ad_Rev.append(round((sum(projected_retenion_month[0:12]) / 100) * calc_conf_rev(plan_cd), 2))
        LTV_wo_ad_Rev.append(round((sum(projected_retenion_month[0:24]) / 100) * calc_conf_rev(plan_cd), 2))
        LTV_wo_ad_Rev.append(round((sum(projected_retenion_month[0:36]) / 100) * calc_conf_rev(plan_cd), 2))
        LTV_wo_ad_Rev.append(round((sum(projected_retenion_month[0:48]) / 100) * calc_conf_rev(plan_cd), 2))
        LTV_wo_ad_Rev.append(round((sum(projected_retenion_month[0:60]) / 100) * calc_conf_rev(plan_cd), 2))
        LTV_w_ad_Rev = []
        LTV_w_ad_Rev.append(round(LTV_wo_ad_Rev[0] + (sum(projected_retenion_month[0:3]) / 100) * calc_conf_w_ad_rev(plan_cd), 2))
        LTV_w_ad_Rev.append(round(LTV_wo_ad_Rev[1] + (sum(projected_retenion_month[0:6]) / 100) * calc_conf_w_ad_rev(plan_cd), 2))
        LTV_w_ad_Rev.append(round(LTV_wo_ad_Rev[2] + (sum(projected_retenion_month[0:9]) / 100) * calc_conf_w_ad_rev(plan_cd), 2))
        LTV_w_ad_Rev.append(round(LTV_wo_ad_Rev[3] + (sum(projected_retenion_month[0:12]) / 100) * calc_conf_w_ad_rev(plan_cd), 2))
        LTV_w_ad_Rev.append(round(LTV_wo_ad_Rev[4] + (sum(projected_retenion_month[0:24]) / 100) * calc_conf_w_ad_rev(plan_cd), 2))
        LTV_w_ad_Rev.append(round(LTV_wo_ad_Rev[5] + (sum(projected_retenion_month[0:36]) / 100) * calc_conf_w_ad_rev(plan_cd), 2))
        LTV_w_ad_Rev.append(round(LTV_wo_ad_Rev[6] + (sum(projected_retenion_month[0:48]) / 100) * calc_conf_w_ad_rev(plan_cd), 2))
        LTV_w_ad_Rev.append(round(LTV_wo_ad_Rev[7] + (sum(projected_retenion_month[0:60]) / 100) * calc_conf_w_ad_rev(plan_cd), 2))
        return exp_dur_subs_mnths, LTV_wo_ad_Rev, LTV_w_ad_Rev

def calc_all_LTV_tp(df_n):
    final_output=pd.DataFrame()
    trial_period=['2-Day Free','3-Day Free','1 Week Free','1 Month Free','3 Month Free','50% off 1 Month','50% off 3 Month','Direct-to-Paid']
    plan_cd=['CF','LC','OVERALL']

    for j in range(len(plan_cd)):
        for i in range(len(trial_period)):
            if plan_cd[j]=='CF':
                this_df=df_n[df_n['signup_plan'].str.contains(plan_cd[j]) & df_n['Trial_Period'].str.startswith(trial_period[i])].reset_index(drop=True)
                _,ltv_amt=calc_exp_dur_subs_mnths_and_LTV_CF_tp(this_df)
                temp_dict={"trial_period" : [trial_period[i]],"plan_type" : [plan_cd[j]], "year_1_amt":[ltv_amt[3]],"year_3_amt":[ltv_amt[5]],"year_5_amt":[ltv_amt[7]]}
                temp_df=pd.DataFrame.from_dict(temp_dict,orient='columns')
                final_output=round(pd.concat([final_output,temp_df]))
            elif plan_cd[j]=='LC':
                if trial_period[i]=='2-Day Free' or trial_period[i]=='3-Day Free':
                    temp_dict={"trial_period" : [trial_period[i]],"plan_type" : [plan_cd[j]], "year_1_amt":[0.0],"year_3_amt":[0.0],"year_5_amt":[0.0]}
                    temp_df=pd.DataFrame.from_dict(temp_dict,orient='columns')
                    final_output=round(pd.concat([final_output,temp_df]))
                else:
                    this_df=df_n[df_n['signup_plan'].str.contains(plan_cd[j]) & df_n['Trial_Period'].str.startswith(trial_period[i])].reset_index(drop=True)
                    _,__,ltv_amt=calc_exp_dur_subs_mnths_and_LTV_LC_tp(this_df,plan_cd[j])
                    temp_dict={"trial_period" : [trial_period[i]],"plan_type" : [plan_cd[j]], "year_1_amt":[ltv_amt[3]],"year_3_amt":[ltv_amt[5]],"year_5_amt":[ltv_amt[7]]}
                    temp_df=pd.DataFrame.from_dict(temp_dict,orient='columns')
                    final_output=round(pd.concat([final_output,temp_df]))
            elif plan_cd[j]=='OVERALL':
                if trial_period[i]=='2-Day Free' or trial_period[i]=='3-Day Free':
                    this_df=df_n[df_n['signup_plan'].str.contains('CF') & df_n['Trial_Period'].str.startswith(trial_period[i])].reset_index(drop=True)
                    _,ltv_amt=calc_exp_dur_subs_mnths_and_LTV_CF_tp(this_df)
                    temp_dict={"trial_period" : [trial_period[i]],"plan_type" : [plan_cd[j]], "year_1_amt":[ltv_amt[3]],"year_3_amt":[ltv_amt[5]],"year_5_amt":[ltv_amt[7]]}
                    temp_df=pd.DataFrame.from_dict(temp_dict,orient='columns')
                    final_output=round(pd.concat([final_output,temp_df]))
                else:
                    this_df=df_n[df_n['signup_plan'].str.contains(plan_cd[j]) & df_n['Trial_Period'].str.startswith(trial_period[i])].reset_index(drop=True)
                    _,__,ltv_amt=calc_exp_dur_subs_mnths_and_LTV_LC_tp(this_df,plan_cd[j])
                    temp_dict={"trial_period" : [trial_period[i]],"plan_type" : [plan_cd[j]], "year_1_amt":[ltv_amt[3]],"year_3_amt":[ltv_amt[5]],"year_5_amt":[ltv_amt[7]]}
                    temp_df=pd.DataFrame.from_dict(temp_dict,orient='columns')
                    final_output=round(pd.concat([final_output,temp_df]))
    return final_output.reset_index(drop=True)


def check_data_exists_tp(client,src_system_id,till_date,project_id, dataset_name):
    sql = """
    select * from `{0}.{1}.pt_ltv_subs_trial_period_by_quarter` 
    where day_dt= Date(@till_date) and src_system_id=@src_system_id
    """.format(project_id, dataset_name)
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("src_system_id", "NUMERIC", src_system_id),
            bigquery.ScalarQueryParameter("till_date", "TIMESTAMP", till_date),
        ]
    )
    df = client.query(sql, job_config=job_config).to_dataframe()

    if len(df) == 0:
        return False
    else:
        return True


def get_data_from_table_tp(client,src_system_id,till_date,project_id, dataset_name):

    sql = """
    select * from `{0}.{1}.pt_ltv_subs_trial_period_by_quarter` 
    where day_dt= @till_date and src_system_id=@src_system_id
    """.format(project_id, dataset_name)
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("src_system_id", "NUMERIC", src_system_id),
            bigquery.ScalarQueryParameter("till_date", "TIMESTAMP", till_date),
        ]
    )
    df = client.query(sql, job_config=job_config).to_dataframe()
    return df

def update_active_ind(client,src_system_id,till_date,project_id, dataset_name):
    sql = """
        update `{0}.{1}.pt_ltv_subs_trial_period_by_quarter`
        set active_ind=False 
        where day_dt= Date(@till_date) and src_system_id=@src_system_id
        """.format(project_id, dataset_name)
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("src_system_id", "NUMERIC", src_system_id),
            bigquery.ScalarQueryParameter("till_date", "TIMESTAMP", till_date),
        ]
    )
    df = client.query(sql, job_config=job_config).to_dataframe()

