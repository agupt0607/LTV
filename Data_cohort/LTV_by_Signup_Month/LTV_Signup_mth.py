from google.cloud import bigquery
import warnings
import pandas as pd
import numpy as np
from SQL import sql_queries as sq
warnings.filterwarnings("ignore")

def get_data_with_free_trial_sm(client,src_system_id,till_date='2021-04-01', start_date='2014-10-01', end_date='2021-3-01'):
    sql = sq.signup_mnth_with_free_trial()
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("src_system_id","NUMERIC",src_system_id),
            bigquery.ScalarQueryParameter("till_date", "DATE", till_date),
            bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
            bigquery.ScalarQueryParameter("end_date", "DATE", end_date)
        ]
    )
    df = client.query(sql, job_config=job_config).to_dataframe()
    return df

def get_data_without_free_trial_sm(client,src_system_id,till_date='2021-04-01', start_date='2014-10-01', end_date='2021-3-01'):
    sql = sq.signup_mnth_without_free_trial()
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

def combine_overall_Starts_sm(LTV_signup_by_month_with_1_2_3_month, LTV_signup_by_month_with_out_1_2_3_month):
    df = LTV_signup_by_month_with_1_2_3_month.copy(deep=True)
    i = 1
    j = 0
    for j in range(0, 24):
        for i in range(1, 79):
            df['Subs_Retained_' + str(i)][j] = LTV_signup_by_month_with_1_2_3_month['Subs_Retained_' + str(i)][j] + \
                                               LTV_signup_by_month_with_out_1_2_3_month['Subs_Retained_' + str(i + 1)][
                                                   j]
    return df


def calc_weighted_avg_retention_rate_sm(df, month):
    weighted_avg_retention_Rate = []
    temp_df = df[df['Month_Start'] == month].reset_index(drop=True)
    for i in range(1, 67):
        if (temp_df['Subs_Retained_' + str(i)][0] != 0 and temp_df['Subs_Retained_' + str(i)][1] != 0):
            weighted_avg_retention_Rate.append(round(temp_df['Subs_Retained_' + str(i)][0] / temp_df['Subs_Retained_' + str(i)][1] * 100, 2))
        else:
            weighted_avg_retention_Rate.append(0)
    return weighted_avg_retention_Rate;


def calc_retained_from_previous_month_sm(df, month):
    calc_wt_avg_ret_rt = calc_weighted_avg_retention_rate_sm(df, month)
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


def calc_avg_retain_first_12_month_sm(df, month):
    ret_pre_month = calc_retained_from_previous_month_sm(df, month)
    return round(np.average(ret_pre_month[0:12]), 2)


def calc_projected_retenion_month_sm(df, month):
    projected_retenion_month = []
    wt_avg_ret_rt = calc_weighted_avg_retention_rate_sm(df, month)
    avg_ret_12_mths = calc_avg_retain_first_12_month_sm(df, month)

    for i in range(0, 60):
        if wt_avg_ret_rt[i] == 0:
            projected_retenion_month.append(projected_retenion_month[i - 1] * avg_ret_12_mths/100)
        elif i == 0 and wt_avg_ret_rt[i] == 0:  # added an extra check seems broken in excel
            projected_retenion_month.append(avg_ret_12_mths)
        else:
            projected_retenion_month.append(wt_avg_ret_rt[i])
    return projected_retenion_month


def calc_exp_dur_subs_mnths_LTV_w_wo_revenue_sm(df, month, given_inp=False):
    projected_retenion_month = calc_projected_retenion_month_sm(df, month)
    duration = 0
    if given_inp == True:
        print('options 3M, 6M, 9M, 1Y, 2Y, 3Y, 4Y, 5Y, ALL')
        duration = input('Enter the Duration from above options')

    if duration == '3M':
        exp_dur_subs_mnths = round(sum(projected_retenion_month[0:3]) / 100, 1)
        LTV_wo_ad_Rev = round((sum(projected_retenion_month[0:3]) / 100) * 4.99, 2)
        LTV_w_ad_Rev = round(LTV_wo_ad_Rev + (sum(projected_retenion_month[0:3]) / 100) * 1.4304, 2)
        return exp_dur_subs_mnths, LTV_wo_ad_Rev, LTV_w_ad_Rev

    elif duration == '6M':
        exp_dur_subs_mnths = round(sum(projected_retenion_month[0:6]) / 100, 1)
        LTV_wo_ad_Rev = round((sum(projected_retenion_month[0:6]) / 100) * 4.99, 2)
        LTV_w_ad_Rev = round(LTV_wo_ad_Rev + (sum(projected_retenion_month[0:6]) / 100) * 1.4304, 2)
        return exp_dur_subs_mnths, LTV_wo_ad_Rev, LTV_w_ad_Rev

    elif duration == '9M':
        exp_dur_subs_mnths = round(sum(projected_retenion_month[0:9]) / 100, 1)
        LTV_wo_ad_Rev = round((sum(projected_retenion_month[0:9]) / 100) * 4.99, 2)
        LTV_w_ad_Rev = round(LTV_wo_ad_Rev + (sum(projected_retenion_month[0:9]) / 100) * 1.4304, 2)
        return exp_dur_subs_mnths, LTV_wo_ad_Rev, LTV_w_ad_Rev

    elif duration == '1Y':
        exp_dur_subs_mnths = round(sum(projected_retenion_month[0:12]) / 100, 1)
        LTV_wo_ad_Rev = round((sum(projected_retenion_month[0:12]) / 100) * 4.99, 2)
        LTV_w_ad_Rev = round(LTV_wo_ad_Rev[3] + (sum(projected_retenion_month[0:12]) / 100) * 1.4304, 2)
        return exp_dur_subs_mnths, LTV_wo_ad_Rev, LTV_w_ad_Rev

    elif duration == '2Y':
        exp_dur_subs_mnths = round(sum(projected_retenion_month[0:24]) / 100, 1)
        LTV_wo_ad_Rev = round((sum(projected_retenion_month[0:24]) / 100) * 4.99, 2)
        LTV_w_ad_Rev = round(LTV_wo_ad_Rev[4] + (sum(projected_retenion_month[0:24]) / 100) * 1.4304, 2)
        return exp_dur_subs_mnths, LTV_wo_ad_Rev, LTV_w_ad_Rev

    elif duration == '3Y':
        exp_dur_subs_mnths = round(sum(projected_retenion_month[0:36]) / 100, 1)
        LTV_wo_ad_Rev = round((sum(projected_retenion_month[0:36]) / 100) * 4.99, 2)
        LTV_w_ad_Rev = round(LTV_wo_ad_Rev[5] + (sum(projected_retenion_month[0:36]) / 100) * 1.4304, 2)
        return exp_dur_subs_mnths, LTV_wo_ad_Rev, LTV_w_ad_Rev

    elif duration == '4Y':
        exp_dur_subs_mnths = round(sum(projected_retenion_month[0:48]) / 100, 1)
        LTV_wo_ad_Rev = round((sum(projected_retenion_month[0:48]) / 100) * 4.99, 2)
        LTV_w_ad_Rev = round(LTV_wo_ad_Rev[6] + (sum(projected_retenion_month[0:48]) / 100) * 1.4304, 2)
        return exp_dur_subs_mnths, LTV_wo_ad_Rev, LTV_w_ad_Rev

    elif duration == '5Y':
        exp_dur_subs_mnths = round(sum(projected_retenion_month[0:60]) / 100, 1)
        LTV_wo_ad_Rev = round((sum(projected_retenion_month[0:60]) / 100) * 4.99, 2)
        LTV_w_ad_Rev = round(LTV_wo_ad_Rev[7] + (sum(projected_retenion_month[0:60]) / 100) * 1.4304, 2)
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
        LTV_wo_ad_Rev.append(round((sum(projected_retenion_month[0:3]) / 100) * 4.99, 2))
        LTV_wo_ad_Rev.append(round((sum(projected_retenion_month[0:6]) / 100) * 4.99, 2))
        LTV_wo_ad_Rev.append(round((sum(projected_retenion_month[0:9]) / 100) * 4.99, 2))
        LTV_wo_ad_Rev.append(round((sum(projected_retenion_month[0:12]) / 100) * 4.99, 2))
        LTV_wo_ad_Rev.append(round((sum(projected_retenion_month[0:24]) / 100) * 4.99, 2))
        LTV_wo_ad_Rev.append(round((sum(projected_retenion_month[0:36]) / 100) * 4.99, 2))
        LTV_wo_ad_Rev.append(round((sum(projected_retenion_month[0:48]) / 100) * 4.99, 2))
        LTV_wo_ad_Rev.append(round((sum(projected_retenion_month[0:60]) / 100) * 4.99, 2))
        LTV_w_ad_Rev = []
        LTV_w_ad_Rev.append(round(LTV_wo_ad_Rev[0] + (sum(projected_retenion_month[0:3]) / 100) * 1.4304, 2))
        LTV_w_ad_Rev.append(round(LTV_wo_ad_Rev[1] + (sum(projected_retenion_month[0:6]) / 100) * 1.4304, 2))
        LTV_w_ad_Rev.append(round(LTV_wo_ad_Rev[2] + (sum(projected_retenion_month[0:9]) / 100) * 1.4304, 2))
        LTV_w_ad_Rev.append(round(LTV_wo_ad_Rev[3] + (sum(projected_retenion_month[0:12]) / 100) * 1.4304, 2))
        LTV_w_ad_Rev.append(round(LTV_wo_ad_Rev[4] + (sum(projected_retenion_month[0:24]) / 100) * 1.4304, 2))
        LTV_w_ad_Rev.append(round(LTV_wo_ad_Rev[5] + (sum(projected_retenion_month[0:36]) / 100) * 1.4304, 2))
        LTV_w_ad_Rev.append(round(LTV_wo_ad_Rev[6] + (sum(projected_retenion_month[0:48]) / 100) * 1.4304, 2))
        LTV_w_ad_Rev.append(round(LTV_wo_ad_Rev[7] + (sum(projected_retenion_month[0:60]) / 100) * 1.4304, 2))
        return exp_dur_subs_mnths, LTV_wo_ad_Rev, LTV_w_ad_Rev

    else:
        print("Please select appropiate option")
        return calc_exp_dur_subs_mnths_LTV_w_wo_revenue_sm(df, month)


def final_output_sm(df):
    output = pd.DataFrame()
    for i in range(1, 13):
        mnths, ltv_wo_ad, ltv_w_ad = calc_exp_dur_subs_mnths_LTV_w_wo_revenue_sm(df, i)
        temp = {"Month": i, "year_1_amt": ltv_w_ad[3], "year_3_amt": ltv_w_ad[5], "year_5_amt": ltv_w_ad[7]}
        output = output.append(temp, ignore_index=True)

    Month_df = pd.DataFrame(
        ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November',
         'December'], columns=['Month_name']).reset_index().rename(columns={"index": "Month"})
    Month_df.loc[Month_df.Month, 'Month'] += 1
    output = output.merge(Month_df, on='Month').drop(['Month'], axis=1).rename(columns={"Month_name": "month_nm"})
    cols = output.columns.tolist()
    cols = cols[-1:] + cols[:-1]
    output = output[cols]
    return output.round(4)

def check_data_exists_sm(client,src_system_id,till_date,project_id, dataset_name):

    sql = """
    select * from `{0}.{1}.pt_ltv_signup_month_by_quarter` 
    where day_dt= Date(@till_date) and src_system_id=@src_system_id
    """.format(project_id, dataset_name)
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("till_date", "TIMESTAMP", till_date),
            bigquery.ScalarQueryParameter("src_system_id", "NUMERIC", src_system_id),
        ]
    )
    df = client.query(sql, job_config=job_config).to_dataframe()

    if len(df) == 0:
        return False
    else:
        return True

def update_active_ind(client,src_system_id,till_date,project_id, dataset_name):
    sql = """
       update `{0}.{1}.pt_ltv_signup_month_by_quarter`
       set active_ind=False
       where day_dt= Date(@till_date) and src_system_id=@src_system_id
       """.format(project_id, dataset_name)
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("till_date", "TIMESTAMP", till_date),
            bigquery.ScalarQueryParameter("src_system_id", "NUMERIC", src_system_id),
        ]
    )
    df = client.query(sql, job_config=job_config).to_dataframe()