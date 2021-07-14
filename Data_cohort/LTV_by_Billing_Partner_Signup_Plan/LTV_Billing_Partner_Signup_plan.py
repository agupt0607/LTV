from google.cloud import bigquery
import warnings
import pandas as pd
import numpy as np
from SQL import sql_queries as sq
warnings.filterwarnings("ignore")

def get_data_without_free_trial_bpsp(client,src_system_id,till_date='2021-04-01', start_date='2014-10-01', end_date='2021-3-01'):
    sql = sq.billing_partner_signup_plan_without_free_trial()
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

def get_data_with_free_trial_bpsp(client,src_system_id,till_date='2021-04-01', start_date='2014-10-01', end_date='2021-3-01'):
    sql =sq.billing_partner_signup_plan_with_free_trial()
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

def combine_overall_Starts_bpsp(LTV_with_out, LTV_with):
    df = LTV_with_out.copy(deep=True)
    i = 1
    j = 0
    for j in range(0, 9):
        for i in range(1, 79):
            df['Subs_Retained_' + str(i)][j] = LTV_with_out['Subs_Retained_' + str(i)][j] + \
                                               LTV_with['Subs_Retained_' + str(i + 1)][j]
    for i in df.index:
        if i % 2 != 0:
            df['subscription_platform'][i] = df['subscription_platform'][i] + '- Total Starts'
    df_LC = df[df['signup_plan'].str.contains('LC')].reset_index(drop=True)
    df_CF = df[df['signup_plan'].str.contains('CF')].reset_index(drop=True)
    return df_LC, df_CF


def calc_weighted_avg_retention_rate_bpsp(df, bill_part):
    weighted_avg_retention_Rate = []
    temp_df = df[df['subscription_platform'].str.contains(bill_part)].reset_index(drop=True)
    if bill_part == 'AMAZON':
        for i in range(1, 53):
            if (temp_df['Subs_Retained_' + str(i)][0] != 0 and temp_df['Subs_Retained_' + str(i)][1] != 0):
                weighted_avg_retention_Rate.append(
                    round(temp_df['Subs_Retained_' + str(i)][0] / temp_df['Subs_Retained_' + str(i)][1] * 100, 2))
            else:
                weighted_avg_retention_Rate.append(0)
        return weighted_avg_retention_Rate;
    else:
        for i in range(1, 67):
            if (temp_df['Subs_Retained_' + str(i)][0] != 0 and temp_df['Subs_Retained_' + str(i)][1] != 0):
                weighted_avg_retention_Rate.append(
                    round(temp_df['Subs_Retained_' + str(i)][0] / temp_df['Subs_Retained_' + str(i)][1] * 100, 2))
            else:
                weighted_avg_retention_Rate.append(0)
        return weighted_avg_retention_Rate;


def calc_retained_from_previous_month_bpsp(df, bill_part):
    calc_wt_avg_ret_rt = calc_weighted_avg_retention_rate_bpsp(df, bill_part)
    retained_from_previous_month = []

    if bill_part == 'AMAZON':
        for i in range(0, 51):
            if (calc_wt_avg_ret_rt[i + 1] != 0 and calc_wt_avg_ret_rt[i] != 0 and calc_wt_avg_ret_rt[i + 1] /
                    calc_wt_avg_ret_rt[i] < 1):
                retained_from_previous_month.append(round(calc_wt_avg_ret_rt[i + 1] / calc_wt_avg_ret_rt[i] * 100, 2))

            elif (calc_wt_avg_ret_rt[i + 1] != 0 and calc_wt_avg_ret_rt[i] and calc_wt_avg_ret_rt[i + 1] /
                  calc_wt_avg_ret_rt[i] >= 1):
                if i != 0:
                    retained_from_previous_month.append(retained_from_previous_month[i - 1])
                else:
                    retained_from_previous_month.append(0)
            else:
                retained_from_previous_month.append(0)
    else:
        for i in range(0, 65):
            if (calc_wt_avg_ret_rt[i + 1] != 0 and calc_wt_avg_ret_rt[i] != 0 and calc_wt_avg_ret_rt[i + 1] /
                    calc_wt_avg_ret_rt[i] < 1):
                retained_from_previous_month.append(round(calc_wt_avg_ret_rt[i + 1] / calc_wt_avg_ret_rt[i] * 100, 2))

            elif (calc_wt_avg_ret_rt[i + 1] != 0 and calc_wt_avg_ret_rt[i] and calc_wt_avg_ret_rt[i + 1] /
                  calc_wt_avg_ret_rt[i] >= 1):
                if i != 0:
                    retained_from_previous_month.append(retained_from_previous_month[i - 1])
                else:
                    retained_from_previous_month.append(0)
            else:
                retained_from_previous_month.append(0)

    return retained_from_previous_month


def calc_avg_retain_first_12_month_bpsp(df, bill_part):
    ret_pre_month = calc_retained_from_previous_month_bpsp(df, bill_part)
    return round(np.average(ret_pre_month[0:12]), 2)


def calc_projected_retenion_month_bpsp(df, bill_part):
    projected_retenion_month = []
    wt_avg_ret_rt = calc_weighted_avg_retention_rate_bpsp(df, bill_part)
    avg_ret_12_mths = calc_avg_retain_first_12_month_bpsp(df, bill_part)

    if bill_part == 'AMAZON':
        for i in range(0, 52):
            if wt_avg_ret_rt[i] == 0:
                projected_retenion_month.append(projected_retenion_month[i - 1] * avg_ret_12_mths/100)
            elif i == 0 and wt_avg_ret_rt[i] == 0:  # added an extra check seems broken in excel
                projected_retenion_month.append(avg_ret_12_mths)
            else:
                projected_retenion_month.append(wt_avg_ret_rt[i])
        for i in range(52, 60):
            projected_retenion_month.append(round(projected_retenion_month[i - 1] * (avg_ret_12_mths / 100), 2))
    else:
        for i in range(0, 60):
            if wt_avg_ret_rt[i] == 0:
                projected_retenion_month.append(projected_retenion_month[i - 1] * avg_ret_12_mths / 100)
            elif i == 0 and wt_avg_ret_rt[i] == 0:  # added an extra check seems broken in excel
                projected_retenion_month.append(avg_ret_12_mths/100)
            else:
                projected_retenion_month.append(wt_avg_ret_rt[i])
    return projected_retenion_month


def calc_conf_rev_CF_bpsp(bill_part,path):
    config_df = pd.read_csv(path)
    GM = float(
        config_df[config_df['bill_part'].str.contains(bill_part)]['Gross Margin'].reset_index(drop=True)[0][0:2]) / 100
    return round(GM * 9.99, 2)


def calc_conf_rev_LC_bpsp(bill_part,path):
    config_df = pd.read_csv(path)
    GM = float(
        config_df[config_df['bill_part'].str.contains(bill_part)]['Gross Margin'].reset_index(drop=True)[0][0:2]) / 100
    return round(GM * 5.99, 2)


def calc_conf_w_ad_rev_LC_bpsp(bill_part,path):
    avg_ad_rev_per_LC_subs_per_month = 1.95
    config_df = pd.read_csv(path)
    LC_subs = float(
        config_df[config_df['bill_part'].str.contains(bill_part)]['% of LC subs'].reset_index(drop=True)[0][0:5]) / 100
    return round(LC_subs * avg_ad_rev_per_LC_subs_per_month, 2)


def calc_exp_dur_subs_mnths_LTV_w_wo_revenue_CF_bpsp(df, bill_part,path, given_inp=False):

    projected_retenion_month = calc_projected_retenion_month_bpsp(df, bill_part)
    duration = 0
    if given_inp == True:
        print('options 3M, 6M, 9M, 1Y, 2Y, 3Y, 4Y, 5Y, ALL')
        duration = input('Enter the Duration from above options')

    if duration == '3M':
        exp_dur_subs_mnths = round(sum(projected_retenion_month[0:3]) / 100, 1)
        LTV_wo_ad_Rev = round((sum(projected_retenion_month[0:3]) / 100) * calc_conf_rev_CF_bpsp(bill_part), 2)
        return exp_dur_subs_mnths, LTV_wo_ad_Rev

    elif duration == '6M':
        exp_dur_subs_mnths = round(sum(projected_retenion_month[0:6]) / 100, 1)
        LTV_wo_ad_Rev = round((sum(projected_retenion_month[0:6]) / 100) * calc_conf_rev_CF_bpsp(bill_part), 2)
        return exp_dur_subs_mnths, LTV_wo_ad_Rev

    elif duration == '9M':
        exp_dur_subs_mnths = round(sum(projected_retenion_month[0:9]) / 100, 1)
        LTV_wo_ad_Rev = round((sum(projected_retenion_month[0:9]) / 100) * calc_conf_rev_CF_bpsp(bill_part), 2)
        return exp_dur_subs_mnths, LTV_wo_ad_Rev

    elif duration == '1Y':
        exp_dur_subs_mnths = round(sum(projected_retenion_month[0:12]) / 100, 1)
        LTV_wo_ad_Rev = round((sum(projected_retenion_month[0:12]) / 100) * calc_conf_rev_CF_bpsp(bill_part), 2)
        return exp_dur_subs_mnths, LTV_wo_ad_Rev

    elif duration == '2Y':
        exp_dur_subs_mnths = round(sum(projected_retenion_month[0:24]) / 100, 1)
        LTV_wo_ad_Rev = round((sum(projected_retenion_month[0:24]) / 100) * calc_conf_rev_CF_bpsp(bill_part), 2)
        return exp_dur_subs_mnths, LTV_wo_ad_Rev

    elif duration == '3Y':
        exp_dur_subs_mnths = round(sum(projected_retenion_month[0:36]) / 100, 1)
        LTV_wo_ad_Rev = round((sum(projected_retenion_month[0:36]) / 100) * calc_conf_rev_CF_bpsp(bill_part), 2)
        return exp_dur_subs_mnths, LTV_wo_ad_Rev

    elif duration == '4Y':
        exp_dur_subs_mnths = round(sum(projected_retenion_month[0:48]) / 100, 1)
        LTV_wo_ad_Rev = round((sum(projected_retenion_month[0:48]) / 100) * calc_conf_rev_CF_bpsp(bill_part), 2)
        return exp_dur_subs_mnths, LTV_wo_ad_Rev
    elif duration == '5Y':
        exp_dur_subs_mnths = round(sum(projected_retenion_month[0:60]) / 100, 1)
        LTV_wo_ad_Rev = round((sum(projected_retenion_month[0:60]) / 100) * calc_conf_rev_CF_bpsp(bill_part), 2)
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
        LTV_wo_ad_Rev.append(round((sum(projected_retenion_month[0:3]) / 100) * calc_conf_rev_CF_bpsp(bill_part,path), 2))
        LTV_wo_ad_Rev.append(round((sum(projected_retenion_month[0:6]) / 100) * calc_conf_rev_CF_bpsp(bill_part,path), 2))
        LTV_wo_ad_Rev.append(round((sum(projected_retenion_month[0:9]) / 100) * calc_conf_rev_CF_bpsp(bill_part,path), 2))
        LTV_wo_ad_Rev.append(round((sum(projected_retenion_month[0:12]) / 100) * calc_conf_rev_CF_bpsp(bill_part,path), 2))
        LTV_wo_ad_Rev.append(round((sum(projected_retenion_month[0:24]) / 100) * calc_conf_rev_CF_bpsp(bill_part,path), 2))
        LTV_wo_ad_Rev.append(round((sum(projected_retenion_month[0:36]) / 100) * calc_conf_rev_CF_bpsp(bill_part,path), 2))
        LTV_wo_ad_Rev.append(round((sum(projected_retenion_month[0:48]) / 100) * calc_conf_rev_CF_bpsp(bill_part,path), 2))
        LTV_wo_ad_Rev.append(round((sum(projected_retenion_month[0:60]) / 100) * calc_conf_rev_CF_bpsp(bill_part,path), 2))
        return exp_dur_subs_mnths, LTV_wo_ad_Rev

    else:
        print("Please select appropiate option")
        return calc_exp_dur_subs_mnths_LTV_w_wo_revenue_CF_bpsp(df, bill_part)


def calc_exp_dur_subs_mnths_LTV_w_wo_revenue_LC_bpsp(df, bill_part,path, given_inp=False):

    projected_retenion_month = calc_projected_retenion_month_bpsp(df, bill_part)
    duration = 0
    if given_inp == True:
        print('options 3M, 6M, 9M, 1Y, 2Y, 3Y, 4Y, 5Y, ALL')
        duration = input('Enter the Duration from above options')

    if duration == '3M':
        exp_dur_subs_mnths = round(sum(projected_retenion_month[0:3]) / 100, 1)
        LTV_wo_ad_Rev = round((sum(projected_retenion_month[0:3]) / 100) * calc_conf_rev_LC_bpsp(bill_part), 2)
        LTV_w_ad_Rev = round(LTV_wo_ad_Rev + (sum(projected_retenion_month[0:3]) / 100) * 1.95, 2)
        return exp_dur_subs_mnths, LTV_wo_ad_Rev, LTV_w_ad_Rev

    elif duration == '6M':
        exp_dur_subs_mnths = round(sum(projected_retenion_month[0:6]) / 100, 1)
        LTV_wo_ad_Rev = round((sum(projected_retenion_month[0:6]) / 100) * calc_conf_rev_LC_bpsp(bill_part), 2)
        LTV_w_ad_Rev = round(LTV_wo_ad_Rev + (sum(projected_retenion_month[0:6]) / 100) * 1.95, 2)
        return exp_dur_subs_mnths, LTV_wo_ad_Rev, LTV_w_ad_Rev

    elif duration == '9M':
        exp_dur_subs_mnths = round(sum(projected_retenion_month[0:9]) / 100, 1)
        LTV_wo_ad_Rev = round((sum(projected_retenion_month[0:9]) / 100) * calc_conf_rev_LC_bpsp(bill_part), 2)
        LTV_w_ad_Rev = round(LTV_wo_ad_Rev + (sum(projected_retenion_month[0:9]) / 100) * 1.95, 2)
        return exp_dur_subs_mnths, LTV_wo_ad_Rev, LTV_w_ad_Rev

    elif duration == '1Y':
        exp_dur_subs_mnths = round(sum(projected_retenion_month[0:12]) / 100, 1)
        LTV_wo_ad_Rev = round((sum(projected_retenion_month[0:12]) / 100) * calc_conf_rev_LC_bpsp(bill_part), 2)
        LTV_w_ad_Rev = round(LTV_wo_ad_Rev[3] + (sum(projected_retenion_month[0:12]) / 100) * 1.95, 2)
        return exp_dur_subs_mnths, LTV_wo_ad_Rev, LTV_w_ad_Rev

    elif duration == '2Y':
        exp_dur_subs_mnths = round(sum(projected_retenion_month[0:24]) / 100, 1)
        LTV_wo_ad_Rev = round((sum(projected_retenion_month[0:24]) / 100) * calc_conf_rev_LC_bpsp(bill_part), 2)
        LTV_w_ad_Rev = round(LTV_wo_ad_Rev[4] + (sum(projected_retenion_month[0:24]) / 100) * 1.95, 2)
        return exp_dur_subs_mnths, LTV_wo_ad_Rev, LTV_w_ad_Rev

    elif duration == '3Y':
        exp_dur_subs_mnths = round(sum(projected_retenion_month[0:36]) / 100, 1)
        LTV_wo_ad_Rev = round((sum(projected_retenion_month[0:36]) / 100) * calc_conf_rev_LC_bpsp(bill_part), 2)
        LTV_w_ad_Rev = round(LTV_wo_ad_Rev[5] + (sum(projected_retenion_month[0:36]) / 100) * 1.95, 2)
        return exp_dur_subs_mnths, LTV_wo_ad_Rev, LTV_w_ad_Rev

    elif duration == '4Y':
        exp_dur_subs_mnths = round(sum(projected_retenion_month[0:48]) / 100, 1)
        LTV_wo_ad_Rev = round((sum(projected_retenion_month[0:48]) / 100) * calc_conf_rev_LC_bpsp(bill_part), 2)
        LTV_w_ad_Rev = round(LTV_wo_ad_Rev[6] + (sum(projected_retenion_month[0:48]) / 100) * 1.95, 2)
        return exp_dur_subs_mnths, LTV_wo_ad_Rev, LTV_w_ad_Rev

    elif duration == '5Y':
        exp_dur_subs_mnths = round(sum(projected_retenion_month[0:60]) / 100, 1)
        LTV_wo_ad_Rev = round((sum(projected_retenion_month[0:60]) / 100) * calc_conf_rev_LC_bpsp(bill_part), 2)
        LTV_w_ad_Rev = round(LTV_wo_ad_Rev[7] + (sum(projected_retenion_month[0:60]) / 100) * 1.95, 2)
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
        LTV_wo_ad_Rev.append(round((sum(projected_retenion_month[0:3]) / 100) * calc_conf_rev_LC_bpsp(bill_part,path), 2))
        LTV_wo_ad_Rev.append(round((sum(projected_retenion_month[0:6]) / 100) * calc_conf_rev_LC_bpsp(bill_part,path), 2))
        LTV_wo_ad_Rev.append(round((sum(projected_retenion_month[0:9]) / 100) * calc_conf_rev_LC_bpsp(bill_part,path), 2))
        LTV_wo_ad_Rev.append(round((sum(projected_retenion_month[0:12]) / 100) * calc_conf_rev_LC_bpsp(bill_part,path), 2))
        LTV_wo_ad_Rev.append(round((sum(projected_retenion_month[0:24]) / 100) * calc_conf_rev_LC_bpsp(bill_part,path), 2))
        LTV_wo_ad_Rev.append(round((sum(projected_retenion_month[0:36]) / 100) * calc_conf_rev_LC_bpsp(bill_part,path), 2))
        LTV_wo_ad_Rev.append(round((sum(projected_retenion_month[0:48]) / 100) * calc_conf_rev_LC_bpsp(bill_part,path), 2))
        LTV_wo_ad_Rev.append(round((sum(projected_retenion_month[0:60]) / 100) * calc_conf_rev_LC_bpsp(bill_part,path), 2))
        LTV_w_ad_Rev = []
        LTV_w_ad_Rev.append(round(LTV_wo_ad_Rev[0] + (sum(projected_retenion_month[0:3]) / 100) * 1.95, 2))
        LTV_w_ad_Rev.append(round(LTV_wo_ad_Rev[1] + (sum(projected_retenion_month[0:6]) / 100) * 1.95, 2))
        LTV_w_ad_Rev.append(round(LTV_wo_ad_Rev[2] + (sum(projected_retenion_month[0:9]) / 100) * 1.95, 2))
        LTV_w_ad_Rev.append(round(LTV_wo_ad_Rev[3] + (sum(projected_retenion_month[0:12]) / 100) * 1.95, 2))
        LTV_w_ad_Rev.append(round(LTV_wo_ad_Rev[4] + (sum(projected_retenion_month[0:24]) / 100) * 1.95, 2))
        LTV_w_ad_Rev.append(round(LTV_wo_ad_Rev[5] + (sum(projected_retenion_month[0:36]) / 100) * 1.95, 2))
        LTV_w_ad_Rev.append(round(LTV_wo_ad_Rev[6] + (sum(projected_retenion_month[0:48]) / 100) * 1.95, 2))
        LTV_w_ad_Rev.append(round(LTV_wo_ad_Rev[7] + (sum(projected_retenion_month[0:60]) / 100) * 1.95, 2))
        return exp_dur_subs_mnths, LTV_wo_ad_Rev, LTV_w_ad_Rev

    else:
        print("Please select appropiate option")
        return calc_exp_dur_subs_mnths_LTV_w_wo_revenue_LC_bpsp(df, bill_part)


def final_output_bpsp(df, plan_cd,path):
    output = pd.DataFrame()
    bill_part = ['AMAZON', 'GOOGLE', 'RECURLY', 'ROKU', 'OVERALL']

    if plan_cd == 'CF':
        for i in range(0, 5):
            mnths, ltv_wo_ad = calc_exp_dur_subs_mnths_LTV_w_wo_revenue_CF_bpsp(df, bill_part[i],path)
            temp = {"billing_partner": bill_part[i], "plan_type": plan_cd, "year_1_amt": ltv_wo_ad[3],
                    "year_3_amt": ltv_wo_ad[5], "year_5_amt": ltv_wo_ad[7]}
            output = output.append(temp, ignore_index=True)
    if plan_cd == 'LC':
        for i in range(0, 5):
            mnths, ltv_wo_ad, ltv_w_ad = calc_exp_dur_subs_mnths_LTV_w_wo_revenue_LC_bpsp(df, bill_part[i],path)
            temp = {"billing_partner": bill_part[i], "plan_type": plan_cd, "year_1_amt": ltv_w_ad[3],
                    "year_3_amt": ltv_w_ad[5], "year_5_amt": ltv_w_ad[7]}
            output = output.append(temp, ignore_index=True)

    #     cols = output.columns.tolist()
    #     cols = cols[-1:] + cols[:-1]
    #     output = output[cols]
    return output.round()

def add_overall_bpsp(df, plan_cd):
    filter_df = df[df['signup_plan'].str.startswith(plan_cd) & (
                df['subscription_platform'].str.endswith('AMAZON') | df['subscription_platform'].str.endswith(
            'GOOGLE') | df['subscription_platform'].str.endswith('RECURLY') | df['subscription_platform'].str.endswith(
            'ROKU'))]
    filter_df = filter_df.drop('signup_plan', axis=1)
    total = filter_df.apply(np.sum)
    total['subscription_platform'] = 'OVERALL'
    filter_df = filter_df.append(pd.DataFrame(total.values, index=total.keys()).T, ignore_index=True)

    total_starts_df = df[
        df['subscription_platform'].str.endswith('Total Starts') & ~df['subscription_platform'].str.endswith(
            'SHOWTIME- Total Starts')]
    total_starts_df = total_starts_df.drop('signup_plan', axis=1)
    total_starts_df = total_starts_df[total_starts_df.subscription_platform != 'SHOWTIME - Total Starts']
    total = total_starts_df.apply(np.sum)
    total['subscription_platform'] = 'OVERALL - Total Starts'
    total_starts_df = total_starts_df.append(pd.DataFrame(total.values, index=total.keys()).T, ignore_index=True)

    #     showtime_df=df[df['subscription_platform'].str.startswith('SHOWTIME') ]
    #     showtime_df=showtime_df.drop('signup_plan', axis=1)

    new_df = pd.concat([filter_df, total_starts_df]).reset_index(drop=True)

    return new_df

def check_data_exists_bpsp(client,src_system_id,till_date,project_id, dataset_name):
    plan_type='LC'
    sql = """
    select * from `{0}.{1}.pt_ltv_billing_partner_by_quarter` 
    where day_dt= Date(@till_date) and plan_type=@plan_type and src_system_id=@src_system_id
    """.format(project_id, dataset_name)
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("src_system_id", "NUMERIC", src_system_id),
            bigquery.ScalarQueryParameter("till_date", "TIMESTAMP", till_date),
            bigquery.ScalarQueryParameter("plan_type", "STRING", plan_type),
        ]
    )
    df = client.query(sql, job_config=job_config).to_dataframe()

    if len(df) == 0:
        return False
    else:
        plan_type = 'CF'
        sql = """
            select * from `{0}.{1}.pt_ltv_billing_partner_by_quarter` 
            where day_dt= Date(@till_date) and plan_type=@plan_type and src_system_id=@src_system_id
            """.format(project_id, dataset_name)
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("src_system_id", "NUMERIC", src_system_id),
                bigquery.ScalarQueryParameter("till_date", "TIMESTAMP", till_date),
                bigquery.ScalarQueryParameter("plan_type", "STRING", plan_type),
            ]
        )
        df = client.query(sql, job_config=job_config).to_dataframe()

        if len(df) == 0:
            return False
        else:
            return True

def update_active_ind(client,src_system_id,till_date,project_id, dataset_name):
    sql = """
        update `{0}.{1}.pt_ltv_billing_partner_by_quarter`
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

