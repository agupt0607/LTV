import os

import pandas as pd
import warnings
from google.cloud import bigquery
from Data_cohort.LTV_by_Signup_Month import LTV_Signup_mth as sm
from Data_cohort.LTV_by_Billing_Partner import LTV_Billing_partner as bp
from Data_cohort.LTV_by_Billing_Partner_Signup_Plan import LTV_Billing_Partner_Signup_plan as bpsp
from Data_cohort.LTV_by_trial_Period import LTV_Trial_Period as tp
from Data_cohort.LTV_by_Subs_Annual_Plan import LTV_subs_Annual_Plan as ap
from Util import Utils as ut
from config import config_param as cp
warnings.filterwarnings("ignore")

# start_date = cp.start_date
# project_id = cp.project_id
# dataset_name = cp.dataset_name
# end_date = cp.end_date

def get_LTV_bySignup_month(client,src_system_id,path,start_date,till_date,end_date,forced_run):
    # if sm.check_data_exists_sm(client,src_system_id,till_date,project_id, dataset_name) and forced_run==0:
    #
    #     return print("Data is present in \'pt_ltv_signup_month_by_quarter\', Please check!")

        data_w_ft  = sm.get_data_with_free_trial_sm(client,src_system_id,till_date, start_date, end_date)
        data_wo_ft = sm.get_data_without_free_trial_sm(client,src_system_id,till_date, start_date, end_date)
        if len(data_w_ft)>0 and len(data_wo_ft)>0:
            table_name = 'pt_ltv_signup_month_by_quarter'
            df = sm.combine_overall_Starts_sm(data_wo_ft, data_w_ft)
            output=sm.final_output_sm(df,path)
            otpt = ut.add_src_system_id(ut.add_current_date(ut.add_quarter_date(output, till_date)),src_system_id)
            if not ut.check_latest_data(client,src_system_id,till_date,project_id, dataset_name,table_name,otpt):
                sm.update_active_ind(client,src_system_id,till_date,project_id, dataset_name)
            else:
                return print("Data is present in \'pt_ltv_signup_month_by_quarter\', Please check!")
            otpt=ut.set_active_ind(otpt)
            if_exists_val = ut.check_if_data_present(project_id, dataset_name, table_name)
            detination_table=dataset_name+'.pt_ltv_signup_month_by_quarter'
            otpt.to_gbq(destination_table=detination_table,
                        project_id=project_id,
                        if_exists=if_exists_val,
                        table_schema=[{'name': 'day_dt', 'type': 'DATE'},{'name': 'active_ind', 'type': 'BOOLEAN'}])
            return print("Data has been populated in table \'pt_ltv_signup_month_by_quarter\'")


def get_LTV_by_Billing_Partner(client,src_system_id,path,start_date,till_date,end_date,forced_run):

    # if bp.check_data_exists_bp(client,src_system_id,till_date,project_id, dataset_name) and forced_run==0:
    #     print("Data is present in pt_ltv_billing_partner_by_quarter, Please check!")
    #     return pd.DataFrame()

    data_w_ft=bp.get_data_with_free_trial_bp(client,src_system_id,till_date, start_date, end_date)
    data_wo_ft=bp.get_data_without_free_trial_bp(client,src_system_id,till_date, start_date, end_date)
    df = bp.combine_overall_Starts_bp(data_wo_ft, data_w_ft)
    new_df = bp.add_overall_bp(df)
    print("Data has been populated in table \'pt_ltv_billing_partner_by_quarter\' for OVERALL plane code")
    return bp.final_output_bp(new_df,path)

def get_LTV_by_Billing_Partner_Signup_Plan(client,src_system_id,path,start_date,till_date,end_date,forced_run):
    # if bpsp.check_data_exists_bpsp(client,src_system_id,till_date,project_id, dataset_name) and forced_run==0:
    #     print("Data is present in \'pt_ltv_billing_partner_by_quarter\', Please check!")
    #     return 1
    if 1 :
        table_name = 'pt_ltv_billing_partner_by_quarter'
        #path = "/usr/local/airflow/dags/dags/cdm_ltv_summary_automation/config/gross_margin-2021-04-01.csv"
        data_w_ft=bpsp.get_data_with_free_trial_bpsp(client,src_system_id,till_date, start_date, end_date)
        data_wo_ft=bpsp.get_data_without_free_trial_bpsp(client,src_system_id,till_date, start_date, end_date)
        df_LC, df_CF = bpsp.combine_overall_Starts_bpsp(data_wo_ft, data_w_ft)
        df_LC_init = bpsp.add_overall_bpsp(df_LC, 'LC')
        df_CF_init = bpsp.add_overall_bpsp(df_CF, 'CF')
        df_LC_new = bpsp.final_output_bpsp(df_LC_init, 'LC',path)
        df_CF_new = bpsp.final_output_bpsp(df_CF_init, 'CF',path)
    df_overall = get_LTV_by_Billing_Partner(client,src_system_id,path,start_date,till_date,end_date,1)
    fin_df_init = df_overall.append(df_LC_new)
    fin_df = fin_df_init.append(df_CF_new)

    output_w_quarter_date = ut.add_quarter_date(fin_df, till_date)
    final_output = ut.add_src_system_id(ut.add_current_date(output_w_quarter_date),src_system_id)
    if not ut.check_latest_data(client,src_system_id,till_date,project_id, dataset_name,table_name,final_output):
        bpsp.update_active_ind(client,src_system_id,till_date,project_id, dataset_name)
    else:
        return print("Data is present in \'pt_ltv_billing_partner_by_quarter\', Please check!")
    final_output = ut.set_active_ind(final_output)
    if_exists_val = ut.check_if_data_present(project_id, dataset_name, table_name)
    destination_table=dataset_name+'.pt_ltv_billing_partner_by_quarter'
    final_output.to_gbq(destination_table=destination_table,
                project_id=project_id,
                if_exists=if_exists_val,
                table_schema=[{'name': 'day_dt', 'type': 'DATE'},{'name': 'active_ind', 'type': 'BOOLEAN'}])

    print("Data has been populated in table \'pt_ltv_billing_partner_by_quarter\'")

    return final_output

def get_LTV_by_Trial_Period(client,src_system_id,path,start_date,till_date,end_date,forced_run):
    # if tp.check_data_exists_tp(client,src_system_id,till_date,project_id, dataset_name)and forced_run==0:
    #     print("Data is present in \'pt_ltv_subs_trial_period_by_quarter\', Please check!")
    #     return 1
    if 1:
        table_name = 'pt_ltv_subs_trial_period_by_quarter'
        df=tp.get_data_tp(client,src_system_id,till_date, start_date, end_date)
        df_setting_row_data = tp.renaming_columns_tp(df)
        df_w_overall = tp.cal_overall_df_tp(df_setting_row_data)
        output = tp.calc_all_LTV_tp(df_w_overall,path)

        output_w_quarter_date=ut.add_quarter_date(output,till_date)
        final_output=ut.add_src_system_id(ut.add_current_date(output_w_quarter_date),src_system_id)

        if not ut.check_latest_data(client,src_system_id,till_date,project_id, dataset_name,table_name,final_output):
           tp.update_active_ind(client,src_system_id,till_date,project_id, dataset_name)
        else:
            return print("Data is present in \'pt_ltv_subs_trial_period_by_quarter\', Please check!")
        final_output = ut.set_active_ind(final_output)
        if_exists_val = ut.check_if_data_present(project_id, dataset_name, table_name)
        destn_table=dataset_name+'.pt_ltv_subs_trial_period_by_quarter'
        final_output.to_gbq(destination_table=destn_table,
                      project_id=project_id,
                      if_exists=if_exists_val,
                      table_schema=[{'name': 'day_dt', 'type': 'DATE'},{'name': 'active_ind', 'type': 'BOOLEAN'}])
        print("Data has been populated in table \'pt_ltv_subs_trial_period_by_quarter\'")
        return final_output

def get_LTV_by_Annual_Plan(client,src_system_id,start_date,till_date,end_date,forced_run):
    # if ap.check_data_exists(client,src_system_id,till_date,project_id, dataset_name) and forced_run==0:
    #     print("Data is present in \'pt_ltv_annual_plan_by_quarter\', Please check!")
    #     return 1
    if 1:
        table_name = 'pt_ltv_annual_plan_by_quarter'
        df=ap.get_data(client,src_system_id,till_date)
        otpt =ut.add_src_system_id(ut.add_current_date(ut.add_quarter_date(ap.annual_LTV(df), till_date)),src_system_id)
        if not ut.check_latest_data(client,src_system_id,till_date,project_id, dataset_name,table_name,otpt):
            ap.update_active_ind(client,src_system_id,till_date,project_id, dataset_name)
        else:
            return print("Data is present in \'pt_ltv_annual_plan_by_quarter\', Please check!")
        otpt = ut.set_active_ind(otpt)
        if_exists_val=ut.check_if_data_present(project_id,dataset_name,table_name)
        destn_table=dataset_name+'.pt_ltv_annual_plan_by_quarter'
        otpt.to_gbq(destination_table=destn_table,
                    project_id=project_id,
                    if_exists=if_exists_val,
                    table_schema=[{'name': 'day_dt', 'type': 'DATE'},{'name': 'active_ind', 'type': 'BOOLEAN'}])
        print("Data has been populated in table \'pt_ltv_annual_plan_by_quarter\'")

# def get_LTV_bySignup_month(till_date):
#     file_path_1 = 'Data_cohort/LTV_by_Signup_Month/LTV_Signup_month_with_free_trial_' + till_date + '.csv'
#     if not path.exists(file_path_1):
#         get_data_with_free_trial_sm(till_date, start_date, end_date)
#         LTV_signup_by_month_with_free_trial = pd.read_csv(
#             'Data_cohort/LTV_by_Signup_Month/LTV_Signup_month_with_free_trial_' + till_date + '.csv')
#     else:
#         LTV_signup_by_month_with_free_trial = pd.read_csv(
#             'Data_cohort/LTV_by_Signup_Month/LTV_Signup_month_with_free_trial_' + till_date + '.csv')
#
#     file_path_2 = 'Data_cohort/LTV_by_Signup_Month/LTV_Signup_month_without_free_trial_' + till_date + '.csv'
#     if not path.exists(file_path_2):
#         get_data_without_free_trial_sm(till_date, start_date, end_date)
#         LTV_signup_by_month_without_free_trial = pd.read_csv(
#             'Data_cohort/LTV_by_Signup_Month/LTV_Signup_month_without_free_trial_' + till_date + '.csv')
#
#     else:
#         LTV_signup_by_month_without_free_trial = pd.read_csv(
#             'Data_cohort/LTV_by_Signup_Month/LTV_Signup_month_without_free_trial_' + till_date + '.csv')
#
#     df = combine_overall_Starts_sm(LTV_signup_by_month_without_free_trial,LTV_signup_by_month_with_free_trial)
#     return final_output_sm(df)
# def get_LTV_by_Billing_Partner(till_date):
#     file_path_1 = 'Data_cohort/LTV_by_Billing_Partner/LTV_Billing_Partner_with_free_trial-' + till_date + '.csv'
#     if not path.exists(file_path_1):
#         get_data_with_free_trial_bp(till_date, start_date, end_date)
#         LTV_billing_partner_with_free_trial = pd.read_csv(
#             'Data_cohort/LTV_by_Billing_Partner/LTV_Billing_Partner_with_free_trial-' + till_date + '.csv')
#     else:
#         LTV_billing_partner_with_free_trial = pd.read_csv(
#             'Data_cohort/LTV_by_Billing_Partner/LTV_Billing_Partner_with_free_trial-' + till_date + '.csv')
#
#     file_path_2 = 'Data_cohort/LTV_by_Billing_Partner/LTV_Billing_Partner_without_free_trial-' + till_date + '.csv'
#     if not path.exists(file_path_2):
#         get_data_without_free_trial_bp(till_date, start_date, end_date)
#         LTV_billing_partner_without_free_trial = pd.read_csv(
#             'Data_cohort/LTV_by_Billing_Partner/LTV_Billing_Partner_without_free_trial-' + till_date + '.csv')
#
#     else:
#         LTV_billing_partner_without_free_trial = pd.read_csv(
#             'Data_cohort/LTV_by_Billing_Partner/LTV_Billing_Partner_without_free_trial-' + till_date + '.csv')
#
#     df = combine_overall_Starts_bp(LTV_billing_partner_without_free_trial, LTV_billing_partner_with_free_trial)
#     new_df = add_overall_bp(df)
#     return final_output_bp(new_df)
# def get_LTV_by_Billing_Partner_Signup_Plan(till_date):
#     file_path_1 = 'Data_cohort/LTV_by_Billing_Partner_Signup_Plan/LTV_Billing_Partner_Signup_Plan_with_free_trial-' + till_date + '.csv'
#     if not path.exists(file_path_1):
#         get_data_with_free_trial_bpsp(till_date, start_date, end_date)
#         LTV_billing_partner_with_free_trial = pd.read_csv(
#             'Data_cohort/LTV_by_Billing_Partner_Signup_Plan/LTV_Billing_Partner_Signup_Plan_with_free_trial-' + till_date + '.csv')
#     else:
#         LTV_billing_partner_with_free_trial = pd.read_csv(
#             'Data_cohort/LTV_by_Billing_Partner_Signup_Plan/LTV_Billing_Partner_Signup_Plan_with_free_trial-' + till_date + '.csv')
#
#     file_path_2 = 'Data_cohort/LTV_by_Billing_Partner_Signup_Plan/LTV_Billing_Partner_Signup_Plan_without_free_trial-' + till_date + '.csv'
#     if not path.exists(file_path_2):
#         get_data_without_free_trial_bpsp(till_date, start_date, end_date)
#         LTV_billing_partner_without_free_trial = pd.read_csv(
#             'Data_cohort/LTV_by_Billing_Partner_Signup_Plan/LTV_Billing_Partner_Signup_Plan_without_free_trial-' + till_date + '.csv')
#
#     else:
#         LTV_billing_partner_without_free_trial = pd.read_csv(
#             'Data_cohort/LTV_by_Billing_Partner_Signup_Plan/LTV_Billing_Partner_Signup_Plan_without_free_trial-' + till_date + '.csv')
#
#     df_LC,df_CF = combine_overall_Starts_bpsp(LTV_billing_partner_without_free_trial, LTV_billing_partner_with_free_trial)
#     df_LC_new = add_overall_bpsp(df_LC, 'LC')
#     df_CF_new = add_overall_bpsp(df_CF, 'CF')
#     return final_output_bpsp(df_LC_new,'LC'),final_output_bpsp(df_CF_new,'CF')
#
#get_LTV_by_Billing_Partner(till_date).to_csv('Output Files/LTV_by_Billing_Partner-' + till_date + '.csv', index=False)

# LC_Results,CF_Results=get_LTV_by_Billing_Partner_Signup_Plan(till_date)
# LC_Results.to_csv('Output Files/LTV_by_Billing_Partner_Signup_Plan-LC-' + till_date + '.csv', index=False)
# CF_Results.to_csv('Output Files/LTV_by_Billing_Partner_Signup_Plan-CF-' + till_date + '.csv', index=False)

def run_all(client,start_date,till_date,end_date):
    result=[]
    for i in range(len(src_system_id)):
        #result.append(get_LTV_bySignup_month(client,src_system_id[i],path,start_date,till_date,end_date,1))
        #result.append(get_LTV_by_Billing_Partner_Signup_Plan(client,src_system_id[i],path,start_date,till_date,end_date,1))
        result.append(get_LTV_by_Trial_Period(client,src_system_id[i],path,start_date,till_date,end_date,1))
        #result.append(get_LTV_by_Annual_Plan(client,src_system_id[i],start_date,till_date,end_date,1))
    return result

def main_ltv(ds, **kwargs):

    global project_id,src_system_id,ad_rev_per_subs,forced_run,dataset_name,path
    project_id = kwargs['params']['GC_BQ_PROJECT']
    ad_rev_per_subs = kwargs['params']['ad_rev_per_subs']
    src_system_id = kwargs['params']['src_system_id']
   # forced_run =kwargs['params']['forced_run']
    dataset_name =kwargs['params']['dataset_name']
    start_date = kwargs['params']['start_date']
    till_date = kwargs['params']['till_date']

    if till_date is None or till_date == "" :
        print(f'No till_date captured in the variables running for current quarter')
        till_date=ds
    if start_date is None or start_date=="":
        print(f'No start_date captured in the variables running for default start date: (2014-10-01)')
        start_date='2014-10-01'
    path = kwargs['params']['path']
    cp.ad_rev_per_subs=float(ad_rev_per_subs)

    till_date=ut.get_first_date_of_quarter(till_date)
    end_date = ut.calc_end_date(till_date)
    client = bigquery.Client(project_id)

    print(f'Running Summary process for till_date: {till_date},start_date: {start_date}, end_date: {end_date}')
    result=run_all(client,start_date,till_date,end_date)
    print(result)
    return result




# client = bigquery.Client('i-dss-cdm-data-dev')
# global project_id,src_system_id,ad_rev_per_subs,forced_run,dataset_name,path
# src_system_id = [115]
# ad_rev_per_subs = 1.95
# till_date = '2021-04-01'
# start_date = '2014-10-01'
# project_id='i-dss-cdm-data-dev'
# dataset_name='cdm_pt'
# end_date = ut.calc_end_date(till_date)
# cp.ad_rev_per_subs=1.95
# path = "/Users/agupt0607/local/dags/dags/cdm_ltv_summary_automation/config/gross_margin-2021-04-01.csv"
# run_all(client,start_date,till_date,end_date)

