from google.cloud import bigquery
import numpy as np
import pandas as pd
from SQL import sql_queries as sq
def get_data(client,src_system_id,till_date):
    sql=sq.annual_plan()
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("src_system_id", "NUMERIC", src_system_id),
            bigquery.ScalarQueryParameter("till_date", "DATE", till_date)
        ]
    )
    df = client.query(sql, job_config=job_config).to_dataframe()
    return df

def annual_CF(df):
    filter_df=df[df['signup_plan_cd'].str.endswith('allaccess_ad_free_annual')]
    cols = filter_df.columns.tolist()
    total_CF= filter_df[cols[3:]].apply(np.sum)
    total_CF['signup_plan_cd'] = 'OVERALL-CF'
    LTV_CF=total_CF['Paid_subs_1Invoice']/total_CF['ttl_trial_subs']*99.99*0.74
    return LTV_CF

def annual_LCS(df):
    filter_df=df[df['signup_plan_cd'].str.endswith('allaccess_annual')]
    cols = filter_df.columns.tolist()
    total_LCS= filter_df[cols[3:]].apply(np.sum)
    total_LCS['signup_plan_cd'] = 'OVERALL-LCS'
    LTV_LCS=total_LCS['Paid_subs_1Invoice']/total_LCS['ttl_trial_subs']*59.99*0.74
    return LTV_LCS

def annual_LTV(df):
    LTV_CF=round(annual_CF(df),2)
    LTV_LCS=round(annual_LCS(df),2)
    overall={'billing_partner':['RECURLY'],'cf_plan':[LTV_CF],'lcs_plan':[LTV_LCS]}
    output=pd.DataFrame(overall).reset_index(drop=True)
    return output

def check_data_exists(client,src_system_id,till_date):

    plan_type='Overall'
    sql = """
    select * from `ltvsubscribers.anit_sandbox.pt_ltv_annual_plan_by_quarter` 
    where day_dt= Date(@till_date) and src_system_id=@src_system_id
    """
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
