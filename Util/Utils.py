import datetime
import dateutil.relativedelta
import pandas as pd

def calc_end_date(till_date):
    d = datetime.datetime.strptime(till_date, "%Y-%m-%d")
    d2 = d - dateutil.relativedelta.relativedelta(months=1)
    return (d2.date())

def add_quarter_date(temp,till_date):
    temp['day_dt'] = till_date
    cols = temp.columns.tolist()
    cols = cols[-1:] + cols[:-1]
    output = temp[cols]
    return output

def add_current_date(temp):
    from datetime import datetime
    datetime.now()
    today = datetime.now()
    temp['dw_create_dt'] = pd.to_datetime(today)
    return temp

def check_if_forced(temp,is_forced):
    if is_forced==True:
        temp['active_ind']=False
        return temp
    else:
        temp['active_ind']=True
        return temp

def check_if_data_present(project_id,dataset,table):
    from google.cloud import bigquery
    sql = "SELECT * FROM `{0}.{1}.{2}`".format(project_id, dataset, table)
    client = bigquery.Client(project_id)
    job = client.query(sql)
    result = job.result()
    if(result.total_rows>0):
        return 'append'
    else:
        return 'replace'

def add_src_system_id(temp,src_system_id):
    temp['src_system_id'] = src_system_id
    return temp