
print("************User Variables************")
import sys
import json
import pandas as pd
import snowflake.connector
import sqlalchemy as db
from sqlalchemy import create_engine as ce
from datetime import datetime
import psycopg2 as ps
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import calendar
print("************ STEP 1: Data Reception ************")
def read_json_file(file):
    data = pd.read_json(file)
    return data
print("************ STEP 2: Connection************")
try :
    ctx = snowflake.connector.connect(
        user='TAILERDAVIS',
        password='Mtfbwy2024!',
        account='wwbqzzj-zt38665',
        warehouse='COMPUTE_WH',
        database='EDGE',
        schema='TAKE_HOME',
    )
    cs = ctx.cursor()
except ps.OperationalError as e:
        raise e 
else:
    print("Successful Connection!")

print("************ STEP 3: Data Cleaning/Validation and Loading************") 
#assigning variables to appropriate data types for easier usage
file = 'account_1.json'
data = read_json_file(file)
for i in data['transactions']:
       date = i['txn_date']
       Descr = i['txn_memo']
       type = i['txn_type']
       amt = i['txn_value']
       float(amt)
       list = [date, Descr , amt, type]
       list_columns = ['DATE', 'DESCRIPTION', 'AMOUNT', 'CREDITORDEBIT']
       write = pd.DataFrame([list] , columns= list_columns)
       pd.to_datetime(write['DATE'])
       #table = 'banking_information'
       #write_pandas(ctx, write, table.upper(), auto_create_table=True)

print("************ STEP 4: Data Analysis************")

qry = ''' select * from BANKING_INFORMATION '''
num_trans_qry = ''' select count('AMOUNT') FROM BANKING_INFORMATION'''
start_qry = '''SELECT DATE, AMOUNT,CREDITORDEBIT FROM TAKE_HOME.BANKING_INFORMATION WHERE CREDITORDEBIT = 'credit' ORDER BY DATE ASC '''
deposit_qry = '''SELECT DATE, AMOUNT, CREDITORDEBIT FROM TAKE_HOME.BANKING_INFORMATION
WHERE CREDITORDEBIT = 'credit' '''
most_activity_qry= ''' SELECT COUNT(DATE), AMOUNT, CREDITORDEBIT FROM TAKE_HOME.BANKING_INFORMATION
GROUP BY AMOUNT, CREDITORDEBIT 
HAVING COUNT(DATE) > 1 ''' 
withdrawl_qry = '''SELECT DATE, AMOUNT, CREDITORDEBIT FROM TAKE_HOME.BANKING_INFORMATION
WHERE CREDITORDEBIT = 'debit' '''

val = pd.DataFrame(values)
val.reset_index()
json_df = val.to_json(orient='records')
with open("data.json", "w") as jsonfile:
    jsonfile.write(json_df)
    print("Write successful")

def snowdf(qry):
    cs.execute(qry)
    result = cs.fetchall()
    columns = [desc[0] for desc in cs.description]
    df = pd.DataFrame(result, columns=columns)
    return df
df = snowdf(qry)
num_trans = snowdf(num_trans_qry)
start= snowdf(start_qry)
deposits = snowdf(deposit_qry)
most = snowdf(most_activity_qry)
withdrawl= snowdf(withdrawl_qry)

week_withdrawl = withdrawl[['AMOUNT','DATE']]
format_date = pd.to_datetime(week_withdrawl['DATE'], format = '%Y-%m-%d')
week_withdrawl.index = format_date

week_deposit = deposits[['AMOUNT','DATE']]
format_date = pd.to_datetime(week_deposit['DATE'], format = '%Y-%m-%d')
week_deposit.index = format_date

cal_withdrawl= week_withdrawl.resample('W-Mon').sum() 
cal_deposit = week_deposit.resample('W-Mon').sum() 
weekly_balance = cal_deposit['AMOUNT'] - cal_withdrawl['AMOUNT']

format_date = pd.to_datetime(df['DATE'], format = '%Y-%m-%d')
df.index = format_date
top_5_days = df.resample('W-Mon').sum().sort_values(by = 'DATE',ascending=False).head(5)

format_date = pd.to_datetime(deposits['DATE'], format = '%Y-%m-%d')
deposits.index = format_date
expected_deposit = deposits.resample('M').mean().sort_values(by = 'DATE',ascending=False)

format_date = pd.to_datetime(withdrawl['DATE'], format = '%Y-%m-%d')
withdrawl.index = format_date
weekly_withdrawl= withdrawl.resample('W-Mon').sum()
weekly_withdrawl.sum()

format_date = pd.to_datetime(deposits['DATE'], format = '%Y-%m-%d')
deposits.index = format_date
last_30_days = deposits.resample('D').last().tail(30).mean()

oldests_trans = df['DATE'].min()

avg_spending = df['AMOUNT'].sum() / len(df['AMOUNT'])
avg_deposit = deposits['AMOUNT'].sum()/len(deposits)

num_days_with_transactions = df['DATE'].nunique()
start_bal = start['AMOUNT'].head(1)


print("************ STEP 5: Returning Data************")

values = ({
    "trans_info" : [
        num_trans,
        num_days_with_transactions,
        oldests_trans
        ]
, "Avgerages" :[
        avg_spending,
        last_30_days,
        0
    ]
    ,"weekly_summary" :[ 
           week_deposit,
           week_withdrawl,
           weekly_balance
        ]
    ,'Deposit':[
     top_5_days
    ,expected_deposit
    ,avg_deposit]
})