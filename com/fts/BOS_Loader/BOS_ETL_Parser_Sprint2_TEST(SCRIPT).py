import pandas as pd
import logging
import csv
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import udf
import re
import time
from pyspark.sql import functions as F
from pyspark.sql.functions import length, col, explode, upper, to_date, date_sub, lag, coalesce, lit, array_sort, when, \
    arrays_zip, size, date_format, explode_outer, from_json, concat, expr, array
import pyspark.sql.functions as f
from pyspark.sql.types import *
from pyspark.sql.utils import AnalysisException
from operator import itemgetter
import datetime, re, requests
from pyspark.sql import Window
from pyspark.sql.functions import concat, col, lit
from pyspark.sql.functions import split
from pyspark.sql.functions import when
from pyspark.sql.functions import current_timestamp
from datetime import datetime
import boto3
from boto3 import client
from boto3.dynamodb.conditions import Key
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame


# read data from S3 to DataFrame
# input: S3
# output df['ROW_ID', 'BOS_FILE_EXTRACT', 'file_path', 'COLUMN_DEF']
def read_s3_to_df(day_path):
    # parameter
    i = 0  # flag for initialize dataframe

    # boto3 client
    s3 = boto3.client('s3')
    conn = client('s3')  # again assumes boto.cfg setup, assume AWS S3

    # read data file by file
    for key in conn.list_objects(Bucket='bos-etl')['Contents']:
        path_key = key['Key']
        if path_key.endswith('cleansed'):
            if day_path in path_key:
                file = s3.get_object(Bucket='bos-etl', Key=path_key)
                txt = (file['Body'].read().decode('latin1'))
                # st_re = txt.replace("", ",")
                st_re_newline = txt.replace("!!! EOS !!!", "\n")
                st_re_split = st_re_newline.split("\n")
                df = pd.DataFrame(st_re_split)
                df.index.name = 'ROW_ID'
                df.rename({0: 'BOS_FILE_EXTRACT'}, axis='columns', inplace=True)
                df["COLUMN_DEF"] = df['BOS_FILE_EXTRACT'].replace(regex=r"\.*", value="")
                # rslt_dfRFT_temp = df[df['COLUMN_DEF'] =='PAT'].head(10)
                # rslt_dfRFT_temp = df
                rslt_dfRFT_temp = df[df['COLUMN_DEF'].isin(['PAT', 'PAX', 'RFT'])]
                if ~rslt_dfRFT_temp.empty:
                    # print(path_key)
                    rslt_dfRFT_temp.insert(0, 'file_path', path_key)
                    if (i == 0):
                        rslt_dfRFT = rslt_dfRFT_temp
                        i = 1
                    else:
                        rslt_dfRFT = pd.concat([rslt_dfRFT, rslt_dfRFT_temp])
    return rslt_dfRFT


# read data as DataFrame
# select day
day_path = 'date=2023-06-17'
# select current system day
# day_path = time.strftime('%Y-%m-%d')
rslt_dfRFT = read_s3_to_df(day_path)
# len(rslt_dfRFT)
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
# DataFrame to Dataframe
from pyspark.sql import SparkSession

# Create PySpark SparkSession
spark = SparkSession.builder \
    .master("local[1]") \
    .appName("SparkByExamples.com") \
    .getOrCreate()
# Create PySpark DataFrame from Pandas
sparkDF = spark.createDataFrame(rslt_dfRFT)


# Split Raw Data
#  Start of repeat
#  END REPEAT
#  END of Data Set Indicator
# <   Carrriage Return
# DCI < SAL < TAX < ITI < FAR < FOP < END < CER < EXC < EXS <
#     < REF <

def split_raw_data(sparkDF):
    split_df_type = sparkDF.withColumn("split", F.split("BOS_FILE_EXTRACT", "")) \
        .withColumn("DCI_SAL", F.element_at("split", 1)) \
        .withColumn("split2", F.split("DCI_SAL", "<")) \
        .withColumn("DCI", F.element_at("split2", 1)) \
        .withColumn("SAL", F.element_at("split2", 2)) \
        .withColumn("TAX", F.element_at("split", 2)) \
        .withColumn("ITI", F.element_at("split", 3)) \
        .withColumn("FAR", F.element_at("split", 4)) \
        .withColumn("FOP", F.element_at("split", 5)) \
        .withColumn("END", F.element_at("split", 6)) \
        .withColumn("CER", F.element_at("split", 7)) \
        .withColumn("EXC", F.element_at("split", 8)) \
        .withColumn("EXS", F.element_at("split", 9)) \
        .withColumn("RFT", F.regexp_extract('BOS_FILE_EXTRACT', '<.+<', 0))

    split_df_type = split_df_type.select("file_path", "COLUMN_DEF", "DCI", "SAL", "TAX", "ITI", "FAR", "FOP",
                                         "END", "CER", "EXC", "EXS", "RFT", "BOS_FILE_EXTRACT")

    return split_df_type


split_df_type = split_raw_data(sparkDF)


# Common
# etl main fc
def etl_fc(df, fc, input, output):
    # fc
    fc = udf(fc, ArrayType(StringType()))

    # input
    fc_list = fc(*input)

    # output
    for i in range(len(output)):
        df = df.withColumn(output[i], fc_list[i])

    return df


# data validation fc
def dv_fc(rule_name, args_name, args, msg, paras=''):
    try:
        if args is not None:
            # rules
            if rule_name == 'check_len':
                # ValueOutOfRange
                if '.' in args:
                    (args, msg) = (args, msg) if (len(str(args).split('.')[0]) <= (paras[0] - paras[1])) & (
                                len(str(args).split('.')[1]) < paras[1]) else ('NULL',
                                                                               msg + ' # ' + args_name + 'OutOfRange: ' + str(
                                                                                   args) + ' out of (' + str(
                                                                                   paras[0]) + ',' + str(
                                                                                   paras[1]) + ') # ')
                elif re.search(r'^[0-9]+$', args):
                    (args, msg) = (args, msg) if (len(str(args).split('.')[0]) <= (paras[0] - paras[1])) else ('NULL',
                                                                                                               msg + ' # ' + args_name + 'OutOfRange: ' + str(
                                                                                                                   args) + ' out of (' + str(
                                                                                                                   paras[
                                                                                                                       0]) + ',' + str(
                                                                                                                   paras[
                                                                                                                       1]) + ') # ')
                else:
                    (args, msg) = (args, msg) if len(str(args).split('.')[0]) <= paras[0] else (
                    'NULL', msg + ' # ' + args_name + 'OutOfRange: ' + str(args) + ' out of ' + str(paras[0]) + ') # ')

            if rule_name == 'check_match':
                # Value1MismatchValue2
                msg = msg if str(args[0]) == str(args[1]) else msg + ' # ' + args_name[0] + 'MisMatch' + args_name[
                    1] + ': (' + args_name[0] + ' , ' + args_name[1] + ') : (' + str(args[0]) + ' , ' + str(
                    args[1]) + ') # '

            if rule_name == 'check_empty':
                # ValueIsEmpty
                msg = msg if args != '' else ' # ' + args_name + 'IsEmpty' + ' # '

        else:
            # ValueIsNull
            msg = msg + ' # ' + args_name + 'IsNone' + ' # '
    except Exception as e:
        print(e)
        # ValueExceptin
        msg = msg + ' # ' + args_name + 'Exception: ' + str(e) + ' # '

    return (args, msg)


# coupons format
def coupons_format(s):
    coupons = 10000
    coupons_dict = {"1": 1000, "2": 200, "3": 30, "4": 4}

    for i in range(len(s)):
        if s[i] in coupons_dict:
            coupons = coupons + coupons_dict[s[i]]

    return str(coupons)[1:]


# Custom
# PAT/PAX/REF : [transaction_date, source, booking_channel, version_no, currency_code, ticket_type]
# sc = SparkContext.getOrCreate();
glueContext = GlueContext(spark)

my_conn_options_loader_map = {
    "dbtable": "flextravelengine.fx_bre_loader_to_pos_map",
    "database": "fts_cp_uat",
    "url": "jdbc:postgresql://flextravel-uat-serverless-pg.chzjoncadzav.ca-central-1.rds.amazonaws.com:5432/fts_cp_uat",
    "customJdbcDriverS3Path": "s3://flex-data-uat-canda-central/DEV/postgresql-42.6.0.jar",
    "customJdbcDriverClassName": "org.postgresql.Driver",
    "user": "flexuatuser",
    "password": "flexnewyearpwd@2022"
}

df_loader_map = glueContext.create_dynamic_frame.from_options(
    connection_type="postgresql",
    connection_options=my_conn_options_loader_map,
    transformation_ctx="df",
).toDF()


def fc_custom(data):
    # var
    transaction_date = current_timestamp()  # <<File Generated Date>>
    source = 'BOS'  # BOS
    booking_channel = 'WEB'  # WEB
    version_no = 1
    currency_code = 'USD'  # always “USD”
    pos = df_loader_map.filter(df_loader_map['loader_name'] == 'BOSSourceFileLoader').select('id').collect()[0][0]

    ''' ticket_type :
         line starts with PAT → ticket type = TKTT
         line starts with PAX→ ticket type = EXCH-TKTT
         line starts with RFT → ticket type = RFND
         '''

    # insert var
    data = data.withColumn('transaction_date', transaction_date) \
        .withColumn('source', F.lit(source)) \
        .withColumn('booking_channel', F.lit(booking_channel)) \
        .withColumn('version_no', F.lit(version_no)) \
        .withColumn('ticket_type', when(col('column_def') == 'PAT', 'TKTT'). \
                    when(col('column_def') == 'PAX', 'EXCH-TKTT'). \
                    when(col('column_def') == 'RFT', 'RFND'). \
                    otherwise('')) \
        .withColumn('currency_code', F.lit(currency_code)) \
        .withColumn('country_code', F.lit('USA')) \
        .withColumn('passenger_count', F.lit(1)) \
        .withColumn('exception', F.lit('')) \
        .withColumn('pos', F.lit(pos))

    return data


bos_df_csv = fc_custom(split_df_type)
# DCI
# PAT/PAX/REF : ['agency_code', 'ticket_number', 'issue_date']
input = ['column_def', 'DCI', 'exception']
output = ['agency_code', 'ticket_number', 'issue_date', 'exception']


def fc_DCI(column_def, DCI, exception):
    # init
    agency_code, ticket_number, issue_date = None, None, None

    # DCI
    if DCI is not None:
        if column_def in ('PAT', 'PAX', 'RFT'):
            # split
            DCI_split = DCI.split("")

            # var
            (agency_code, exception) = dv_fc('check_empty', 'agent_code', DCI_split[1], exception) if len(
                DCI_split) > 1 else (None, exception)  # 2nd field from 1st group (VLNC-DCI)
            (ticket_number, exception) = dv_fc('check_empty', 'ticket_number', DCI_split[4] + DCI_split[5],
                                               exception) if len(DCI_split) > 5 else (
            None, exception)  # 5th field + 6th field from 1st group (BACN-DCI+ BDNR-DCI)
            (issue_date, exception) = dv_fc('check_empty', 'issue_date',
                                            datetime.strptime(DCI_split[7], '%d%b%y').strftime("%Y-%m-%d"),
                                            exception) if len(DCI_split) > 7 else (
            None, exception)  # 8th field from 1st group (DAIS-DCI). Field is reported as YYMMDD

    return [agency_code, ticket_number, issue_date, exception]


bos_df_csv = etl_fc(bos_df_csv, fc_DCI, input, output)
# SAL
# PAT/PAX : ['pnr', 'tour_code', 'passenger_name', 'coupon_used', 'original_fare', 'fare_amount', 'exchange_rate', 'commission_amount', 'original_currency', 'tax_amount', 'total_amount', 'commission','RFT']

input = ['column_def', 'SAL', 'RFT', 'exception']
output = ['pnr', 'tour_code', 'passenger_name', 'coupon_used', 'original_fare', 'fare_amount', 'exchange_rate',
          'commission_amount', 'original_currency', 'tax_amount', 'total_amount', 'commission', 'exception']


def fc_SAL(column_def, SAL, RFT, exception):
    # init
    pnr, tour_code, passenger_name, coupon_used, original_fare, fare_amount, exchange_rate, commission_amount, original_currency, tax_amount, total_amount, commission = None, None, None, None, None, None, None, None, None, None, None, None

    # SAL
    if SAL is not None:
        if column_def == 'PAT' or column_def == 'PAX':
            # split
            SAL_split = SAL.split("")

            # var
            pnr, exception = dv_fc('check_empty', 'pnr', SAL_split[14],
                                   exception)  # 15th field from 2nd group (PNRR-SAL). Note: it will be empty for RFND.
            tour_code = SAL_split[16]  # 17th field from 2nd group (TOUR-SAL). Note: it will be empty for RFND.
            passenger_name = SAL_split[15]  # 16th field from 2nd group (PXNM-SAL) for non-RFND.
            coupon_used = SAL_split[6]  # 7th field from 2nd group (CPUI-SAL) for non-RFND
            original_fare = SAL_split[8]  # 9th field from 2nd group
            original_currency = SAL_split[9]  # 10th field from 2nd group (CUOF-SAL)
            tax_amount = SAL_split[11]  # 12th field from 2nd group (TTAX-SAL)
            total_amount = SAL_split[7]  # 8th field from 2nd group (TDAM-SAL)
            fare_amount = SAL_split[10] if SAL_split[
                                               10] != '0.00' else original_fare  # 11th field from 2nd group (EQFR-SAL)… Note: if is 0.00, use the same as ORIGINAL_FARE
            exchange_rate = round(float(fare_amount) / float(original_fare), 3) if float(
                original_fare) != 0 else None  # FARE_AMOUNT / ORIGINAL_FARE
            commission_amount = fare_amount  # same as FARE_AMOUNT

            ''' COMMISSION:
            #JSON. Example: [{"type":"BASE","amount":4.26,"currency":"CAD","commissionRate":3.0}]
                       amount: 13th field from 2nd group (COAM-SAL)
                       commissionRate: 14th field from 2nd group (CORT-SAL)'''
            amount = SAL_split[12]
            commissionRate = SAL_split[13]
            type1 = 'BASE'
            currency = 'USD'

            commission = '[{' + f'"type":"{type1}","amount":{amount},"currency":"{currency}","commissionRate":{commissionRate}' + '}]'

    # RFT
    if RFT is not None:
        if column_def == 'RFT':
            # split
            RFT_split = RFT.split('<')
            RFT1_split = RFT_split[0].replace('<', '')  # RCAM RCRT
            RFT1_element = RFT1_split.split('')
            RFT2_element = RFT_split[1].split('')

            '''commission:
            JSON. Example: [{"type":"BASE","amount": -4.26,"currency":"USD","commissionRate":3.0}]
            type: BASE
            amount: 11th field from REF * (-1) because commission is being refunded
            commissionrate: 12th field from REF
            currency: USD'''

            # var
            type1 = 'BASE'
            amount = str(float(RFT1_element[10]) * -1) if float(RFT1_element[10]) != 0.00 else '0.00'
            commissionRate = RFT1_element[11]
            currency = 'USD'

            commission = '[{' + f'"type":"{type1}","amount":{amount},"currency":"{currency}","commissionRate":{commissionRate}' + '}]'

            # passenger_name
            passenger_name = RFT2_element[1]  # 22nd field from 2nd group (PXNM-REF)

            # var
            type1 = 'BASE'
            amount = str(float(RFT1_element[10]) * -1) if float(RFT1_element[10]) != 0.00 else RFT1_element[10]
            commissionRate = RFT1_element[11]
            currency = 'USD'

            commission = '[{' + f'"type":"{type1}","amount":{amount},"currency":"{currency}","commissionRate":{commissionRate}' + '}]'

            # passenger_name
            passenger_name = RFT2_element[1]  # 22nd field from 2nd group (PXNM-REF)

            # vars
            original_currency = 'USD'  # always USD
            currency_code = 'USD'  # always USD
            exchange_rate = 1.00  # 1.00
            fare_amount = float(RFT1_element[8]) * -1 if float(RFT1_element[8]) != 0.00 else RFT1_element[
                8]  # FARE x (-1)
            original_fare = fare_amount  # same as fare_amount
            commission_amount = fare_amount  # same as fare_amount
            tax_amount = float(RFT1_element[9]) * -1 if float(RFT1_element[9]) != 0.00 else RFT1_element[
                9]  # TTAX x (-1)
            total_amount = float(RFT1_element[5]) * -1 if float(RFT1_element[5]) != 0.00 else RFT1_element[
                5]  # RDAM x (-1)

    # check empty
    if commission == '[]':
        _, exception = dv_fc('check_empty', 'commission', "", exception)

    return [pnr, tour_code, passenger_name, coupon_used, original_fare, fare_amount, exchange_rate, commission_amount,
            original_currency, tax_amount, total_amount, commission, exception]


bos_df_csv = etl_fc(bos_df_csv, fc_SAL, input, output)
# TAX
# PAT/PAX : ['tax']

input = ['column_def', 'TAX', 'original_currency']
output = ['tax']


def fc_TAX(column_def, TAX, original_currency):
    # init
    tax = None

    # TAX
    if TAX is not None:
        if column_def == 'PAT' or column_def == 'PAX':
            # split
            TAX_split = TAX.split('<')[0].replace('', '').split('<')

            # var
            ''' TAX:
            Field will be <null> if is RFND
            Note: this group may have a loop
            JSON. Example: [{"type":"CA","amount":7.12,"currency":"CAD"},{"type":"YR","amount":16.00,"currency":"CAD"}]
            type: 2nd field of the loop from 3rd group (TMFT-TAX)
            amount: 1st field of the loop from 3rd group (TMFA-TAX)
            currency: 10th field from 2nd group (CUOF-SAL)'''
            tax = ''
            for i in range(len(TAX_split)):
                # split
                element = TAX_split[i].split('')

                # element
                type1 = element[1]
                amount = element[0]
                currency = original_currency

                # tax
                s = ',' if i != 0 else '['
                tax = tax + s + '{' + f'"type":"{type1}","amount":{amount},"currency":"{currency}"' + '}'

            tax = tax + ']'

            # # check empty
            # if tax == '[]':
            #     _, exception = dv_fc('check_empty', 'tax', "", exception)

    return [tax]


bos_df_csv = etl_fc(bos_df_csv, fc_TAX, input, output)
# ITI
# PAT/PAX : ['legs']

input = ['column_def', 'ITI', 'ticket_number', 'currency_code', 'original_currency', 'issue_date', 'FAR', 'exception']
output = ['legs', 'exception']


def fc_ITI(column_def, ITI, ticket_number, currency_code, original_currency, issue_date, FAR, exception):
    # init
    legs = None

    if column_def == 'PAT' or column_def == 'PAX':
        # FAR
        if FAR is not None:
            FAR_split = re.findall(r'[A-PT-ZR\s]\d+.\d{2}', FAR.replace("<", "").replace("", "").split("END")[0])
            # Q_split = re.findall(r'[\d\s][a-zA-Z]\d+\.\d+', FAR.split("END")[0])
            # Q_fare = [float(x[2:]) for x in Q_split ] # filter start alpha
            # Q_fare_toal = sum(Q_fare) if len(Q_fare) != 0 else 0
            # FAR_split = re.findall(r'[a-zA-Z]{3}[\d\.]+', re.sub(r'Q\d+\.\d{2}', "", FAR.split("END")[0]).replace(" ",""))
            Q_split = re.findall(r'\d+\.\d+', FAR.replace("<", "").replace("", "").split("END")[0])[:-1]
            Q_fare = [float(x) for x in Q_split]  # filter start alpha
            Q_fare_toal = round(sum(Q_fare), 2) if len(Q_fare) != 0 else 0
            if len(FAR_split) > 0:
                FAR_split = [x[1:] for x in FAR_split]  # filter start alpha
            if (len(FAR_split) > 1):
                # if two more than two ticket legs, direction from right -> left
                FAR_split.reverse()
                # toal_far
                fare_total = FAR_split[0]
                # leg far
                fare_legs = FAR_split[1:]
            elif (len(FAR_split) == 1):
                fare_total = FAR_split[0]
                fare_legs = -0
            else:
                fare_total = -0
                fare_legs = 0

        # ITI
        if ITI is not None:
            # split
            ITI_split = ITI.replace('', '').replace('<', '').split('<')
            if (len(ITI_split) > 1):
                # if two more than two ticket legs, direction from right -> left
                ITI_split.reverse()

            # var
            '''LEGS:
            NOTE: If the ticket type is RFND, LEGS field needs to be empty
            Note: this info is provided in the ITI group. Each ticket may have 4 legs max. After the 5th leg, the ticket is considered as conjunction. In the file, if there is a loop, the ticket has a conjunction ticket.Example: 1st leg, 2nd leg, 3rd leg, 4th leg + loop + 5th leg…If the leg is empty, it means that the ticket stopped in the previous leg. Don’t load empty values in the Json.
            JSON. example: [{"departure":"FLR","destination":"YYZ","seatClass":"C","conjunction":"1111234567890","carrier":"AC","tripCode":"876","departureOn":"2022-12-30","designator":"","stopOver":"X","flyerCode":"","fare":259.25,"currency": "CAD","originalFare":259.25,"originalCurrency":"CAD"}]
            departure: from 4th group (ORAC-ITI) → Leg 1: 4th field | Leg 2: 13th field | Leg 3: 22nd field | Leg 4: 31st field
            destination: from 4th group (DSTC-ITI) → Leg 1: 5th field | Leg 2: 14th field | Leg 3: 23rd field | Leg 4: 32nd field
            seatClass: 4th group (CLSC-ITI) → Leg 1: 8th field | Leg 2: 17th field | Leg 3: 26th field | Leg 4: 35th field
            conjunction: 1st field from 4th group (CJNR-ITI). If is empty, use the same as TICKET_NUMBER

            if the ticket includes more than 4 legs: in ITI section, the 39th field (the field after 4th repeat’s designator) will give a new BDNR (10 digits), use the BACN from DCI + new BDNR as conjunction for the following legs
            carrier: from 4th group (CARR-ITI) → Leg 1: 6th field | Leg 2: 15th field | Leg 3: 24th field | Leg 4: 33rd field
            tripCode: 4th group (FTNR-ITI) → Leg 1: 7th field | Leg 2: 16th field | Leg 3: 25th field | Leg 4: 34th field
            departured on: 4th group (FTDA-ITI) → Leg 1: 9th field | Leg 2: 18th field | Leg 3: 27th field | Leg 4: 36th field. NOTE: you need to store this format in the databse: YYYY-MM-DD but the file has JAN01 for example. Use the same procedure/logic from CAT file loader in order to convert into date

            Check the logic from CAT (Java code) with Haibinhg and Santhosh because there are some tricks in the code but the logic is:
            File will come as JUL01 (they don’t report the year) → convert into 2023-07-01
            If the departure date is before the issue date, the year will be ISSUE_DATE +1 year. Example: issue date is 2023-07-01 and departure date is JUN01, then the departure date will be 2024-06-01
            If the departure date is after or equals to the issue date, the year will be the same as Issue Date. Example: issue date is 2023-07-01 and departure date is DEC01, then the departure date will be 2023-12-01
            If the departure date is after the issue date (but after december 31st), the year will be the same as Issue Date + 1 year. Example: issue date is 2023-07-01 and departure date is JAN01, then the departure date will be 2024-01-01
            designator: 4th group (FBTD-ITI) → Leg 1: 11th field | Leg 2: 20th field | Leg 3: 29th field | Leg 4: 38th field
            stopOver: from 4th group (STPO-ITI) → Leg 1: 3rd field | Leg 2: 12th field | Leg 3: 21st field | Leg 4: 30th field
            flyerCode: <empty>
            fare: you need to check the fare construction group and then apply the same logic as CAT loader (use the same rules applied on these stories: FTS-1518, FTS-1188 item 2, FTS-1188 and FTS-1502)
            currency: same as CURRENCY_CODE
            originalFare: you need to check the fare construction group and then apply the same logic as CAT loader (use the same rules applied on these stories: FTS-1518 and FTS-1188 item 2)
            originalCurrency: same as ORIGINAL_CURRENCY'''

            # if re.match(r'^\d{4}-\d{2}-\d{2}$',str(issue_date)):
            legs = ''
            k = 0  # index for fare_legs segment
            for i in range(len(ITI_split)):
                # split
                element = ITI_split[i].split('')

                # segment
                j = 30  # index for element
                if (ticket_number is not None):
                    conjunction = ticket_number if len(element[0]) == 0 else (ticket_number[0:3] + element[0])
                    currency = currency_code
                    originalCurrency = original_currency
                    while (j > 0):
                        if len(element[j]) != 0:
                            # element
                            departure = element[j]
                            destination = element[j + 1]
                            seatClass = element[j + 4]
                            carrier = element[j + 2]
                            tripCode = element[j + 3]
                            designator = element[j + 7]
                            stopOver = element[j - 1]
                            flyerCode = ''

                            # fare
                            # far if 'X' != 0, take one from fare_leg
                            if (stopOver == 'X'):
                                fare = str('0.00')
                            else:
                                if (fare_legs == 0):
                                    fare = 'NULL'
                                elif (k < len(fare_legs)):
                                    fare = fare_legs[k]
                                    k = k + 1
                                else:
                                    fare = 'NULL'

                            originalFare = fare

                            # departureOn
                            # dep_on_date(issue_year-mm-dd)if dep_on_date(mm-dd) > issue_data(mm-dd) else   dep_on_date((issue_year+1)-mm-dd))
                            dep_on_date = element[j + 5]
                            if len(dep_on_date) != 0:
                                if re.match(r'^\d{4}-\d{2}-\d{2}$', str(issue_date)):
                                    issue_year = issue_date[0:4]
                                    dep_day = datetime.strptime(dep_on_date + '2024', '%d%b%Y').strftime(
                                        "%m-%d")  # 2024 is leap year to avoid   '02-28', just for transformation, not use it afterward
                                    dep_year = str(issue_year) if dep_day >= issue_date[5:] else str(
                                        int(issue_year) + 1)
                                    departureOn = str(pd.datetime.strptime(dep_on_date + dep_year, '%d%b%Y'))[0:10]
                                else:
                                    departureOn = ''
                            else:
                                departureOn = ''

                            # legs
                            s = '[' if ((i == len(ITI_split) - 1) and (j == 3)) else ','
                            legs = s + '{' + f'"departure":"{departure}","destination":"{destination}","seatClass":"{seatClass}","conjunction":"{conjunction}","carrier":"{carrier}","tripCode":"{tripCode}","departureOn":"{departureOn}","designator":"{designator}","stopOver":"{stopOver}","flyerCode":"{flyerCode}","fare":{fare},"currency":"{currency}","originalFare":{originalFare},"originalCurrency":"{originalCurrency}"' + '}' + legs

                        j = j - 9

            legs = legs + ']'

            # data validation
            # check fare total
            leg_fare = '%.2f' % sum([float(x[7:]) for x in re.findall(r'\"fare\"\:\d+\.\d+', legs)])
            (args, exception) = dv_fc(rule_name='check_match', args_name=('leg_fare', 'total_fare'),
                                      args=(float(Q_fare_toal), float(fare_total)), msg=exception, paras=None)

            # check legs match fare
            h = len(fare_legs) if fare_legs != 0 else 0
            (args, exception) = dv_fc(rule_name='check_match', args_name=('leg', 'fare'), args=(k, h), msg=exception,
                                      paras=None)

            # check empty
            if legs == '[]':
                _, exception = dv_fc('check_empty', 'legs', "", exception)

    return [legs, exception]


bos_df_csv = etl_fc(bos_df_csv, fc_ITI, input, output)
# FAR
# PAT/PAX : [fare_construction]

input = ['column_def', 'FAR', 'exception']
output = ['fare_construction', 'exception']


def fc_FAR(column_def, FAR, exception):
    # init
    fare_construction = None
    if FAR is not None:
        if column_def == 'PAT' or column_def == 'PAX':
            # split
            FAR_split = FAR.replace('', '').replace('<', '').split('<')  # filter the last one which is ''

            # var
            '''FARE_CONSTRUCTION:
            Note: the FAR group might have a loop, that’s why we need to use sequence 1, sequence 2, etc.
            JSON. Example: [{"sequence":1,"content":"AX373911153791006*0626/ 122948"},{"sequence":2,"content":"YHZ PD YMQ54.56CAD54.56END"}]
            content: 1st field from 5th group (FRCA-FAR)'''
            fare_construction = ''
            for i in range(len(FAR_split)):
                element = FAR_split[i].split('')
                content = element[0]
                s = ',' if i != 0 else '['
                fare_construction = fare_construction + s + '{' + f'"sequence":{i + 1},"content":"{content}"' + '}'

            fare_construction = fare_construction + ']'

            # check empty
            if fare_construction == '[]':
                _, exception = dv_fc('check_empty', 'fare_construction', "", exception)

    return [fare_construction, exception]


bos_df_csv = etl_fc(bos_df_csv, fc_FAR, input, output)
# FOP
# PAT/PAX : [payment]

input = ['column_def', 'FOP', 'currency_code', 'EXC', 'RFT', 'exception']
output = ['payment']


def fc_FOP(column_def, FOP, currency_code, EXC, RFT, exception):
    # init
    payment = None

    # PAT PAX
    if column_def == 'PAT' or column_def == 'PAX':
        # EXC
        if EXC is not None:
            if column_def == 'PAX':
                # split
                EXC_split = EXC.split('<')
                EXC2_split = EXC_split[1].replace('', '').split('<')  # loop of RACN + RDNR + CDGT
                EXC2_element = EXC2_split[0].split("")

        # FOP
        if FOP is not None:
            # split
            FOP_split = FOP.replace('', '').replace('<', '').split('<')

            # var
            '''PAYMENT:
            JSON. Example: [{"mode":"CC","type":"CCXX","amount":0.00,"accountNumber":"","approvalCode":"","invoiceNumber":"","currency":"CAD"}]
            mode: 1st field of the loop from 7th group (FPTP-FOP). Use only the first 2 chars
            type: 1st field of the loop from 7th group (FPTP-FOP)
            amount: 6th field of the loop from 7th group (FPAM-FOP)
            accountNumber: 2nd field of the loop from 7th group (FPAC-FOP)
            approvalCode: 5th field of the loop from 7th group (APLC-FOP)
            invoiceNumber: <empty>
            currency: same as CURRENCY_CODE

            for EXCH, the PAYMENT will be like the example below:
            Besides the regular FOP above, we need to add the EX info. Example: [{"mode":"EX","type":"EX","amount":0.00,"accountNumber":"451123456789001","approvalCode":"","invoiceNumber":"","currency":"CAD"},{"mode":"CC","type":"CCXX","amount":250.00,"accountNumber":"","approvalCode":"","invoiceNumber":"","currency":"CAD"}]
            accountNumber: 1st field + 2nd field + 3rd field + 19th field from 10th group (NACN-EXC+ NDNR-EXC + NCDT-EXC + RCPU-EXC). Note: RCPU is char and needs to be converted as number (use the same logic as “coupons field” from refund_legs.
            amount: 0.00'''

            '''PAX payment (json)
            IF FPTP (Form of Payment Type) field is NOT empty
            regular json object {} follows the same rules indicated in PAT
            JSON. Example: [{"mode":"CC","type":"CCXX","amount":0.00,"accountNumber":"","approvalCode":"","invoiceNumber":"","currency":"CAD"}]

            IF FPTP (Form of Payment Type) field is empty
            a json object as below
            Example: {"mode":"EX","type":"EX","amount":0.00,"accountNumber":"451123456789001","approvalCode":"","invoiceNumber":"","currency":"USD"}
            mode:  EX
            type: EX
            amount: take the FPAM (Form of Payment AmounT) field from FOP section
            accountNumber: EXC GROUP : 8th field (3 digit) + 9th field (10 digit) + 10th field (1 digit) + 22nd field  requirements confirmed
            approvalCode: leave empty
            invoiceNumber: leave empty
            currency: same as CURRENCY_CODE
            ** this condition is to be investigated and added to requirementafter 1st test
            in case the original ticket has more than 4 legs, two { EX json } is required, because originating ticket has 2 conjunction number
            1st {EX} :  the account number contains the first 4 legs' conjunction
            2nd {EX} ; the account number contains the following legs' conjunction'''

            payment = ''
            currency = currency_code
            for i in range(len(FOP_split)):
                # split
                FOP_element = FOP_split[i].split("")

                # payment
                # element
                mode, type1, accountNumber = '', '', ''
                if column_def == 'PAT':
                    mode = FOP_element[0][:2]
                    type1 = FOP_element[0]
                    accountNumber = FOP_element[1]
                    amount = FOP_element[5]
                    approvalCode = FOP_element[4]

                if column_def == 'PAX':
                    mode = 'EX'
                    type1 = 'EX'
                    accountNumber = EXC2_element[0] + EXC2_element[1] + EXC2_element[2] + EXC2_element[
                        13] if EXC2_element is not None else ''
                    amount = 0.00
                    approvalCode = ""

                invoiceNumber = ""

                s = ',' if i != 0 else '['
                payment = payment + s + '{' + f'"mode":"{mode}","type":"{type1}","amount":{amount},"accountNumber":"{accountNumber}","approvalCode":"{approvalCode}","invoiceNumber":"{invoiceNumber}","currency":"{currency}"' + '}'

            payment = payment + ']'

    # RFT
    if RFT is not None:
        if column_def == 'RFT':
            RFT_split = RFT.split('<')
            RFT2_split = RFT_split[1]  # FPTP, FPAC, AMDU
            RFT2_element = RFT2_split.split('')

            '''REF payment:
            JSON. Example: [{"mode":"CC","type":"CCVI4000","amount":0.00,"accountNumber":"VI************5960","approvalCode":"","invoiceNumber":"","currency":"USD"}]
            mode: 30th field from REF: use the first 2 characters only
            type: 30th field from REF
            amount: 26th field from REF
            accountnumber: 31th field from REF
            approvalcode: leave empty
            invoicenumber: leave empty
            currency: USD'''
            mode = RFT2_element[8][:2]
            type1 = RFT2_element[8]
            amount = RFT2_element[3]
            accountNumber = RFT2_element[9]
            approvalCode = ''
            invoiceNumber = ''
            currency = 'USD'

            payment = '[{' + f'"mode":"{mode}","type":"{type1}","amount":{amount},"accountNumber":"{accountNumber}","approvalCode":"{approvalCode}","invoiceNumber":"{invoiceNumber}","currency":"{currency}"' + '}]'

    # check empty
    if payment == '[]':
        _, exception = dv_fc('check_empty', 'payment', "", exception)

    return [payment, exception]


bos_df_csv = etl_fc(bos_df_csv, fc_FOP, input, output)
# RFT
# RFT : [refund_legs]

input = ['column_def', 'RFT', 'ticket_number', 'EXC', 'coupon_used', 'exception']
output = ['refund_legs', 'org_ticket_no', 'coupon_used', 'exception']


def fc_RFT(column_def, RFT, ticket_number, EXC, coupon_used, exception):
    # init
    refund_legs, org_ticket_no = None, None

    # RFT
    if RFT is not None:
        if column_def == 'RFT':
            # split
            RFT_split = RFT.split('<')
            RFT_split_1 = re.search('.+<', RFT).group().replace('', '').replace('<', '').split(
                '<')  # loop RACN + RDNR + RCPN
            RFT_split_2 = RFT_split[1]  # ODOI

            # var
            # refund_legs
            '''REFUND_LEGS:
            note: to be populated if ticket_type is RFND only
            JSON. example: [{"sequence":1,"ticketNumber":"0011259634355","coupons":"1000","issueDate":"2022-05-03"}]
            ticketNumber: 17th field + 18th fied from 2nd group (RACN-REF+ RDNR-REF). Note: it may have a loop here, so you need to use the sequence 1, sequence 2, etc in the JSON
            issueDate: 20th fied from 2nd group (ODOI-REF).
            coupons: 19th fied from 2nd group (RCPN-REF). Note: we receive this field as chars. You need to convert into numbers, use the same logic as CAT Loader (for example, the field comes as RR, you need to convert into “1200”. Or might come as VRRV, for example, you need to convert into “0230”. You need to apply the number/sequence only for the letter R; to the other letters (or blank) you need to put “0”)'''

            # element
            issueDate = str(datetime.strptime(RFT_split_2.split("")[0], "%Y%m%d").strftime("%Y-%m-%d")) if len(
                RFT_split_2.split("")[0]) != 0 else ""
            for i in range(len(RFT_split_1)):
                element = RFT_split_1[i].split("")

                # element
                ticketNumber = element[0] + element[1]
                coupons = coupons_format(element[2])
                first_refund_legs_ticketNumber = ticketNumber if i == 0 else ''
                first_refund_legs_coupon_used = coupons if i == 0 else ''

                # refund_legs
                refund_legs = ''
                s = ',' if i != 0 else '['
                refund_legs = refund_legs + s + '{' + f'"sequence":{str(i + 1)},"ticketNumber":"{ticketNumber}","coupons":"{coupons}","issueDate":"{issueDate}"' + '}'

            refund_legs = refund_legs + ']'

            # coupon_used
            coupon_used = first_refund_legs_coupon_used  # For RFND → same as 1st “coupons field” from REFUND_LEGS

            # org_ticket_no
            org_ticket_no, exception = dv_fc('check_empty', 'org_ticket_no', first_refund_legs_ticketNumber, exception)

            # check empty
            if refund_legs == '[]':
                _, exception = dv_fc('check_empty', 'refund_legs', "", exception)

    # EXC
    if EXC is not None:
        if column_def == 'PAX':
            # split
            EXC_split = EXC.split('<')
            EXC2_split = EXC_split[1].replace('', '').split('<')  # loop of RACN + RDNR
            EXC2_element = EXC2_split[0].split("")

        # var
        # org_ticket_no
        '''ORG_TICKET_NO:
            TKTT → same as TICKET_NUMBER
            EXCH → 8th field from EXC (3 digit) + 9th field from EXC (10 digit) – this is the field that indicates the originating ticket subjected to exchange
            RFND → same as 1st ticketNumber from REFUND_LEGS'''

        if column_def == 'PAT':
            org_ticket_no, exception = dv_fc('check_empty', 'org_ticket_no', ticket_number, exception)
        elif column_def == 'PAX':
            org_ticket_no, exception = dv_fc('check_empty', 'org_ticket_no', EXC2_element[0] + EXC2_element[1],
                                             exception)
        else:
            org_ticket_no = 'NULL'

    return [refund_legs, org_ticket_no, coupon_used, exception]


bos_df_csv = etl_fc(bos_df_csv, fc_RFT, input, output)

# data validation before insert DB
# PAT/PAX/RFT : ['exception', 'original_fare', 'exchange_rate', 'fare_amount', 'tax_amount', 'total_amount', 'agency_code']

input = ['column_def', 'exception', 'original_fare', 'exchange_rate', 'fare_amount', 'tax_amount', 'total_amount',
         'agency_code', 'source', 'coupon_used']
output = ['exception', 'original_fare', 'exchange_rate', 'fare_amount', 'tax_amount', 'total_amount', 'agency_code',
          'source', 'coupon_used']


def fc(column_def, exception, original_fare, exchange_rate, fare_amount, tax_amount, total_amount, agency_code, source,
       coupon_used):
    # len validatin
    if column_def != 'RFT':
        # original_fare
        (original_fare, exception) = dv_fc(rule_name='check_len', args_name='original_fare', args=original_fare,
                                           msg=exception, paras=(12, 5))

        # exchange_rate
        (exchange_rate, exception) = dv_fc(rule_name='check_len', args_name='exchange_rate', args=exchange_rate,
                                           msg=exception, paras=(12, 5))

        # fare_amount
        (fare_amount, exception) = dv_fc(rule_name='check_len', args_name='fare_amount', args=fare_amount,
                                         msg=exception, paras=(12, 5))

        # tax_amount
        (tax_amount, exception) = dv_fc(rule_name='check_len', args_name='tax_amount', args=tax_amount, msg=exception,
                                        paras=(12, 5))

        # total_amount
        (total_amount, exception) = dv_fc(rule_name='check_len', args_name='total_amount', args=total_amount,
                                          msg=exception, paras=(12, 5))

        # agency_code
        (agency_code, exception) = dv_fc(rule_name='check_len', args_name='agency_code', args=agency_code,
                                         msg=exception, paras=(10, 0))

        # source
        (source, exception) = dv_fc(rule_name='check_len', args_name='source', args=source, msg=exception,
                                    paras=(10, 0))

        # coupon_used
        (coupon_used, exception) = dv_fc(rule_name='check_len', args_name='coupon_used', args=coupon_used,
                                         msg=exception, paras=(10, 0))

    return [exception, original_fare, exchange_rate, fare_amount, tax_amount, total_amount, agency_code, source,
            coupon_used]


bos_df_csv = etl_fc(bos_df_csv, fc, input, output)
sc = SparkContext.getOrCreate();
glueContext = GlueContext(sc)
spark = glueContext.spark_session

my_conn_options = {
    "dbtable": "flextravel.fx_trans_file",
    "database": "fts_cp_uat",
    "url": "jdbc:postgresql://flextravel-uat-serverless-pg.chzjoncadzav.ca-central-1.rds.amazonaws.com:5432/fts_cp_uat",
    "customJdbcDriverS3Path": "s3://flex-data-uat-canda-central/DEV/postgresql-42.6.0.jar",
    "customJdbcDriverClassName": "org.postgresql.Driver",
    "user": "flexuatuser",
    "password": "flexnewyearpwd@2022"
}

my_conn_options1 = {
    "dbtable": "flextravel.general_info",
    "database": "fts_cp_uat",
    "url": "jdbc:postgresql://flextravel-uat-serverless-pg.chzjoncadzav.ca-central-1.rds.amazonaws.com:5432/fts_cp_uat",
    "customJdbcDriverS3Path": "s3://flex-data-uat-canda-central/DEV/postgresql-42.6.0.jar",
    "customJdbcDriverClassName": "org.postgresql.Driver",
    "user": "flexuatuser",
    "password": "flexnewyearpwd@2022"
}

my_conn_options2 = {
    "dbtable": "public.fx_trans_bre_interim_bos_sprint2",
    "database": "fts_cp_uat",
    "url": "jdbc:postgresql://flextravel-uat-serverless-pg.chzjoncadzav.ca-central-1.rds.amazonaws.com:5432/fts_cp_uat",
    "customJdbcDriverS3Path": "s3://flex-data-uat-canda-central/DEV/postgresql-42.6.0.jar",
    "customJdbcDriverClassName": "org.postgresql.Driver",
    "user": "flexuatuser",
    "password": "flexnewyearpwd@2022"
}

df = glueContext.create_dynamic_frame.from_options(
    connection_type="postgresql",
    connection_options=my_conn_options,
    transformation_ctx="df",
)

df_GI = glueContext.create_dynamic_frame.from_options(
    connection_type="postgresql",
    connection_options=my_conn_options1,
    transformation_ctx="df",
)
# GI_df = df_GI.toDF().select("id","tids_code")

# # bos_df_csv=  bos_df_csv.withColumn( "Orginal_currency",F.when(length(col("Org_currency"))>5,'').otherwise(bos_df_csv.Org_currency))

# bos_df_final = bos_df_csv.select('agency_code', 'ticket_number', 'issue_date', 'pnr', 'tour_code', 'passenger_name', 'coupon_used', 'original_fare', 'fare_amount', 'exchange_rate', 'commission_amount', 'original_currency', 'tax_amount', 'total_amount', 'commission', 'org_ticket_no', 'tax', 'legs', 'fare_construction', 'payment','refund_legs','exception')

# Bos_df_GI= bos_df_final.join(GI_df,
#                bos_df_csv.agency_code == GI_df.tids_code,
#                "left")


# Bos_df_GI=Bos_df_GI.withColumnRenamed("id","agent_id")

# Bos_df_GI.show(2, truncate=False)

# df1 = df.toDF().select("id", "supplier_id","sup_srv_map_id","file_name").where(df["file_name"] =='06.07_DCALLRECORDSAM')
# #df2=df1.withColumnRenamed("sup_srv_map_id","col0").withColumnRenamed("file_name","col1")
# # df1 = df.filter(f=lambda x: x["sup_srv_map_id"] in [65])

# df1 = df.toDF()["id", "supplier_id","sup_srv_map_id","file_name"]
# df1 = df1[df1["file_name"] =='06.07_DCALLRECORDSAM']


# df1.show()

# Bos_df_file= Bos_df_GI.join(df1,
#                bos_df_csv.tax_on_commission == df1.file_name,
#                "left")

# Bos_df_file.show()

# FTS - 2598 ["Tax_Amt"].cast(IntegerType()) -> DoubleType, ["Total_amt"].cast(IntegerType()) -> DoubleType
Bos_df_file = bos_df_csv
Bos_df_file = Bos_df_file.withColumn("original_fare", Bos_df_file["original_fare"].cast(DoubleType())).withColumn(
    "fare_amount", Bos_df_file["fare_amount"].cast(DoubleType())).withColumn("tax_amount",
                                                                             Bos_df_file["tax_amount"].cast(
                                                                                 DoubleType())) \
    .withColumn("total_amount", Bos_df_file["total_amount"].cast(DoubleType())).withColumn("exchange_rate", Bos_df_file[
    "exchange_rate"].cast(DoubleType()))

Bos_df_file = Bos_df_file.withColumn("commission_amount",
                                     Bos_df_file["commission_amount"].cast(DoubleType())).withColumn("issue_date",
                                                                                                     Bos_df_file[
                                                                                                         "issue_date"].cast(
                                                                                                         TimestampType()))

Bos_df_write = Bos_df_file.select('transaction_date', 'source', 'booking_channel', 'version_no', 'currency_code',
                                  'ticket_type', 'country_code', 'passenger_count', 'agency_code', 'ticket_number',
                                  'issue_date', 'pnr', 'tour_code', 'passenger_name', 'coupon_used', 'original_fare',
                                  'fare_amount', 'exchange_rate', 'commission_amount', 'original_currency',
                                  'tax_amount', 'total_amount', 'commission', 'org_ticket_no', 'tax', 'legs',
                                  'fare_construction', 'payment', 'refund_legs', 'exception', 'file_path', 'pos')

Bos_df_write_dyn = DynamicFrame.fromDF(Bos_df_write, glueContext, 'Bos_df_write_dyn')

df3 = glueContext.write_dynamic_frame_from_options(
    frame=Bos_df_write_dyn,
    connection_type="postgresql",
    connection_options=my_conn_options2,
    transformation_ctx="dynamic_frame"
)
# df3.show()
job.commit()