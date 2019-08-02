import sys
import os
import argparse

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, last_day, when, isnull, col, sum, count, round

context = SparkContext()
spark = SparkSession(context)

try:
    parser = argparse.ArgumentParser()
    parser.add_argument('--payment_data', required=True, help='Payment Data File Path')
    parser.add_argument('--transaction_type_data', required=True, help='Transaction Types File Path')
    parser.add_argument('--transaction_type_backend_data', required=True, help='Transaction Types (Backend) Data File Path')
    parser.add_argument('--merchant_business_type_data', required=True, help='Merchant Business Types File Path')
    parser.add_argument('--mcc_data', required=True, help='Merchant Category Codes File Path')

    args = parser.parse_args()

    PAYMENT_DATA_PATH = args.payment_data
    TRANSACTION_TYPE_DATA_PATH = args.transaction_type_data
    TRANSACTION_TYPE_BACKEND_DATA_PATH = args.transaction_type_backend_data
    MERCHANT_BUSINESS_TYPE_DATA_PATH = args.merchant_business_type_data
    MCC_DATA_PATH = args.mcc_data
except Exception as e:
    print('Error reading input arguments: {}'.format(str(e)))
    exit(1)

df = spark \
    .read \
    .option('header', True) \
    .option('inferSchema', True) \
    .csv(PAYMENT_DATA_PATH)

# './data/acme_december_2018.csv'
# './data/transaction_type.csv'
# './data/transaction_type_backend.csv'
# './data/merchant_business_type.csv'
# './data/mcc_data.csv'

# Check dataframe's schema
df.printSchema()

# Check dataframe's record count
print(df.count())

### COLUMN 1
df = df.withColumn('fi_code', lit('42'))

### COLUMN 2
df = df.withColumn('date_last_day', last_day(df.date)).drop('date').withColumnRenamed('date_last_day', 'date')

### COLUMN 3
df = df.withColumn('service_system_type',
  when(df.payment_method == 'payment01', 'CPF') \
  .otherwise('OTH')
)

### COLUMN 4
df = df.withColumn('country',
    when(df.card_country_issuer_code == 'A029', 'Local') \
    .when((df.card_country_issuer_code != 'A029') | ~isnull(df.card_country_issuer_code), 'Inter') \
    .otherwise(df.card_country_issuer_code)
)

df_01 = df.where("payment_method == 'payment01'")
df_02 = df.where("payment_method != 'payment01'")

# Read transaction type details file
transaction_type_df = spark \
    .read \
    .option('header', True) \
    .csv(TRANSACTION_TYPE_DATA_PATH)

# Join payment and transaction type dataframes by country, card_type & card_brand
joined_df_01 = df_01.alias('a').join(
    transaction_type_df.alias('b'), 
    [
      col('a.country') == col('b.country'),
      col('a.card_type') == col('b.card_type'),
      col('a.card_brand') == col('b.card_brand')
    ],
    'leftouter'
).select(
    [
      'date',
      'fi_code',
      'service_system_type',
      'a.country',
      'a.payment_method',
      'a.card_brand',
      'a.card_type',
      'backend_name',
      'transaction_type_code',
      'merchant_category_id',
      'card_country_issuer_code',
      'amount'
    ]
)

# If card_type, card_country_issuer_code or card_brand is null, consider transaction_type_code as 099999
joined_df_01 = joined_df_01.withColumn('transaction_type_code', 
    when(isnull(joined_df_01.card_type) | isnull(joined_df_01.card_country_issuer_code) | isnull(joined_df_01.card_brand), '099999') \
    .otherwise(joined_df_01.transaction_type_code)
).withColumnRenamed('transaction_type_code', 'transaction_type')

# Read transaction type backend details
tt_backend_df = spark \
      .read \
      .option('header', True) \
      .csv(TRANSACTION_TYPE_BACKEND_DATA_PATH)

# Join payments from payment method other than 'payment01' with transaction type backend details
joined_df_02 = df_02.alias('a').join(
    tt_backend_df.alias('b'), 
    [
      col('a.backend_name') == col('b.backend_name'),
    ],
    'leftouter'
).select(
    [
      'date',
      'fi_code',
      'service_system_type',
      'a.country',
      'a.payment_method',
      'a.card_brand',
      'a.card_type',
      'b.backend_name',
      'b.transaction_type_code',
      'merchant_category_id',
      'card_country_issuer_code',
      'amount'
    ]
).withColumnRenamed('transaction_type_code', 'transaction_type')

# There are 621 rows with missing backend name
# joined_df_02.groupBy(
#     'backend_name',
#     'transaction_type'
# ).count().orderBy('backend_name').show()

# Join two dataframes containing transaction_type into one
col_04_df = joined_df_01.union(joined_df_02)

# Check count
print(col_04_df.count())

# Read merchant business type details
merchant_business_type_df = spark \
      .read \
      .option('header', True) \
      .option('inferSchema', True) \
      .csv(MERCHANT_BUSINESS_TYPE_DATA_PATH)

# Add 'service_system_type' column to merchant business type dataframe with 'CPF' as value for easier join
merchant_business_type_df = merchant_business_type_df.withColumn('service_system_type', lit('CPF'))

col_05_df = col_04_df.alias('a').join(
    merchant_business_type_df.alias('b'),
    [
      col('a.card_brand') == col('b.card_brand'),
      col('a.service_system_type') == col('b.service_system_type')
    ],
    'leftouter'
).select(
  [
    'date',
    'fi_code',
    'a.service_system_type',
    'country',
    'payment_method',
    'a.card_brand',
    'card_type',
    'card_country_issuer_code',
    'a.backend_name',
    'transaction_type',
    'merchant_category_id',
    'merchant_business_type_code',
    'amount'
  ]
).withColumnRenamed('merchant_business_type_code', 'merchant_business_type')

# Read merchant category code
mcc_data_df = spark \
      .read \
      .option('header', True) \
      .option('inferSchema', True) \
      .csv(MCC_DATA_PATH)

col_06_df = col_05_df.alias('a').join(
    mcc_data_df.alias('b'),
    [
      col('a.merchant_category_id') == col('b.id')
    ],
    'leftouter'
).select(
  [
    'date',
    'fi_code',
    'service_system_type',
    'country',
    'payment_method',
    'card_brand',
    'card_type',
    'card_country_issuer_code',
    'backend_name',
    'transaction_type',
    'a.merchant_category_id',
    'merchant_business_type',
    'b.code',
    'amount'    
  ]
).withColumnRenamed('code', 'merchant_category_code')

# Fix merchant_category_code for service_system_type (CPF) with missing merchant_category_id or merchant_category_code
col_06_df = col_06_df.withColumn('merchant_category_code', 
    when((col_06_df.service_system_type == 'CPF') & (isnull(col_06_df.merchant_category_id) | isnull(col_06_df.merchant_category_code)), '9999') \
    .otherwise(col_06_df.merchant_category_code)
)

col_07_08_df = col_06_df.groupBy(
    'fi_code',
    'date',
    'service_system_type',
    'transaction_type',
    'merchant_business_type',
    'merchant_category_code'
).agg(sum(col_06_df.amount), count('date'))

col_07_08_df = col_07_08_df.withColumnRenamed('sum(amount)', 'amount') \
                  .withColumnRenamed('count(date)', 'number')

col_09_df = col_07_08_df.withColumn('average_amount', col_07_08_df.amount / col_07_08_df.number)

col_09_df = col_09_df.withColumn('terminal_average_amount_range',
    when(col_09_df.average_amount <= 500, '94560000001')
    .when((col_09_df.average_amount > 500) & (col_09_df.average_amount <= 1000), '94560000002')
    .when((col_09_df.average_amount > 1000) & (col_09_df.average_amount <= 2000), '94560000003')
    .when((col_09_df.average_amount > 2000) & (col_09_df.average_amount <= 5000), '94560000004')
    .when((col_09_df.average_amount > 5000) & (col_09_df.average_amount <= 10000), '94560000005')
    .when((col_09_df.average_amount > 10000) & (col_09_df.average_amount <= 30000), '94560000006')
    .when(col_09_df.average_amount > 30000, '94560000007')
).drop('average_amount')

# Check sum amount between original and transformed data
col_09_df.select(sum(col_09_df.amount)).show()
df.select(sum('amount')).show()

dates = df.select('date').distinct().rdd.collect()

# if not os.path.exists('/output'):
#     try:
#         original_umask = os.umask(0)
#         os.makedir('/output', 0777)
#     finally:
#         os.umask(original_umask)

# Export data to CSV
for date in dates:
    file_name_date = date[0].strftime('%Y%m%d')
    filter_date = date[0].strftime('%Y-%m-%d')

    file_name = './{prefix}_{date}.csv'.format(
        prefix = 'ACME',
        date = file_name_date
    )

    col_09_df \
        .where("date = '{}'".format(filter_date)) \
        .toPandas() \
        .to_csv(
            file_name,
            sep='|',
            float_format='%.2f',
            line_terminator='\r\n',
            index=False
        )