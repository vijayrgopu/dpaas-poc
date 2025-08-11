%sql
TRUNCATE TABLE adxdemo.dim_subscription;
TRUNCATE TABLE adxdemo.fplt;
TRUNCATE TABLE adxdemo.scd_in_subs_status;
TRUNCATE TABLE adxdemo.scd_sales_document;
TRUNCATE TABLE adxdemo.subscription_params;
TRUNCATE TABLE adxdemo.vbak;
TRUNCATE TABLE adxdemo.vbap;
TRUNCATE TABLE adxdemo.vbkd;
TRUNCATE TABLE adxdemo.vbrk;
TRUNCATE TABLE adxdemo.vbrp;

%python

from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql import DataFrame,Row
from pyspark.sql.window import Window
from pyspark.sql.functions import to_date,date_add,when,col,lit,coalesce,concat,lag,lead,current_timestamp,date_format
from pyspark.sql.types import StringType
from delta.tables import *

sapHANAUrl    = "jdbc:sap://10.50.194.178:30015"
sapHANADriver = "com.sap.db.jdbc.Driver"
hanaUser      = ""
hanaPassword  = ""

def get_dim_subscription(sapHANAUrl,sapHANADriver,hanaUser,hanaPassword) -> DataFrame:

    dim_subscription_sql = '''(select client
                                        ,sales_document
                                        ,sales_document_item
                                        ,dim_sub.created_date_item              
                                        ,dim_sub.payment_method 
                                        ,dim_sub.contract_start_date_bp
                                        ,dim_sub.contract_end_date_bp
                                        ,dim_sub.sales_document_type
                                        ,dim_sub.subscription_category
                                        ,dim_sub.offer_type
                                        ,dim_sub.route_to_market
                                        ,dim_sub.billing_frequency
                                        ,dim_sub.bill_plan_number
                                        ,dim_sub.subscription_type
                                from    TRNFRM_ECC_CCM.dim_subscription dim_sub 
                                )'''
    df_src_data = spark.read.format("jdbc") \
        .option("url", sapHANAUrl) \
        .option("driver", sapHANADriver) \
        .option("user", hanaUser) \
        .option("password", hanaPassword) \
        .option("numPartitions", 10) \
        .option("dbtable", dim_subscription_sql) \
        .load()

    return df_src_data

df_dim_subscription = get_dim_subscription(sapHANAUrl,sapHANADriver,hanaUser,hanaPassword)
df_dim_subscription.write.mode("overwrite").saveAsTable("dpaas_uccatalog_stg.adxdemo.DIM_SUBSCRIPTION")

