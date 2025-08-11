%python
from pyspark.errors import PySparkException

#get all catalogs
df_catalogs = spark.sql("SHOW CATALOGS");

#filter samples and system catalogs as they are not required
df_catalogs_filtered = df_catalogs.where((df_catalogs.catalog != "samples") & (df_catalogs.catalog != "system"))
df_catalogs_filtered.show()

#for each catalog get all the databases/schemas
for catalog in df_catalogs_filtered.select("catalog").rdd.flatMap(lambda x : x).collect():
    #print(catalog)
    sql_stmt_show_schemas = "SHOW SCHEMAS IN " + catalog
    df_schema_all = spark.sql(sql_stmt_show_schemas)
    #filter out the information_schema as this is system specific
    df_schema = df_schema_all.where(df_schema_all.databaseName != "information_schema")
    #df_schema.show()
    for schema in df_schema.select("DATABASENAME").rdd.flatMap(lambda db : db).collect():
        #print(schema)
        #List of all tables from the above catalog
        if schema != 'default':
            try:
                tables = spark.catalog.listTables(schema)
                for table in tables:
                    if table.tableType != 'VIEW' and table.tableType != 'EXTERNAL':
                            tbl_name_full = table.namespace[0] + "." + table.name
                            sql_stmt = "DESCRIBE DETAIL " + tbl_name_full
                            df = spark.sql(sql_stmt).select(*['NAME','SIZEINBYTES'])
                            df.write.mode("append").saveAsTable("dpaas_uccatalog_stg.adxdemo.TABLE_INFO")
            except PySparkException as ex:
                        print("Error Class       : " + ex.getErrorClass())
                        print("Message parameters: " + str(ex.getMessageParameters()))
                        print("SQLSTATE          : " + ex.getSqlState())
                        print(ex)

------------------------------------------------------------------
%sql
--query by schema size
SELECT 
    substring_index(NAME,".",1) AS CATLOG_NAME, 
    substring_index(substring_index(NAME,".",2),".",-1) AS SCHEMA_NAME, 
    ROUND(SUM(ROUND(SIZEINBYTES/1000000000,2)),2) AS SCHEMA_SIZEINGB 
FROM adxdemo.table_info
WHERE SIZEINBYTES > 0
GROUP BY ALL
HAVING SCHEMA_SIZEINGB > 0
ORDER BY SCHEMA_SIZEINGB DESC
--------------------------------------------------------------------

%sql
--query by table size
SELECT 
    substring_index(NAME,".",1) AS CATLOG_NAME, 
    substring_index(substring_index(NAME,".",2),".",-1) AS SCHEMA_NAME, 
    substring_index(NAME,".",-1) AS TABLE_NAME,
    SIZEINBYTES,
    ROUND(SIZEINBYTES/1000000000,2) AS SIZEINGB 
FROM adxdemo.table_info
WHERE SIZEINBYTES > 0
ORDER BY SIZEINBYTES DESC

--------------------------------------------------------------------------


      
