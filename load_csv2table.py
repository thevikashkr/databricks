'''
Scripts loading CSV files to target table. There are different modes:
  - Incremental load using PARTITION BY columns
  - Partitioned table load (insert overwrite partitions or append)
  - Partitioned table load with primary key to reload data in partitions (insert overwrite partitions or append)
  - Overwrite load

Example usage:

args_list = [
    '-c', 'osi_bw_prod_config', 
    '-cd', 'dbfs:/mnt/etlproductsupply/osi0bw/prod/code', 
    '-td', 'dp_osi_bw_global', 
    '-tt', 'customer_payment_final_pq', 
    '-i', 'dbfs:/mnt/etlproductsupply/osi0bw/prod/input/KAP_CUST_2*.CSV', 
    '-p', 'sap_srce_sys_id, sap_client_id, company_code_id, pymt_doc_id, pymt_item_id', 
    '-a', 'dbfs:/mnt/etlproductsupply/osi0bw/prod/archive', 
    '-sft', 'sap_mod_date', 
    '-sfn', 'load_from_file', 
    '-bdt', 'bd_mod_time_stamp', 
    '-d', ',', 
    '-hd', 'true', 
    '-sp', 'dbfs:/mnt/datahubproductsupply/consolidated/bw-global/final/CUSTOMER_PAYMENT_FINAL_PQ', 
    '-md', 'load_process_control', 
    '-mt', 'load_process_plc'
]
load_csv2table.main(args_list)
'''

import sys, os, glob, shutil
sys.path.append('/dbfs/mnt/etlproductsupply/shared/prod/code/python')
#sys.path.append('/dbfs/mnt/mount_blob/shared0scripts/python')

from datetime import datetime
from pyspark.sql.types import *
from pyspark.sql.functions import *
import re, os, ast, collections, time, socket, getpass, json


def main_argparse(logging , args_lst):
    ''' Parse input parameters '''
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config_module", required=True)
    parser.add_argument("-cd", "--config_module_dir", help="Config module FS dir path", required=True)
    parser.add_argument("-td", "--target_db_name", help="Target database", required=True)
    parser.add_argument("-tt", "--target_tb_name", help="Target table", required=True)
    parser.add_argument("-sft", "--load_tmstp_col", default="load_tmstp", help="Source file date column name (default=load_tmstp)")
    parser.add_argument("-sfn", "--load_from_file_col", default="load_from_file", help="Source file name column name (default=load_from_file)")
    parser.add_argument("-bdt", "--bd_mod_tmstp_col", default="bd_mod_tmstp", help="Big Data load timestamp column name (default=bd_mod_tmstp)")
    parser.add_argument("-i", "--input_path", help="Input file path", required=True)
    parser.add_argument("-d", "--delimiter", default="|", help="Source file delimiter (default=|)", required=False)
    parser.add_argument("-p", "--primary_key", help="Table primary key(s)", required=False)
    parser.add_argument("-a", "--archive_dir", nargs="*", help="Archive directory", required=False)
    parser.add_argument("-pa", "--partition_cols", nargs="*", help="Table partition columns (format = column_name:position_in_source_file)", required=False)
    parser.add_argument("-hd", "--header", default="false", choices=['false','true'], help="First row is header (default=false)", required=False)
    parser.add_argument("-sp" , "--second_path", nargs="+", help="Additional DBFS path(s) for parquet data", required=False)
    parser.add_argument("-md" , "--metadata_db", help="Metadata Database", required=False)
    parser.add_argument("-mt" , "--metadata_tb", help="Metadata Table", required=False)
    parser.add_argument("-od" , "--overwrite_data", default="overwrite", choices=['overwrite','append'], help="Overwrite data (default=overwrite, append)", required=False)
    #Print parameters
    args = parser.parse_args(args=args_lst)
    for arg in vars(args):
      logging.info("Input param: {} = {}".format(arg, getattr(args, arg)))
    return args


def run_sql_script(logging, spark_session, params, script_path):
    ''' Run post SQL script '''
    logging.info("The post SQL script ({}) has started".format(script_path))
    import run_spark_sql as r
    import importlib
    importlib.reload(r)
    hql = r.get_hqls(spark_session, script_path).collect()[0][1]
    r.execute_hql_script(logging, spark_session, hql, params.HIVE_PARAMETERS)
    logging.info("The post SQL script has finished")
    return 0


def insert_metadata(logging, spark_session, meta_params):
    ''' Insert load metadata '''
    logging.info("Insert metadata into {}.{}".format(meta_params['meta_db_name'], meta_params['meta_tb_name']))
    meta_insert_sql = "insert into " + meta_params['meta_db_name'] + "." + meta_params['meta_tb_name'] + " "\
                      "select \
                           '" + meta_params['db_name'] + "' AS db_name, \
                           '" + meta_params['table_name'] + "' AS table_name, \
                           '" + meta_params['load_start_utc_tmstp'] + "' AS load_start_utc_tmstp, \
                           '" + meta_params['load_end_utc_tmstp'] + "' AS load_end_utc_tmstp, \
                           '" + meta_params['load_duration_seconds_cnt'] + "' AS load_duration_seconds_cnt, \
                           '" + meta_params['load_method_name'] + "' AS load_method_name, \
                           '" + meta_params['load_src_path'] + "' AS load_src_path, \
                           '" + meta_params['load_tgt_path'] + "' AS load_tgt_path, \
                           '" + meta_params['load_std_log_text'] + "' AS load_std_log_text, \
                           '" + meta_params['table_disk_size_mb_cnt'] + "' AS table_disk_size_mb_cnt, \
                           '" + meta_params['create_by_user_name'] + "' AS create_by_user_name, \
                           '" + meta_params['create_on_host_name'] + "' AS create_on_host_name"

    spark_session.sql(meta_insert_sql)


def post_process_data(logging, spark_session, file_name, input_df):
    ''' Run post processing'''

    import os
    import pyspark.sql.functions as f

    file_path=os.path.splitext(file_name)[0]
    base_name=os.path.basename(file_path)

    post_processed_df = input_df

    if base_name == 'NTWTFS':
        logging.info("Post process data for {} file".format(base_name))
        
        shpmt_dlvr_keys_accrued_df = \
            input_df.select("shpmt_id", "dlvry_id", "billg_prpsl_num", "tot_trans_costs_amt")\
                .withColumn("compound_key", f.concat(f.col("shpmt_id"), f.lit("_"), f.col("dlvry_id"), f.lit("_"), f.lit("ACCRUAL")))\
                .filter("billg_prpsl_num NOT LIKE '%ACCRUAL%'")\
                .groupBy("compound_key").agg(f.sum('tot_trans_costs_amt').alias('sum_tot_trans_costs_amt'))\
                .filter("sum_tot_trans_costs_amt != 0")

        post_processed_df = input_df\
            .withColumn("compound_key", f.concat(f.col("shpmt_id"), f.lit("_"), f.col("dlvry_id"), f.lit("_"), f.col("billg_prpsl_num")))\
            .join(shpmt_dlvr_keys_accrued_df, "compound_key", how="left")\
            .filter("sum_tot_trans_costs_amt is null")\
            .drop("compound_key").drop("sum_tot_trans_costs_amt")

    return post_processed_df


def main(args_lst):
    ''' Main '''
    try:
        import logging
        import utils
        logging.basicConfig(
                stream=sys.stderr,
                level=logging.INFO,
                format='%(levelname)s %(asctime)s %(funcName)s %(message)s',
                datefmt='%m/%d/%Y %I:%M:%S %p')
        # Turn off not needed logs
        logging.getLogger("py4j").setLevel(logging.ERROR)
        # get parameters
        args = main_argparse(logging, args_lst)
        # Load config module, change the DBFS to FS format if needed
        sys.path.append(args.config_module_dir.replace("dbfs:/", '/dbfs/'))
        params = utils.ConfParams.build_from_module(logging, args.config_module, 0, "")

        # set variables
        target_db_name=args.target_db_name        # dd_trans_vsblt_bw
        target_tb_name=args.target_tb_name        # transport_type_na_dim_2
        load_tmstp_col=args.load_tmstp_col
        load_from_file_col=args.load_from_file_col
        bd_mod_tmstp_col=args.bd_mod_tmstp_col
        input_path=args.input_path                #='/mnt/mount_blob/dd_trans_vsblt_bw/staging/*.csv*'
        delimiter=args.delimiter                  #='|'
        is_header = (args.header == "true")       #='true/false'
        metadata_plc = {}

        # Create a spark session
        spark_session = utils.get_spark_session(logging, "load_tvb_{}".format(target_tb_name), 'yarn', params.SPARK_DEFAULT_PARAMS)

        spark_session.sql('REFRESH TABLE {}.{}'.format(target_db_name, target_tb_name))

        # Get target table setup
        target_table_df = spark_session.table('{}.{}'.format(target_db_name, target_tb_name))
        target_table_cols = target_table_df.schema.fieldNames()

        create_table_df = spark_session.sql('show create table {}.{}'.format(target_db_name, target_tb_name))
        create_table_str = ""
        for row in create_table_df.head(1):
            #print("row = {}".format(row[0]))
            create_table_str = row[0]
            location_search = re.search(r"['|\"]dbfs:[a-zA-Z0-9\/_-]+['|\"]", row[0], re.IGNORECASE)
            if location_search:
                target_tb_dir_path = location_search.group(0).strip("' ")
                logging.info("Table location: {}".format(target_tb_dir_path))
                if len(target_tb_dir_path) == 0:
                    raise Exception('Cannot extract target location path from CREATE TABLE statement.')
            else:
               raise Exception('Cannot get target location path from CREATE TABLE statement.')

        #print("location_search    = {}".format(location_search.group(0)))
        #print("target_tb_dir_path = {}".format(target_tb_dir_path))

        # Read CSV file(s)
        logging.info("Reading CSV file(s): {}".format(input_path))

        files = [f for f in glob.glob("/dbfs" + input_path, recursive=False)]
        
        # Init metadata 
        metadata_plc['load_std_log_text'] = ''
        metadata_plc['table_disk_size_mb_cnt'] = ''
        metadata_plc['meta_db_name'] = args.metadata_db
        metadata_plc['meta_tb_name'] = args.metadata_tb
        metadata_plc['db_name'] = target_db_name
        metadata_plc['table_name'] = target_tb_name
        metadata_plc['create_by_user_name'] = 'airflow'
        metadata_plc['create_on_host_name'] = socket.gethostname()
        metadata_plc['load_method_name'] = spark_session.conf.get("spark.app.name")
        metadata_plc['load_tgt_path'] = target_tb_dir_path

        for f in files:
            # Skip empty file
            if os.stat(f).st_size == 0:
                logging.info("  - Skipping empty file: {}".format(f))
                continue

            spark_session.sql('REFRESH TABLE {}.{}'.format(target_db_name, target_tb_name))
            if args.partition_cols:
                spark_session.sql('MSCK REPAIR TABLE {}.{}'.format(target_db_name, target_tb_name))

            target_table_df = spark_session.table('{}.{}'.format(target_db_name, target_tb_name))
            filePath = f.replace("/dbfs", '')
            logging.info("  - {}".format(filePath))
            load_start = datetime.now()
            metadata_plc['load_src_path'] = filePath
            metadata_plc['load_start_utc_tmstp'] = load_start.strftime('%Y%m%d%H%M%S')
         
            csvReadDataDf = spark_session.read.format('csv')\
                .options(header=is_header, delimiter='{}'.format(delimiter), inferSchema='false', mode='DROPMALFORMED')\
                .load("{}".format(filePath))

            # Check for malformed records
            jsonSchema = csvReadDataDf.withColumn("_corrupt_record", lit("").cast(StringType())).schema.json()
            newSchema = StructType.fromJson(json.loads(jsonSchema))

            csvCheckBadRecordsDf = spark_session.read.format('csv')\
                .options(header=is_header, delimiter='{}'.format(delimiter), mode='PERMISSIVE')\
                .schema(newSchema)\
                .load("{}".format(filePath))

            badDf = csvCheckBadRecordsDf.filter("_corrupt_record IS NOT NULL").cache()
            cntRec = badDf.cache().count()

            if cntRec > 0:
                filePathErrorDir = os.path.dirname(filePath) + "/error"
                filePathErrorFile = os.path.basename(filePath) + ".ERROR"
                filePathError = filePathErrorDir + "/" + filePathErrorFile
                badDf.drop("_corrupt_record").coalesce(1).write.options(header=is_header, delimiter='{}'.format(delimiter), quote='\u0000').mode("overwrite").csv(filePathError)
                logging.info("  - Bad Records sended to {}".format(filePathError))

            # Remove non alpha chars from column names if there is header
            csvReadDataCleanDf = csvReadDataDf\
                .withColumn("{}".format(load_from_file_col), input_file_name())\
                .withColumn("{}".format(load_tmstp_col), regexp_extract(input_file_name(), "(20[0-9]{6})", 1))\
                .withColumn("{}".format(bd_mod_tmstp_col), to_utc_timestamp(from_unixtime(unix_timestamp()), 'PRT').cast(StringType()))

            if is_header == True:
                col_num = 0
                for col in csvReadDataDf.columns:
                    if col == load_from_file_col or col == load_tmstp_col or col == bd_mod_tmstp_col:
                        col_num = col_num + 1
                    else:
                        csvReadDataCleanDf = csvReadDataCleanDf.withColumnRenamed(col, re.sub('[^A-Za-z0-9_]+', '', col) + "_{}".format(col_num))
                        col_num = col_num + 1
                #print(csvReadDataCleanDf.schema.fieldNames())

            # Change null to empty string
            csv_table_cols = csvReadDataCleanDf.schema.fieldNames()
            csvFinalDataDf = csvReadDataCleanDf.na.fill("", csv_table_cols)

            # Change column names and data type for source data
            logging.info("Set target coulmn names and datatype")

            selExpr = []
            part_table_types = []
            parts_ordered = []
            target_table_types = target_table_df.dtypes.copy()
            source_table_types = csvFinalDataDf.dtypes.copy()

            # Change column order for partitioned table
            if args.partition_cols:
                partition_cols_dict = []
                for col in args.partition_cols:
                    col_split = col.split(":")
                    #partition_cols_dict[col_split[0]] = int(col_split[1])
                    partition_cols_dict.append((col_split[0], int(col_split[1])))

                #partition_cols_dict = ast.literal_eval(args.partition_cols)
                #print("partition_cols_dict = {}".format(partition_cols_dict))
                partition_cols_sort = sorted(partition_cols_dict.copy(), key=lambda tup: tup[1])
                #print("partition_cols_sort = {}".format(partition_cols_sort))
                for col, val in partition_cols_dict:
                    parts_ordered.append(col)
                #print("parts_ordered = {}".format(parts_ordered))
                for col in target_table_types:
                    if col[0] in parts_ordered:
                        logging.info("Partitioned column = {}".format(col[0]))
                        part_table_types.append(col)
                #print("part_table_types = {}".format(part_table_types))

                for col in part_table_types:
                    target_table_types.remove(col)

                for part_column, part_column_pos in partition_cols_sort: 
                    for col in part_table_types:
                        if part_column in col:
                            logging.info("Match {} in {}".format(part_column, col))
                            target_table_types.insert(int(part_column_pos)-1, col)

            #logging.info("target_table_types: {}".format(target_table_types))

            i=0
            for col in target_table_types:
                target_table_name=col[0]
                target_table_type=col[1]
                try:
                  source_table_name=source_table_types[i][0]
                except IndexError:
                  source_table_name=target_table_name
            
                if target_table_name == load_from_file_col or target_table_name == load_tmstp_col or target_table_name == bd_mod_tmstp_col:
                  selExpr.append("{}".format(target_table_name))
                else:
                  #selExpr.append("CAST(TRIM({}) AS {}) AS {}".format(source_table_name, target_table_type, target_table_name))
                  selExpr.append("CAST(( CASE WHEN regexp_extract(TRIM({}), '-$', 0) = '-' THEN CONCAT('-', regexp_replace(TRIM({}), '-$', '')) ELSE TRIM({}) END ) AS {}) AS {}".format(source_table_name, source_table_name, source_table_name, target_table_type, target_table_name))
                i += 1

            #logging.info("SELECT EXPR: {}".format(selExpr))
            csvSelDataDf = csvFinalDataDf.selectExpr(selExpr)

            if args.partition_cols and args.primary_key:
                #logging.info("SELECT PART EXPR: {}".format(target_table_cols))
                csvSelPartDataDf = csvSelDataDf.selectExpr(target_table_cols)
            
                # get partitions and data from target table
                csvPartsDf = csvSelPartDataDf.select(parts_ordered).distinct().cache()
                partDataToOverwriteDf = target_table_df.join(broadcast(csvPartsDf), parts_ordered, how='inner').selectExpr(target_table_cols)
                
                primary_key=args.primary_key.split(",")   #["trans_type_id"]
                order_by = [ "{}".format(load_tmstp_col), "{}".format(bd_mod_tmstp_col) ]  # ["load_tmstp", "bd_mod_tmstp"]
            
                # Calculate newest row for given key
                partition_by_cols = ", ".join(primary_key)
                order_by_cols = " DESC, ".join(order_by) + " DESC"
                
                load_rank_expr = "ROW_NUMBER() OVER (PARTITION BY {} ORDER BY {})".format(partition_by_cols, order_by_cols)
                
                logging.info("Union and get latest data ({})".format(load_rank_expr))
                
                unionDF = partDataToOverwriteDf\
                    .union(csvSelPartDataDf)\
                    .withColumn("load_rank", expr(load_rank_expr))\
                    .filter("load_rank=1").drop("load_rank").cache()

                spark_session.sql("SET hive.exec.dynamic.partition = true")
                spark_session.sql("SET hive.exec.dynamic.partition.mode = nonstrict ")
                spark_session.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
            
                logging.info("Insert data to partitionaed table {}.{} (partitioned by: {})".format(target_db_name, target_tb_name, parts_ordered))
                unionDF.write.mode("{}".format(args.overwrite_data)).partitionBy(parts_ordered).parquet(target_tb_dir_path)
                spark_session.sql('MSCK REPAIR TABLE {}.{}'.format(target_db_name, target_tb_name))
                spark_session.sql('REFRESH TABLE {}.{}'.format(target_db_name, target_tb_name))

                if args.second_path:
                    for dbfs_path in args.second_path:
                        logging.info("Insert data to path: {} (partitioned by: {})".format(dbfs_path, parts_ordered))
                        unionDF.write.mode("{}".format(args.overwrite_data)).partitionBy(parts_ordered).parquet(dbfs_path)

            elif args.partition_cols:
                #logging.info("SELECT PART EXPR: {}".format(target_table_cols))
                csvSelPartDataDf = csvSelDataDf.selectExpr(target_table_cols)

                spark_session.sql("SET hive.exec.dynamic.partition = true")
                spark_session.sql("SET hive.exec.dynamic.partition.mode = nonstrict ")
                spark_session.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

                logging.info("Insert data to path: {} (partitioned by: {})".format(target_tb_dir_path, parts_ordered))
                csvSelPartDataDf.write.mode("{}".format(args.overwrite_data)).partitionBy(parts_ordered).parquet(target_tb_dir_path)
                spark_session.sql('MSCK REPAIR TABLE {}.{}'.format(target_db_name, target_tb_name))
                spark_session.sql('REFRESH TABLE {}.{}'.format(target_db_name, target_tb_name))

                if args.second_path:
                    for dbfs_path in args.second_path:
                        logging.info("Insert data to path: {} (partitioned by: {})".format(dbfs_path, parts_ordered))
                        csvSelPartDataDf.write.mode("{}".format(args.overwrite_data)).partitionBy(parts_ordered).parquet(dbfs_path)

            elif args.primary_key:

                #logging.info("SELECT EXPR: {}".format(selExpr))

                primary_key=args.primary_key.split(",")   #["trans_type_id"]
                order_by = [ "{}".format(load_tmstp_col), "{}".format(bd_mod_tmstp_col) ]  # ["load_tmstp", "bd_mod_tmstp"]
            
                # Calculate newest row for given key
                partition_by_cols = ", ".join(primary_key)
                order_by_cols = " DESC, ".join(order_by) + " DESC"
                
                load_rank_expr = "ROW_NUMBER() OVER (PARTITION BY {} ORDER BY {})".format(partition_by_cols, order_by_cols)
                
                logging.info("Union and get latest data ({})".format(load_rank_expr))
                
                unionDF = target_table_df\
                    .union(csvSelDataDf)\
                    .withColumn("load_rank", expr(load_rank_expr))\
                    .filter("load_rank=1").drop("load_rank")

                # Insert data to final table
                logging.info("Insert data to final table {}.{} ({})".format(target_db_name, target_tb_name, target_tb_dir_path))
                
                postProcessDF = post_process_data(logging, spark_session, filePath, unionDF)
                postProcessDF.write.format("parquet").mode("{}".format(args.overwrite_data)).save("{}".format(target_tb_dir_path))

                if args.second_path:
                    for dbfs_path in args.second_path:
                        spark_session.sql('REFRESH TABLE {}.{}'.format(target_db_name, target_tb_name))
                        new_target_table_df = spark_session.table('{}.{}'.format(target_db_name, target_tb_name))
                        logging.info("Insert data to path: {}".format(dbfs_path))
                        new_target_table_df.write.format("parquet").mode("{}".format(args.overwrite_data)).save("{}".format(dbfs_path))

            else:
                #logging.info("SELECT EXPR: {}".format(selExpr))

                spark_session.sql("SET hive.exec.dynamic.partition = true")
                spark_session.sql("SET hive.exec.dynamic.partition.mode = nonstrict ")
    
                logging.info("Insert data (append) to final table {}.{} ({})".format(target_db_name, target_tb_name, target_tb_dir_path))
                csvSelDataDf.write.format("parquet").mode("append").save("{}".format(target_tb_dir_path))
                #csvSelDataDf.write.insertInto(
                #    tableName='{}.{}'.format(target_db_name, target_tb_name),
                #    overwrite=False)

                if args.second_path:
                    for dbfs_path in args.second_path:
                        logging.info("Insert data (append) to path: {}".format(dbfs_path))
                        csvSelDataDf.write.format("parquet").mode("{}".format(args.overwrite_data)).save("{}".format(dbfs_path))

            logging.info("Refresh table {}.{}".format(target_db_name, target_tb_name))
            spark_session.sql('REFRESH TABLE {}.{}'.format(target_db_name, target_tb_name))

            #if args.load_sql_path:
            #    ret = run_sql_script(logging, spark_session, params, args.load_sql_path)
            #    if ret > 0:
            #        sys.exit(1)

            if args.archive_dir:
                dbutils = utils.get_databricks_dbutils(spark_session) 
                for arch_dir in args.archive_dir:
                    archive_dir_fix = arch_dir.replace("/dbfs/", "dbfs:/")
                    f_fix = f.replace("/dbfs/", "dbfs:/")
                    dest_path_fix = archive_dir_fix + "/" + os.path.basename(f)
                    dbutils.fs.cp(f_fix, dest_path_fix, False)
                    #shutil.copy2(f, archive_dir_fix)
                    logging.info("Archivied: {}/{}".format(archive_dir_fix, os.path.basename(f)))

                os.remove(f)
                logging.info("Deleted: {}".format(f))

            if args.partition_cols:
                spark_session.sql('MSCK REPAIR TABLE {}.{}'.format(target_db_name, target_tb_name))

            load_end = datetime.now()
            duration = load_end - load_start
            duration_sec = str(int(duration.total_seconds()))
            metadata_plc['load_end_utc_tmstp'] = load_end.strftime('%Y%m%d%H%M%S')
            metadata_plc['load_duration_seconds_cnt'] = duration_sec

            if args.metadata_db and args.metadata_tb:
                insert_metadata(logging, spark_session, metadata_plc)
            #break

        #sys.exit(1)
        logging.info("Execution ended")

    except Exception as exception:
        error_desc = "exception: {} at {}".format(type(exception), datetime.now())
        print (error_desc)
        sys.stderr.write(error_desc)
        print("=" * 80)
        print("exception: {} at {}".format(type(exception), datetime.now()))
        logging.error(exception)
        sys.stdout.flush()
        sys.exit(1)


if __name__ == "__main__":
    main(sys.argv[1:])
