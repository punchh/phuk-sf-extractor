from db_utility import gen_snowflake_conn, gen_mysql_conn
from pathlib import Path
import csv
import logging
import json
from datetime import datetime, timedelta

logging.getLogger().setLevel(logging.INFO)

logging.info('getting the logger ready\n')


def daterange(date1, date2):
    """
    :param date1: Start date
    :param date2: End date
    :return: Return range of dataes
    """
    for n in range(int((date2 - date1).days)):
        yield date1 + timedelta(n)


def get_start_date(sql_conn, sql_cursor,mysql_phuk_sf_extractor):
    try:
        select_query_str="select date(max(job_end_time)) as date from {mysql_phuk_sf_extractor}  where load_status='Success' "\
            .format(mysql_phuk_sf_extractor=mysql_phuk_sf_extractor)
        sql_cursor.execute(select_query_str)
        records = sql_cursor.fetchall()
        start_date = records[0]['date']
        return start_date
    except Exception as e:
        logging.info("Exception occured is", e)


def insert_job_log(conn,cursor,job_start_time,insert_record_count,mysql_phuk_sf_extractor,status):
    try:
        insert_query_str = "insert into {mysql_phuk_sf_extractor}(job_start_time,job_end_time," \
                           "load_status,rows_inserted)VALUES(%s,%s,%s,%s)".\
            format(mysql_phuk_sf_extractor=mysql_phuk_sf_extractor)
        args = (job_start_time, datetime.now(),status, insert_record_count)
        cursor.execute(insert_query_str, args)
        conn.commit()
        logging.info("inserted record to mysql table %s" )
        return
    except Exception as e:
        logging.info("Error inserting record to mysql table %s" %e)


def uploads_to_s3(target_file_path,s3_target_bucket,s3_target_key,job_start_time,mysql_phuk_sf_extractor,record_count,conn, cursor
                  ):
        import boto3
        from botocore.exceptions import NoCredentialsError
        s3 = boto3.client('s3')
        try:
                s3.upload_file(target_file_path, s3_target_bucket, s3_target_key)
                logging.info("Upload Successfully")
                logging.info("File is uploaded to the path %s" % s3_target_bucket+s3_target_key)
                insert_job_log(conn,cursor,job_start_time,record_count,
                                   mysql_phuk_sf_extractor,status='Success')
                return True
        except FileNotFoundError:
                logging.info("The file was not found")
                insert_job_log(conn, cursor, job_start_time, record_count,
                               mysql_phuk_sf_extractor, status='Failed')
                return False
        except NoCredentialsError:
                insert_job_log(conn, cursor, job_start_time, record_count,
                           mysql_phuk_sf_extractor, status='Failed')
                logging.info("Credentials not available")
                return False
        except Exception as e:
                logging.info("Failed uploading a file %s" %e)
                insert_job_log(conn, cursor, job_start_time, record_count,
                               mysql_phuk_sf_extractor, status='Failed')


def load_csv(records,target_file_path,s3_target_bucket,s3_target_key,job_start_time,mysql_phuk_sf_extractor,record_count,
             conn, cursor):
        feilds = ["_METADATA__PARENT_UUID",
                  "_METADATA__TIMESTAMP",
                  "_METADATA__UUID",
                  "_METADATA__VERSION",
                  "_METADATA_CLIENT_IP",
                  "_METADATA_CLIENT_NAME",
                  "_METADATA_EVENT_TYPE",
                  "_METADATA_INPUT_ID",
                  "_METADATA_INPUT_LABEL",
                  "_METADATA_INPUT_TYPE",
                  "_METADATA_RESTREAM_COUNT",
                  "_METADATA_TOKEN",
                  "BUSINESS_ID",
                  "CATEGORY",
                  "DATE",
                  "EMAIL",
                  "EVENT",
                  "HOSTNAME",
                  "KLASS",
                  "METHOD_NAME",
                  "OBJECT_ID",
                  "PROCESSED",
                  "RAILS_ENV",
                  "RESELLER_ID",
                  "SG_MESSAGE_ID",
                  "TIMESTAMP",
                  "USER_ID",
                  "ASM_GROUP_ID",
                  "RESPONSE",
                  "IP",
                  "SG_EVENT_ID",
                  "SMTP_ID",
                  "USERAGENT",
                  "URL",
                  "REASON",
                  "STATUS",
                  "ATTEMPT",
                  "MARKETING_CAMPAIGN_NAME",
                  "URL_OFFSET",
                  "SG_USER_ID",
                  "MARKETING_CAMPAIGN_ID",
                  "TLS",
                  "CERT_ERR",
                  "TYPE",
                  "SG_CONTENT_TYPE",
                  "FOOTER_SEGMENT_ID",
                  "BODY_SEGMENT_ID",
                  "HEADER_SEGMENT_ID",
                  "UNIQUE_ARG_KEY",
                  "NEWSLETTER",
                  "BUSINESS_UUID"]
        # writing to csv file
        try:
            with open(target_file_path, 'w') as csvfile:
                    # creating a csv writer object
                    csvwriter = csv.writer(csvfile)
                    # writing the fields
                    csvwriter.writerow(feilds)
                    # writing the data rows
                    csvwriter.writerows(records)
            logging.info("Csv file created")
            uploads_to_s3(target_file_path,s3_target_bucket,s3_target_key,job_start_time,mysql_phuk_sf_extractor,record_count,
                          conn, cursor)
        except Exception as e:
            logging.info("Exception while writing %s" % e)



def main(snowflake_user,snowflake_secret,target_file_local_path,s3_target_bucket,s3_target_key,mysql_phuk_sf_extractor):
        try:
            from datetime import date,datetime, timedelta
            job_start_time = datetime.now()
            yesterday_date = date.today() - timedelta(1)
            snowflake_secret_home = str(Path.home()) + snowflake_secret
            target_file_name = "/pizzahut_uk_" + str(yesterday_date) + ".csv"
            target_file_path= str(Path.home())+"/"+target_file_local_path + target_file_name
            s3_target_key= s3_target_key + target_file_name
            sql_conn, sql_cursor = gen_mysql_conn(
                dbuser="mysql",
                secret_file=snowflake_secret_home
            )
            start_date=get_start_date(sql_conn, sql_cursor,mysql_phuk_sf_extractor)
            conn, cursor = gen_snowflake_conn(
                    dbuser=snowflake_user,
                    secret_file=snowflake_secret_home,
                    verbose=0,
            )
            from datetime import datetime, timedelta, date
            start_dt = datetime.strptime(str(start_date), '%Y-%m-%d').date()
            end_dt = datetime.strptime(str(date.today()), '%Y-%m-%d').date()

            for dt in daterange(start_dt, end_dt):

                query_str = """select * from {tablename} WHERE TO_DATE(timestamp )='{dt}' """.\
                        format(tablename="PIZZAHUT_PRODUCTION_ODS.PUBLIC.SENDGRID_EVENTS",dt=dt)
                logging.info(query_str)
                cursor.execute(query_str)
                records = cursor.fetchall()
                record_count=len(records)
                logging.info("the total number of records are %s" %record_count)
                load_csv(records, target_file_path, s3_target_bucket, s3_target_key,job_start_time,mysql_phuk_sf_extractor,record_count,
                         sql_conn, sql_cursor)
        except Exception as e:
            logging.info("Exception while querying %s" %e)
        finally:
            sql_conn.close()
            sql_cursor.close()


if __name__ == '__main__':
        import argparse
        parser = argparse.ArgumentParser()
        parser.add_argument("--snowflake_user", default="dataeng")
        parser.add_argument("--snowflake_secret", default="/.cred/snowflake_secret_phuk")
        parser.add_argument("--target_file_local_path", default="daily_pizzahut_uk_files")
        parser.add_argument("--s3_target_bucket", default="pizzahut-prod-ra")
        parser.add_argument("--s3_target_key", default="FILES_SHARED/YUM2MANTHAN/BI/MARKETING/Customer360Project/Punch_Activity_Data")
        parser.add_argument("--mysql_phuk_sf_extractor", default="audit.mysql_phuk_sf_extractor")
        args = parser.parse_args()
        args = vars(args)
        logging.info("Cmd line args:\n{}".format(json.dumps(args, sort_keys=True, indent=4)))
        main(**args)
        print("Program ran successfully")