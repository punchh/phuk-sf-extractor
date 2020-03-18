from db_utility import gen_snowflake_conn
from pathlib import Path
import csv
import logging
import json

logging.getLogger().setLevel(logging.INFO)

logging.info('getting the logger ready\n')

def uploads_to_s3(target_file_path,s3_target_bucket,s3_target_key ):
        import boto3
        from botocore.exceptions import NoCredentialsError

        # ACCESS_KEY = 'AKIAYRCJSSIESEGTCU4R'
        # SECRET_KEY = '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'


        s3 = boto3.client('s3')

        try:
                s3.upload_file(target_file_path, s3_target_bucket, s3_target_key)
                print("Upload Successful")
                return True
        except FileNotFoundError:
                print("The file was not found")
                return False
        except NoCredentialsError:
                print("Credentials not available")
                return False


def load_csv(records,target_file_path,s3_target_bucket,s3_target_key ):
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
        with open(target_file_path, 'w') as csvfile:
                # creating a csv writer object
                csvwriter = csv.writer(csvfile)

                # writing the fields
                csvwriter.writerow(feilds)

                # writing the data rows
                csvwriter.writerows(records)
        print("done with writing")
        uploads_to_s3(target_file_path,s3_target_bucket,s3_target_key)


def main(snowflake_user,snowflake_secret,target_file_local_path,s3_target_bucket,s3_target_key ):
        from datetime import date, timedelta
        yesterday_date = date.today() - timedelta(1)
        snowflake_secret_home = str(Path.home()) + snowflake_secret
        target_file_name = "/pizzahut_uk_" + str(yesterday_date) + ".csv"
        target_file_path= str(Path.home())+"/"+target_file_local_path + target_file_name
        s3_target_key= s3_target_key + target_file_name
        conn, cursor = gen_snowflake_conn(
                dbuser=snowflake_user,
                secret_file=snowflake_secret_home,
                verbose=0,
        )
        print("connecte",conn,cursor)
        query_str = """select * from {tablename} WHERE TO_DATE(timestamp )=DATEADD(Day ,-1, current_date) """.\
                format(tablename="PIZZAHUT_PRODUCTION_ODS.PUBLIC.SENDGRID_EVENTS")
        cursor.execute(query_str)
        records = cursor.fetchall()
        load_csv(records, target_file_path,s3_target_bucket,s3_target_key)


if __name__ == '__main__':
        import argparse
        parser = argparse.ArgumentParser()
        parser.add_argument("--snowflake_user", default="dataeng")
        parser.add_argument("--snowflake_secret", default="/.cred/snowflake_secret_phuk")
        parser.add_argument("--target_file_local_path", default="daily_pizzahut_uk_files")
        parser.add_argument("--s3_target_bucket", default="pizzahut-prod-ra")
        parser.add_argument("--s3_target_key", default="FILES_SHARED/YUM2MANTHAN/BI/MARKETING/Customer360Project/Punch_Activity_Data")
        args = parser.parse_args()
        args = vars(args)
        logging.info("Cmd line args:\n{}".format(json.dumps(args, sort_keys=True, indent=4)))
        main(**args)
        print("Program ran successfull")