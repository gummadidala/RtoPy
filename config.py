import os
import re
import sqlalchemy as sq
import yaml
import boto3
import time
from datetime import datetime, timedelta
import pandas as pd
import warnings
warnings.filterwarnings("ignore", category=SyntaxWarning)


s3 = boto3.client("s3")
ath = boto3.client("athena")


def get_aurora_engine():
    with open("Monthly_Report_AWS.yaml") as f:
        cred = yaml.safe_load(f)
    engine = sq.create_engine(
        f"mysql+pymysql://{cred['RDS_USERNAME']}:{cred['RDS_PASSWORD']}@{cred['RDS_HOST']}/{cred['RDS_DATABASE']}?charset=utf8mb4"
    )
    return engine


def get_date_from_string(
    x,
    s3bucket=None,
    s3prefix=None,
    table_include_regex_pattern="_dy_",
    table_exclude_regex_pattern="_outstand|_report|_resolv|_task|_tpri|_tsou|_tsub|_ttyp|_kabco|_maint|_ops|_safety|_alert|_udc|_summ",
):
    if type(x) == str:
        re_da = re.compile(r"\d+(?= *days ago)")
        if x == "today":
            x = datetime.today().strftime("%F")
        elif x == "yesterday":
            x = (datetime.today() - timedelta(days=1)).strftime("%F")
        elif re_da.search(x):
            d = int(re_da.search(x).group())
            x = (datetime.today() - timedelta(days=d)).strftime("%F")
        elif x == "first_missing":
            if s3bucket is not None and s3prefix is not None:
                s3 = boto3.resource("s3")
                all_dates = [
                    re.search(r"(?<=date\=)(\d+-\d+-\d+)", obj.key)
                    for obj in s3.Bucket(s3bucket).objects.filter(Prefix=s3prefix)
                ]
                all_dates = [date_.group() for date_ in all_dates if date_ is not None]
                first_missing = datetime.strptime(max(all_dates), "%Y-%m-%d") + timedelta(days=1)
                first_missing = min(first_missing, datetime.today() - timedelta(days=1))
                x = first_missing.strftime("%F")
            else:
                raise Exception("Must include arguments for s3bucket and s3prefix")
    else:
        x = x.strftime("%F")
    return x


def query_athena(query, database, staging_dir):
    response = ath.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": staging_dir},
    )
    output_bucket = os.path.basename(staging_dir)

    # Wait for s3 object to be created
    while "Contents" not in s3.list_objects(
        Bucket=output_bucket, Prefix=response["QueryExecutionId"]
    ):
        time.sleep(1)

    # Read csv from s3
    key = response["QueryExecutionId"] + ".csv"
    df = pd.read_csv(os.path.join(staging_dir, key))
    return df
