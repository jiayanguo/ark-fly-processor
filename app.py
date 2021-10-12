from datetime import date, timedelta, datetime
from pytz import timezone
import io
import csv
import boto3
import codecs
from botocore.exceptions import ClientError
import urllib

S3_BUCKET ="ark-fly"
TRADING_OBJECT_KEY_PATTERN="dailytradingtrans/{today}-trading.csv"
HOLDING_OBJECT_KEY_PATTERN="holdings/{date}_{fund}_holdings.csv"
ARKK = "ARKK"
ARKQ = "ARKQ"
ARKW = "ARKW"
ARKG = "ARKG"
ARKF = "ARKF"
PRNT = "PRNT"
IZRL = "IZRL"
ARKX = "ARKX"

S3_BUCKET ="ark-fly"
OBJECT_KEY_PATTERN="newholdings/{today}-trading.csv"

TMP_NEW_HOLDINGS_FILE="/tmp/newholdings.csv"

ark_trading_map = {
    ARKK: [],
    ARKQ: [],
    ARKW: [],
    ARKG: [],
    ARKF: [],
    PRNT: [],
    IZRL: [],
    ARKX: []
}

ark_holding_map = {
    ARKK: [],
    ARKQ: [],
    ARKW: [],
    ARKG: [],
    ARKF: [],
    PRNT: [],
    IZRL: [],
    ARKX: []
}

new_holding_map = {
    ARKK: [],
    ARKQ: [],
    ARKW: [],
    ARKG: [],
    ARKF: [],
    PRNT: [],
    IZRL: [],
    ARKX: []
}

def get_from_s3(object_name):
    client = boto3.client('s3')
    try:
        response = client.get_object(Bucket = S3_BUCKET, Key = object_name)
        return response
    except ClientError as ex:
        raise

def find_new_holdings(today, object_key):
    find_tradings(object_key)
    for key, value in ark_trading_map.items():
        find_holdings(today, key)
        if value:
            new_holding_map[key] = [x for x in value if x['Ticker'] not in ark_holding_map[key]]

def get_date():
    tz = timezone('EST')
    today = datetime.now(tz).strftime("%Y-%m-%d")
    return today

def find_holdings(date, fund):
    # find previous date holdings
    date = (datetime.strptime(date, '%Y-%m-%d') - timedelta(days = 1)).strftime("%Y-%m-%d")
    try:
        obj = get_from_s3(HOLDING_OBJECT_KEY_PATTERN.format(date = date, fund=fund.lower()))
        for row in csv.DictReader(codecs.getreader("utf-8")(obj["Body"])):
            if row['fund']:
                ark_holding_map[row['fund'].upper()].append(row['ticker'])
    except ClientError as ex:
        if ex.response['Error']['Code'] == 'NoSuchKey':
            find_holdings(date, fund)
        else:
            raise Exception("Issue to connect S3" + str(ex))

def find_tradings(object_key):
    obj = get_from_s3(object_key)
    if obj:
        for row in csv.DictReader(codecs.getreader("utf-8")(obj["Body"])):
            if row['Direction'] == 'Buy':
                ark_trading_map[row['Fund'].upper()].append(row)

def save_to_csv(result):
    headers = result[0].keys()
    with open(TMP_NEW_HOLDINGS_FILE, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=headers)
        writer.writeheader()
        for trade in result:
            writer.writerow(trade)

def upload_to_s3(date):
    client = boto3.client('s3')
    try:
        response = client.upload_file(TMP_NEW_HOLDINGS_FILE, S3_BUCKET, OBJECT_KEY_PATTERN.format(today=date))
    except Exception as error:
        raise Exception("Failed to upload to s3! " + str(error))

def main(object_key):
    today = get_date()
    find_new_holdings(today, object_key)
    result = []
    for key, value in new_holding_map.items():
        if value:
            result.extend(value)
    if result:
        print("Found new holdings.")
        save_to_csv(result)
        upload_to_s3(today)
    else:
        print ("No new holdings")

if __name__ == '__main__':
    main(TRADING_OBJECT_KEY_PATTERN.format(today="2021-10-11"))

def lambda_handler(event, context):
    try:
        key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
        main(key)
    except Exception as error:
        print("Failed to process trading information " + str(error))
    return {
        "status":200
    }