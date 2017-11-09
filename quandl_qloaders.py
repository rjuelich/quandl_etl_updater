import boto3
import json
from io import BytesIO
from zipfile import ZipFile
import urllib.request
import sys
import configparser
#import dynamodb
import os
import time
import quandl
import logging
from botocore.exceptions import ClientError
import decimal
import pprint
from boto3.dynamodb.conditions import Key

from dynamodb.dynamo_accessor import DynamoAccessor

####
# Global logging
####
logger = logging.getLogger()
logging.basicConfig(level=logging.INFO)
logger.info("Logging Configured...")

"""
  Encode numerical data types to  decimal values for JSON
"""
class DecimalEncoder(json.JSONEncoder):
  def default(self, o):
    if isinstance(o, decimal.Decimal):
      if o % 1 > 0:
        return float(o)
      else:
        return int(o)
    return super(DecimalEncoder, self).default(o)

"""
  Localized DBAccessor
"""
class DbAccessor(DynamoAccessor):
  def __init__(self, config):
    super().__init__(config)

"""
  TODO: Move to common and or utils
"""
def getConfig(configFile, env):
  logger.debug("loading config file: " + configFile)
  logger.debug("env: " + env)
  config = None

  cfg = configparser.ConfigParser()
  cfg.read(configFile)
  config = cfg[env]

  return config

"""
  Class provides support for loading quandl time series data sets and standard
  quandl data set tables using the quandl api when proper authentication tokens
  are provided
"""
class QuandlDSLoader:

  """
    Initialize loader with target quandl dataset and type
    @arg dataset: Is the string name of the dataset to be loaded
    @arg dbaccessor: Is the dynamodb service resource
    @arg dtype: Is the quandl dataset type (time series/standard)
  """
  def __init__(self, qdata, dbaccessor, dtype):
    self.url_base = 'https://www.quandl.com/api/v3/databases/'
    self.tgtdata  = qdata
    self.dtype    = dtype
    self.apiK     = 'ybLViDkXwrA2SCaj5Gam' #'sgqVEeaBkXrpLEArif89'
    self.dynamodb = dbaccessor._dynamodb

    self.tables  = list(self.dynamodb.tables.all())
    self.tables  = str(self.tables)
    self.qdcodes = self.getqcodes()

    logger.info("Debug: self.tgtdata: "  + self.tgtdata)
    logger.info("Debug: self.tables: "   + self.tables)
    logger.info("Debug: self.url_base: " + self.url_base)

  """
    Retrieves the quandl code for the specified data table or list of codes for
    the specified data set. If time-series database, return codes for all
    available underlying datasets. If table database, return     table code
  """
  def getqcodes(self):
    qdcodes = []

    if self.dtype == 'ts': #db type is time series returl all quandl ds codes 1:*
      urlstr = self.url_base + self.tgtdata + "/codes"
      url    = urllib.request.urlopen(urlstr)
      bytes  = url.read()
      zip    = ZipFile(BytesIO(bytes))
      for name in zip.namelist():
        for line in zip.open(name).readlines():
          string = str(line, "utf-8").split(',')
          fcode  = str(string[0])
          ycode  = fcode.split('/')
          qdcodes.append(str(ycode[1]))
    else: #db type is standard return corresponding code 1:1
      qdcodes.append(self.tgtdata)

    logger.info("Debug: codeList: " + str(qdcodes))

    return qdcodes

  """
    Constructs a new dynamodb table for time-series, HASH='type', RANGE='date'
    otherwise HASH=hashk='ticker', RANGE=rangek='comp_name'
  """
  def makeTable(self):
    hashk  = ''
    rangek = ''

    if self.dtype == 'ts':
      logger.info('loading TS dataset')
      hashk  = 'type'
      rangek = 'date'
    else:
      logger.info('loading Table dataset')
      hashk  = 'ticker'
      rangek = 'comp_name'

    keys = [
             {
               'AttributeName': hashk,
               'KeyType': 'HASH'
             },
             {
               'AttributeName': rangek,
               'KeyType': 'RANGE'
             }
           ]
    atts = [
             {'AttributeName': hashk, 'AttributeType' : 'S'},
             {'AttributeName': rangek, 'AttributeType': 'S'}
           ]
    gidx = [
             { 'IndexName': 'GUIdx',
               'KeySchema': [
                {
                  'AttributeName': hashk,
                  'KeyType': 'HASH'
                 }
               ],
               'Projection': {'ProjectionType': 'KEYS_ONLY'},
               'ProvisionedThroughput': {
               'ReadCapacityUnits' : 1,
               'WriteCapacityUnits': 1
               }
             }
           ]
    try: #Check that table name meets dynamo's 3 char minimum. Append the last char if not. Then create the table.
      dtable = self.tgtdata
      if len(self.tgtdata) < 3:
          dtable = self.tgtdata + str(self.tgtdata)[-1]
      table = self.dynamodb.create_table(
                TableName=dtable,
                KeySchema=keys,
                AttributeDefinitions=atts,
                GlobalSecondaryIndexes=gidx,
                ProvisionedThroughput={
                  'ReadCapacityUnits' : 5,
                  'WriteCapacityUnits': 10
                }
              )
    except ClientError as e:
      if e.response['Error']['Code'] == 'ResourceInUseException':
        logger.info("Table already exists")
      else:
        logger.info("Unexpected error: %s" % e)
      exit(1)

    time.sleep(15) # Allow time for new dynamodb table creation before uploading data.

  """
    Download target dataset or data table from Quandl and upload to DynamoDB
  """
  def fetchds(self):
    quandl.ApiConfig.api_key = self.apiK

    for code in self.qdcodes:
      logger.info("Debug: code: " + code)
      if self.dtype == 'tb':
        ext = code.split("_")
        logger.info("Debug: ext: " + str(ext[1]))
        qdata = quandl.Datatable(str(ext[0]) + '/' + str(ext[1])).data()
      else:
        qdata = quandl.Dataset(self.tgtdata + '/' + code).data()
      headers = qdata.column_names
      logger.info("Debug: headers: " + str(headers))
      qdata_list = qdata.to_list()
      qlen       = len(qdata_list)

      qidx = 0 #qdata index

      if self.dtype == 'ts': #set the date type fields and update index
        headers[0]   = 'date'

      for i in range(qidx, qlen): #for each q data index
        item = {}
        if self.dtype == 'ts':
          item = {"type": code}
        hlen = len(headers)

        for h in range(0, hlen):
          item[headers[h]] = str(qdata_list[i][h])

        # Define dtable to table name meets 3 char minimum
        dtable = self.tgtdata
        if len(dtable) < 3:
            dtable = dtable + str(dtable)[-1]

        table = self.dynamodb.Table(dtable)
        item  = json.dumps(item)
        resp  = table.put_item(Item=json.loads(item))
        logger.info("put_item response...")
        logger.info(json.dumps(resp, indent=4, cls=DecimalEncoder))

  """
    Downloads an quandl dataset and updates the corresponding dynamodb table
  """
  def fetchds_update(self):
    quandl.ApiConfig.api_key = self.apiK

    for code in self.qdcodes:
      logger.info("Debug: code: " + code)
      if self.dtype == 'tb':
        ext = code.split("_")
        logger.info("Debug: ext: " + str(ext[1]))
        qdata = quandl.Datatable(str(ext[0]) + '/' + str(ext[1])).data()
        latest = str(qdata.meta['end_date'])
        logger.info("Debug: latest: " + latest)
        qdata = quandl.Datatable(str(ext[0]) + '/' +
                str(ext[1])).data(params={'start_date':latest})
      else:
        dtable = self.tgtdata
        if len(dtable) < 3:
          dtable = dtable + str(dtable)[-1]
        table = self.dynamodb.Table(dtable)
        resp = table.scan(FilterExpression=Key('type').eq(code))
        qdata  = quandl.Dataset(self.tgtdata + '/' + code).data()
        count = int(resp['Count'])

        if count < 1:
          last = str(qdata.meta['start_date'])
        else:
          last = resp['Items'][-1]['date']
        latest = str(qdata.meta['end_date'])
        qdata  = quandl.Dataset(self.tgtdata + '/' + code).data(
                   params={'start_date':last, 'end_date':latest})
      headers = qdata.column_names
      logger.info("Debug: headers: " + str(headers))
      qdata_list = qdata.to_list()
      qlen       = len(qdata_list)
      qidx       = 0 #qdata index

      if self.dtype == 'ts': #set the date type fields and update index
        headers[0]   = 'date'

      for i in range(qidx, qlen): #for each q data index
        item = {}
        if self.dtype == 'ts':
          item = {"type": code}
        hlen = len(headers)
        for h in range(0, hlen):
          item[headers[h]] = str(qdata_list[i][h])

        # Define dtable to table name meets 3 char minimum
        dtable = self.tgtdata
        if len(dtable) < 3:
          dtable = dtable + str(dtable)[-1]
        table = self.dynamodb.Table(dtable)
        item  = json.dumps(item)
        resp  = table.put_item(Item=json.loads(item))
        logger.info("put_item response...")
        logger.info(json.dumps(resp, indent=4, cls=DecimalEncoder))

  """
    loads the specified quandl data table or dataset into dynamodb
  """
  def load(self):
    dtable = self.tgtdata
    if len(dtable) < 3:
      dtable = dtable + str(dtable)[-1]
    if "'" + dtable + "'" not in self.tables:
      self.makeTable() #self.tgtdata, self.dtype)
      self.fetchds()
    #if self.tgtdata not in self.tables:
    #  self.makeTable()
    #  self.fetchds()
    else:
      self.fetchds_update()

"""
  main: for test purposes, see usage...
  Dataset define list of target datasets based on 'quandl_datasets' entry in
  quandl_load.config (a comma-separated list of quandl codes. Table datasets
  have an underscore (_) in their code (i.e., ZACKS_EE), time-series codes do not
"""
if __name__ == '__main__':

  conf  = ''
  qcode = ''
  db    = None

  if len(sys.argv) == 2:
    conf    = getConfig(sys.argv[1], "dev")
    dsets   = conf['quandl_datasets'].split(',')

    try:
      db = DbAccessor(conf)
    except:
      logger.fatal('Quandl load failure', exc_info=True, stack_info=True)
      exit(1)

    dslen = len(dsets)

    for d in range(0, dslen):
      qcode = dsets[d]
      if "_" in dsets[d]:
        logger.info('Loading standard dataset ' + qcode)
        dtype = 'tb'
      else:
        logger.info('Loading ts dataset: ' + qcode)
        dtype = 'ts'

      tsl = QuandlDSLoader(qcode, db, dtype)
      tsl.load()
  else:
    logger.fatal("\nUsage: python args...\n" +
                   "  argv[0]: program\n" +
                   "  argv[1]: config file\n" +
                   "  argv[2]: datasetcode\n" +
                   "Ex: python quandl_qloaders_dev.py quandl_load.cfg ZACKS/CP\n"
                 )
    exit(1)
