import boto3
import json

#
# Base class for accessing DynamoDB tables
#
class DynamoAccessor:
    def __init__(self, config):
        self._datasetsTable = config['db_datasets_table']
        self._credentialsTable = config['db_credentials_table']
        self._regionName = config['db_region_name']
        if 'aws_profile_name' in config:
            self._awsProfileName = config['aws_profile_name']
        else:
            self._awsProfileName = None
        if 'db_endpoint_url' in config:
            self._dbEndpointUrl = config['db_endpoint_url']
        else:
            self._dbEndpointUrl = None
        self._dynamodb = None
        self.initDynamoDb()

    def initDynamoDb(self):
        """
        Initializes the DynamoDB session with Stansberry credentials
        :return: db object
        """
        if self._awsProfileName is not None:
            print('Using AWS profile', self._awsProfileName)
            sess = boto3.session.Session(profile_name=self._awsProfileName)
        else:
            print('Using default AWS profile')
            sess = boto3.session.Session()
        if self._dbEndpointUrl is not None:
            self._dynamodb = sess.resource('dynamodb', region_name=self._regionName, endpoint_url=self._dbEndpointUrl)
        else:
            self._dynamodb = sess.resource('dynamodb', region_name=self._regionName)

    def getTableSpec(self, ds_id):
        """
        Reads a dataset specification from the DB
        :param db: the DynamoDB object
        :param string ds_id: the dataset ID
        :return: the table specification map
        """
        table = self._dynamodb.Table(self._datasetsTable)
        response = table.get_item(
            Key={
                'ds_id': ds_id
            }
        )
        item = response['Item']
        # print("GetItem succeeded:")
        # print(json.dumps(item, indent=4, cls=DecimalEncoder))
        return item

    def getQuandlApiKey(self):
        """
        Reads the Quandl API key from the DB
        :param db: the DynamoDB object
        :return: the key string
        """
        table = self._dynamodb.Table(self._credentialsTable)
        response = table.get_item(
            Key={
                'service_id': 'quandl'
            }
        )
        item = response['Item']
        #print("GetItem succeeded:")
        #print(json.dumps(item, indent=4, cls=DecimalEncoder))
        return item['quandl_info']['api_key']

    def insert(self, table_name, json_data):
        """
        Insert JSON formatted data into a specified DynamoDB table
        :param table_name: the name of the DynamoDB table to be inserted into
        :param json_data: the JSON formatted data to be inserted
        :return: the dict returned after insertion
        """
        table = self._dynamodb.Table(table_name)
        return table.put_item(Item=json.loads(json_data))
