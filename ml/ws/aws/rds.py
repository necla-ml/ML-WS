'''
TODO: move outside ML-WS as Eigen specific deployment
'''

import os
import re
import time
import json
import boto3
import itertools as it
from urllib import parse
from datetime import datetime
from botocore.exceptions import ClientError

from ml import logging
from . import secrets
from . import utils


# globals contain env specific global values
GLOBALS = secrets.aws_env()

# ref
# https://aws.amazon.com/blogs/database/using-the-data-api-to-interact-with-an-amazon-aurora-serverless-mysql-database/
# https://forums.aws.amazon.com/thread.jspa?messageID=921843
# https://github.com/chanzuckerberg/aurora-data-api

DATA_API_TYPE_MAP = {
    bytes: "blobValue",
    dict: "stringValue",
    bool: "booleanValue",
    float: "doubleValue",
    int: "longValue",
    str: "stringValue",
    list: "arrayValue",
    tuple: "arrayValue"
}

#--------------------------------------------------------------------------------
# Helper Functions
#--------------------------------------------------------------------------------
def render_python_list_to_rds_tuple(lst) -> str:
    """
    Helper function to convert python list into rds compatible tuple to be used with `IN`
    Params:
        lst - list of element to be converted to rds tuple
    Returns: string of rendered list (e.g list([1,2,3]) ---> (1,2,3))
    """
    str_lst = str(tuple(lst))
    if len(lst) <= 1:
       str_lst = str_lst.replace(',', '')
    return str_lst

def safe_escape_string(s):
    # Filter nonstandard characters from input, need to fix for complex object/json inputs.
    return re.sub('[^-|.:_A-Za-z0-9 ]+', '', str(s))

def _render_value(value):
    if value.get("isNull"):
        return None
    elif "arrayValue" in value:
        if "arrayValues" in value["arrayValue"]:
            return [_render_value(nested) for nested in value["arrayValue"]["arrayValues"]]
        else:
            return list(value["arrayValue"].values())[0]
    else:
        return list(value.values())[0]

def parse_db_timestamp(d):
    if type(d) == str:
        d = datetime.strptime(d, '%Y-%m-%d %H:%M:%S.%f')
    return datetime.timestamp(d)

def render_data_api_response(response):
    if "records" in response:
        column_names = list(map(lambda x: x['name'], response.get('columnMetadata', [])))
        logging.debug(column_names, response['records'])
        for i, record in enumerate(response["records"]):
            response["records"][i] = {column_names[j]: _render_value(v) for j, v in enumerate(response["records"][i])}
    return response

def render_data_api_response_batch(response, column_names):
    if response:
        result = {}
        result["records"] = []
        for i, record in enumerate(response):
            result["records"].append({column_names[j]: _render_value(v) for j, v in enumerate(record["generatedFields"])})
    return result

def get_data_type(x):
    return DATA_API_TYPE_MAP.get(type(x), 'stringValue')

def is_json(myjson):
    if type(myjson) == 'str':
        try:
            json_object = json.loads(myjson)
        except ValueError as e:
            return False
    return True

# ex: x::org_types -> org_types
def get_cast(format_match):
    return format_match[2:]

def create_parameter(i, v):
    value_type = get_data_type(v)
    value = {value_type: v}
    if value_type == 'arrayValue':
        if v:
            element_type = '{}s'.format(get_data_type(v[0]))
        else:
            element_type = 'stringValues'
        value[value_type] = {element_type: v}
    elif v == 'null':
        value = {'isNull': True}

    return {'name': 'name{}'.format(i), 'value': value}

def get_all_params(sql):
    """
    Given sql statement with :param get param list
    Params:
        sql - sql statement with :values
    Returns:
        list of params
        e.g(sql = SELECT * FROM TABLE WHERE value = :value) ---> [value]
    """
    pattern = r":(\b\w*)"
    substring = re.findall(pattern, sql)
    return substring

def create_parameter_dict(d, sql=None) -> list:
    """
    Create parameter given dictionary and/or sql statement
    Params: 
        d - dict of values to be parameterize
        sql - sql statement to fetch parameter names from (optional)
    Returns:
        rds compatible parameter list
    """
    if not d:
        d = {}
    # add keys that might not be in the provided dictionary if sql statement given
    if sql:
        keys = get_all_params(sql)
        for key in keys:
            if key not in d:
                d[key] = None
    # convert to rds compatible types
    d = stringify_for_data_api_query_dict(d)
    param = []
    # generate param list
    for k, v in d.items():
        value_type = get_data_type(v)
        value = {value_type: v}
        if value_type == 'arrayValue':
            if v:
                element_type = '{}s'.format(get_data_type(v[0]))
            else:
                element_type = 'stringValues'
            value[value_type] = {element_type: v}
        elif v == 'null':
            value = {'isNull': True}

        param.append({'name': '{}'.format(k), 'value': value})

    return param

def format_sql(raw_sql, data):
    # using parameterized str for now.
    if not data:
        data = []
    data = tuple(map(stringify_for_data_api_query, data))

    def parameterize_string(s):
        cnt = it.count()
        return re.sub(r"(%s[a-zA-Z_:]*)", lambda x: ':name{}{}'.format(next(cnt), get_cast(x.group(0))), s)

    sql = parameterize_string(raw_sql)

    parameters = [create_parameter(i, v) for i, v in enumerate(data)]
    return sql, parameters

def remove_special(s):
    return re.sub(r'[^a-zA-Z0-9]+', ' ', s)

def quote_escape(x):
    return parse.quote(str(x), '| @:')

def try_parse_date(s):
    try:
        return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%fZ")
    except:
        return None

def stringify_for_data_api_query(x, manual_escape=False):
    type_x = type(x)

    if type_x in (bool, list, tuple, bytes, float, int):
        return x
    elif type_x is dict:
        return json.dumps(x)
    elif x is None:
        return 'null'

    if manual_escape:
        v = try_parse_date(x)
        if v:
            return str(v)
        return quote_escape(x)

    # Default to string.
    return str(x)

def stringify_for_data_api_query_dict(d, manual_escape=False) -> dict:
    for k, x in d.items():
        type_x = type(x)
        if type_x in (bool, list, tuple, bytes, float, int):
            continue
        # Default to string.
        d[k] = str(x)
        if type_x is dict:
            d[k] = json.dumps(x)
        elif x is None:
            d[k] = 'null'
        if manual_escape:
            v = try_parse_date(x)
            if v:
                d[k] = str(v)
            d[k] = quote_escape(x)
    return d

def make_list(items):
    return '({})'.format(','.join(map(lambda x: "'{}'".format(stringify_for_data_api_query(x, True)), items)))

def format_string_list(s):
    """
    Convert comma separated list of items as string to escaped sql string list format
    ex: "'1', '2'" => ('1', '2')
    """
    return make_list(re.sub("['\" ]", "", s).split(','))

def unique_on_key(elements, key):
    """
    select unique items filtering nulls
    """
    return list({element.get(key): element for element in elements if element and key in element}.values())

class DataAccessLayerException(Exception):
   pass

class RDS:
    def __init__(self, env='DEV', client=None):
        self.__env = env
        self.rds_client = client or boto3.client('rds-data')

    @property
    def env(self):
        return self.__env

    @env.setter
    def env(self, env):
        self.__env = env

    def execute_statement(self, sql, sql_parameters={}):
        """
        Helper function to execute sql statements
        Params:
            sql - sql statement to execute (e.g SELECT * FROM table1 WHERE key1 = :key1)
            sql_parameters - sql param dict (e.g {key1: value1})
        Returns:
            RDS data api response rendered with corresponding column names
        """
        # generate params from the given dict 
        sql_params = create_parameter_dict(d=sql_parameters, sql=sql)

        params = {}
        params['sql'] = sql
        params['parameters'] = sql_params
        params['includeResultMetadata'] = True
        response = self.rds_client.execute_statement(**params, **GLOBALS['RDS'][self.__env])
        response = render_data_api_response(response)
        return response

    def batch_execute_statement(self, sql, sql_parameter_sets, column_names=None, generate_parameter_sets=False):
        """
        Batch insert and update
        """
        if generate_parameter_sets:
            # generate parameter set
            parameter_sets = [ create_parameter_dict(d, sql) for d in sql_parameter_sets ]
        else:
            parameter_sets = sql_parameter_sets

        params = {
            'sql': sql,
            'parameterSets': parameter_sets
        }
        response = self.rds_client.batch_execute_statement(**params, **GLOBALS['RDS'][self.__env])
        response = response.get('updateResults')
        if column_names:
            # render response from the batch
            response = render_data_api_response_batch(response, column_names=column_names)
        return response

def get_nvr_credentials(rds, site_id, private_key) -> dict:
    """Get nvr_credentials given site_id
    Params:
        rds: RDS object to call sql execution
        site_id: site id 
        private_key: private key to decrypt the password
    Returns: 
        credentials(dict)
    """
    from .utils import decrypt
    sql = """SELECT * FROM nvr_credentials WHERE site_id = :site_id"""
    sql_parameters = {'site_id': int(site_id)}
    site_id = None
    credentials= {}
    try:
        response = rds.execute_statement(sql, sql_parameters)
        records = response.get('records')
        if records:
            credentials = records[0]
            # decypt password with private key
            passwd = credentials.get('passwd').encode()
            decrypted_passwd = decrypt(private_key=private_key, value=passwd)
            credentials['passwd'] = decrypted_passwd
    except Exception as e:
        logging.error(f"{e}, assume credentials in the environment")
        return os.environ
    else:
        return credentials

_RDS = RDS(env='DEV')

def rds(env='DEV'):
    _RDS.env = env
    return _RDS

def site_info(name, env='DEV'):
    key = secrets.kvs_private_key()
    site = utils.get_secret(f'eigen/{env.lower()}/deployment')[name]
    info = get_nvr_credentials(rds(env), site['id'], key)
    del site['id']
    info.update(site)
    return info

def site_stream_url(name, ch, profile=None, env='DEV'):
    info = site_info(name, env)
    if info:
        user, passwd = info.get('username', None), info.get('passwd', None)
        ip, port = info['ip'], info.get('port', 554)
        from ...streaming.access import stream_url
        return stream_url(info['vendor'], user=user, passwd=passwd, ip=ip, port=port, ch=ch, profile=profile)

def main():
    pass

if __name__ == '__main__':
    main()


    
