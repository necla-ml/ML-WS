import os
import boto3
from ml import logging
import botocore
from time import time
from pathlib import Path

from ..common import ParallelExecutor

logging.getLogger().setLevel('INFO')

# s3 client with 50 pool connections for multithreaded download
s3_client = boto3.client('s3', config=botocore.client.Config(max_pool_connections=50))

def download_key(bucket, key, pth, reload=False):
    """
    Download given key from s3 bucket
    Params:
        key - key to download from bucket
        bucket - name of s3 bucket
        pth - path to save the downloaded key
    Returns: absolute path to key
    """
    t = time()
    try:
        pth = Path(pth)
        if pth.exists() and not reload:
            logging.info(f'{key} exists..reload to overwrite')
        else:
            with open(pth, 'wb') as f:
                s3_client.download_fileobj(bucket, key, f)
    except Exception as e:
        logging.error(f'Error downloading key: {key}: {e}')
        raise e
    else:
        elapse = time() - t
        logging.info(f'Downloaded key: {key} from bucket: {bucket} in {elapse:.3f}sec')

def delete_key(bucket, key):
    """
    Delete given key from s3 bucket
    Params:
        key - key to be deleted
        bucket - name of the bucket
    Returns: True if success else False
    """
    result = True
    try:
        s3_client.delete_object(Bucket=bucket, Key=key)
    except Exception as e:
        result = False
        logging.error(e)
    return result

def list_objects(bucket):
    """
    List objects in a bucket
    Params:
        bucket - name of the bucket
    Return: list of object keys
    """
    lst = []
    try:
        response = s3_client.list_objects(
            Bucket=bucket,
            MaxKeys=100
        )
    except Exception as e:
        raise e
    else:
        for res in response['Contents']:
            lst.append(res.get('Key'))
    
    return lst

def download_keys(bucket=None, keys=None, reload=False):
    """
    Download multiple keys in parallel
    Params: 
        bucket - name of the bucket
        keys - dict with s3 key and destination path (key:path)
    Returns: 
        None
    Constraints:
        Max keys = 50
    """
    pe = ParallelExecutor(max_workers=len(keys))
    tasks = [ (download_key, bucket, key, path, reload) for key, path in keys.items() ]
    t = time()
    results = pe.run(tasks)
    for res in results:
        if isinstance(res, Exception):
            raise res
    elapse = time() - t
    logging.info(f'Operation completed in {elapse:.3f}sec')
    pe.close()
    






