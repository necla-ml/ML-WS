import os
import json
import shlex
import boto3
import base64
import requests
import subprocess
from cryptography.fernet import Fernet
from botocore.exceptions import ClientError
from ml import logging

def get_secret(secret_id="KinesisVideoStreamProducerCredentials"):
    """"
    Helper function to get the credentials from AWS secret manager
    Params: 
        secret_id: id to be fetched from secret store
    Returns: 
        secret(json)
    """
    secret = None
    # init Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name='us-east-1',
    )
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_id)
    except ClientError as e:
        logging.error(f"{e}, assume secrets in the environment")
        return os.environ
    else:
        # secret could be string or binary
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
        else:
            secret = base64.b64decode(get_secret_value_response['SecretBinary']) 
    
    return json.loads(secret) 

def encrypt(private_key, value):
    """
    Encrypt value with the private key
    Params: 
        private_key: encoded str or str
        value: (encoded str or dict or str)value to be encrypted by the private key
    Returns: 
        encrypted value(string)
    """
    cipher_key = private_key.encode() if not isinstance(private_key, bytes) else private_key # encode private key
    # init fernet
    cipher = Fernet(cipher_key)
    if isinstance(value, dict):
        value = json.dumps(value)
    value = value.encode() if not isinstance(value, bytes) else value # encode value
    encrypted_value = None
    try:
        encrypted_value = cipher.encrypt(value) # encypt with cipher
        encrypted_value = encrypted_value.decode()
    except Exception as e:
        logging.error(e)

    return encrypted_value

def decrypt(private_key, value):
    """
    Decrypt value with the private key
    Params: 
        private_key: string encoded or string 
        value: value to be decrypted by the private key(str or byte)
    Returns: 
        decrypted value(string)
    """
    cipher_key = private_key.encode() if not isinstance(private_key, bytes) else private_key # encode private key
    # init fernet
    cipher = Fernet(cipher_key) 
    value = str(value).encode() if not isinstance(value, bytes) else value # encode value
    decrypted_value = None
    try:
        decrypted_value = cipher.decrypt(value).decode("utf-8")
        decrypted_value = json.loads(decrypted_value)
    except Exception as e:
        logging.warning(f"Not in json, assume string value")
    return decrypted_value

def get_task_id_on_instance(url='http://localhost:51678/v1/tasks'):
    """
    Get task id of the tasks running on the instance
    Params:
        url - localhost url with port and path
    Returns:
        dict with task_id and docker_id
    """
    # request localhost url for tasks on an instance
    response = requests.get(url)
    content_text = response.text
    content = json.loads(content_text)

    '''
    {
        'Tasks':
        [
            {'Arn': 'arn:aws:ecs:us-east-1:734908081819:task/eigen-streams-ec2/cd4206be944649b5a8438e6612bb8ce3',
             'DesiredStatus': 'RUNNING',
             'KnownStatus': 'RUNNING',
             'Family': 'eigen_stream',
             'Version': '7',
             'Containers': [{'DockerId': 'f62f70674418cf60e60beb2afee695d68e6653a185fdda71b3f3cf03f9e85566',
                              'DockerName': 'ecs-eigen_stream-7-ml-ws-f68c96eaa1d5f6a8b301',
                              'Name': 'ml-ws'}]},
            {'Arn': 'arn:aws:ecs:us-east-1:734908081819:task/eigen-streams-ec2/892886ad971b464f9a42267213d0bed1',
             'DesiredStatus': 'RUNNING',
             'KnownStatus': 'RUNNING',
             'Family': 'eigen_stream',
             'Version': '7',
             'Containers': [{'DockerId': '28fa111adb953e8c2720dc8428bd27e39866f0b967a6394b1b4323b7915c2930',
                              'DockerName': 'ecs-eigen_stream-7-ml-ws-96dbfcbbf6bb948fb301',
                              'Name': 'ml-ws'}]}
        ]
    }
    '''

    # get docker id from the cpuset inside container
    with open('/proc/1/cpuset', 'r') as f:
        cpuset = f.read()
    docker_id = os.path.basename(cpuset)
    tasks = content.get('Tasks')
    out = {}
    # NOTE: there could be multiple containers running on single instance
    # get task_id based on docker_id
    for task in tasks:
        docker_id = docker_id.strip()
        containers = task.get('Containers')
        if containers:
            task_docker_id = containers[0]['DockerId'].strip()
            if task_docker_id == docker_id:
                arn = task.get('Arn')
                task_id = os.path.basename(arn)
                out = {
                    'task_id': task_id,
                    'docker_id': docker_id
                }

    return out

def transcode_to_h264(pth, cmd=None):
    """
    Transcode video to h264(annexb)
    params: 
        pth - path to video to be transcoded
    Returns:
        path to transcoded video
    """
    output_pth = None
    try:
        basename = os.path.basename(pth).split('.')[0]
        output = ''.join(basename)
        output_pth = os.path.dirname(basename) + f'{output}.h264'
        
        if not cmd:
            cmd = f'''ffmpeg -i {pth} \
            -c:v copy \
            -bsf h264_mp4toannexb \
            -y {output_pth}'''

        cmd = shlex.split(cmd)
        output = subprocess.call(cmd)
    except Exception as e:
        logging.error(f'Error transcoding the video at {pth}: {e}')

    return output_pth

def setup_test():
    import subprocess
    import shlex
    import socket
    # XXX: ActiveMQ broker TEST server
    # stomp protocol @port 61613
    try:
        cmd = shlex.split('docker run --rm --name activemq -d -p 61616:61616 -p 8161:8161 -p 61613:61613 rmohr/activemq')
        subprocess.run(cmd)
    except Exception as e:
        logging.error(e)
    ip = socket.gethostbyname(socket.gethostname())
    broker_creds = {
            'server': ip,
            'user': 'admin',
            'passwd': 'admin',
            'port': 61613
    }
    return broker_creds

if __name__ == '__main__':
    get_task_id_on_instance()


