# Copyright (c) 2017-present, NEC Laboratories America, Inc. ("NECLA"). 
# All rights reserved.
#
# This source code is licensed under the license found in the LICENSE file in
# the root directory of this source tree. An additional grant of patent rights
# can be found in the PATENTS file in the same directory
from ml import logging

from .. import AVSource
from . import KVProducer, DEFAULT_FPS_VALUE

class NUUOProducer(KVProducer):
    def __init__(self,
                 ip=None,
                 port=None,
                 user='admin',
                 passwd='admin',
                 accessKey=None,
                 secretKey=None,
                 site_id=None,
                 env='DEV'):
        # AWS/KVS credentials given or from secret store or from env
        super(NUUOProducer, self).__init__(accessKey, secretKey)
        from ...ws.aws.rds import RDS, get_nvr_credentials
        rds = RDS(env=env)
        cred = get_nvr_credentials(rds, site_id, self.privateKey)
        self.ip = ip or cred.get('ip', None)
        self.port = port or cred.get('port', None)
        self.user = user or cred.get('username', None)
        self.passwd = passwd or cred.get('passwd', None)
        logging.info(f"Streaming on behalf of user={self.user} from {self.ip}:{self.port}")
    
    def open(self, *args, **kwargs):
        '''
        Args:
            area:
        Kwargs:
            area(str): camera area
            fps(int): video frame rate
            profile(str | int): str for Titan with fixed mapping and int for Crystal
                Original: 0
                Low: 1
                Minimum: 2
            timeout(int, Tuple[int, int]): connection timeouot and read timeout of requests
        '''
        area = args[0]
        fps = kwargs.pop('fps', DEFAULT_FPS_VALUE)
        profile = kwargs.pop('profile', 'Low')
        timeout = kwargs.pop('timeout', (15, 30))
        with_audio = kwargs.pop('with_audio', False)
        self.src =  AVSource.create(f"nuuo://{self.ip}:{self.port}", user=self.user, passwd=self.passwd)
        return self.src.open(area, fps=fps, 
                                profile=profile, 
                                decoding=False, 
                                exact=True, 
                                with_audio=with_audio, 
                                timeout=timeout)
