from fractions import Fraction
from time import time, sleep
from xml.etree import ElementTree as ET

import requests, base64

from ml import av, cv, logging
from ml.av import h264
from ml.av import NALU_t
from ml.time import (
    time,
    sleep,
    fromFileTime,
)
from .avsource import AVSource

NAMESPACES = {
    'SOAP-ENV': 'http://schemas.xmlsoap.org/soap/envelope/',
    'xsi': 'http://www.w3.org/2001/XMLSchema-instance',
    'np': 'http://tempuri.org/np.xsd',
}

SOAP_REQ = """<np:{api} xmlns:np='http://tempuri.org/np.xsd'>
                {body}
            </np:{api}>"""

SOAP_REQ_ENV = """<np:{api}>
                {body}
            </np:{api}>"""

SOAP_ENV = """<SOAP-ENV:Envelope 
        xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/" 
        xmlns:SOAP-ENC="http://schemas.xmlsoap.org/soap/encoding/" 
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
        xmlns:xsd="http://www.w3.org/2001/XMLSchema" 
        xmlns:np="http://tempuri.org/np.xsd">
        <SOAP-ENV:Body>
            {req}
        </SOAP-ENV:Body>
    </SOAP-ENV:Envelope>
"""

SOAP_XML = """<?xml version="1.0" encoding="UTF-8"?>
    {soap}"""

def api_url(name, ip, port):
    return f'http://{ip}:{port}/api/{name}'

def auth(user='admin', passwd='admin'):
    return base64.b64encode(f'{user}:{passwd}'.encode()).decode()

def gSOAP_headers(user='admin', passwd='admin', version='2.7', **kwargs):
    headers = {
        'Authorization': f'Basic {auth(user, passwd)}',
        'User-Agent': f'gSOAP/{version}',
        'Connection': 'Keep-Alive',
        'Content-Type': 'text/xml; charset=utf-8',
        'Accept-Encoding': 'gzip, deflate',
    }

    headers.update(kwargs)
    return headers

def gSOAP_request(api, body, env=True):
    req = SOAP_REQ_ENV.format(api=api, body=body) if env else SOAP_REQ.format(api=api, body=body)
    soap = SOAP_ENV.format(req=req) if env else req
    return SOAP_XML.format(soap=soap)

from abc import ABC, abstractmethod

class NUUOException(Exception):
    r"""API exception
    """

class API(object):
    r"""Stateless SOAP APIs for NUUO Titan/Crystal.
    """

    PROFILES_IDX_NAME = [
        'Original',
        'Low',
        'Minimum',
    ]

    PROFILES_NAME_IDX = {
        'Original': 0,
        'Low': 1,
        'Minimum': 2.
    }

    def __init__(self, ip, port, user='admin', passwd='admin'):
        super(self.__class__, self).__init__()
        self.ip = ip
        self.port = int(port)
        self.user = user
        self.passwd = passwd

    def getSyncOEMString(self, debug=False):
        """
        /api/RPC:
            <np:getSyncOEMString>
                <oemStringCodeList>
                    <code>TlVVTw==</code>
                </oemStringCodeList>
            </np:getSyncOEMString>
            <np:getSyncOEMStringResponse>
                <syncOEMString>TlVVTw==</syncOEMString>
            </np:getSyncOEMStringResponse>
        """
        url = api_url('RPC', self.ip, self.port)
        headers = gSOAP_headers(user=self.user, passwd=self.passwd, version='2.7')
        payload = gSOAP_request(api='getSyncOEMString', 
                                body=f"""<oemStringCodeList>
                    <code>TlVVTw==</code>
                </oemStringCodeList>""",
        )

        if debug:
            level = logging.getLogger().level
            logging.getLogger().setLevel(logging.DEBUG)
        try:
            with requests.post(url, headers=headers, data=payload) as resp:
                # Crystal or Titan
                logging.debug(f'REQ: {resp.request.url}')
                logging.debug(f' {resp.request.body}')
                logging.debug(f'RESP: {resp.headers}')
                logging.debug(resp.text)
                syncOEMString = ET.fromstring(resp.text).find(
                    "./SOAP-ENV:Body"
                    "/np:getSyncOEMStringResponse"
                    "/syncOEMString", NAMESPACES)
                debug and logging.getLogger().setLevel(level)
                return None if syncOEMString is None else base64.b64decode(syncOEMString.text).decode()
        except requests.exceptions.ConnectionError as e:
            # Titan
            logging.warning(f"No response from np:getSyncString")
            debug and logging.getLogger().setLevel(level)
            return None

    def getTitanDeviceTreeList(self, debug=False):
        '''Enumerate camera streams from VMS over HTTP/SOAP.
        '''

        url = api_url('', self.ip, self.port)
        headers = gSOAP_headers(user=self.user, passwd=self.passwd, version='2.7')
        payload = gSOAP_request(api='getTitanDeviceTreeList', 
                                body='''<type>0</type><titanId>0</titanId>'''
        )
        deployment = [] # dict(area=, sensor=, profiles=)+

        if debug:
            level = logging.getLogger().level
            logging.getLogger().setLevel(logging.DEBUG)

        with requests.post(url, headers=headers, data=payload) as resp:
            logging.debug(f'REQ: {resp.request.url}')
            logging.debug(f' {resp.request.body}')
            logging.debug(f'RESP: {resp.headers}')
            '''
            <SOAP-ENV:Envelop>
            <SOAP-ENV:Body>
                <np:getDeviceTreeListResponse>
                <deviceTreeList>
                    <item>
                        <id>...</id>
                        <name>titan_8040R</name>
                        <description></description>
                        <childEntityList>
                            ...
                            <item>
                                <id>...</id>
                                <name>First Floor Entry</name>
                                <childEntityList>
                                    <item>
                                        <id>
                                            ...
                                            <localId>50341665</localId>
                                        </id>
                                        <name>sensor</name>
                                        <description></description>
                                        <numberOfStreams>1</numberOfStreams>
                                        <outputProfileList>
                                            <item>
                                                <streamIndex>0</streamIndex>
                                                <frameRate></frameRate>
                                                <bitrate>unknown</bitrate>
                                                <resolution></resolution>
                                                <codec>H264</codec>
                                                <quality></quality>
                                                <profileName>Original</profileName>
                                            </item>
                                            <item>
                                                <streamIndex>0</streamIndex>
                                                <frameRate>10~30</frameRate>
                                                <bitrate>unknown</bitrate>
                                                <resolution>QCIF~FullHD</resolution>
                                                <codec>H264/MJPEG</codec>
                                                <quality>Low</quality>
                                                <profileName>Low</profileName>
                                            </item>
                                            <item>
                                                <streamIndex>0</streamIndex>
                                                <frameRate>0.1~30</frameRate>
                                                <bitrate>unknown</bitrate>
                                                <resolution>QCIF~FullHD</resolution>
                                                <codec>H264/MJPEG</codec>
                                                <quality>Low</quality>
                                                <profileName>Minimum</profileName>
                                            </item>
                                            ...
                                        </outputProfileList>
                                    </item>
                                </childEntityList>
                            </item>
                            ...
                        </childEntityList>
                    </item>
            '''
            root = ET.fromstring(resp.text)
            VMS = root.find(
                "./SOAP-ENV:Body"
                "/np:getDeviceTreeListResponse"
                "/deviceTreeList"
                "/item/[name='titan_8040R']", NAMESPACES)
            devices = VMS.find("./childEntityList", NAMESPACES)
            for dev in devices:
                area = dev.find('./name').text
                if area is None:
                    continue

                entities = dev.find('./childEntityList', NAMESPACES)
                profiles = {}
                for entity in entities:
                    name = entity.find('./name').text
                    if name == 'sensor':
                        sensor = entity.find('./id/localId').text
                        for profile in entity.find('./outputProfileList'):
                            streamIndex = int(profile.find('./streamIndex').text)
                            profileName = profile.find('./profileName').text
                            frameRate = profile.find('./frameRate').text
                            bitrate = profile.find('./bitrate').text
                            resolution = profile.find('./resolution').text
                            quality = profile.find('./quality').text
                            codec = profile.find('./codec').text
                            codec = codec and codec.split('/') or None
                            codec = codec and list(map(lambda cc: cc.replace('.', '').lower(), codec))
                            profiles[profileName] = dict(
                                streamIndex = streamIndex,
                                frameRate = frameRate and tuple(map(float, frameRate.split('~'))) or None,
                                bitrate = bitrate != 'unknown' and int(bitrate) or None,
                                resolution = resolution and resolution.split('~') or None,
                                codec = codec,
                                quality = quality,
                            )
                        deployment.append(dict(area=area, sensor=sensor, profiles=profiles)) 
                        break
        debug and logging.getLogger().setLevel(level)
        return deployment

    def login(self, sessionId=None, serverId=None, debug=False):
        r'''
        /api/event_session:
            <np:login xmlns:np="http://tempuri.org/np.xsd">
                <supportedProtocolVersion>
                    ...
                    <version>2.1.4.0</version>
                </supportedProtocolVersion>
                <clientApplicationInfo
            </np:login>
            <np:loginResponse xmlns:np="http://tempuri.org/np.xsd">
                <result>true</result>
                <errorCode>
                <sessionId>52477</sessionId>
                <willBeExpire>
                <timeAboutToExpire>
                <syncOEMString>
                <agreeProtocolVersion>2.1.4.0</agreeProtocolVersion>
                <serverInfo>
            </np:loginResponse>
        '''

        url = api_url('event_session', self.ip, self.port)
        headers = gSOAP_headers(user=self.user, passwd=self.passwd, version='2.7')
        if serverId:
            assert self.session and 'login' in self.session
            payload = gSOAP_request(api='login', 
                                    body=f'''<supportedProtocolVersion>
                            <version>2.0.0.0</version><version>2.1.0.0</version><version>2.1.2.0</version>
                            <version>2.1.3.0</version><version>2.1.4.0</version>
                        </supportedProtocolVersion>
                        <clientApplicationInfo>
                            <oemStringCodeList><code>TlVVTw==</code></oemStringCodeList>
                            <appRoleType>7</appRoleType>
                            <platform>300</platform>
                            <matrixId>0</matrixId>
                        </clientApplicationInfo>
                        <serverId>{serverId}</serverId>
                        <sessionId>{sessionId}</sessionId>''',
                        env=False
            )
        else:
            payload = gSOAP_request(api='login', 
                                    body='''<supportedProtocolVersion>
                            <version>2.0.0.0</version><version>2.1.0.0</version><version>2.1.2.0</version>
                            <version>2.1.3.0</version><version>2.1.4.0</version>
                        </supportedProtocolVersion>
                        <clientApplicationInfo>
                            <oemStringCodeList><code>TlVVTw==</code></oemStringCodeList>
                            <appRoleType>7</appRoleType>
                            <platform>300</platform>
                            <matrixId>0</matrixId>
                        </clientApplicationInfo>''',
                        env=False
            )

        if debug:
            level = logging.getLogger().level
            logging.getLogger().setLevel(logging.DEBUG)
        with requests.post(url, headers=headers, data=payload) as resp:
            logging.debug(f'REQ: {resp.request.url}')
            logging.debug(f' {resp.request.headers}')
            logging.debug(f' {resp.request.body}')
            logging.debug(f'RESP: {resp.headers}, {resp.status_code}')
            logging.debug(f' {resp.text}')
            login = ET.fromstring(resp.text)
            debug and logging.getLogger().setLevel(level)
            return { element.tag: element.text for element in login }

    def startReceive(self, sessionId, debug=False):
        '''
        <np:startReceive xmlns:np="http://tempuri.org/np.xsd">
            <sessionId>52477</sessionId>
        </np:startReceive>
        '''
        url = api_url('event_session', self.ip, self.port+1)
        headers = gSOAP_headers(user=self.user, passwd=self.passwd, version='2.7')
        payload = gSOAP_request(api='startReceive', 
                                body=f'''<serverId>{sessionId}</serverId>''',
                                env=False
        )

        if debug:
            level = logging.getLogger().level
            logging.getLogger().setLevel(logging.DEBUG)
        try:
            with requests.post(url, headers=headers, data=payload) as resp:
                logging.debug(f'REQ: {resp.request.url}')
                logging.debug(f' {resp.request.headers}')
                logging.debug(f' {resp.request.body}')
                logging.debug(f'RESP: {resp.headers}, {resp.status_code}')
                logging.debug(f' {resp.text}')
        except requests.exceptions.ConnectionError as e:
            logging.warning(f"No response from np:startReceive")
        debug and logging.getLogger().setLevel(level)

    def getServerList(self, sessionId, debug=False):
        '''
        /api/RPC:
            <np:getServerList>
                <sessionId>52477</sessionId>
                <serverIdList></serverIdList>
            </np:getServerList>
            <np:getServerListResponse>
                <failList>
                <serverList>
                    <item
                    <item xsi:type="RecordingServer">
                        <id>3814084320248837336</id>
                        <name>Chino Recording NVR</name>
                        <enable>
                        ...
                        <serverVersion>
                        ...
                        </item>
                    <item
                    <item
                    </serverList>
            </np:getServerListResponse>
        '''
        
        url = api_url('RPC', self.ip, self.port)
        headers = gSOAP_headers(user=self.user, passwd=self.passwd, version='2.8')
        payload = gSOAP_request(api='getServerList', 
                                body=f"""<sessionId>{sessionId}</sessionId>""",
        )

        if debug:
            level = logging.getLogger().level
            logging.getLogger().setLevel(logging.DEBUG)
            
        servers = []
        with requests.post(url, headers=headers, data=payload) as resp:
            logging.debug(f'REQ: {resp.request.url}')
            logging.debug(f' {resp.request.body}')
            logging.debug(f'RESP: {resp.headers}')
            logging.debug(resp.text)
            root = ET.fromstring(resp.text)
            serverList = root.find(
                "./SOAP-ENV:Body"
                "/np:getServerListResponse"
                "/serverList", NAMESPACES)

            for server in serverList:
                attr = f"{{{NAMESPACES['xsi']}}}type"
                if attr in server.attrib and server.attrib[attr] == 'RecordingServer':     
                    id = server.find('id').text
                    name = server.find('name').text
                    enable = bool(server.find('enable').text)
                    version = server.find('serverVersion').text
                    if enable:
                        servers.append(dict(
                            id=id,
                            name=name,
                            type=server.attrib[attr],
                            enable=enable,
                            version=version,
                        ))
                    

        debug and logging.getLogger().setLevel(level)
        if not servers:
            logging.warning(f"Found no server of type 'RecordingServer'")
        return servers

    def getDeviceList(self, sessionId, serverId, debug=False):
        '''
        /api/RPC:
            <np:getDeviceList>
                <sessionId>52477</sessionId>
                <serverId>3814084320248837336</serverId>
                <unitDeviceIdList></unitDeviceIdList>
            </np:getDeviceList>
            <np:getDeviceListResponse>
                <failList>
                <deviceList>
                    <item xsi:type="VideoDevice">
                        <id>
                        <name>
                            Medical Entry
                            </name>
                        <enable>
                        <info>
                        <uiOrder>
                        <interfaceList>
                        <brand>
                        <model>
                        <loginName>
                        <password>
                        <childList>
                            <item
                                xsi:type="Camera">
                                <id>
                                    3659174698549248
                                    </id>
                                <name>
                                <enable>
                                <info>
                                <uiOrder>
                                <audioAllocateSourceId>
                                <outputProfileList>
                                </item>
                            <item
                            <item
                            <item
                            <item
                            <item
                            </childList>
                        <feasibleBrand>
                        <feasibleModel>
                        <selectProtocol>
                    </item>
                    ...    
                </deviceList>
            </np:getDeviceListResponse>
        '''

        from collections import OrderedDict
        url = api_url('RPC', self.ip, self.port)
        headers = gSOAP_headers(user=self.user, passwd=self.passwd, version='2.8')
        payload = gSOAP_request(api='getDeviceList', 
                                body=f"""<sessionId>{sessionId}</sessionId>
                                    <serverId>{serverId}</serverId>""",
        )

        if debug:
            level = logging.getLogger().level
            logging.getLogger().setLevel(logging.DEBUG)

        devices = []
        with requests.post(url, headers=headers, data=payload) as resp:
            logging.debug(f'REQ: {resp.request.url}')
            logging.debug(f' {resp.request.body}')
            logging.debug(f'RESP: {resp.headers}')
            logging.debug(resp.text)
            root = ET.fromstring(resp.text)
            cams = root.find(
                "./SOAP-ENV:Body"
                "/np:getDeviceListResponse"
                "/deviceList", NAMESPACES)
            for cam in cams:
                assert cam.attrib[f"{{{NAMESPACES['xsi']}}}type"] == 'VideoDevice'
                area = cam.find('name').text
                enable = bool(cam.find('enable').text)
                brand = cam.find('brand').text
                model = cam.find('model').text
                device = cam.find(
                    "childList"
                    "/item[@xsi:type='Camera']", NAMESPACES)
                id = device.find('id').text
                profiles = OrderedDict()
                for profile in device.find('outputProfileList'):
                    # FIXME NUUO/Crystal may return no codec info
                    streamIndex = int(profile.find('streamIndex').text)
                    codec = profile.find('codec').text or ['h264']
                    frameRate = int(profile.find('frameRate').text or 0)
                    bitrate = int(profile.find('bitrate').text or 0)
                    profileName = profile.find('profileName').text
                    profiles[profileName] = dict(streamIndex=streamIndex, codec=codec, frameRate=frameRate, bitrate=bitrate)
                devices.append(dict(
                    id=id,
                    brand=brand,
                    model=model,
                    area=area,
                    enable=enable,
                    profiles=profiles,
                ))
        return devices

    def getCameraAssociateList(self, sessionId, debug=False):
        r"""
        /api/RPC:
            <np:getCameraAssociateList>
                <sessionId>52477</sessionId>
            </np:getCameraAssociateList>
            <np:getCameraAssociateListResponse>
                <cameraAssociateList>
                    <item serverId="3814084320248837336">
                        <cameraList>...
                    </item>
                </cameraAssociateList>
            </np:getCameraAssociateListResponse>
        """
        if self.session is None:
            raise NUUOException('No session established')
        
        url = api_url('RPC', self.ip, self.port)
        headers = gSOAP_headers(user=self.user, passwd=self.passwd, version='2.8', Connection='close')
        payload = gSOAP_request(api='getCameraAssociateList', 
                                body=f"""<sessionId>{sessionId}</sessionId>""",
        )

        if debug:
            level = logging.getLogger().level
            logging.getLogger().setLevel(logging.DEBUG)
        
        with requests.post(url, headers=headers, data=payload) as resp:
            logging.debug(f'REQ: {resp.request.url}')
            logging.debug(f' {resp.request.headers}')
            logging.debug(f' {resp.request.body}')
            logging.debug(f'RESP: {resp.headers}, {resp.status_code}')
            logging.debug(f' {resp.text}')
        
        if debug:
            logging.getLogger().setLevel(level)
        
        # TODO parse associate list
        return resp.text

    def getServerConnectionStatus(self, sessionId, debug=False):
        r"""
        /api/RPC:
            <np:getServerConnectionStatus>
                <sessionId>52477</sessionId>
                <serverIdList></serverIdList>
            </np:getServerConnectionStatus>
            <np:getServerConnectionStatusResponse>
                 <failList>
                <statusList>
            </np:getServerConnectionStatusResponse>
        """
        if self.session is None:
            raise NUUOException('No session established')
        
        url = api_url('RPC', self.ip, self.port)
        headers = gSOAP_headers(user=self.user, passwd=self.passwd, version='2.8', Connection='close')
        payload = gSOAP_request(api='getServerConnectionStatus', 
                                body=f"""<sessionId>{sessionId}</sessionId>
                <serverIdList></serverIdList>""",
        )

        if debug:
            level = logging.getLogger().level
            logging.getLogger().setLevel(logging.DEBUG)
        
        with requests.post(url, headers=headers, data=payload) as resp:
            logging.debug(f'REQ: {resp.request.url}')
            logging.debug(f' {resp.request.headers}')
            logging.debug(f' {resp.request.body}')
            logging.debug(f'RESP: {resp.headers}, {resp.status_code}')
            logging.debug(f' {resp.text}')
        
        if debug:
            logging.getLogger().setLevel(level)
        
        # TODO parse resp
        return resp.text

    def AddressConfirm(self, sessionId, serverId, debug=False):
        r"""
        /api/RPC: gSOAP/2.8
            <np:AddressConfirm>
                <sessionId>52477</sessionId>
                <serverId>3814084320248837336</serverId>
            </np:AddressConfirm>
            <np:AddressConfirmResponse>
            </np:AddressConfirmResponse>
        """
        if self.session is None:
            raise NUUOException('No session established')
        
        url = api_url('RPC', self.ip, self.port+1)
        headers = gSOAP_headers(user=self.user, passwd=self.passwd, version='2.8', Connection='close')
        payload = gSOAP_request(api='AddressConfirm', 
                                body=f"""<sessionId>{sessionId}</sessionId>
                <serverId>{serverId}</serverId>""",
        )

        if debug:
            level = logging.getLogger().level
            logging.getLogger().setLevel(logging.DEBUG)
        with requests.post(url, headers=headers, data=payload) as resp:
            logging.debug(f'REQ: {resp.request.url}')
            logging.debug(f' {resp.request.headers}')
            logging.debug(f' {resp.request.body}')
            logging.debug(f'RESP: {resp.headers}, {resp.status_code}')
            logging.debug(f' {resp.text}')
            assert resp.ok
        debug and logging.getLogger().setLevel(level)
        
        # TODO parse resp
        return resp.text

    def live(self, cfg, sessionId=None, timeout=None, debug=False):
        url = api_url('live', self.ip, self.port)
        if sessionId is None:
            # Titan
            cam, profile, codec = cfg
            params = {
                'sensor': cam['sensor'],
                'profile': profile,
            }
            headers = {
                'Authorization': f'Basic {auth(self.user, self.passwd)}',
                'Connection': 'Keep-Alive',
            }
            if debug:
                level = logging.getLogger().level
                logging.getLogger().setLevel(logging.DEBUG)
            from ml.requests.multipart import MultipartStreamDecoder
            with requests.get(url, params=params, headers=headers, stream=True, timeout=timeout) as resp:
                logging.debug(f'REQ: {resp.request.url}')
                logging.debug(f' {resp.request.body}')
                logging.debug(f'RESP: {resp.headers}')
                for part in MultipartStreamDecoder.from_response(resp):
                    media, cc = part.headers[b'Content-Type'].decode().split('/')
                    yield dict(KeyFrame=part.headers[b'isKeyFrame'] == b'true',
                               time=time(), payload=part.content), (media, cc)
            debug and logging.getLogger().setLevel(level)
        else:
            '''
            # Crystal
            /api/live:
                <np:live xmlns:np="http://tempuri.org/np.xsd">
                    <sessionId>52477</sessionId>
                    <serverId>3814084320248837336</serverId>
                    <cameraId>
                        <server>3814084320248837336</server>
                        <device>3659174698549248</device>
                    </cameraId>
                    <profile>1</profile>
                    <includeContent>0</includeContent>
                </np:live>
            '''
            serverId, cam, profile, codec = cfg
            deviceId = cam['id']
            url = api_url('live', self.ip, self.port+1)
            headers = gSOAP_headers(user=self.user, passwd=self.passwd, version='2.7')
            payload = gSOAP_request(api='live', 
                                    body=f"""<sessionId>{sessionId}</sessionId>
                    <serverId>{serverId}</serverId>
                    <cameraId>
                        <server>{serverId}</server>
                        <device>{deviceId}</device>
                    </cameraId>
                    <profile>{self.PROFILES_NAME_IDX[profile]}</profile>
                    <includeContent>0</includeContent>""",
                        env=False
            )

            if debug:
                level = logging.getLogger().level
                logging.getLogger().setLevel(logging.DEBUG)
            from ml.requests.multipart import HTTPRequestStreamDecoder, HTTPError, HTTPStatus, Timeout, RequestException
            error_count = 0
            while True:
                # XXX The streaming server may be unavailable temporarily, givng error response for retry
                try:
                    with requests.post(url, headers=headers, data=payload, stream=True, timeout=timeout) as resp:
                        logging.debug(f'REQ: {resp.request.url}')
                        logging.debug(f' {resp.request.headers}')
                        logging.debug(f' {resp.request.body}')
                        logging.debug(f'RESP: {resp.headers}, code={resp.status_code}')
                        prev = []
                        for req in HTTPRequestStreamDecoder.from_response(resp):
                            media, cc = req.headers['Content-Type'].split('/')
                            logging.debug(req.headers)
                            if '264' in cc and req.headers['IsKeyFrame'] == 'true':
                                # XXX Sometimes a key frame contains no IDR but SEI from Crystal
                                prev.append(req)
                                prev_time = req.headers['IsKeyFrame']
                            else:
                                if prev:
                                    if len(prev) > 1:
                                        logging.warning(f"Conatenating NALUs from {len(prev)} key frames")
                                    body = b"".join(r.body for r in prev)
                                    yield dict(KeyFrame=True, time=fromFileTime(int(prev[-1].headers['time'])),
                                               payload=body), (media, cc)
                                    prev.clear()                    
                                
                                yield dict(KeyFrame=False, time=fromFileTime(int(req.headers['time'])),
                                           payload=req.body), (media, cc)
                                
                    
                # XXX Reraise exceptions for top level to handle
                except HTTPError as e:
                    if True:
                        # FIXME Login session error after long term streaming from NUUO
                        if e.args[0] == HTTPStatus.BAD_REQUEST:
                            logging.error(f"Streaming failed for {e}")
                            error_count += 1
                            if error_count >= 3:
                                logging.error(f"Login again is of no use, quit")
                                raise e
                            logging.info(f"Retry to log in after 1s")
                            sleep(1)
                            self.login()
                        else:
                            raise e
                    else:
                        logging.error(f"HTTP error: {e}")
                        raise e
                except Timeout as e:
                    logging.error(f"Timeout: {e}")
                    raise e
                except RequestException as e:
                    logging.error(f"RequestException: {e}")
                    raise e
            debug and logging.getLogger().setLevel(level)

class NVR(ABC):
    r"""Class of NUUO Recording Server accessible through NUUO Management Server.
    """

    @classmethod
    def create(cls, ip, port, user='admin', passwd='admin'):
        api = API(ip, port, user, passwd)
        OEM = api.getSyncOEMString()
        if OEM is None:
            return Titan8040R(api)
        else:
            return Crystal(api)

    def __init__(self, api):
        super(NVR, self).__init__()
        self.api = api
        self.session = None

    def __getattr__(self, attr):
        return getattr(self.api, attr)

    @abstractmethod
    def query(self,):
        pass

    @abstractmethod
    def connect(self):
        pass

    def disconnect(self):
        # TODO logout APIs
        self.session = None
    
    @abstractmethod
    def startStreaming(self):
        pass

    def __iter__(self):
        if self.session and 'deployment' in self.session:
            deployment = self.session['deployment'] 
            return iter(isinstance(deployment, dict) and deployment.items() or deployment)
        else:
            raise StopIteration

class Titan8040R(NVR):
    def query(self, area=r'.*', profile='Original', codec=None, exact=False):
        '''Query the existing deployment for desired cameras mathcing the criteria.
        The criteria are fuzzy by default and exact for area matching if required.
        Besides, the query is always case insensitive.

        Args:
            area: None for matching all
            profile: None for matching all
            codec: None for matching all

        Returns:
            res: list of tuples of (cam, profile, codec) for streaming, where:
                - cam for matched camera
                - profile for matched profile
                - codec for matched codec
        '''

        if self.session is None or 'deployment' not in self.session:
            logging.warning(f"No connection established")
            return None

        import re
        res = []
        area = re.compile(area, re.IGNORECASE)
        if profile:
            profile = self.api.PROFILES_IDX_NAME[profile] if isinstance(profile, int) else profile.capitalize()
        else:
            profile = None
        codec = codec or None
        for cam in self.session['deployment']:
            if profile:
                # Area matching
                logging.debug(f"area={cam['area']}")
                match = area.fullmatch(cam['area']) if exact else area.search(cam['area'])
                if match:
                    # Single profile matching
                    if isinstance(profile, int):
                        prof = self.api.PROFILES_IDX_NAME[profile]
                    else:
                        prof = profile.capitalize()
                    
                    if prof in cam['profiles']:
                        cfg = cam['profiles'][prof]
                    else:
                        logging.warning(f"'{cam['area']}' has no profile '{prof}'")
                        continue

                    # Single codec matching
                    codecs = cfg['codec'] or []
                    if codec is None:
                        codec = codecs and codecs[0]
                    codec = av.codec(codec)[0]
                    if codec in codecs:
                        res.append((cam, prof, codec))
                    else:
                        logging.warning(f"'{cam['area']}' has no profile '{prof}' with codec '{codec}' in {codecs}")
            else:
                res.append(cam)
        return res

    def connect(self):
        if self.session:
            raise NUUOException(f"Session active, disconnect beforehand")
        else:
            self.session = dict(deployment=self.api.getTitanDeviceTreeList())
    
    def startStreaming(self, cfg, timeout=None, debug=False):
        '''Start live streaming from a camera channel managed by NVR over HTTP

        Args:
            cam: camera deployment
            profile: streaming profile in ['Original' | 'High' | 'Low' | 'Minimum']
            codec: stream encoding in ['H.264' | 'MJPG']
        '''
        return self.api.live(cfg, timeout=timeout, debug=debug)

class Crystal(NVR):
    r"""NUUO Crystal NVR.
    """

    def query(self, area=r'.*', profile='Original', codec='H.264', exact=False) -> list:
        '''Query the existing deployment for desired cameras mathcing the criteria.
        The criteria are fuzzy by default and exact for area matching if required.
        Besides, the query is always case insensitive.

        Args:
            area: None for matching all
            profile: None for matching all
            codec: None for matching all

        Returns:
            res: list of tuples of (cam, profile, codec) for streaming, where:
                - cam for matched camera
                - profile for matched profile
                - codec for matched codec
        '''
        if self.session is None or 'login' not in self.session:
            logging.warning(f"No session established")
            return None
        if 'deployment' not in self.session:
            logging.warning(f"No connection established")
            return None

        import re
        res = []
        area = re.compile(area, re.IGNORECASE)
        if profile:
            profile = self.api.PROFILES_IDX_NAME[profile] if isinstance(profile, int) else profile.capitalize()
        codec = codec or None
        for serverId, setup in self:
            cams = setup['devices']
            for cam in cams:
                if profile:
                    # Area matching
                    logging.debug(f"area={cam['area']}")
                    match = area.fullmatch(cam['area']) if exact else area.search(cam['area'])
                    if match:
                        # Single profile matching
                        if isinstance(profile, int):
                            prof = self.api.PROFILES_IDX_NAME[profile]
                        else:
                            prof = profile.capitalize()
                        
                        if prof in cam['profiles']:
                            cfg = cam['profiles'][prof]
                        else:
                            logging.warning(f"'{cam['area']}' has no profile '{prof}'")
                            continue

                        # Single codec matching
                        codecs = cfg['codec'] or []
                        codec = av.codec(codec)[0]
                        if codec in codecs:
                            res.append((serverId, cam, prof, codec))
                        else:
                            logging.warning(f"'{cam['area']}' has no profile '{prof}' with codec '{codec}' in {codecs}")
                else:
                    res.append(cam)
        return res

    def connect(self):
        login = self.api.login()
        if login['result'] == 'false':
            raise NUUOException(f"Failed to log in to {self.user}:{self.passwd}@{self.ip}:{self.port}")

        self.session = dict(login=login) 
        sessionId = login['sessionId']
        servers = self.api.getServerList(sessionId)
        self.session['deployment'] = deployment = {}
        for server in servers:
            serverId = server['id']
            devices = self.api.getDeviceList(sessionId, serverId)
            deployment[serverId] = dict(server=server, devices=devices)
    
    def startStreaming(self, cfg, timeout=None, debug=False):
        '''Start live streaming from a camera channel managed by NVR over HTTP
        
        Args:
            cfg: model specific streaming configuration returned from a query
                serverId: the server id the device belongs to
                cam: camera device
                profile: streaming profile in ['Original' | 'High' | 'Low' | 'Minimum']
                codec: stream encoding in ['H.264' | 'MJPG']
        Exceptions:
            HTTPError:
            Timeout:
            RequestException:
        '''
        if self.session and 'login' in self.session:
            sessionId = self.session['login']['sessionId']
            logging.info(f"Live streaming with timeout={timeout}")
            return self.api.live(cfg, sessionId, timeout=timeout, debug=debug)
        else:
            raise StopIteration

class NUUOSource(AVSource):
    def __init__(self, *args, **kwargs):
        if args[0].startswith('nuuo://'):
            import re
            url = args[0]
            match = re.match(r'nuuo://(.+):(.+)@(.+):(.+)', url)
            if match:
                user, passwd, ip, port = match.group(1), match.group(2), match.group(3), int(match.group(4))
            else:
                match = re.match(r'nuuo://(.+):(.+)', url)
                if match is None:
                    raise ValueError(f"Missing IP and port in the URL: {', '.join(args)}")
                ip, port = match.group(1), int(match.group(2))
        else:
            ip, port = args[:2]
        user = kwargs.pop('user', 'admin')
        passwd = kwargs.pop('passwd', 'admin')
        self.nvr = NVR.create(ip, port, user, passwd)

    def open(self, area, profile='Original', decoding=True, exact=False, with_audio=False, **kwargs):
        """Start streaming from NUUO/NVR over HTTP multiparts.
        Streaming may be interleaved with media of different formats.

        Args:
            area: target cameara stream(s) to connect and receive
            profile: codec profile
            fps: preset FPS
            decoding: option to decode stream or not
            exact: area query to match exactly or not
#           workaround: dealing with the last zero byte of PPS leading to three consecutive zero bytes
        """
        
        logging.info(f"Connecting nuuo://{self.nvr.user}:{self.nvr.passwd}@{self.nvr.ip}:{self.nvr.port}")
        self.nvr.connect()

        sessions = []
        cfgs = self.nvr.query(area, profile=profile, exact=exact)
        for cfg in cfgs:
            try:
                cam, profile, codec = cfg[-3:]
                logging.info(f"Starting streaming: {profile}/{codec}@{cam['area']}")
                stream = self.nvr.startStreaming(cfg, timeout=kwargs.pop('timeout', None))
#                workaround = not decoding and '264' in codec
                codec = av.CodecContext.create(codec, 'r')
                session = dict(
                    stream=stream,
                    cam=cam,
                    profile=profile,
                    start=time(),
                    video=dict(
                        stream=stream,
                        type=None,
                        codec=codec,
                        format='BGR',
                        decoding=decoding,
                        width=None,
                        height=None,
                        fps=kwargs.get('fps', None),
                        time=0,
                        count=0,
                        keyframe=False,
#                        workaround=workaround,
                    ),
                )
                if with_audio:
                    # XXX Unknown audio codec yet
                    session['audio'] = dict(
                        stream=stream,
                        type=None,
                        codec=None,
                        format='s16',
                        decoding=decoding,
                        sample_rate=8000,
                        channels=1,
                        time=0,
                        count=0,
                    )
            except Exception as e:
                logging.error(f"Failed to start streaming from {cam['area']}: {e}")
                raise e
            else:
                logging.info(f"Established a streaming session with {cam['area']}")
                sessions.append(session)
        return sessions
    
    def process_video(self, session, packet, timestamp):
        r"""
        Args:
            timestamp(float): aboslute time from Crystal or now for Titan in UNIX time
        """
        media = session['video']
        decoding = media['decoding']
        media['time_base'] = time_base = packet.time_base or Fraction(1, 8000 * media['fps'])
        if decoding:
            codec = media['codec']
            logging.debug(f"[{media['count']}] video/{media['type']} => {codec.name}")
            try:
                frames = codec.decode(packet)
                assert len(frames) == 1, f"Only one frame at a time is expected but got {len(frames)} frames in one packet"
            except Exception as e:
                logging.error(f"Failed to decode packet of size {packet.size}: {e}")
                return None
            else:
                frame = frames[0]
                media['width'] = frame.width
                media['height'] = frame.height
        else:
            # awslabs/amazon-kinesis-video-streams-producer-sdk-cpp#357
            # XXX NUUO NALU weird format of three consecutive zero bytes
            NALUs = []
            for (pos, _, _, type), nalu in h264.NALUParser(memoryview(packet), workaround=True):
                # assert isValidNALU(nalu), f"frame[{meta['count']+1}] NALU(type={type}) at {pos} without START CODE: {nalu[:8].tobytes()}"
                if type in (NALU_t.SPS, NALU_t.PPS, NALU_t.IDR, NALU_t.NIDR):
                    NALUs.append(nalu)
                    logging.debug(f"frame[{media['count']+1}] {NALU_t(type).name} at {pos}: {nalu[:8].tobytes()}")
                else:
                    logging.debug(f"frame[{media['count']+1}] skipped {NALU_t(type).name} at {pos}: {nalu[:8].tobytes()} ending with {nalu[-1:].tobytes()}")
            packet = av.Packet(bytearray(b''.join(NALUs)))
            frame = packet

        duration = float((packet.duration or int(1 / time_base / media['fps'])) * time_base)
        if media['count'] == 0:
            media['count'] += 1
            logging.info(f"Assume absolute start timestamp={timestamp} to reset session start")
            session['start'] = media['time0'] = timestamp
            media['duration'] = duration
            media['time'] = session['start'] # absolute or now
        else:
            media['count'] += 1
            media['fps_rt'] = media['count'] / (time() - session['start'])
            expected = media['time'] + media['duration']
            offset = (timestamp - expected) / 2
            offset = min(offset, duration / 2) if offset > 0 else max(offset, -duration / 2)
            media['duration'] = duration + offset
            media['time'] = session['start'] + (expected - media['time0'])
            logging.debug(f"Adaptive frame duration: offet={offset:.3f}s, duration={media['duration']:.3f}s")
            logging.debug(f"media['time']=expected={expected:.3f}s, timestamp={timestamp:.3f}s")

        # dts/pts are made adaptive w.r.t. absolute media['time']
        if decoding:
            format = media['format']
            if format == 'BGR':
                frame = frame.to_rgb().to_ndarray()[:,:,::-1]
            elif format == 'RGB':
                frame = frame.to_rgb().to_ndarray()
        return media, frame
    
    def process_audio(self, session, packet):
        media = session['audio']
        format = media.get('format', 's16')
        sample_rate = media.get('sample_rate', 8000)
        channels = media.get('channels', 1)
        media['time_base'] = time_base = packet.time_base or Fraction(1, sample_rate)
        media['pts'] = media['dts'] = packet.pts or media['count'] * packet.size
        media['time'] = session['start'] + media['pts'] * time_base
        media['count'] += 1
        cc = av.codec(media['type'])[0]
        if media['codec'] is None and media['decoding']:
            media['codec'] = codec = av.CodecContext.create(cc, 'r')
            codec.format = av.AudioFormat(format)
            codec.sample_rate = sample_rate
            codec.channels = channels
        if media['decoding']:
            codec = media['codec']
            logging.debug(f"[{media['count']}] audio/{media['type']} => {codec.name}")
            try:
                frames = codec.decode(packet)
                assert len(frames) == 1, f"Only one frame at a time is expected but got {len(frames)} frames in one packet"
            except Exception as e:
                logging.error(f"Failed to decode packet of size {packet.size}: {e}")
                return None
            else:
                frame = frames[0]
                return media, frame.to_ndarray()
        else:
            return media, packet

    def read(self, session, media=None, format='BGR'):
        """Read frames from NUUO stream.
        """
        stream = session['stream']
        while True:
            try:
                pkt, (m, cc) = next(stream)
            except Exception as e:
                logging.info(f"Failed to read a frame: {e}")
                raise e
            now = time()
            skip = False
            if media is None:
                # Skip if not desired
                skip = m not in session
            elif media != m:
                # Skip until desired
                skip = True
            if skip:
                logging.debug(f"Skpping {m}/{cc} until {media}")
            else:
                break

        media = session[m]
        payload = pkt['payload']
        packet = av.Packet(payload)
        media['type'] = cc
        media['keyframe'] = pkt['KeyFrame']
        timestamp = pkt['time'] # from Crystal or now
        if m == 'video':
            media['format'] = format
            res = self.process_video(session, packet, timestamp)
        elif m == 'audio':
            res = self.process_audio(session, packet)
        else:
            raise ValueError(f"Unexpected reading '{m}'")
        return res and (m, *res)