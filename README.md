# Streaming Web Service

This package conatains a KVS producer binding in Python to upload H.264 ES from DeepLens cameras and NUUO NVRs.
This is the back end code base to interface with devices, VMS and AWS/KVS.  
Production code must not hard-code AWS credentials but use execution roles instead.

## Installation

Install from NECLA-ML anaconda repo:

```sh
conda install -c necla-ml ml-ws
```

## Usage
- RTSP streaming pipeline with GStreamer and RTCP time sync
```py
import logging
from time import time
from datetime import datetime

from ml.gst import RTSPPipeline, MESSAGE_TYPE, RTSP_CONFIG

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

# pipeline config
cfg = RTSP_CONFIG(
    location = 'rtsp://server_address:554/path',
    latency = 5000,
    protocols = 'tcp', # tcp | udp | http | udp_mcast | unknown | tls

    encoding = 'H264', # h264 | h265
    encoding_device = 'cpu', # cpu | gpu

    framerate = 10,
    scale = (720, 1280) # (H, W)
)
# init pipeline
pipeline = RTSPPipeline(cfg)
# start pipeline
pipeline.start()

frame_idx = 0
try:
    while True:
        msg_type, message = pipeline.read()
        if msg_type == MESSAGE_TYPE.FRAME:
            frame = message.data
            timestamp = message.timestamp
            duration = message.duration

            timestamp_frame = datetime.utcfromtimestamp(timestamp).strftime('%m-%d-%Y %H:%M:%S')
            timestamp_now = datetime.utcfromtimestamp(time()).strftime('%m-%d-%Y %H:%M:%S')

            logging.info(f'Frame: {frame_idx} | Current: {timestamp_now} | Frame: {timestamp_frame}')
            frame_idx +=1 
        elif msg_type == MESSAGE_TYPE.EOS:
            raise message
        elif msg_type == MESSAGE_TYPE.ERROR:
            raise message
        else:
            print('Unknown message type')
            break
except KeyboardInterrupt:
    logging.info('Stopping stream...')
except Exception as e:
    logging.error(f'Error: {e}')

pipeline.close()
```

## DeepLens Setup

After logged in to the Deeplens cam, enable port 8883 for Greengrass:

```sh
sudo ufw allow 8883/tcp
```

Grant permission to access `/dev/video*`:

```sh
sudo usermod -a -G video aws_cam
```

## Amazon Kinesis Video Stream Producer SDK binding

The streaming upload is provided by the producer SDK from Amazon.
The local development instructions builds the SDK for Python bindings.

### Streaming from Amazon DeepLens

### Video Encoder Format

For better streaming performance, it is recommended to configure the camera encoder settings beforehand.
For example, reducing the GOP seems to reduce the rendering delay on Amazon KVS console.
The DeepLens camera encoder settings can be configured by the following command:

```sh
sudo /opt/awscam/camera/installed/bin/mxuvc --ch 1 [resolution | framerate | gop | bitrate | framesize | ...]
sudo /opt/awscam/camera/installed/bin/mxuvc --ch 1 resolution 1280 720
sudo /opt/awscam/camera/installed/bin/mxuvc --ch 1 framerate  15
sudo /opt/awscam/camera/installed/bin/mxuvc --ch 1 gop        15
sudo /opt/awscam/camera/installed/bin/mxuvc --ch 1 bitrate    2000000
```

### Streaming Service Deployment

In practice, continuous streaming should be deployed as a system service to start on boot as follows.
The user may be specified and the stream name is automatically captured from the device serial number.
The service deployment should be done on the device.

```sh
make deploy SVC=deeplens
```

To manually control the service:
```sh
make [start | stop | restart | status | log]
```

### KVS Programming APIs for NUUO

See examples in `tests/` for the API usage.

### KVS Programming APIs for DeepLens

The Python binding wraps and simplifies the KVS SDK APIs for streaming.
The following code segment shows the setup to upload a video stream for 60s:

```py
from ws.streaming import Producer
streamer = Producer(resolution=720, fps=15, gop=15, bitrate=2000000)
streamer.connect('MyKVStream')
streamer.upload(60)
streamer.disconnect()
```

## Local Development

To utilize GPUs and compile CUDA modules, additional GPU packages are necessary:

- `cudatoolkit` as a dependency of `pytorch` should have been installed
- `cudatoolkit-dev` requires extra space >=16GB for installation

To contribute to this project, follow the development flow:

1. Fork this repo in the beginning
2. Uninstall WS/ML through `conda remove --force ml ws`
3. Install/Build dependencies: `conda install -c necla-ml ml kvs-producer-sdk`
4. Switch to the `dev` branch for development and testing followed by merge back to `main`
    
    ```
    make pull      # Pull submodules recursively
    make dev-setup # Switch to dev branch and build the package for local installation
    git commit ... # Check in modified files
    git push       # Push to the dev branch on the repo
    make merge     # Merge back to the main branch and make a pull request afterwards
    ```
