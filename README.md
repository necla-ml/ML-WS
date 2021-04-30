# Streaming Web Service

This package conatains a KVS producer binding in Python to upload H.264 ES from DeepLens cameras and NUUO NVRs.
This the back end code base to interface with devices, VMS and AWS/KVS.  
Production code must not hard-code AWS credentials but use execution roles instead.

## Installation

Clone the repo on `GitLab.com`:

```sh
git clone https://gitlab.com/geteigen/aws.git ~/projects/AWS
cd ~/projects/AWS
```

Install system dependencis and base conda environment for streaming service.

```sh
make dep-linux conda 
make conda
```

Restart the shell to create and activate conda environment `ws37`:

```sh
conda create -n ws37 python=3.7
conda activate ws37
conda install -c necla-ml ml-ws
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

## Streaming from Amazon DeepLens

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

## KVS Programming APIs for NUUO

See examples in `tests/` for the API usage.

## KVS Programming APIs for DeepLens

The Python binding wraps and simplifies the KVS SDK APIs for streaming.
The following code segment shows the setup to upload a video stream for 60s:

```py
from ws.streaming import Producer
streamer = Producer(resolution=720, fps=15, gop=15, bitrate=2000000)
streamer.connect('MyKVStream')
streamer.upload(60)
streamer.disconnect()
```