import sys
from pathlib import Path

from ml import logging

from .avsource import AVSource, openAV
from ..ws.aws import s3
from ..ws.aws import utils

def get_s3_video(url, *args, transcode=False, bucket='eigen-stream-videos', pth='assets/', **kwargs):
    """
    Download video from s3 and transcode
    params: 
        url - s3 key name
        transcode - transcode video to h264
        bucket - s3 key bucket
    Returns:
        video path or url(str)
    """
    # create path if not exist
    key = url.split('s3://')[1]
    pth = Path(pth)
    pth.mkdir(parents=True, exist_ok=True)
    download_path = Path(f'{pth}/{key}')
    # download from s3
    s3.download_key(key=key, pth=download_path, bucket=bucket, reload=True)
    if download_path.exists():
        if transcode:
            # transcode video to h264
            download_path = utils.transcode_to_h264(pth=download_path)
            if not pth:
                logging.error(f'Error transcoding video at: {download_path}')
                sys.exit(1)
    else:
        logging.error(f'Failed downloading key: {key} from bucket: {bucket}')
        sys.exit(1)

    return str(download_path)

class S3Source(AVSource):
    def __init__(self, *args, **kwargs):
        '''Single streaming session at a time.

        Args:
            s3 key name
        '''
        self.url = args[0]
        self.path = None
        
    def open(self, *args, **kwargs):
        try:
            if not self.path:
                transcode = kwargs.pop('transcode', False)
                bucket = kwargs.pop('bucket', 'eigen-stream-videos')
                self.path = get_s3_video(self.url, transcode=transcode, bucket=bucket)
            session = openAV(self.path, **kwargs)
        except Exception as e:
            logging.error(f"Failed to open {self.url}: {e}")
            return None
        else:
            return session
