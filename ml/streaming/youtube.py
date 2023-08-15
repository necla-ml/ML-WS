import os
import shlex
import subprocess

from ml import logging

from .avsource import AVSource, openAV

def yt_hls_url(url, *args, file_name='video.h264', **kwargs):
    """
    Get hls url if live stream else download video and transcode
    params: 
        url - youtube url to download video from
        start - start video from this timestamp(00:00:15)
        end - end video after this timestamp(00:00:10)
    Returns:
        video path or url(str)
    """
    res = None
    start = kwargs.pop('start', None)
    # NOTE: enforce 5 min limit on non-live youtube videos
    end = kwargs.pop('end', None)
    if not end:
        end = '00:05:00'
    try:
        # video is live --> get hls url and stream
        res = subprocess.run(['youtube-dl', '-f', '95', '-g', url], stdout=subprocess.PIPE) \
            .stdout.decode('utf-8') \
            .strip()
        if not res:
            logging.warning(f"video is not live --> transcode to h264 and stream")
            url = subprocess.run(['youtube-dl','-f', 'best', '-g', url], stdout=subprocess.PIPE).stdout.decode('utf-8').strip()
            cmd = f'ffmpeg '
            if start:
                cmd += f'-ss {start} '
            cmd += f'-i {url} ' 
            if end:
                cmd += f'-t {end} '
            cmd += f'-an \
                -s 1280x720 \
                -g 15 \
                -r 15 \
                -b 2M \
                -vcodec h264 \
                -bf 0 \
                -bsf h264_mp4toannexb \
                -y {file_name}'

            cmd = shlex.split(cmd)
            output = subprocess.call(cmd)
            res = os.path.abspath(file_name)
    except Exception as e:
        # TODO: handle transcoding error with proper errno key
        #sys.exit(errno.)
        logging.info(e)

    return res

class YTSource(AVSource):
    def __init__(self, *args, **kwargs):
        '''Single streaming session at a time.

        Args:
            url: must be live stream e.g. https://www.youtube.com/watch?v=1icACWHoRTo | https://youtu.be/1icACWHoRTo
        '''
        self.url = args[0]
        self.path = None
        
    def open(self, *args, **kwargs):
        try:
            if not self.path:
                self.path = yt_hls_url(self.url, *args, **kwargs)
            session = openAV(self.path, *args, **kwargs)
        except Exception as e:
            logging.error(f"Failed to open {self.url}: {e}")
            raise e
        else:
            return session