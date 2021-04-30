import os
import sys
import time
import errno
import shlex
import subprocess
from ml import av, logging
from ml.shutil import run as sh
from .avsource import AVSource, openAV

def fb_mpd_url(url, *args, file_name='video.h264', **kwargs):
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
    end = kwargs.pop('end', None)
    user = kwargs.pop('user', None)
    passwd = kwargs.pop('passwd', None)
    try:
        # video is live --> get mpd url and stream
        if user is None:
            res = sh(f"youtube-dl -f best -g {url}").split('\n')[0]
        else:
            res = sh(f"youtube-dl --username {user} --password {passwd} -f best -g {url}").split('\n')[0]
    except Exception as e:
        logging.info(f"Failed to retrieve DASH MPD url: {e}")
    return res

class FBSource(AVSource):
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
                self.path = fb_mpd_url(self.url, *args, **kwargs)
            session = openAV(self.path, *args, **kwargs)
        except Exception as e:
            logging.error(f"Failed to open {self.url}: {e}")
            raise e
        else:
            return session