from pathlib import Path
from ml.utils import Config

ASSETS = Path(__file__).parent / 'assets'

assets = Config()
assets.bitstream_workaround = Config(
    path= ASSETS / 'bitstream-workaround.264',
    desc="NALU ending with a trailing zero byte requires workaround for KVS",   
)
assets.bitstream_short = Config(
    path= ASSETS / 'store720p-short.264',
    desc="Short H.264 bitstream for loopback",   
)
assets.video_mp4 = Config(
    path= ASSETS / 'store720p.mp4',
    desc="H.264 video in MP4",   
)