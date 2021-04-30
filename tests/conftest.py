import pytest

def pytest_addoption(parser):
    parser.addoption(
        "--inf", action="store_true", help="Infinite live playback or not"
    )

