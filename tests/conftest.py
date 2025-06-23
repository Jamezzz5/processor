import os
import sys
import pytest
from processor.main import set_log

@pytest.fixture(scope='session', autouse=True)
def ensure_correct_directory():
    """
    Checks that tests are started in the correct directory.

    :return: None
    """
    main_file = 'main.py'
    if not os.path.exists(main_file):
        os.chdir('..')
    processor_dir = 'processor'
    if os.path.exists(processor_dir):
        os.chdir(processor_dir)
    if processor_dir not in sys.path:
        sys.path.insert(0, processor_dir)
    assert os.path.exists(main_file)


@pytest.fixture(scope='session', autouse=True)
def turn_on_logs():
    set_log()
