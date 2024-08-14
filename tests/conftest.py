import os
import pytest
from main import set_log


@pytest.fixture(scope='session', autouse=True)
def ensure_correct_directory():
    """
    Checks that tests are started in the correct directory.

    :return: None
    """
    main_file = 'main.py'
    if not os.path.exists(main_file):
        os.chdir('..')
    assert os.path.exists(main_file)


@pytest.fixture(scope='session', autouse=True)
def turn_on_logs():
    set_log()
