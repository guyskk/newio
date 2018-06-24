import os


def test_build():
    assert os.system('inv build') == 0
    assert os.system('pip install dist/*') == 0
