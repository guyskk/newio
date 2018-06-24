#! /bin/bash
set -ex

pip install -r tools/requires-dev.txt
pip install -r tools/requires-newio.txt
pip install -r tools/requires-newio-kernel.txt

pre-commit install
