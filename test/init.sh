#!/bin/bash

virtualenv -p python3 venv

pip install pymysql
pip install requests

source venv/bin/activate