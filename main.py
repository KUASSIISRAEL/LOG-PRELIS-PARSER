import copy
import csv
import io
import os
import pathlib
import re
import json
from datetime import *
from typing import Dict, List, Tuple

import advertools as adv
import numpy as np
import pandas as pd
from dateutil.easter import *
from dateutil.parser import *
from dateutil.relativedelta import *
from dateutil.rrule import *

from constants import (ANNULATION_REGEX, ANNULATIONS_REGEX, LOG_PATH_DIR,
                       PAN_REGEX, PRELIS_PATH_DIR, RESPONSE_200,
                       VIREMENT_REGEX, VIREMENTS_REGEX)


class PrelisFactory:
    def __init__(self, filename: str):
        self._filename = filename
        self._lines = None
        self._recipe = None

    @property
    def filename(self):
        return self._filename

    @filename.setter
    def filename(self, new_filename: str):
        self._filename = new_filename

    @property
    def recipe(self):
        return self._recipe

    @recipe.setter
    def recipe(self, new_recipe: List):
        self._recipe = new_recipe

    @property
    def lines(self):
        return self._lines

    @lines.setter
    def lines(self, new_lines: List):
        self._lines = new_lines

    def readFile(self) -> List[Tuple]:
        filepath = pathlib.Path(__file__).parent
        filepath = filepath / f'{PRELIS_PATH_DIR}/{self.filename}'
        with open(filepath) as file:
            self.lines = file.readlines()
            for index, line in enumerate(self.lines):
                line = line.replace('\n', '')
                line = line.replace('  ', '')
                self.lines[index] = tuple(line.split(' '))
            file.close()
        return self.lines

    def extractDate(self) -> List[Dict]:
        self.recipe = {}
        for index, (pan, toChange) in enumerate(self.lines):
            date = pan[2:12]
            self.recipe[str(pan[-4:])] = {
                'ID': index,
                'pan': pan,
            }
            self.recipe[str(pan[-4:])].update({
                'datetime': str(datetime(
                    int(f"20{date[4:6]}"),
                    int(date[2:4]),
                    int(date[0:2]),
                    int(date[6:8]),
                    int(date[8:10]),
                ))
            })
        return self.recipe

    def treat(self):
        datas = self.readFile()
        datas = self.extractDate()
        return datas


class LogFactory:
    def __init__(self, filename: str):
        self._filename = filename
        self._lines = []
        self._recipe = None

    @property
    def filename(self):
        return self._filename

    @filename.setter
    def filename(self, new_filename: str):
        self._filename = new_filename

    @property
    def recipe(self):
        return self._recipe

    @recipe.setter
    def recipe(self, new_recipe: List):
        self._recipe = new_recipe

    @property
    def lines(self):
        return self._lines

    @lines.setter
    def lines(self, new_lines: List):
        self._lines = new_lines

    def readFile(self) -> List[str]:
        parent = pathlib.Path(__file__).parent
        filepath = parent / f'{LOG_PATH_DIR}/{self.filename}'
        with open(filepath, encoding="Latin-1") as file:
            string = file.read()
            pattern = re.compile(r'\s+'.join(ANNULATIONS_REGEX))
            rollbacks = re.findall(pattern, string)
            pattern = re.compile(r'\s+'.join(VIREMENTS_REGEX))
            transfers = re.findall(pattern, string)
            self.lines = rollbacks + transfers
            file.close()
        return self.lines

    def build(self) -> List[dict]:
        datetime, response, pan = None, None, None
        for index, line in enumerate(self.lines):
            spliter = line.split('\n')
            for item in spliter:
                pattern = re.compile(PAN_REGEX)
                matcher = pattern.findall(item)
                if len(matcher) > 0:
                    pan = matcher[0]
                    pan = pan.replace('pan =', '')
                    pan = pan.strip()

                pattern = re.compile(ANNULATION_REGEX)
                matcher = pattern.findall(item)
                if len(matcher) > 0:
                    response = matcher[0]
                    response = json.loads(response)

                pattern = re.compile(VIREMENT_REGEX)
                matcher = pattern.findall(item)
                if len(matcher) > 0:
                    response = matcher[0]
                    response = json.loads(response)

                pattern = re.compile(RESPONSE_200)
                matcher = pattern.findall(item)
                if len(matcher) > 0:
                    replaces = "INFO Response code 200"
                    str_date = matcher[0].replace(replaces, '')
                    datetime = parse(str_date)

            self.lines[index] = (
                pan,
                pan[-4:],
                str_date,
                response
            )
        return self.lines

    def treat(self):
        datas = self.readFile()
        datas = self.build()
        return datas


def main():
    datas = []
    prelis = PrelisFactory('PRELIS_21252')
    prelis = prelis.treat()
    logs = LogFactory('midemv-2021-09-08.log')
    logs = logs.treat()
    for item in logs:
        datas.append({
            'logs': {
                'pan': item[0],
                'transNumber': item[3].get('transactionNumber'),
                'datetime': item[2]
            },
            'prelis': prelis.get(item[1])
        })
    print(json.dumps(datas))


if __name__ == '__main__':
    main()
