import copy
import csv
import io
import os
import pathlib
import re
from datetime import *
from typing import Dict, List, Tuple

import advertools as adv
import numpy as np
import pandas as pd
from dateutil.easter import *
from dateutil.parser import *
from dateutil.relativedelta import *
from dateutil.rrule import *

from constants import ANNULATION_REGEX, LOG_PATH_DIR, VIREMENT_REGEX


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
        self.recipe = copy.deepcopy(self.lines)
        for index, (pan, toChange) in enumerate(self.recipe):
            date = pan[2:12]
            self.recipe[index] = {
                'ID': index,
                'pan': pan,
                'toChange': toChange,
                'fourLastNumbers': pan[-4:],
                'dateObject': {
                    'litteral': date,
                    'days': date[0:2],
                    'months': date[2:4],
                    'years': date[4:6],
                    'hours': date[6:8],
                    'minutes': date[8:10],
                },
            }
            date = self.recipe[index].get('dateObject')
            self.recipe[index].update({
                'datetime': datetime(
                    int(f"20{date.get('years')}"),
                    int(date.get('months')),
                    int(date.get('days')),
                    int(date.get('hours')),
                    int(date.get('minutes')),
                )
            })
            self.recipe[index].update({
                'isoDatetime': datetime(
                    int(f"20{date.get('years')}"),
                    int(date.get('months')),
                    int(date.get('days')),
                    int(date.get('hours')),
                    int(date.get('minutes')),
                ).isocalendar()
            })
        return self.recipe

    def build(self):
        datas = self.readFile()
        datas = self.extractDate()
        return datas


class LogFactory:
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

    def readFile(self):
        parent = pathlib.Path(__file__).parent
        filepath = parent / f'{LOG_PATH_DIR}/{self.filename}'
        with open(filepath, encoding="Latin-1") as file:
            string = file.read()
            pattern = re.compile(r'\s+'.join(ANNULATION_REGEX))
            rollbacks = re.findall(pattern, string)
            pattern = re.compile(r'\s+'.join(VIREMENT_REGEX))
            transfers = re.findall(pattern, string)
            self.lines = rollbacks.extend(transfers)
            file.close()
        return self.lines


def main():
    instance = LogFactory('midemv-2021-09-08.log')
    datas = instance.readFile()
    print(datas)


if __name__ == '__main__':
    main()
