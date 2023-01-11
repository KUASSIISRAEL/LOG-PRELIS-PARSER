import copy
import pathlib
import re
import os
import sys
from datetime import *
from typing import Dict, List, Tuple

import advertools as adv
import numpy as np
import pandas as pd
from dateutil.easter import *
from dateutil.parser import *
from dateutil.relativedelta import *
from dateutil.rrule import *

from constants import LOG_PATH_DIR, PRELIS_PATH_DIR


class PrelisFactory:
    def readFile(self, filename: str) -> List[Tuple]:
        filepath = pathlib.Path(__file__).parent
        filepath = filepath / f'{PRELIS_PATH_DIR}/{filename}'
        with open(filepath) as file:
            lines = file.readlines()
            for index, line in enumerate(lines):
                line = line.replace('\n', '')
                line = line.replace('  ', '')
                lines[index] = tuple(line.split(' '))
            file.close()
        return lines

    def extractDate(self, datas: list) -> List[Dict]:
        recipient = copy.deepcopy(datas)
        for index, (pan, toChange) in enumerate(recipient):
            date = pan[2:12]
            recipient[index] = {
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
            date = recipient[index].get('dateObject')
            recipient[index].update({
                'datetime': datetime(
                    int(f"20{date.get('years')}"),
                    int(date.get('months')),
                    int(date.get('days')),
                    int(date.get('hours')),
                    int(date.get('minutes')),
                )
            })
            recipient[index].update({
                'isoDatetime': datetime(
                    int(f"20{date.get('years')}"),
                    int(date.get('months')),
                    int(date.get('days')),
                    int(date.get('hours')),
                    int(date.get('minutes')),
                ).isocalendar()
            })
        return recipient


class LogFactory:
    def readFile(self, filename: str) -> str:
        parent = pathlib.Path(__file__).parent
        filepath = parent / f'{LOG_PATH_DIR}/{filename}'
        adv.logs_to_df(
            log_file=filepath,
            output_file=os.path.join(parent, 'outputs/access_logs.parquet'),
            errors_file=os.path.join(parent, 'outputs/log_errors.csv'),
            log_format='common',
            fields=None
        )
        logs_df = pd.read_parquet('access_logs.parquet')


def main():
    instance = PrelisFactory()
    datas = instance.readFile('PRELIS_21252')
    recipient = instance.extractDate(datas)
    print(recipient)


if __name__ == '__main__':
    instance = LogFactory()
    datas = instance.readFile('midemv-2021-09-08.log')
    print(datas)
