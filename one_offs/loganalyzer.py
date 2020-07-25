"""
Given a list of log files for 'test' kinds,
examine how long it takes to reach various milestones
by looking for keywords.
"""

from datetime import timedelta
from dateutil.parser import parse
import glob
import re
import os
import pandas as pd


TERMS = {
    '=== Task Starting ===': 'task_start_label',
    'SUITE-START': 'suite_start',
    'SUITE-END': 'suite_end',
    '=== Task Finished ===': 'task_end_label',
}
columns = [
    'task_id',
    'start_timestamp',
    'task_start_label',
    'suite_start',
    'suite_end',
    'task_end_label',
    'end_timestamp'
]

last_date = "2019-01-01"
first_timestamp = None
offset = None


def extract_time(line):
    global offset, first_timestamp
    base_time = line.split()[0]
    # account for the change in days
    full_timestamp = "{}T{}".format(last_date, base_time)
    ts = parse(full_timestamp)
    if first_timestamp and offset is None:
        if ts < first_timestamp:
            offset = timedelta(seconds=(first_timestamp - ts).total_seconds())
        else:
            offset = timedelta(seconds=0)
    return ts + offset


TC_LINE = re.compile(r'^\[taskcluster (\d+-\d+-\d+[T ]\d+:\d+:\d+)')
BARE_LINE = re.compile(r'^\d+:\d+:\d+')


def extract_taskcluster_timestamp(line):
    global last_date, first_timestamp
    timestamp_str = TC_LINE.match(line).groups()[0]
    if 'T' in timestamp_str:
        last_date = timestamp_str.split('T')[0]
    else:
        last_date = timestamp_str.split(' ')[0]
    if not first_timestamp:
        first_timestamp = parse(timestamp_str)
    return parse(timestamp_str)


def extract_timestamp(line, previous=None):
    if TC_LINE.match(line):
        return extract_taskcluster_timestamp(line)
    elif BARE_LINE.match(line):
        return extract_time(line)
    else:
        return previous


def analyze_logfile(filename):
    print(filename)
    global last_date, offset, first_timestamp
    last_date = None
    offset = None
    first_timestamp = None
    task_id = filename.split(os.path.sep)[-1].replace('.log','')
    data = dict()
    data['task_id'] = task_id
    timestamp = None
    with open(filename) as f:
        for line in f:
            timestamp = extract_timestamp(line, previous=timestamp)
            if len(line.strip()) == 0:
                continue
            for term in TERMS.keys():
                if term in line:
                    data[TERMS[term]] = timestamp
            if 'start_timestamp' not in data:
                data['start_timestamp'] = timestamp
        data['end_timestamp'] = timestamp

    if 'suite_start' not in data:
        return

    return pd.DataFrame(
        [[data[k] for k in columns]],
        columns=columns)


def main():
    df = pd.DataFrame()
    with open('tests.3', 'r') as f:
        testfiles = [t.strip() for t in f.readlines()]
    for filename in testfiles:
        # results = analyze_logfile('./A567usqlTIy_xGN3V7BQUA.log')
        results = analyze_logfile(filename)
        if results is not None:
            print(results)
            df = df.append(results)

    print(df)
    df.to_csv('results3.csv')

    # df2 = pd.DataFrame()
    # df['setup_time'] = df['task_start_label'] - df['start_timestamp']


if __name__ == "__main__":
    main()
