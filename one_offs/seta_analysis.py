import argparse
import asyncio
import copy
import logging
import os
from collections import defaultdict
from datetime import datetime, timedelta

import pandas as pd
import re
import json

from measuring_ci.pushlog import scan_pushlog
from taskhuddler.aio.graph import TaskGraph
import taskcluster

LOG_LEVEL = logging.INFO

# AWS artisinal log handling, they've already set up a handler by the time we get here
log = logging.getLogger()
log.setLevel(LOG_LEVEL)
# some modules are very chatty
logging.getLogger("taskcluster").setLevel(logging.INFO)
logging.getLogger("aiohttp").setLevel(logging.INFO)



async def scan_project():
    """Scan a project's recent history for complete task graphs."""
    pushlog_url = 'https://hg.mozilla.org/{project}/json-pushes?version=2'
    pushes = await scan_pushlog(pushlog_url,
                                project='integration/autoland',
                                product='firefox',
                                starting_push=74500)
                                
    queue = taskcluster.Queue(options={'rootUrl':'https://taskcluster.net/'})

    removals_re = re.compile(r'(\d+) tasks by ([\w-]+)[,\s]')


    data = defaultdict(dict)

    for project in pushes:
        # output_fh = open('optimizer-results-{}.txt'.format(project.replace('/', '_')), 'w')
        for push in pushes[project]:
            taskgraph = pushes[project][push]['taskgraph']
            try:
                logfile_response = queue.getLatestArtifact(taskgraph, 'public/logs/live_backing.log')
            except:
                continue
            logfile = logfile_response['response'].content.decode("utf-8") 
            tests_created = 0
            for line in logfile.splitlines():
                if 'during optimization' in line and 'Replaced' not in line:
                    tests = re.findall(removals_re, line)
                elif 'Creating task with taskId' in line and 'for test-' in line:
                    tests_created += 1 

            for value, key in tests:
                data[key][taskgraph] = value

            data['tests-created'][taskgraph] = tests_created
            # tests.append((tests_created, 'tests-created'))
            total_tests = sum(int(t[0]) for t in tests) + tests_created
            data['total-tests'][taskgraph] = total_tests

            # tests.append((total_tests,'total-tests'))
            data['tests-created-percentage'][taskgraph] = "{0:2.2f}%".format(100*tests_created/total_tests)
            # tests.append(("{0:2.2f}%".format(100*tests_created/total_tests),'tests-created-percentage'))
            # output_fh.write(f"{taskgraph}: {tests}\n")
                
        df = pd.DataFrame(data)    
        df.fillna(0, inplace=True)
        print(df)
        project = project.replace('/', '_')
        df.to_csv(f'{project}.csv')


async def main():
    await scan_project()


def lambda_handler():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())


if __name__ == '__main__':
    logging.basicConfig(level=LOG_LEVEL)
    # Use command-line arguments instead of json blob if not running in AWS Lambda
    lambda_handler()
