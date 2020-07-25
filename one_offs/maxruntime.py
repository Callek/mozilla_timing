import asyncio
import logging
import os
from datetime import timedelta

import dateutil
import pandas as pd

from taskhuddler.aio.graph import TaskGraph

LOG_LEVEL = logging.INFO

# AWS artisinal log handling, they've already set up a handler by the time we get here
log = logging.getLogger()
log.setLevel(LOG_LEVEL)
# some modules are very chatty
logging.getLogger("taskcluster").setLevel(logging.INFO)
logging.getLogger("aiohttp").setLevel(logging.INFO)


columns = [
    'name',
    'task_id',
    'state',
    'worker_type',
    'scheduled',
    'started',
    'resolved',
    'max_run_time',
]


async def analyze_runtimes(graphid, semaphore):
    """Wrap an async function with semaphores."""
    cache_file = './runtimes.out/{}.csv'.format(graphid)

    async with semaphore:
        print(graphid)
        if os.path.exists(cache_file):
            return pd.read_csv(cache_file)
        graph = await TaskGraph(graphid)
        results = list()
        for task in graph.tasks():
            max_run_time = task.json['task']['payload'].get('maxRunTime')
            if not max_run_time:
                print(task, "has no max run time")
                continue
            max_run_time = timedelta(seconds=int(max_run_time))
            for run in task.json['status'].get('runs', list()):
                started = run.get('started')
                resolved = run.get('resolved')
                scheduled = run.get('scheduled')
                if started and resolved and scheduled:
                    resolved = dateutil.parser.parse(resolved)
                    started = dateutil.parser.parse(started)
                    scheduled = dateutil.parser.parse(scheduled)
                else:
                    continue
                results.append([
                    task.name,
                    task.taskid,
                    run['state'],
                    task.json['status']['workerType'],
                    scheduled,
                    started,
                    resolved,
                    max_run_time,
                ])
    df = pd.DataFrame(results, columns=columns)
    df.to_csv(cache_file, index=False)
    return df


async def scan_releases():
    """Scan recent history for complete task graphs."""

    with open('taskgraphs_autoland', 'r') as f:
        taskgraph_ids = [l.strip() for l in f.readlines()]
        # taskgraph_ids = taskgraph_ids[:10]
    # taskgraph_ids = ['fzZLIZGPRWS7Fc-PGlR1Rw']
    log.info("Found %d taskgraph IDs", len(taskgraph_ids))

    tasks = list()
    semaphore = asyncio.Semaphore(10)

    for graph_id in taskgraph_ids:
        tasks.append(asyncio.ensure_future(
            analyze_runtimes(
                graph_id,
                semaphore=semaphore,
            )))

    log.info('Gathering %d task graphs', len(tasks))
    task_details = await asyncio.gather(*tasks)

    # df = pd.DataFrame(columns=columns)
    df = pd.concat(task_details)

    print(df)
    df.to_csv('task_run_details.csv')


def main():
    """AWS Lambda entry point."""
    os.environ['TC_CACHE_DIR'] = '../taskgraph_cache/'
    loop = asyncio.get_event_loop()
    loop.run_until_complete(scan_releases())


if __name__ == '__main__':
    logging.basicConfig(level=LOG_LEVEL)
    main()
