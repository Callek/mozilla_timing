import argparse
import asyncio
import copy
import logging
import os

import pandas as pd
import yaml

from measuring_ci.costs import taskgraph_cost, worker_unit_cost, fetch_all_worker_costs
from measuring_ci.shipit import fetch_shipit_taskgraph_ids
from taskhuddler.aio.graph import TaskGraph

LOG_LEVEL = logging.INFO

# AWS artisinal log handling, they've already set up a handler by the time we get here
log = logging.getLogger()
log.setLevel(LOG_LEVEL)
# some modules are very chatty
logging.getLogger("taskcluster").setLevel(logging.INFO)
logging.getLogger("aiohttp").setLevel(logging.INFO)


from collections import defaultdict
from datetime import timedelta

import pandas as pd


def parse_args():
    """Extract arguments."""
    parser = argparse.ArgumentParser(description="CI Costs")
    parser.add_argument('--config', type=str, default='oneoff.yml')
    return parser.parse_args()


async def _semaphore_wrapper(action, args, semaphore):
    """Wrap an async function with semaphores."""
    async with semaphore:
        return await action(*args)


def categorize_version(product, version):
    """Return a descriptive string of the release type.

    Perhaps this should be in mozilla_version
    """
    if product == 'devedition':
        return product
    elif 'b' in version:
        return 'beta'
    elif 'a' in version:
        return 'nightly'
    elif 'esr' in version:
        return 'esr'
    return 'release'


async def scan_releases(config):
    """Scan recent history for complete task graphs."""
    config = copy.deepcopy(config)
    cost_dataframe_columns = [
        'product', 'groupid',
        'graph_date', 'category',
        'phase', 'version', 'build_number',
        'totalcost', 'idealcost', 'taskcount',
    ]

    with open('seta_unaffected_graphs.txt', 'r') as f:
        taskgraph_ids = [l.strip() for l in f.readlines()]
        taskgraph_ids = taskgraph_ids[:10]
    # taskgraph_ids = ['fzZLIZGPRWS7Fc-PGlR1Rw']
    log.info("Found %d taskgraph IDs", len(taskgraph_ids))

    tasks = list()
    semaphore = asyncio.Semaphore(10)

    for graph_id in taskgraph_ids:
        tasks.append(asyncio.ensure_future(
            _semaphore_wrapper(
                TaskGraph,
                args=(graph_id,),
                semaphore=semaphore,
            )))

    log.info('Gathering %d task graphs', len(tasks))
    taskgraphs = await asyncio.gather(*tasks)

    costs = list()

    log.info('Calculating costs')
    worker_costs = fetch_all_worker_costs(
        tc_csv_filename=config['costs_csv_file'],
        scriptworker_csv_filename=config.get('costs_scriptworker_csv_file'),
    )
    for graph in taskgraphs:
        full_cost, final_runs_cost = taskgraph_cost(graph, worker_costs)
        print("{} cost: ${}, ran for {}".format(graph.groupid, full_cost, graph.total_compute_time()))


async def main(args):
    """Main program."""
    with open(args['config'], 'r') as cfg:
        config = yaml.load(cfg)
    config['backfill_count'] = args.get('backfill_count', None)

    await scan_releases(config)


def lambda_handler(args, context):
    """AWS Lambda entry point."""
    assert context  # not currently used
    if 'config' not in args:
        args['config'] = 'oneoff.yml'
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(args))


if __name__ == '__main__':
    logging.basicConfig(level=LOG_LEVEL)
    # Use command-line arguments instead of json blob if not running in AWS Lambda
    lambda_handler(vars(parse_args()), {'dummy': 1})
