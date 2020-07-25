import argparse
import asyncio
import copy
import logging
import os
import re
import pandas as pd
import yaml
from collections import defaultdict
from datetime import datetime, timedelta
from measuring_ci.costs import fetch_all_worker_costs, worker_unit_cost
from taskhuddler.aio.graph import TaskGraph

LOG_LEVEL = logging.INFO

# AWS artisinal log handling, they've already set up a handler by the time we get here
log = logging.getLogger()
log.setLevel(LOG_LEVEL)
# some modules are very chatty
logging.getLogger("taskcluster").setLevel(logging.INFO)
logging.getLogger("aiohttp").setLevel(logging.INFO)



def taskgraph_cost_by_kinds(graph, worker_costs):
    """Calculate the cost of a taskgraph."""

    start_date = graph.earliest_start_time

    # total_wall_time_buckets = {k:defaultdict(timedelta) for k in graph.kinds}
    # final_task_wall_time_buckets = {k:defaultdict(timedelta) for k in graph.kinds}
    total_wall_time_buckets = dict()
    final_task_wall_time_buckets = dict()

    for task in graph.tasks():
        worker = task.json['status']['workerType']
        kind = task.kind
        name = task.name
        if not name.startswith('test'):
            continue
        name = re.sub('-\d+$', '', name)
        name = re.sub('-\d+-', '', name)
        # name = name.split('/')[0]
        if name not in total_wall_time_buckets:
            total_wall_time_buckets[name] = defaultdict(timedelta)
            final_task_wall_time_buckets[name] = defaultdict(timedelta)
        total_wall_time_buckets[name][worker] += sum(task.run_durations(), timedelta(0))

        if task.completed:
            final_task_wall_time_buckets[name][worker] += task.resolved - task.started

    total_cost = 0.0
    final_task_costs = 0.0
    kind_df = pd.DataFrame(columns=['name', 'cost', 'time'])
    known_workers = worker_costs['worker_type'].unique()
    for kind in total_wall_time_buckets:
        kind_cost = 0.0
        kind_hours = timedelta(0)
        for worker in total_wall_time_buckets[kind]:
            if worker not in known_workers:
                continue
            hours = total_wall_time_buckets[kind][worker].total_seconds() / (60 * 60)
            unit_cost = worker_unit_cost(worker_costs, worker_type=worker, date=start_date)
            cost = unit_cost * hours
            total_cost += cost
            kind_cost += cost
            kind_hours += total_wall_time_buckets[kind][worker]

            hours = final_task_wall_time_buckets[kind][worker].total_seconds() / (60 * 60)
            # unit_cost = worker_unit_cost(worker_costs, worker_type=bucket, date=start_date)
            cost = unit_cost * hours
            final_task_costs += cost
        kind_df = kind_df.append({'name': kind, 'cost': kind_cost, 'time': kind_hours}, ignore_index=True)

    print(kind_df)
    kind_df.to_csv('names_longer_{}.csv'.format(graph.groupid))
    return total_cost, final_task_costs


def parse_args():
    """Extract arguments."""
    parser = argparse.ArgumentParser(description="CI Costs")
    parser.add_argument('--config', type=str, default='oneoff.yml')
    parser.add_argument('--graph', type=str)
    return parser.parse_args()


async def scan_releases(config):
    """Scan recent history for complete task graphs."""
    config = copy.deepcopy(config)
    os.environ['TC_CACHE_DIR'] = config['TC_CACHE_DIR']
    cost_dataframe_columns = [
        'product', 'groupid',
        'graph_date', 'category',
        'phase', 'version', 'build_number',
        'totalcost', 'idealcost', 'taskcount',
    ]

    graph_id = config['graph']

    taskgraph = await TaskGraph(graph_id)

    costs = list()

    log.info('Calculating costs')
    worker_costs = fetch_all_worker_costs(
        tc_csv_filename=config['costs_csv_file'],
        scriptworker_csv_filename=config.get('costs_scriptworker_csv_file'),
    )

    kinds = [t.kind for t in taskgraph.tasks()]
    from collections import Counter
    print(Counter(kinds))
    full_cost, final_runs_cost = taskgraph_cost_by_kinds(taskgraph, worker_costs)
    print(full_cost)
    print(taskgraph.total_compute_time())


async def main(args):
    """Main program."""
    with open(args['config'], 'r') as cfg:
        config = yaml.load(cfg)
    config['backfill_count'] = args.get('backfill_count', None)
    config['graph'] = args.get('graph')

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
