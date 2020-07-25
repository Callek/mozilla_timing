"""
Download all the logfiles for tests from the given task graph.

Used with loganalyzer.py to get timing information.

"""

import os
from taskhuddler import TaskGraph
from taskcluster.queue import Queue

os.environ['TC_CACHE_DIR'] = 's3://mozilla-releng-metrics/taskgraph_cache/'
graph_id = 'dxoowHVSQJOOu50nkCBvCQ'
graph = TaskGraph(graph_id)
q = Queue({'rootUrl': 'https://taskcluster.net'})
log_artifact = 'public/logs/live_backing.log'

for task in graph.tasks():
    if 'test' not in task.name:
        continue
    try:
        r = q.getLatestArtifact(task.taskid, log_artifact)
    except:
        continue

    with open("logfiles_{}/{}.log".format(graph_id, task.taskid), 'w') as f:
        f.write(r['response'].content.decode())
    print(task.taskid)
