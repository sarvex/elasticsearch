# Licensed to Elasticsearch under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance  with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on
# an 'AS IS' BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied. See the License for the specific
# language governing permissions and limitations under the License.

import random
import os
import tempfile
import shutil
import subprocess
import time
import argparse
import logging
import sys
import re

from datetime import datetime
try:
  from elasticsearch import Elasticsearch
  from elasticsearch.exceptions import ConnectionError
  from elasticsearch.exceptions import TransportError
except ImportError as e:
  print('Can\'t import elasticsearch please install `sudo pip install elasticsearch`')
  raise e


'''This file executes a basic upgrade test by running a full cluster restart.

The upgrade test starts 2 or more nodes of an old elasticserach version, indexes
a random number of documents into the running nodes and executes a full cluster restart.
After the nodes are recovered a small set of basic checks are executed to ensure all
documents are still searchable and field data can be loaded etc.

NOTE: This script requires the elasticsearch python client `elasticsearch-py` run the following command to install:

  `sudo pip install elasticsearch`

if you are running python3 you need to install the client using pip3. On OSX `pip3` will be included in the Python 3.4
release available on `https://www.python.org/download/`:

  `sudo pip3 install elasticsearch`

See `https://github.com/elasticsearch/elasticsearch-py` for details

In order to run this test two different version of elasticsearch are required. Both need to be unpacked into
the same directory:

```
   $ cd /path/to/elasticsearch/clone
   $ mkdir backwards && cd backwards
   $ wget  https://download.elasticsearch.org/elasticsearch/elasticsearch/elasticsearch-1.3.1.tar.gz
   $ wget  https://download.elasticsearch.org/elasticsearch/elasticsearch/elasticsearch-0.90.13.tar.gz
   $ tar -zxvf elasticsearch-1.3.1.tar.gz && tar -zxvf elasticsearch-0.90.13.tar.gz
   $ cd ..
   $ python dev-tools/upgrade-tests.py --version.backwards 0.90.13 --version.current 1.3.1
```
'''

BLACK_LIST = {'1.2.0' : { 'reason': 'Contains a major bug where routing hashes are not consistent with previous version',
                          'issue': 'https://github.com/elasticsearch/elasticsearch/pull/6393'},
              '1.3.0' : { 'reason': 'Lucene Related bug prevents upgrades from 0.90.7 and some earlier versions ',
                          'issue' : 'https://github.com/elasticsearch/elasticsearch/pull/7055'}}
# sometimes returns True
def rarely():
  return random.randint(0, 10) == 0

# usually returns True
def frequently():
  return not rarely()

# asserts the correctness of the given hits given they are sorted asc
def assert_sort(hits):
  values = [hit['sort'] for hit in hits['hits']['hits']]
  assert values, 'expected non emtpy result'
  val = min(values)
  for x in values:
    assert x >= val, f'{x} >= {val}'
    val = x

# asserts that the cluster health didn't timeout etc.
def assert_health(cluster_health, num_shards, num_replicas):
  assert (cluster_health['timed_out'] == False
          ), f'cluster health timed out {cluster_health}'


# Starts a new elasticsearch node from a released & untared version.
# This node uses unicast discovery with the provided unicast host list and starts
# the nodes with the given data directory. This allows shutting down and starting up
# nodes on the same data dir simulating a full cluster restart.
def start_node(version, data_dir, node_dir, unicast_host_list, tcp_port, http_port):
  es_run_path = os.path.join(node_dir, f'elasticsearch-{version}',
                             'bin/elasticsearch')
  foreground = '-f' if version.startswith('0.90.') else ''
  return subprocess.Popen(
      [
          es_run_path,
          f'-Des.path.data={data_dir}',
          '-Des.cluster.name=upgrade_test',
          f'-Des.discovery.zen.ping.unicast.hosts={unicast_host_list}',
          '-Des.discovery.zen.ping.multicast.enabled=false',
          '-Des.script.disable_dynamic=true',
          f'-Des.transport.tcp.port={tcp_port}',
          f'-Des.http.port={http_port}',
          foreground,
      ],
      stdout=subprocess.PIPE,
      stderr=subprocess.PIPE,
  )


# Indexes the given number of document into the given index
# and randomly runs refresh, optimize and flush commands
def index_documents(es, index_name, type, num_docs):
  logging.info(f'Indexing {num_docs} docs')
  for id in range(0, num_docs):
    es.index(index=index_name, doc_type=type, id=id, body={'string': str(random.randint(0, 100)),
                                                           'long_sort': random.randint(0, 100),
                                                           'double_sort' : float(random.randint(0, 100))})
    if rarely():
      es.indices.refresh(index=index_name)
    if rarely():
      es.indices.flush(index=index_name, force=frequently())
  if rarely():
      es.indices.optimize(index=index_name)
  es.indices.refresh(index=index_name)

# Runs a basic number of assertions including:
#  - document counts
#  - match all search with sort on double / long
#  - Realtime GET operations
# TODO(simonw): we should add stuff like:
#  - dates including sorting
#  - string sorting
#  - docvalues if available
#  - global ordinal if available
def run_basic_asserts(es, index_name, type, num_docs):
  count = es.count(index=index_name)['count']
  assert count == num_docs, 'Expected %r but got %r documents' % (num_docs, count)
  for _ in range(0, num_docs):
    random_doc_id = random.randint(0, num_docs-1)
    doc = es.get(index=index_name, doc_type=type, id=random_doc_id)
    assert doc, f'Expected document for id {random_doc_id} but got {doc}'

  assert_sort(es.search(index=index_name,
                  body={
                    'sort': [
                      {'double_sort': {'order': 'asc'}}
                    ]
                  }))

  assert_sort(es.search(index=index_name,
                  body={
                    'sort': [
                      {'long_sort': {'order': 'asc'}}
                    ]
                  }))


# picks a random version or and entire random version tuple from the directory
# to run the backwards tests against.
def pick_random_upgrade_version(directory, lower_version=None, upper_version=None):
  if lower_version and upper_version:
    return lower_version, upper_version
  assert os.path.isdir(directory), f'No such directory {directory}'
  versions = [
      build_tuple(version) for version in map(
          lambda x: x[len('elasticsearch-'):],
          filter(
              lambda x: re.match(r'^elasticsearch-\d+[.]\d+[.]\d+$', x),
              os.listdir(directory),
          ),
      ) if version not in BLACK_LIST
  ]
  versions.sort()

  if lower_version: # lower version is set - picking a higher one
    versions = filter(lambda x : x > build_tuple(lower_version), versions)
    assert (
        len(versions) >= 1
    ), f'Expected at least 1 higher version than {lower_version} version in {directory} '
    random.shuffle(versions)
    return lower_version, build_version(versions[0])
  if upper_version:
    versions = filter(lambda x : x < build_tuple(upper_version), versions)
    assert (
        len(versions) >= 1
    ), f'Expected at least 1 lower version than {upper_version} version in {directory} '
    random.shuffle(versions)
    return build_version(versions[0]), upper_version
  assert (
      len(versions) >= 2
  ), f'Expected at least 2 different version in {directory} but found {len(versions)}'
  random.shuffle(versions)
  versions = versions[:2]
  versions.sort()
  return build_version(versions[0]), build_version(versions[1])

def build_version(version_tuple):
  return '.'.join([str(x) for x in version_tuple])

def build_tuple(version_string):
  return [int(x) for x in version_string.split('.')]

# returns a new elasticsearch client and ensures the all nodes have joined the cluster
# this method waits at most 30 seconds for all nodes to join
def new_es_instance(num_nodes, http_port, timeout = 30):
  logging.info(f'Waiting for {num_nodes} nodes to join the cluster')
  for _ in range(0, timeout):
    # TODO(simonw): ask Honza if there is a better way to do this?
    try:
      es = Elasticsearch([
      {'host': '127.0.0.1', 'port': http_port + x}
        for x in range(0, num_nodes)])
      es.cluster.health(wait_for_nodes=num_nodes)
      es.count() # can we actually search or do we get a 503? -- anyway retry
      return es
    except (ConnectionError, TransportError):
      pass
    time.sleep(1)
  assert False, f'Timed out waiting for {num_nodes} nodes for {timeout} seconds'

def assert_versions(bwc_version, current_version, node_dir):
  assert [int(x) for x in bwc_version.split('.')] < [
      int(x) for x in current_version.split('.')
  ], f'[{bwc_version}] must be < than [{current_version}]'
  for version in [bwc_version, current_version]:
    assert (
        version not in BLACK_LIST
    ), f"Version {version} is blacklisted - {BLACK_LIST[version]['reason']}, see {BLACK_LIST[version]['issue']}"
    dir = os.path.join(node_dir, f'elasticsearch-{current_version}')
    assert os.path.isdir(
        dir
    ), f'Expected elasticsearch-{version} install directory does not exists: {dir}'

def full_cluster_restart(node_dir, current_version, bwc_version, tcp_port, http_port):
  assert_versions(bwc_version, current_version, node_dir)
  num_nodes = random.randint(2, 3)
  nodes = []
  data_dir = tempfile.mkdtemp()
  logging.info(
      f'Running upgrade test from [{bwc_version}] to [{current_version}] seed: [{seed}] es.path.data: [{data_dir}] es.http.port [{http_port}] es.tcp.port [{tcp_port}]'
  )
  try:
    logging.info(f'Starting {num_nodes} BWC nodes of version {bwc_version}')
    unicast_addresses = ','.join(
        [f'127.0.0.1:{tcp_port + x}' for x in range(0, num_nodes)])
    nodes.extend(
        start_node(
            bwc_version,
            data_dir,
            node_dir,
            unicast_addresses,
            tcp_port + id,
            http_port + id,
        ) for id in range(0, num_nodes))
    es = new_es_instance(num_nodes, http_port)
    es.indices.delete(index='test_index', ignore=404)
    num_shards = random.randint(1, 10)
    num_replicas = random.randint(0, 1)
    logging.info(
        f'Create index with [{num_shards}] shards and [{num_replicas}] replicas'
    )
    es.indices.create(index='test_index', body={
        # TODO(simonw): can we do more here in terms of randomization - seems hard due to all the different version
        'settings': {
            'number_of_shards': num_shards,
            'number_of_replicas': num_replicas
        }
    })
    logging.info('Nodes joined, waiting for green status')
    health = es.cluster.health(wait_for_status='green', wait_for_relocating_shards=0)
    assert_health(health, num_shards, num_replicas)
    num_docs = random.randint(10, 100)
    index_documents(es, 'test_index', 'test_type', num_docs)
    logging.info('Run basic asserts before full cluster restart')
    run_basic_asserts(es, 'test_index', 'test_type', num_docs)
    logging.info('kill bwc nodes -- prepare upgrade')
    for node in nodes:
      node.terminate()

    # now upgrade the nodes and rerun the checks
    tcp_port = tcp_port + len(nodes) # bump up port to make sure we can claim them
    http_port = http_port + len(nodes)
    logging.info(
        f'Full Cluster restart starts upgrading to version [elasticsearch-{current_version}] es.http.port [{http_port}] es.tcp.port [{tcp_port}]'
    )
    nodes = []
    unicast_addresses = ','.join(
        [f'127.0.0.1:{tcp_port + x}' for x in range(0, num_nodes)])
    nodes.extend(
        start_node(
            current_version,
            data_dir,
            node_dir,
            unicast_addresses,
            tcp_port + id,
            http_port + id,
        ) for id in range(0, num_nodes + 1))
    es = new_es_instance(num_nodes+1, http_port)
    logging.info('Nodes joined, waiting for green status')
    health = es.cluster.health(wait_for_status='green', wait_for_relocating_shards=0)
    assert_health(health, num_shards, num_replicas)
    run_basic_asserts(es, 'test_index', 'test_type', num_docs)
    # by running the indexing again we try to catch possible mapping problems after the upgrade
    index_documents(es, 'test_index', 'test_type', num_docs)
    run_basic_asserts(es, 'test_index', 'test_type', num_docs)
    logging.info(
        f"[SUCCESS] - all test passed upgrading from version [{bwc_version}] to version [{current_version}]"
    )
  finally:
    for node in nodes:
      node.terminate()
    time.sleep(1) # wait a second until removing the data dirs to give the nodes a chance to shutdown
    shutil.rmtree(data_dir) # remove the temp data dir

if __name__ == '__main__':
  logging.basicConfig(format='[%(levelname)s] [%(asctime)s] %(message)s', level=logging.INFO,
                      datefmt='%Y-%m-%d %I:%M:%S %p')
  logging.getLogger('elasticsearch').setLevel(logging.ERROR)
  logging.getLogger('urllib3').setLevel(logging.WARN)
  parser = argparse.ArgumentParser(description='Tests Full Cluster Restarts across major version')
  parser.add_argument('--version.backwards', '-b', dest='backwards_version', metavar='V',
                      help='The elasticsearch version to upgrade from')
  parser.add_argument('--version.current', '-c', dest='current_version', metavar='V',
                      help='The elasticsearch version to upgrade to')
  parser.add_argument('--seed', '-s', dest='seed', metavar='N', type=int,
                      help='The random seed to use')
  parser.add_argument('--backwards.dir', '-d', dest='bwc_directory', default='backwards', metavar='dir',
                      help='The directory to the backwards compatibility sources')

  parser.add_argument('--tcp.port', '-p', dest='tcp_port', default=9300, metavar='port', type=int,
                      help='The port to use as the minimum port for TCP communication')
  parser.add_argument('--http.port', '-t', dest='http_port', default=9200, metavar='port', type=int,
                      help='The port to use as the minimum port for HTTP communication')

  parser.set_defaults(bwc_directory='backwards')
  parser.set_defaults(seed=int(time.time()))
  args = parser.parse_args()
  node_dir = args.bwc_directory
  current_version = args.current_version
  bwc_version = args.backwards_version
  seed = args.seed
  random.seed(seed)
  bwc_version, current_version = pick_random_upgrade_version(node_dir, bwc_version, current_version)
  tcp_port = args.tcp_port
  http_port = args.http_port
  try:
    full_cluster_restart(node_dir, current_version, bwc_version, tcp_port, http_port)
  except:
    logging.warn('REPRODUCE WITH: \n\t`python %s --version.backwards %s --version.current %s --seed %s --tcp.port %s --http.port %s`'
                   % (sys.argv[0], bwc_version, current_version, seed, tcp_port, http_port))
    raise
