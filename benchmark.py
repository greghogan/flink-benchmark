#!/usr/bin/env python

################################################################################
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

import argparse
import collections
import datetime
import heapq
import json
import os
import random
import subprocess

JAR = 'examples/gelly/flink-gelly-examples_2.11-1.4-SNAPSHOT.jar'
SAMPLES = 8
SEED = random.randint(-2**63, (2**63)-1)
WARMUP = 2

parser = argparse.ArgumentParser(description='Benchmark Gelly algorithms with Apache Flink.')
parser.add_argument('log_dir',
                    help='directory to output results (must not exist)')
parser.add_argument('--jar', default=JAR,
                    help='location of the Gelly examples jar (default: {})'.format(JAR))
parser.add_argument('--samples', default=SAMPLES,
                    help='sum the integers (default: {})'.format(SAMPLES))
parser.add_argument('--seed', default=SEED,
                    help='sum the integers (default: random)')
parser.add_argument('--warmup', type=int, default=WARMUP,
                    help='number of warm-up executions per execution (default: {})'.format(WARMUP))

args = parser.parse_args()
os.mkdir(args.log_dir)

TYPES = ['byte', 'nativeByte', 'short', 'nativeShort', 'char', 'nativeChar', 'integer', 'nativeInteger', 'long', 'nativeLong', 'float', 'nativeFloat', 'double', 'nativeDouble', 'string', 'nativeString']
COPYABLE_VALUE_TYPES = ['byte', 'short', 'char', 'integer', 'long', 'float', 'double', 'string']
NON_TRANSFORMABLE_TYPES = ['byte', 'nativeByte', 'short', 'nativeShort', 'char', 'nativeChar', 'integer', 'nativeInteger', 'nativeLong']


class Configuration:
    def __init__(self, algorithm, initial_scale, types, name=None, **parameters):
        self.algorithm = algorithm
        self.initial_scale = initial_scale
        self.types = types
        self.name = name
        self.parameters = parameters


AdamicAdar = Configuration('AdamicAdar', 10, COPYABLE_VALUE_TYPES,
    simplify='undirected', mirror_results=None)

ConnectedComponents = Configuration('ConnectedComponents', 16, NON_TRANSFORMABLE_TYPES,
    simplify='undirected')

ClusteringCoefficientDirected = Configuration('ClusteringCoefficient', 14, COPYABLE_VALUE_TYPES, 'ClusteringCoefficientDirected',
    simplify='directed', order='directed')
ClusteringCoefficientUndirected = Configuration('ClusteringCoefficient', 14, COPYABLE_VALUE_TYPES, 'ClusteringCoefficientUndirected',
    simplify='undirected', order='undirected')

EdgeList = Configuration('EdgeList', 20, NON_TRANSFORMABLE_TYPES)

GraphMetricsDirected = Configuration('GraphMetrics', 16, TYPES, 'GraphMetricsDirected',
    simplify='directed', order='directed')
GraphMetricsUndirected = Configuration('GraphMetrics', 16, TYPES, 'GraphMetricsUndirected',
    simplify='undirected', order='undirected')

HITS = Configuration('HITS', 16, TYPES,
    simplify='directed')

JaccardIndex = Configuration('JaccardIndex', 12, COPYABLE_VALUE_TYPES,
    simplify='undirected', mirror_results=None)

PageRank = Configuration('PageRank', 16, TYPES,
    simplify='directed')

TriangleListingDirected = Configuration('TriangleListing', 14, COPYABLE_VALUE_TYPES, 'TriangleListingDirected',
    simplify='directed', order='directed', permute_results=None)
TriangleListingUndirected = Configuration('TriangleListing', 14, COPYABLE_VALUE_TYPES, 'TriangleListingUndirected',
    simplify='undirected', order='undirected', permute_results=None)


ALGORITHMS = [
    AdamicAdar,
    ConnectedComponents,
    ClusteringCoefficientDirected,
    ClusteringCoefficientUndirected,
    EdgeList,
    GraphMetricsDirected,
    GraphMetricsUndirected,
    HITS,
    JaccardIndex,
    PageRank,
    TriangleListingDirected,
    TriangleListingUndirected
]


class Execution:
    def __init__(self, config, type, seeds):
        self.algorithm = config.algorithm
        self.name = config.name
        self.type = type
        self.seeds = seeds

        self.parameters = []
        for key, value in config.parameters.iteritems():
            if value:
                self.parameters.extend(['--' + key, value])
            else:
                # boolean keys do not have a value
                self.parameters.extend(['--' + key])

        self.running = True
        self.failures = collections.defaultdict(int)

        self.executions = collections.Counter()
        self.low_scale = config.initial_scale
        self.current_scale = self.low_scale

        for i in reversed(range(args.warmup)):
            self.__execute(self.current_scale - 1 - i, False)

    def run(self):
        runtime_ms = self.__execute(self.current_scale, True)

        self.executions.update({self.current_scale: 1})
        if self.executions[self.current_scale] == args.samples:
            self.low_scale += 1
            self.current_scale = self.low_scale
        elif self.executions[self.current_scale] % 2 == 0:
            self.current_scale += 1
        else:
            self.current_scale = self.low_scale

        return runtime_ms

    def __execute(self, scale, write_job_details):
        if self.failures[scale] > 2 or not self.running:
            return

        program = [
            "bin/flink", "run", "-q", args.jar,
            "--algorithm", self.algorithm,
            "--input", "RMatGraph",
                "--type", self.type,
                "--scale", str(scale),
                "--seed", seeds[self.executions[scale]],
            "--output", "hash"]

        if self.name:
            program.extend(["--__job_name", self.name])

        program.extend(self.parameters)

        if write_job_details:
            details = args.log_dir + "/" + datetime.datetime.now().strftime("%Y%m%d_%H%M%S_%f") + ".json"
            program.extend(["--__job_details_path", details])

        returncode = subprocess.call(program)

        if returncode != 0:
            self.failures[scale] += 1
            print("Failure for algorithm {}, scale {}, returncode = {}".format(self.algorithm, scale, returncode))

            if len(self.failures) > 2:
                self.running = False
                print("Disabling algorithm {}".format(self.algorithm))

            return

        if write_job_details:
            return Execution.__read_runtime_ms(details)

    def __repr__(self):
        return "{} {} {}".format(self.algorithm, self.type, self.current_scale)

    @staticmethod
    def __read_runtime_ms(filename):
        with open(filename, 'r') as file:
            data = json.loads(file.read())
            return data['runtime_ms']

prng = random.Random(SEED)
seeds = [str(prng.randint(-2**63, (2**63)-1)) for i in range(args.samples)]

executions = [(0, Execution(config, type, seeds)) for config in ALGORITHMS for type in config.types]
heapq.heapify(executions)

while len(executions) > 0:
    runtime_ms, execution = heapq.heappop(executions)
    result = execution.run()
    if result:
        runtime_ms += result
        heapq.heappush(executions, (runtime_ms, execution))
