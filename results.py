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

from __future__ import print_function

import argparse
import collections
import json
import os
import re

import locale
locale.setlocale(locale.LC_ALL, '')

parser = argparse.ArgumentParser(description='Summarize results for Gelly algorithms with Apache Flink.')
parser.add_argument('log_dir', nargs='+',
                    help='directory of results')
parser.add_argument('--disable_validation', action='store_true',
                    help='disable validation of accumulators for each algorithm, scale, and seed (default: False)')

args = parser.parse_args()

class ExecutionResults():
    def __init__(self, directory):
        self.directory = directory

        results = collections.defaultdict(list)
        accumulators_cache = {}

        for filename in sorted(os.listdir(directory)):
            with open(directory + "/" + filename, 'r') as file:
                try:
                    data = json.loads(file.read())
                    parameters = data['parameters']

                    name = parameters['__job_name'] if parameters.has_key('__job_name') else parameters['algorithm']

                    results[(name, int(parameters['scale']), parameters['type'])].append(data)
                except:
                    pass

        self.timings = {}

        for alg_scale_type, alg_scale_type_data in results.iteritems():
            algorithm, scale, idType = alg_scale_type

            count = 0
            total_runtime_ms = 0

            for data in alg_scale_type_data:
                count += 1
                total_runtime_ms += data['runtime_ms']

                if not args.disable_validation:
                    seed = data['parameters']['seed']

                    # strip leading AbstractID
                    accumulators = {k[33:] if re.search('^[0-9a-f]{32}-', k) else k:v for k, v in data['accumulators'].iteritems()}

                    fields = (algorithm, scale, seed)
                    if fields in accumulators_cache:
                        prior_accumulators = accumulators_cache[fields]
                        if accumulators != prior_accumulators:
                            raise Exception("accumulators={} does not match accumulators={} for algorithm={}, scale={}, seed={}"
                                .format(accumulators, prior_accumulators, algorithm, scale, seed))
                    else:
                        accumulators_cache[fields] = accumulators

            self.timings[alg_scale_type] = (count, total_runtime_ms / 1000.0 / count)


if __name__ == '__main__':
    results = []
    executions = set()

    for directory in args.log_dir:
        result = ExecutionResults(directory)
        results.append(result)
        for fields in result.timings:
            executions.add(fields)

    last_algorithm = ''
    last_scale = 0

    for fields in sorted(executions):
        algorithm, scale, idType = fields

        if last_algorithm and last_algorithm != algorithm:
            print('')
            print('')
        elif last_scale > 0 and last_scale != scale:
            print('')

        last_algorithm = algorithm
        last_scale = scale

        print('{0}, scale={1}, {2:<13}: runtime='.format(algorithm, scale, idType), end='')

        first_timing = 0

        for idx, result in enumerate(results):
            if fields in result.timings:
                count, total_runtime_ms = result.timings[fields]
                print('{0:>8.3f} ({1})'.format(total_runtime_ms, count), end='')

                if idx == 0:
                    first_timing = total_runtime_ms
                    print(' ' * 3, end='')
                else:
                    if first_timing > 0:
                        print(' ({0:>6.2f}%)   '.format(100*(total_runtime_ms-first_timing)/first_timing), end='')
                    else:
                        print(' ' * 13, end='')
            else:
                if idx == 0:
                    print(' ' * 15, end='')
                else:
                    print(' ' * 25, end='')

        print()
