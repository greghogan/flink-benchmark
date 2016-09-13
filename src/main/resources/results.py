#!/usr/bin/env python

################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

from __future__ import print_function

import collections
import json
import sys

class ExecutionResults():
  def __init__(self, filename):
    self.filename = filename

    results = collections.defaultdict(list)

    with open(filename, 'r') as file:
      for line in file:
        try:
          data = json.loads(line)
          results[(data['algorithm'], data['scale'], data['idType'])].append(data)
        except:
          pass

    self.timings = {}

    for fields, list_of_timings in results.iteritems():
      count = 0
      total_runtime_ms = 0

      for result in results[fields]:
        count += 1
        total_runtime_ms += result['runtime_ms']

      self.timings[fields] = (count, total_runtime_ms / 1000.0 / count)

if __name__ == '__main__':
  results = []
  executions = set()

  for filename in sys.argv[1:]:
    result = ExecutionResults(filename)
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

    print('{0}, scale={1}, {2:<6}: runtime='.format(algorithm, scale, idType), end='')

    first_timing = 0

    for idx, result in enumerate(results):
      if fields in result.timings:
        count, total_runtime_ms = result.timings[fields]
        print('{0:>8.3f} ({1})'.format(total_runtime_ms, count), end='')

        if idx == 0:
          first_timing = total_runtime_ms
          print('   ', end='')
        else:
          if first_timing > 0:
            print(' ({0:>6.2f}%)'.format(100*(total_runtime_ms-first_timing)/first_timing), end='')
          else:
            print('          ', end='')
      else:
        print('               ', end='')

    print('')
