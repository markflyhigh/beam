#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Test Pipeline, a wrapper of Pipeline

TPipeline has a functionality to parse options from command line arguments
and build pipeline options. It's designed for tests that needs runner,
like runnable-on-service tests and integration tests.

"""

import argparse

from apache_beam.pipeline import Pipeline
from apache_beam.utils.options import PipelineOptions

class TPipeline(Pipeline):

  def __init__(self, runner=None, argv=None):
    options = self.test_pipeline_options()
    super(TPipeline, self).__init__(runner, options, argv)

  def test_pipeline_options(self):
    """Create a pipeline options from command line argument: --test_options"""
    parser = argparse.ArgumentParser()
    parser.add_argument('--test_options',
                       type=str,
                       action='store',
                       help='only run tests providing service options')
    known, argv = parser.parse_known_args()

    if known.test_options is None:
      return PipelineOptions([])
    else:
      options_list = known.test_options.split()
      return PipelineOptions(options_list)
