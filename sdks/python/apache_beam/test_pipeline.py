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

"""Test Pipeline, only used for runnable-on-service tests"""

import argparse

from apache_beam.pipeline import Pipeline
from apache_beam.utils.options import PipelineOptions

class TPipeline(Pipeline):

  def __init__(self, runner=None, options=None, argv=None):
    super(TPipeline, self).__init__(runner, options, argv)
    if self.options is None:
      self.options = self.test_pipeline_options()

  def test_pipeline_options(self):
    """create a pipeline options from command line argument: --test_options"""
    parser = argparse.ArgumentParser()
    parser.add_argument('--test_options',
                       type=str,
                       default='--runner DirectPipelineRunner',
                       action='store',
                       help='only run tests providing service options')
    argv = parser.parse_args()
    options_list = argv.test_options.split()
    return PipelineOptions(options_list)
