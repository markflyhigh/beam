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

"""Wrapper of Beam runners that's built for running and verifying e2e tests."""
from __future__ import print_function

import time
import logging

from apache_beam.internal import pickler
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import TestOptions
from apache_beam.runners.dataflow.dataflow_runner import DataflowRunner
from apache_beam.runners.runner import PipelineState

log = logging.getLogger(__name__)
__all__ = ['TestDataflowRunner']

DEFAULT_STREAMING_TIMEOUT = 5 * 60


class TestDataflowRunner(DataflowRunner):

  def _is_in_terminate_state(self, job_state):
    return job_state in [
        PipelineState.JOB_STATE_STOPPED, PipelineState.JOB_STATE_DONE,
        PipelineState.JOB_STATE_FAILED, PipelineState.JOB_STATE_CANCELLED,
        PipelineState.JOB_STATE_UPDATED, PipelineState.JOB_STATE_DRAINED]

  def run_pipeline(self, pipeline):
    """Execute test pipeline and verify test matcher"""
    options = pipeline._options.view_as(TestOptions)
    on_success_matcher = options.on_success_matcher

    # [BEAM-1889] Do not send this to remote workers also, there is no need to
    # send this option to remote executors.
    options.on_success_matcher = None

    self.result = super(TestDataflowRunner, self).run(pipeline)
    if self.result.has_job:
      project = pipeline._options.view_as(GoogleCloudOptions).project
      region_id = pipeline._options.view_as(GoogleCloudOptions).region
      job_id = self.result.job_id()
      # TODO(markflyhigh)(BEAM-1890): Use print since Nose dosen't show logs
      # in some cases.
      print (
          'Found: https://console.cloud.google.com/dataflow/jobsDetail'
          '/locations/%s/jobs/%s?project=%s' % (region_id, job_id, project))

    if not options.view_as(StandardOptions).streaming:
      self.result.wait_until_finish()
    else:
      if not self.wait_for_running():
        return self.result

    print('check if matcher?')
    if on_success_matcher:
      from hamcrest import assert_that as hc_assert_that
      hc_assert_that(self.result, pickler.loads(on_success_matcher))

    return self.result

  def wait_for_running(self):
    print('start wait for running.')
    if not self.result.has_job:
      raise IOError('Failed to get the Dataflow job id.')

    start_time = time.time()
    while time.time() - start_time <= DEFAULT_STREAMING_TIMEOUT:
      job_state = self.result.state
      if self._is_in_terminate_state(job_state) or self.result.state == PipelineState.RUNNING:
        log.info('Job is in state: %s.', job_state)
        return True
      time.sleep(5)

    log.info('Timeout when wait for job %s in Running state.', self.result.job_id)
    return False
