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

"""Test configurations for nose

This module contains nose plugin hooks that configures Beam tests which
includes ValidatesRunner test and E2E integration test.

"""

# import logging

from datetime import datetime

from nose.plugins import Plugin

# log = logging.getLogger(__name__)


class BeamTestPlugin(Plugin):
  """A nose plugin for Beam testing that registers command line options

  This plugin is registered through setuptools in entry_points.
  """

  def options(self, parser, env):
    """Add '--test-pipeline-options' to command line option to avoid
    unrecognized option error thrown by nose.

    The value of this option will be processed by TestPipeline and used to
    build customized pipeline for ValidatesRunner tests.
    """
    Plugin.options(self, parser, env)
    parser.add_option('--test-pipeline-options',
                      action='store',
                      type=str,
                      help='providing pipeline options to run tests on runner')


class TestTimestampPlugin(Plugin):
  """A nose plugin that prints timestamp when test function get executed."""

  name = 'timestamp'
  enabled = True
  score = 2

  def options(self, parser, env):
    super(TestTimestampPlugin, self).options(parser, env)

  def configure(self, options, conf):
    """Configure plugin."""
    # log.info('start config.')
    super(TestTimestampPlugin, self).configure(options, conf)
    # self._add_timestamp = conf.verbosity >= 2
    self._add_timestamp = True

  def setOutputStream(self, stream):
    """Get handle on output stream so the plugin can print timestamp."""
    self.stream = stream

  def startTest(self, test):
    """Add a timestamp before test name."""
    # log.info('start test!!')
    time_format = '%H:%M:%S.%f'
    if self._add_timestamp:
      timestamp = datetime.now().strftime(time_format)[:-3]
      self.stream.write('%s ' % timestamp)

