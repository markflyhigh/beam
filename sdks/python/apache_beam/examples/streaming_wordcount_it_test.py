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

"""End-to-end test for the streaming wordcount example."""

import logging
import unittest

from hamcrest.core.core.allof import all_of
from nose.plugins.attrib import attr

from apache_beam.testing.pipeline_verifiers import PipelineStateMatcher
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.examples import streaming_wordcount

from google.cloud import pubsub

INPUT_TOPIC = 'markliu_input_t'
OUTPUT_TOPIC = 'markliu_output_t'
INPUT_SUB = 'markliu_input_s'
OUTPUT_SUB = 'markliu_output_s'

PS = [INPUT_TOPIC, OUTPUT_TOPIC, INPUT_SUB, OUTPUT_SUB]


class StreamingWordCountIT(unittest.TestCase):

  def setUp(self):
    self.topics = []
    self.subscriptions = []

  @attr('test')
  def test_streaming_wordcount_it(self):
    test_pipeline = TestPipeline(is_integration_test=True)

    # Set up PubSub environment
    self.pubsub_client = pubsub.Client(project=test_pipeline.get_option('project'))

    i_topic = self.pubsub_client.topic(INPUT_TOPIC)
    o_topic = self.pubsub_client.topic(OUTPUT_TOPIC)
    i_subscription = i_topic.subscription(INPUT_SUB)
    o_subscription = o_topic.subscription(OUTPUT_SUB)
    self.topics.append(i_topic)
    self.topics.append(o_topic)
    self.subscriptions.append(i_subscription)
    self.subscriptions.append(o_subscription)

    self.cleanup()

    i_topic.create()
    o_topic.create()
    i_subscription.create()
    o_subscription.create()

    # Set extra options to the pipeline for test purpose
    pipeline_verifiers = [PipelineStateMatcher()]
    extra_opts = {'on_success_matcher': all_of(*pipeline_verifiers),
                  'input_topic': i_topic.full_name,
                  'output_topic': o_topic.full_name}

    print('\nextra_opts: \n%s' % extra_opts)

    self.inject_data(i_topic)

    # Get pipeline options from command argument: --test-pipeline-options,
    # and start pipeline job by calling pipeline main function.
    streaming_wordcount.run(test_pipeline.get_full_options_as_args(**extra_opts))

    




  def tearDown(self):
    self.cleanup()

  def cleanup(self):
    for topic in self.topics:
      if topic.exists():
        topic.delete()
      else:
        print('No clean up for topic %s' % topic.full_name)

    for sub in self.subscriptions:
      if sub.exists():
        sub.delete()
      else:
        print('No clean up for subscription %s.' % sub.full_name)

  def inject_data(self, topic):
    nums = ' '.join(range(0, 500))
    topic.publish(nums)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  unittest.main()