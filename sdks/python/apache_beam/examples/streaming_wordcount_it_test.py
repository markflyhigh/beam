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
import time

from hamcrest.core.core.allof import all_of
from nose.plugins.attrib import attr

from apache_beam.testing.pipeline_verifiers import PipelineStateMatcher
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.examples import streaming_wordcount
from apache_beam.testing import test_utils

from google.cloud import pubsub

INPUT_TOPIC = 'markliu_input_t'
OUTPUT_TOPIC = 'markliu_output_t'
INPUT_SUB = 'markliu_input_s'
OUTPUT_SUB = 'markliu_output_s'

DEFAULT_INPUT_NUMBER_LIMIT = 500


class StreamingWordCountIT(unittest.TestCase):

  def setUp(self):
    self.test_pipeline = TestPipeline(is_integration_test=True)

    # Set up PubSub environment
    self.pubsub_client = pubsub.Client(project=self.test_pipeline.get_option('project'))
    self.input_topic = self.pubsub_client.topic(INPUT_TOPIC)
    self.output_topic = self.pubsub_client.topic(OUTPUT_TOPIC)
    self.input_sub = self.input_topic.subscription(INPUT_SUB)
    self.output_sub = self.output_topic.subscription(OUTPUT_SUB)

    self._cleanup_pubsub()

    self.input_topic.create()
    self.output_topic.create()
    test_utils.wait_for_topics_created([self.input_topic, self.output_topic])
    self.input_sub.create()
    self.output_sub.create()

  @attr('test')
  def test_streaming_wordcount_it(self):
    # Set extra options to the pipeline for test purpose
    pipeline_verifiers = [PipelineStateMatcher()]
    extra_opts = {
                  'input_sub': self.input_sub.full_name,
                  'output_topic': self.output_topic.full_name,
                  # 'on_success_matcher': all_of(*pipeline_verifiers),
    }

    print('extra_opts: \n%s' % extra_opts)

    # wait until subs are available
    test_utils.wait_for_subscriptions_created([self.input_sub])

    # Get pipeline options from command argument: --test-pipeline-options,
    # and start pipeline job by calling pipeline main function.
    streaming_wordcount.run(self.test_pipeline.get_full_options_as_args(**extra_opts))

    # Generate input data and inject to PubSub.
    print('\nstart inject data')
    self.inject_numbers(self.input_topic, DEFAULT_INPUT_NUMBER_LIMIT)

    # Pull and verify PubSub messages.
    print('\nwait for message from output.')
    messages = self.wait_for_message(self.output_sub, DEFAULT_INPUT_NUMBER_LIMIT)

    if messages:
      assert len(set(messages)) == DEFAULT_INPUT_NUMBER_LIMIT
      messages.sort()
      for i in range(10):
        print('Out Subs: %s', messages[i])

    # Cancel streaming. (create a helper)

    print('Test is done.')


  def _cleanup_pubsub(self):
    test_utils.cleanup_subscription([self.input_sub, self.output_sub])
    test_utils.cleanup_topics([self.input_topic, self.output_topic])

  def tearDown(self):
    self._cleanup_pubsub()

  # def cleanup(self):
  #   for topic in self.topics:
  #     if topic.exists():
  #       topic.delete()
  #     else:
  #       print('No clean up for topic %s' % topic.full_name)
  #
  #   for sub in self.subscriptions:
  #     if sub.exists():
  #       sub.delete()
  #     else:
  #       print('No clean up for subscription %s.' % sub.full_name)

  def inject_numbers(self, topic, num_messages):
    """Inject numbers as test data to PubSub."""
    logging.debug('Injecting %d numbers to topic %s', num_messages, topic.full_name)
    print('\ninject numbers to topic %s' % topic.full_name)
    for n in range(num_messages):
      topic.publish(str(n))

  def wait_for_message(self, subscription, expected_messages, max_wait_time=300):
    logging.debug('Start wait for messages from sub %s', subscription)
    total_messages = []
    start_time = time.time()
    while time.time() - start_time <= max_wait_time:
      pulled = subscription.pull(max_messages=50)
      for ack_id, message in pulled:
        total_messages.append(message.data)
        subscription.acknowledge([ack_id])

      if len(total_messages) >= expected_messages:
        print('%d messages are recieved. Stop waiting.', len(total_messages))
        return total_messages
      if len(total_messages) % 10 == 0:
        print('received %d messages.' % len(total_messages))

    raise RuntimeError('Timeout waiting for all messages appear from '
                       'subscription %s after %d sec.' %
                       (subscription, max_wait_time))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  unittest.main()