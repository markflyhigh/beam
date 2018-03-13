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

from google.cloud import pubsub

INPUT_TOPIC = 'markliu_input_t'
OUTPUT_TOPIC = 'markliu_output_t'
INPUT_SUB = 'markliu_input_s'
OUTPUT_SUB = 'markliu_output_s'

PS = [INPUT_TOPIC, OUTPUT_TOPIC, INPUT_SUB, OUTPUT_SUB]

log = logging.getLogger(__name__)


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
    extra_opts = {
                  'input_topic': i_topic.full_name,
                  'output_topic': o_topic.full_name,
                  # 'on_success_matcher': all_of(*pipeline_verifiers),
    }

    print('extra_opts: \n%s' % extra_opts)

    # Get pipeline options from command argument: --test-pipeline-options,
    # and start pipeline job by calling pipeline main function.
    streaming_wordcount.run(test_pipeline.get_full_options_as_args(**extra_opts))

    # Inject data
    # print('\nwait for 100s.')
    # time.sleep(100)
    print('\nstart inject data')
    self.inject_data(i_topic, 500)

    # Pull messages
    print('\nwait for message from input.')
    messages = self.wait_for_message(i_subscription, 500)

    # Verify messages
    if messages:
      assert len(set(messages)) == 500
      messages.sort()
      for i in range(10):
        log.info('In Subs: %d', i)

    # Pull messages
    print('\nwait for message from output.')
    messages = self.wait_for_message(o_subscription, 500)

    # Verify messages
    if messages:
      assert len(set(messages)) == 500
      messages.sort()
      for i in range(10):
        log.info('Out Subs: %d', i)

    # Cancel streaming. (create a helper)

    log.info('Test is done.')


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

  def inject_data(self, topic, num_messages):
    log.info('inject numbers to topic %s', topic.full_name)
    nums = ' '.join(map(str, range(num_messages)))
    topic.publish(nums)

  def wait_for_message(self, subscription, expected_messages, max_wait_time=300):
    log.info('Start to wait for message from sub %s', subscription)
    total_messages = []
    start_time = time.time()
    while time.time() - start_time <= max_wait_time:
      pulled = subscription.pull(max_messages=10)
      for ack_id, message in pulled:
        total_messages.append(message.data)
        subscription.acknowledge([ack_id])

      if len(total_messages) >= expected_messages:
        log.info('%d messages are recieved. Stop waiting.', len(total_messages))
        return total_messages
    print('Timeout waiting for messages in 5mins.')
    return []


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  unittest.main()