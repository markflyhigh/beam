# coding=utf-8
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

from google.cloud import pubsub


class BeamPubSubException(Exception):
  pass


class BeamPubSubClient(object):

  def __init__(self, project_id):
    self.pubsub_client = pubsub.Client(project=project_id)
    self.topics = []
    self.subs = []

  def create_topic(self, name):
    """Create a new topic with given name.

    Args:
      name: A string of topic name.

    Returns:
      A string of full topic name in format of "projects/{project_id}/topics/{topic}".
    """
    if not name:
      raise AttributeError('Invalid topic name: %s.', name)

    new_topic = self.pubsub_client.topic(name)
    if not self.exist_topic(new_topic):
      new_topic.create()
    return new_topic.full_name

  def create_subcription(self, name, topic):
    """Create a new subscription to a given topic."""
    if not topic:
      raise AttributeError('Invalid topic name: %s.', topic)
    elif not self.pubsub_client.topic(topic).exists():
      raise BeamPubSubException('Failed to create subscription %s. Topic %s does not exists. Please create topic first.', name, topic)

    new_sub = self.pubsub_client.subscription(name)
    if new_sub.exists():
      raise BeamPubSubException('Subscription %s already exists.', new_sub.full_name)

    new_sub.create()
    return new_sub.full_name

  def delete_topic(self, topic):
    """Delete a topic with given name."""


  def delete_subscription(self, subscription):
    raise NotImplementedError

  def exist_topic(self, topic):
    return False

  def exist_subscription(self, subscription):
    return False

  def cleanup(self):
    raise NotImplementedError
