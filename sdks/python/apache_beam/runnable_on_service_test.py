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

"""Unit test for runnable on service tests"""

import unittest
import logging
import pytest

from apache_beam.transforms import Create
from apache_beam.transforms.util import assert_that, equal_to
from apache_beam.test_pipeline import TPipeline


class RunnableOnServiceTest(unittest.TestCase):

  @pytest.mark.RunnableOnService
  def test_first(self):
    """Test that @RuuanbelOnService works"""
    pass

  @pytest.mark.RunnableOnService
  def test_second(self):
    """Test that small test pipeline can run locally or on dataflow service"""
    pipeline = TPipeline()
    pcoll = pipeline | 'label' >> Create([[1, 2, 3]])
    assert_that(pcoll, equal_to([[1, 2, 3]]))
    pipeline.run()

  def test_third(self):
    """Test that test without @RunableOnService label can be ignored"""
    pipeline = TPipeline()
    pcoll = pipeline | 'label' >> Create([[1, 2, 3]])
    assert_that(pcoll, equal_to([[1, 2, 3]]))
    pipeline.run()

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  unittest.main()
