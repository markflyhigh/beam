/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.dataflow.testing;

import static com.google.common.base.Preconditions.checkArgument;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.beam.runners.dataflow.DataflowPipelineJob;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.util.MonitoringUtil;
import org.apache.beam.runners.dataflow.util.MonitoringUtil.JobMessagesHandler;
import org.apache.beam.runners.dataflow.util.TimeUtil;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.SerializableMatcher;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.util.GcsUtil;
import org.apache.beam.sdk.util.IOChannelFactory;
import org.apache.beam.sdk.util.IOChannelUtils;
import org.apache.beam.sdk.util.NoopPathValidator;
import org.apache.beam.sdk.util.TestCredential;
import org.apache.beam.sdk.util.Transport;
import org.apache.beam.sdk.values.PCollection;

import com.google.api.client.http.LowLevelHttpResponse;
import com.google.api.client.json.Json;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.model.JobMessage;
import com.google.api.services.dataflow.model.JobMetrics;
import com.google.api.services.dataflow.model.MetricStructuredName;
import com.google.api.services.dataflow.model.MetricUpdate;
import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.common.io.CharStreams;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;
import java.math.BigDecimal;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/** Tests for {@link TestDataflowRunner}. */
@RunWith(JUnit4.class)
public class TestDataflowRunnerTest {
  @Rule public ExpectedException expectedException = ExpectedException.none();
  @Mock private MockHttpTransport transport;
  @Mock private MockLowLevelHttpRequest request;
  @Mock private GcsUtil mockGcsUtil;

  private TestDataflowPipelineOptions options;
  private Dataflow service;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    when(transport.buildRequest(anyString(), anyString())).thenReturn(request);
    doCallRealMethod().when(request).getContentAsString();
    service = new Dataflow(transport, Transport.getJsonFactory(), null);

    options = PipelineOptionsFactory.as(TestDataflowPipelineOptions.class);
    options.setAppName("TestAppName");
    options.setProject("test-project");
    options.setTempLocation("gs://test/temp/location");
    options.setTempRoot("gs://test");
    options.setGcpCredential(new TestCredential());
    options.setDataflowClient(service);
    options.setRunner(TestDataflowRunner.class);
    options.setPathValidatorClass(NoopPathValidator.class);
  }

  @Test
  public void testToString() {
    assertEquals("TestDataflowRunner#TestAppName",
        new TestDataflowRunner(options).toString());
  }

  @Test
  public void testRunBatchJobThatSucceeds() throws Exception {
    Pipeline p = TestPipeline.create(options);
    PCollection<Integer> pc = p.apply(Create.of(1, 2, 3));
    PAssert.that(pc).containsInAnyOrder(1, 2, 3);

    DataflowPipelineJob mockJob = Mockito.mock(DataflowPipelineJob.class);
    when(mockJob.getState()).thenReturn(State.DONE);
    when(mockJob.getProjectId()).thenReturn("test-project");
    when(mockJob.getJobId()).thenReturn("test-job");

    DataflowRunner mockRunner = Mockito.mock(DataflowRunner.class);
    when(mockRunner.run(any(Pipeline.class))).thenReturn(mockJob);

    TestDataflowRunner runner = (TestDataflowRunner) p.getRunner();
    when(request.execute()).thenReturn(
        generateMockMetricResponse(true /* success */, true /* tentative */));
    assertEquals(mockJob, runner.run(p, mockRunner));
  }

  @Test
  public void testRunBatchJobThatFails() throws Exception {
    Pipeline p = TestPipeline.create(options);
    PCollection<Integer> pc = p.apply(Create.of(1, 2, 3));
    PAssert.that(pc).containsInAnyOrder(1, 2, 3);

    DataflowPipelineJob mockJob = Mockito.mock(DataflowPipelineJob.class);
    when(mockJob.getState()).thenReturn(State.FAILED);
    when(mockJob.getProjectId()).thenReturn("test-project");
    when(mockJob.getJobId()).thenReturn("test-job");

    DataflowRunner mockRunner = Mockito.mock(DataflowRunner.class);
    when(mockRunner.run(any(Pipeline.class))).thenReturn(mockJob);

    TestDataflowRunner runner = (TestDataflowRunner) p.getRunner();
    try {
      runner.run(p, mockRunner);
    } catch (AssertionError expected) {
      return;
    }
    // Note that fail throws an AssertionError which is why it is placed out here
    // instead of inside the try-catch block.
    fail("AssertionError expected");
  }

  @Test
  public void testBatchPipelineFailsIfException() throws Exception {
    Pipeline p = TestPipeline.create(options);
    PCollection<Integer> pc = p.apply(Create.of(1, 2, 3));
    PAssert.that(pc).containsInAnyOrder(1, 2, 3);

    DataflowPipelineJob mockJob = Mockito.mock(DataflowPipelineJob.class);
    when(mockJob.getState()).thenReturn(State.RUNNING);
    when(mockJob.getProjectId()).thenReturn("test-project");
    when(mockJob.getJobId()).thenReturn("test-job");
    when(mockJob.waitUntilFinish(any(Duration.class), any(JobMessagesHandler.class)))
        .thenAnswer(new Answer<State>() {
          @Override
          public State answer(InvocationOnMock invocation) {
            JobMessage message = new JobMessage();
            message.setMessageText("FooException");
            message.setTime(TimeUtil.toCloudTime(Instant.now()));
            message.setMessageImportance("JOB_MESSAGE_ERROR");
            ((MonitoringUtil.JobMessagesHandler) invocation.getArguments()[1])
                .process(Arrays.asList(message));
            return State.CANCELLED;
          }
        });

    DataflowRunner mockRunner = Mockito.mock(DataflowRunner.class);
    when(mockRunner.run(any(Pipeline.class))).thenReturn(mockJob);

    when(request.execute()).thenReturn(
        generateMockMetricResponse(false /* success */, true /* tentative */));
    TestDataflowRunner runner = (TestDataflowRunner) p.getRunner();
    try {
      runner.run(p, mockRunner);
    } catch (AssertionError expected) {
      assertThat(expected.getMessage(), containsString("FooException"));
      verify(mockJob, atLeastOnce()).cancel();
      return;
    }
    // Note that fail throws an AssertionError which is why it is placed out here
    // instead of inside the try-catch block.
    fail("AssertionError expected");
  }

  @Test
  public void testRunStreamingJobThatSucceeds() throws Exception {
    options.setStreaming(true);
    Pipeline p = TestPipeline.create(options);
    PCollection<Integer> pc = p.apply(Create.of(1, 2, 3));
    PAssert.that(pc).containsInAnyOrder(1, 2, 3);

    DataflowPipelineJob mockJob = Mockito.mock(DataflowPipelineJob.class);
    when(mockJob.getState()).thenReturn(State.RUNNING);
    when(mockJob.getProjectId()).thenReturn("test-project");
    when(mockJob.getJobId()).thenReturn("test-job");

    DataflowRunner mockRunner = Mockito.mock(DataflowRunner.class);
    when(mockRunner.run(any(Pipeline.class))).thenReturn(mockJob);

    when(request.execute()).thenReturn(
        generateMockMetricResponse(true /* success */, true /* tentative */));
    TestDataflowRunner runner = (TestDataflowRunner) p.getRunner();
    runner.run(p, mockRunner);
  }

  @Test
  public void testRunStreamingJobThatFails() throws Exception {
    options.setStreaming(true);
    Pipeline p = TestPipeline.create(options);
    PCollection<Integer> pc = p.apply(Create.of(1, 2, 3));
    PAssert.that(pc).containsInAnyOrder(1, 2, 3);

    DataflowPipelineJob mockJob = Mockito.mock(DataflowPipelineJob.class);
    when(mockJob.getState()).thenReturn(State.RUNNING);
    when(mockJob.getProjectId()).thenReturn("test-project");
    when(mockJob.getJobId()).thenReturn("test-job");

    DataflowRunner mockRunner = Mockito.mock(DataflowRunner.class);
    when(mockRunner.run(any(Pipeline.class))).thenReturn(mockJob);

    when(request.execute()).thenReturn(
        generateMockMetricResponse(false /* success */, true /* tentative */));
    TestDataflowRunner runner = (TestDataflowRunner) p.getRunner();
    try {
      runner.run(p, mockRunner);
    } catch (AssertionError expected) {
      return;
    }
    // Note that fail throws an AssertionError which is why it is placed out here
    // instead of inside the try-catch block.
    fail("AssertionError expected");
  }

  @Test
  public void testCheckingForSuccessWhenPAssertSucceeds() throws Exception {
    DataflowPipelineJob job =
        spy(new DataflowPipelineJob("test-project", "test-job", options, null));
    Pipeline p = TestPipeline.create(options);
    PCollection<Integer> pc = p.apply(Create.of(1, 2, 3));
    PAssert.that(pc).containsInAnyOrder(1, 2, 3);

    TestDataflowRunner runner = (TestDataflowRunner) p.getRunner();
    when(request.execute()).thenReturn(
        generateMockMetricResponse(true /* success */, true /* tentative */));
    doReturn(State.DONE).when(job).getState();
    assertEquals(Optional.of(true), runner.checkForSuccess(job));
  }

  @Test
  public void testCheckingForSuccessWhenPAssertFails() throws Exception {
    DataflowPipelineJob job =
        spy(new DataflowPipelineJob("test-project", "test-job", options, null));
    Pipeline p = TestPipeline.create(options);
    PCollection<Integer> pc = p.apply(Create.of(1, 2, 3));
    PAssert.that(pc).containsInAnyOrder(1, 2, 3);

    TestDataflowRunner runner = (TestDataflowRunner) p.getRunner();
    when(request.execute()).thenReturn(
        generateMockMetricResponse(false /* success */, true /* tentative */));
    doReturn(State.DONE).when(job).getState();
    assertEquals(Optional.of(false), runner.checkForSuccess(job));
  }

  @Test
  public void testCheckingForSuccessSkipsNonTentativeMetrics() throws Exception {
    DataflowPipelineJob job =
        spy(new DataflowPipelineJob("test-project", "test-job", options, null));
    Pipeline p = TestPipeline.create(options);
    PCollection<Integer> pc = p.apply(Create.of(1, 2, 3));
    PAssert.that(pc).containsInAnyOrder(1, 2, 3);

    TestDataflowRunner runner = (TestDataflowRunner) p.getRunner();
    when(request.execute()).thenReturn(
        generateMockMetricResponse(true /* success */, false /* tentative */));
    doReturn(State.RUNNING).when(job).getState();
    assertEquals(Optional.absent(), runner.checkForSuccess(job));
  }

  private LowLevelHttpResponse generateMockMetricResponse(boolean success, boolean tentative)
      throws Exception {
    MetricStructuredName name = new MetricStructuredName();
    name.setName(success ? "PAssertSuccess" : "PAssertFailure");
    name.setContext(
        tentative ? ImmutableMap.of("tentative", "") : ImmutableMap.<String, String>of());

    MetricUpdate metric = new MetricUpdate();
    metric.setName(name);
    metric.setScalar(BigDecimal.ONE);

    MockLowLevelHttpResponse response = new MockLowLevelHttpResponse();
    response.setContentType(Json.MEDIA_TYPE);
    JobMetrics jobMetrics = new JobMetrics();
    jobMetrics.setMetrics(Lists.newArrayList(metric));
    // N.B. Setting the factory is necessary in order to get valid JSON.
    jobMetrics.setFactory(Transport.getJsonFactory());
    response.setContent(jobMetrics.toPrettyString());
    return response;
  }

  @Test
  public void testStreamingPipelineFailsIfServiceFails() throws Exception {
    DataflowPipelineJob job =
        spy(new DataflowPipelineJob("test-project", "test-job", options, null));
    Pipeline p = TestPipeline.create(options);
    PCollection<Integer> pc = p.apply(Create.of(1, 2, 3));
    PAssert.that(pc).containsInAnyOrder(1, 2, 3);

    TestDataflowRunner runner = (TestDataflowRunner) p.getRunner();
    when(request.execute()).thenReturn(
        generateMockMetricResponse(true /* success */, false /* tentative */));
    doReturn(State.FAILED).when(job).getState();
    assertEquals(Optional.of(false), runner.checkForSuccess(job));
  }

  @Test
  public void testStreamingPipelineFailsIfException() throws Exception {
    options.setStreaming(true);
    Pipeline p = TestPipeline.create(options);
    PCollection<Integer> pc = p.apply(Create.of(1, 2, 3));
    PAssert.that(pc).containsInAnyOrder(1, 2, 3);

    DataflowPipelineJob mockJob = Mockito.mock(DataflowPipelineJob.class);
    when(mockJob.getState()).thenReturn(State.RUNNING);
    when(mockJob.getProjectId()).thenReturn("test-project");
    when(mockJob.getJobId()).thenReturn("test-job");
    when(mockJob.waitUntilFinish(any(Duration.class), any(JobMessagesHandler.class)))
        .thenAnswer(new Answer<State>() {
          @Override
          public State answer(InvocationOnMock invocation) {
            JobMessage message = new JobMessage();
            message.setMessageText("FooException");
            message.setTime(TimeUtil.toCloudTime(Instant.now()));
            message.setMessageImportance("JOB_MESSAGE_ERROR");
            ((MonitoringUtil.JobMessagesHandler) invocation.getArguments()[1])
                .process(Arrays.asList(message));
            return State.CANCELLED;
          }
        });

    DataflowRunner mockRunner = Mockito.mock(DataflowRunner.class);
    when(mockRunner.run(any(Pipeline.class))).thenReturn(mockJob);

    when(request.execute()).thenReturn(
        generateMockMetricResponse(false /* success */, true /* tentative */));
    TestDataflowRunner runner = (TestDataflowRunner) p.getRunner();
    try {
      runner.run(p, mockRunner);
    } catch (AssertionError expected) {
      assertThat(expected.getMessage(), containsString("FooException"));
      verify(mockJob, atLeastOnce()).cancel();
      return;
    }
    // Note that fail throws an AssertionError which is why it is placed out here
    // instead of inside the try-catch block.
    fail("AssertionError expected");
  }

  @Test
  public void testBatchOnCreateMatcher() throws Exception {
    Pipeline p = TestPipeline.create(options);
    PCollection<Integer> pc = p.apply(Create.of(1, 2, 3));
    PAssert.that(pc).containsInAnyOrder(1, 2, 3);

    final DataflowPipelineJob mockJob = Mockito.mock(DataflowPipelineJob.class);
    when(mockJob.getState()).thenReturn(State.DONE);
    when(mockJob.getProjectId()).thenReturn("test-project");
    when(mockJob.getJobId()).thenReturn("test-job");

    DataflowRunner mockRunner = Mockito.mock(DataflowRunner.class);
    when(mockRunner.run(any(Pipeline.class))).thenReturn(mockJob);

    TestDataflowRunner runner = (TestDataflowRunner) p.getRunner();
    p.getOptions().as(TestPipelineOptions.class)
        .setOnCreateMatcher(new TestSuccessMatcher(mockJob, 0));

    when(request.execute()).thenReturn(
        generateMockMetricResponse(true /* success */, true /* tentative */));
    runner.run(p, mockRunner);
  }

  @Test
  public void testStreamingOnCreateMatcher() throws Exception {
    options.setStreaming(true);
    Pipeline p = TestPipeline.create(options);
    PCollection<Integer> pc = p.apply(Create.of(1, 2, 3));
    PAssert.that(pc).containsInAnyOrder(1, 2, 3);

    final DataflowPipelineJob mockJob = Mockito.mock(DataflowPipelineJob.class);
    when(mockJob.getState()).thenReturn(State.DONE);
    when(mockJob.getProjectId()).thenReturn("test-project");
    when(mockJob.getJobId()).thenReturn("test-job");

    DataflowRunner mockRunner = Mockito.mock(DataflowRunner.class);
    when(mockRunner.run(any(Pipeline.class))).thenReturn(mockJob);

    TestDataflowRunner runner = (TestDataflowRunner) p.getRunner();
    p.getOptions().as(TestPipelineOptions.class)
        .setOnCreateMatcher(new TestSuccessMatcher(mockJob, 0));

    when(mockJob.waitUntilFinish(any(Duration.class), any(JobMessagesHandler.class)))
        .thenReturn(State.DONE);

    when(request.execute()).thenReturn(
        generateMockMetricResponse(true /* success */, true /* tentative */));
    runner.run(p, mockRunner);
  }

  @Test
  public void testBatchOnSuccessMatcherWhenPipelineSucceeds() throws Exception {
    Pipeline p = TestPipeline.create(options);
    PCollection<Integer> pc = p.apply(Create.of(1, 2, 3));
    PAssert.that(pc).containsInAnyOrder(1, 2, 3);

    final DataflowPipelineJob mockJob = Mockito.mock(DataflowPipelineJob.class);
    when(mockJob.getState()).thenReturn(State.DONE);
    when(mockJob.getProjectId()).thenReturn("test-project");
    when(mockJob.getJobId()).thenReturn("test-job");

    DataflowRunner mockRunner = Mockito.mock(DataflowRunner.class);
    when(mockRunner.run(any(Pipeline.class))).thenReturn(mockJob);

    TestDataflowRunner runner = (TestDataflowRunner) p.getRunner();
    p.getOptions().as(TestPipelineOptions.class)
        .setOnSuccessMatchers(new ArrayList<SerializableMatcher>(
            Arrays.asList(new TestSuccessMatcher(mockJob, 1))));

    when(request.execute()).thenReturn(
        generateMockMetricResponse(true /* success */, true /* tentative */));
    runner.run(p, mockRunner);
  }

  @Test
  public void testBatchOnSuccessMatchersWhenPipelineSucceeds() throws Exception {
    Pipeline p = TestPipeline.create(options);
    PCollection<Integer> pc = p.apply(Create.of(1, 2, 3));
    PAssert.that(pc).containsInAnyOrder(1, 2, 3);

    final DataflowPipelineJob mockJob = Mockito.mock(DataflowPipelineJob.class);
    when(mockJob.getState()).thenReturn(State.DONE);
    when(mockJob.getProjectId()).thenReturn("test-project");
    when(mockJob.getJobId()).thenReturn("test-job");

    DataflowRunner mockRunner = Mockito.mock(DataflowRunner.class);
    when(mockRunner.run(any(Pipeline.class))).thenReturn(mockJob);

    TestDataflowRunner runner = (TestDataflowRunner) p.getRunner();

    ArrayList<SerializableMatcher> list = new ArrayList<>();
    list.add(new TestSuccessMatcher(mockJob, 1));
    list.add(new AlwaysSuccessMatcher());
//    list.add(new WordCountOnSuccessMatcher(
//        "/tmp/WordCountIT-2016-08-02-14-34-51-014/output/results-00000-of-00001"));
//    list.add(new TestFailureMatcher());
    p.getOptions().as(TestPipelineOptions.class)
        .setOnSuccessMatchers(list);

    when(request.execute()).thenReturn(
        generateMockMetricResponse(true /* success */, true /* tentative */));
    runner.run(p, mockRunner);
  }

  @Test
  public void testStreamingOnSuccessMatcherWhenPipelineSucceeds() throws Exception {
    options.setStreaming(true);
    Pipeline p = TestPipeline.create(options);
    PCollection<Integer> pc = p.apply(Create.of(1, 2, 3));
    PAssert.that(pc).containsInAnyOrder(1, 2, 3);

    final DataflowPipelineJob mockJob = Mockito.mock(DataflowPipelineJob.class);
    when(mockJob.getState()).thenReturn(State.DONE);
    when(mockJob.getProjectId()).thenReturn("test-project");
    when(mockJob.getJobId()).thenReturn("test-job");

    DataflowRunner mockRunner = Mockito.mock(DataflowRunner.class);
    when(mockRunner.run(any(Pipeline.class))).thenReturn(mockJob);

    TestDataflowRunner runner = (TestDataflowRunner) p.getRunner();
    p.getOptions().as(TestPipelineOptions.class)
        .setOnSuccessMatchers(new ArrayList<SerializableMatcher>(
            Arrays.asList(new TestSuccessMatcher(mockJob, 1))));

    when(mockJob.waitUntilFinish(any(Duration.class), any(JobMessagesHandler.class)))
        .thenReturn(State.DONE);

    when(request.execute()).thenReturn(
        generateMockMetricResponse(true /* success */, true /* tentative */));
    runner.run(p, mockRunner);
  }

  @Test
  public void testBatchOnSuccessMatcherWhenPipelineFails() throws Exception {
    Pipeline p = TestPipeline.create(options);
    PCollection<Integer> pc = p.apply(Create.of(1, 2, 3));
    PAssert.that(pc).containsInAnyOrder(1, 2, 3);

    final DataflowPipelineJob mockJob = Mockito.mock(DataflowPipelineJob.class);
    when(mockJob.getState()).thenReturn(State.FAILED);
    when(mockJob.getProjectId()).thenReturn("test-project");
    when(mockJob.getJobId()).thenReturn("test-job");

    DataflowRunner mockRunner = Mockito.mock(DataflowRunner.class);
    when(mockRunner.run(any(Pipeline.class))).thenReturn(mockJob);

    TestDataflowRunner runner = (TestDataflowRunner) p.getRunner();
    p.getOptions().as(TestPipelineOptions.class)
        .setOnSuccessMatchers(new ArrayList<SerializableMatcher>(
            Arrays.asList(new TestFailureMatcher())));

    when(request.execute()).thenReturn(
        generateMockMetricResponse(false /* success */, true /* tentative */));
    try {
      runner.run(p, mockRunner);
    } catch (AssertionError expected) {
      verify(mockJob, Mockito.times(1)).waitUntilFinish(
          any(Duration.class), any(JobMessagesHandler.class));
      return;
    }
    fail("Expected an exception on pipeline failure.");
  }

  @Test
  public void testStreamingOnSuccessMatcherWhenPipelineFails() throws Exception {
    options.setStreaming(true);
    Pipeline p = TestPipeline.create(options);
    PCollection<Integer> pc = p.apply(Create.of(1, 2, 3));
    PAssert.that(pc).containsInAnyOrder(1, 2, 3);

    final DataflowPipelineJob mockJob = Mockito.mock(DataflowPipelineJob.class);
    when(mockJob.getState()).thenReturn(State.FAILED);
    when(mockJob.getProjectId()).thenReturn("test-project");
    when(mockJob.getJobId()).thenReturn("test-job");

    DataflowRunner mockRunner = Mockito.mock(DataflowRunner.class);
    when(mockRunner.run(any(Pipeline.class))).thenReturn(mockJob);

    TestDataflowRunner runner = (TestDataflowRunner) p.getRunner();
    p.getOptions().as(TestPipelineOptions.class)
        .setOnSuccessMatchers(new ArrayList<SerializableMatcher>(
            Arrays.asList(new TestFailureMatcher())));
    when(mockJob.waitUntilFinish(any(Duration.class), any(JobMessagesHandler.class)))
        .thenReturn(State.FAILED);

    when(request.execute()).thenReturn(
        generateMockMetricResponse(false /* success */, true /* tentative */));
    try {
      runner.run(p, mockRunner);
    } catch (AssertionError expected) {
      verify(mockJob, Mockito.times(1)).waitUntilFinish(any(Duration.class),
          any(JobMessagesHandler.class));
      return;
    }
    fail("Expected an exception on pipeline failure.");
  }

  static class AlwaysSuccessMatcher extends TypeSafeMatcher<PipelineResult>
      implements SerializableMatcher<PipelineResult> {
    @Override
    protected boolean matchesSafely(PipelineResult pipelineResult) {
      return true;
    }
    @Override
    public void describeTo(Description description) {
    }
  }

  static class TestSuccessMatcher extends BaseMatcher<PipelineResult> implements
      SerializableMatcher<PipelineResult> {
    private final DataflowPipelineJob mockJob;
    private final int called;

    public TestSuccessMatcher(DataflowPipelineJob job, int times) {
      this.mockJob = job;
      this.called = times;
    }

    @Override
    public boolean matches(Object o) {
      if (!(o instanceof PipelineResult)) {
        fail(String.format("Expected PipelineResult but received %s", o));
      }
      try {
        verify(mockJob, Mockito.times(called)).waitUntilFinish(
            any(Duration.class), any(JobMessagesHandler.class));
      } catch (IOException | InterruptedException e) {
        throw new AssertionError(e);
      }
      assertSame(mockJob, o);
      return true;
    }

    @Override
    public void describeTo(Description description) {
    }
  }

  static class TestFailureMatcher extends BaseMatcher<PipelineResult> implements
      SerializableMatcher<PipelineResult> {
    @Override
    public boolean matches(Object o) {
      fail("OnSuccessMatcher should not be called on pipeline failure.");
      return false;
    }

    @Override
    public void describeTo(Description description) {
    }
  }

  static class WordCountOnSuccessMatcher extends TypeSafeMatcher<PipelineResult>
      implements SerializableMatcher<PipelineResult> {

    private static final Logger LOG = LoggerFactory.getLogger(WordCountOnSuccessMatcher.class);

    private static final String EXPECTED_CHECKSUM = "8ae94f799f97cfd1cb5e8125951b32dfb52e1f12";
    private String actualChecksum;

    private final String outputPath;

    WordCountOnSuccessMatcher(String outputPath) {
      checkArgument(
          !Strings.isNullOrEmpty(outputPath),
          "Expected valid output path, but received %s", outputPath);

      this.outputPath = outputPath;
    }

    @Override
    protected boolean matchesSafely(PipelineResult pResult) {
      try {
        // Load output data
        List<String> outputs = readLines(outputPath);

        // Verify outputs. Checksum is computed using SHA-1 algorithm
        actualChecksum = hashing(outputs);
        LOG.info("Generated checksum for output data: {}", actualChecksum);

        return actualChecksum.equals(EXPECTED_CHECKSUM);
      } catch (IOException e) {
        throw new RuntimeException(
            String.format("Failed to read from path: %s", outputPath));
      }
    }

    private List<String> readLines(String path) throws IOException {
      List<String> readData = new ArrayList<>();

      IOChannelFactory factory = IOChannelUtils.getFactory(path);

      // Match inputPath which may contains glob
      Collection<String> files = factory.match(path);

      // Read data from file paths
      int i = 0;
      for (String file : files) {
        try (Reader reader =
                 Channels.newReader(factory.open(file), StandardCharsets.UTF_8.name())) {
          List<String> lines = CharStreams.readLines(reader);
          readData.addAll(lines);
          LOG.info(
              "[{} of {}] Read {} lines from file: {}", i, files.size() - 1, lines.size(), file);
        }
        i++;
      }
      return readData;
    }

    private String hashing(List<String> strs) {
      List<HashCode> hashCodes = new ArrayList<>();
      for (String str : strs) {
        hashCodes.add(Hashing.sha1().hashString(str, StandardCharsets.UTF_8));
      }
      return Hashing.combineUnordered(hashCodes).toString();
    }

    @Override
    public void describeTo(Description description) {
      description
          .appendText("Expected checksum is (")
          .appendText(EXPECTED_CHECKSUM)
          .appendText(")");
    }

    @Override
    protected void describeMismatchSafely(PipelineResult pResult, Description description) {
      description
          .appendText("was (")
          .appendText(actualChecksum)
          .appendText(")");
    }
  }
}
