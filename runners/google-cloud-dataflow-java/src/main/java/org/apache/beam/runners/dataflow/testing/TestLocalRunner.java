package org.apache.beam.runners.dataflow.testing;

import static org.hamcrest.MatcherAssert.assertThat;

import com.google.common.base.Joiner;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.runners.direct.DirectRunner.DirectPipelineResult;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.PipelineRunner;
import org.apache.beam.sdk.testing.SerializableMatcher;
import org.apache.beam.sdk.testing.TestPipelineOptions;

/**
 * Created by markliu on 8/5/16.
 */
public class TestLocalRunner extends PipelineRunner<DirectPipelineResult>{
  private final DirectRunner runner;

  public TestLocalRunner(TestLocalRunnerOptions options) {
    runner = DirectRunner.fromOptions(options);
  }

  /**
   * Constructs a runner from the provided options.
   */
  public static TestLocalRunner fromOptions(PipelineOptions options) {
    TestDataflowPipelineOptions dataflowOptions = options.as(TestDataflowPipelineOptions.class);
    String tempLocation = Joiner.on("/").join(
        dataflowOptions.getTempRoot(),
        dataflowOptions.getJobName(),
        "output",
        "results");
    dataflowOptions.setTempLocation(tempLocation);
    return new TestLocalRunner(options.as(TestLocalRunnerOptions.class));
  }

  @Override
  public DirectPipelineResult run(Pipeline pipeline) {
    return run(pipeline, runner);
  }

  private DirectPipelineResult run(Pipeline pipeline, DirectRunner runner) {
    TestPipelineOptions options = pipeline.getOptions().as(TestPipelineOptions.class);
    DirectPipelineResult result = runner.run(pipeline);

    for (SerializableMatcher matcher : options.getOnSuccessMatchers()) {
      assertThat(result, matcher);
    }
    return result;
  }


  //////////////////////////////////////////////////////////
  interface TestLocalRunnerOptions extends DirectOptions, TestPipelineOptions {}
}
