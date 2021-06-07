package DSPPCode.mapreduce.kmeans_runner.impl;

import DSPPCode.mapreduce.kmeans_runner.question.KMeansRunner;

public class KMeansRunnerImpl extends KMeansRunner {

  @Override
  protected boolean isLastIteration(String[] args, int currentIteration) {
    // String oldcenter =
    if(currentIteration == true)
      return true;
    return false;
  }
}
