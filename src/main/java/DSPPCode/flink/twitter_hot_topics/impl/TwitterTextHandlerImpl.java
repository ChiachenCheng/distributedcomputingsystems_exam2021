package DSPPCode.flink.twitter_hot_topics.impl;

import DSPPCode.flink.twitter_hot_topics.question.Twitter;
import DSPPCode.flink.twitter_hot_topics.question.TwitterTextHandler;

public class TwitterTextHandlerImpl extends TwitterTextHandler {

  @Override
  public String map(String twitterJson) throws Exception {
    Twitter twitter = fromJson(twitterJson);
    if (twitter.getUser().getLang().equals("en")) {
      System.err.println(twitter.getText());
      return twitter.getText();
    }
    else return "the";
  }
}
