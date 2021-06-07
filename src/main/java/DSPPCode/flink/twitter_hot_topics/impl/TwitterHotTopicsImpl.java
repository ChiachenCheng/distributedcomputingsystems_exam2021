package DSPPCode.flink.twitter_hot_topics.impl;

import DSPPCode.flink.twitter_hot_topics.question.StopWordsOperation;
import DSPPCode.flink.twitter_hot_topics.question.TwitterHotTopics;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;
import java.util.List;

public class TwitterHotTopicsImpl extends TwitterHotTopics {

  @Override
  protected DataStream<Tuple2<String, Integer>> findHotTopics(DataStream<String> twitterText) {
    twitterText.print();
    DataStream<Tuple2<String, Integer>> inp = twitterText.flatMap(
        new FlatMapFunction<String, Tuple2<String, Integer>>() {
          @Override
          public void flatMap(String s, Collector<Tuple2<String, Integer>> collector)
              throws Exception {
            String[] strs = s.split(" ");
            int l = strs.length;
            List<String> stop = StopWordsOperation.getStopWords();
            for(int i=0;i<l;i++){
              String s0 = strs[i];
              s0 = s0.replace("?","").replace(".","").replace(",","").replace("!","");
              // System.err.println(s0);
              // while(s0.charAt(s.length()-1)>'z' || s0.charAt(s0.length()-1)<'A'){
              //   System.err.print(s0);
              //   s0 = s0.substring(0,s0.length()-1);
              // }
              if(stop.contains(s0))
                continue;
              collector.collect(new Tuple2<String, Integer>(s0, 1));
            }
          }
        }
    );
    inp.print();
    DataStream<Tuple2<String, Integer>> ans = inp.keyBy(0).reduce(
        new ReduceFunction<Tuple2<String, Integer>>() {
          @Override
          public Tuple2<String, Integer> reduce(Tuple2<String, Integer> stringIntegerTuple2,
              Tuple2<String, Integer> t1) throws Exception {
            System.err.println("10");
            return new Tuple2<String, Integer>(stringIntegerTuple2.f0, stringIntegerTuple2.f1 + t1.f1);
          }
        }
    ).filter((tup)->tup.f1 >= HOT_TOPIC_THRESHOLD);
    ans.print();
    return ans;
  }
}
