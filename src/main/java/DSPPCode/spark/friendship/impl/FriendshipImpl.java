package DSPPCode.spark.friendship.impl;

import DSPPCode.spark.friendship.question.Friendship;
import com.clearspring.analytics.util.Lists;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class FriendshipImpl extends Friendship {

  @Override
  public JavaPairRDD<String, Iterable<String>> findFriendShip(JavaRDD<String> lines) {
    // JavaPairRDD<String, Iterable<String>> input = lines.mapToPair(
    //     new PairFunction<String, String, Iterable<String>>() {
    //       @Override
    //       public Tuple2<String, Iterable<String>> call(String s) throws Exception {
    //         String[] strs = s.split(" ");
    //         String s0 = strs[0];
    //         List<String> list1=Arrays.asList(strs);
    //         List<String> arrList = new ArrayList<String>(list1);
    //         arrList.remove(s0);
    //         Iterable<String> itr = arrList;
    //         return new Tuple2<String, Iterable<String>>(s0, itr);
    //       }
    //     }
    // );
    JavaPairRDD<String, String> input = lines.flatMapToPair(
        new PairFlatMapFunction<String, String, String>() {
          @Override
          public Iterator<Tuple2<String, String>> call(String s) throws Exception {
            String[] strs = s.split(" ");
            List<Tuple2<String, String>> list = new ArrayList<>();
            int l = strs.length;
            for(int i=1;i<l;i++)
              list.add(new Tuple2<String, String>(strs[i], strs[0]));
            return list.iterator();
          }
        }
    ).groupByKey().flatMapToPair(
        new PairFlatMapFunction<Tuple2<String, Iterable<String>>, String, String>() {
          @Override
          public Iterator<Tuple2<String, String>> call(
              Tuple2<String, Iterable<String>> stringIterableTuple2) throws Exception {
            ArrayList<String> strl = (ArrayList<String>) Lists.newArrayList(stringIterableTuple2._2);
            String[] strs = strl.toArray(new String[0]);
            List<String> jo = orderedPairs(strs);
            List<Tuple2<String, String>> list = new ArrayList<>();
            for(int i=0;i<jo.size();i++) {
              System.err.print(jo.get(i));
              System.err.print(" ");
              list.add(new Tuple2<String, String>(jo.get(i), stringIterableTuple2._1()));
            }
            return list.iterator();
          }
        }
    );
    JavaPairRDD<String, Iterable<String>> ans = input.groupByKey();
    // JavaPairRDD<String, String> joinput = input.cogroup(input).flatMapToPair(
    //     new PairFlatMapFunction<Tuple2<String, Tuple2<Iterable<String>, Iterable<String>>>, String, String>() {
    //       @Override
    //       public Iterator<Tuple2<String, String>> call(
    //           Tuple2<String, Tuple2<Iterable<String>, Iterable<String>>> stringTuple2Tuple2)
    //           throws Exception {
    //
    //         return null;
    //       }
    //     }
    // )
    // //     .mapToPair(
    // //     (tup) -> new Tuple2<String, String>(tup._1, tup._2()._1() + tup._2()._1())
    // // );
    // input.foreach(
    //     new VoidFunction<Tuple2<String, String>>() {
    //       @Override
    //       public void call(Tuple2<String, String> stringStringTuple2) throws Exception {
    //         System.err.print(stringStringTuple2._1());
    //         System.err.print(" ");
    //         System.err.println(stringStringTuple2._2);
    //       }
    //     }
    // );
    // joinput.foreach(
    //     new VoidFunction<Tuple2<String, String>>() {
    //       @Override
    //       public void call(Tuple2<String, String> stringStringTuple2) throws Exception {
    //         System.err.print(stringStringTuple2._1());
    //         System.err.print(" ");
    //         System.err.println(stringStringTuple2._2);
    //       }
    //     }
    // );
    return ans;
  }
  @Override
  public List<String> orderedPairs(String[] strs) {
    Arrays.sort(strs);
    List<String> ans = new ArrayList<>();
    int l = strs.length;
    for(int i=0;i<l;i++){
      for(int j=i+1;j<l;j++)
        ans.add(strs[i] + strs[j]);
    }
    return ans;
  }
}
