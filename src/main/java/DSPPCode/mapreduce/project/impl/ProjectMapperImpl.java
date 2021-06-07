package DSPPCode.mapreduce.project.impl;

import DSPPCode.mapreduce.project.question.ProjectMapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import java.util.StringTokenizer;

public class ProjectMapperImpl extends ProjectMapper {

  @Override
  public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {
    StringTokenizer itr = new StringTokenizer(value.toString());
    while (itr.hasMoreTokens()) {
      String str = itr.nextToken();
      String[] strs = str.split(",");
      for(int i=0; i<strs.length; i++)
        System.err.print(strs[i] + " + ");
      System.err.println();
      Long n = Long.parseLong(strs[0]);
      n += 1;
      String ans = n.toString() + "," + strs[1];
      Text word = new Text();
      word.set(ans);
      System.err.println(str);
      System.err.println(ans);
      context.write(word, null);
    }
  }
}
