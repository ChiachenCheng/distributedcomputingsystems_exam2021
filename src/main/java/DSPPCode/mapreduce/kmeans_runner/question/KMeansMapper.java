package DSPPCode.mapreduce.kmeans_runner.question;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import DSPPCode.mapreduce.kmeans_runner.question.utils.CentersOperation;

public class KMeansMapper extends Mapper<LongWritable, Text, Text, Text> {

  private List<List<Double>> centers = new ArrayList<>();

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String[] dimensions;
    List<Double> point = new ArrayList<>();
    int centerIndex = 1;
    double minDistance = Double.MAX_VALUE;

    if (centers.size() == 0) {
      // 获取广播的聚类中心集路径
      String centersPath = context.getCacheFiles()[0].toString();
      // 将聚类中心加载到集合centers
      centers = CentersOperation.getCenters(centersPath, true);
    }

    // 解析数据点
    dimensions = value.toString().split("[,\\t]");
    for (int i = 0; i < dimensions.length - 1; i++) {
      point.add(Double.parseDouble(dimensions[i]));
    }

    // 遍历聚类中心集并计算与数据点的距离
    for (int i = 0; i < centers.size(); i++) {
      double distance = 0;
      List<Double> center = centers.get(i);
      // 计算数据点与当前聚类中心之间的距离
      for (int j = 0; j < center.size(); j++) {
        distance += Math.pow((point.get(j) - center.get(j)), 2);
      }
      distance = Math.sqrt(distance);
      // 如果距离小于当前记录的最小距离则将数据点分配给当前聚类中心（类别号标识）
      if (distance < minDistance) {
        minDistance = distance;
        centerIndex = i + 1;
      }
    }

    // 从输入值中截取数据点
    String pointData = value.toString().split("\t")[0];
    if (KMeansRunner.isLastIteration) {
      context.write(new Text(pointData), new Text(String.valueOf(centerIndex)));
    } else {
      // 输出以类别号为键，数据点为值的键值对
      context.write(new Text(String.valueOf(centerIndex)), new Text(pointData));
    }
  }
}
