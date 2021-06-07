package DSPPTest.student.flink.twitter_hot_topics;


// import DSPPCode.flink.digital_conversion.impl.DigitalConversionImpl;
import DSPPCode.flink.twitter_hot_topics.impl.TwitterHotTopicsImpl;
import DSPPCode.flink.twitter_hot_topics.question.TwitterHotTopics;
import DSPPTest.student.TestTemplate;

import org.junit.Test;


import static DSPPTest.util.FileOperator.deleteFolder;
import static DSPPTest.util.FileOperator.readFile2String;

import static DSPPTest.util.FileOperator.readFolder2String;
import static DSPPTest.util.Verifier.verifyList;

/**
 * @author chenqh
 * @version 1.0.0
 * @date 2019-11-30
 */
public class TwitterHotTopicsTest extends TestTemplate {


  @Test
  public void test() throws Exception {

    String inputFile = root + "/flink/twitter_hot_topics/input";
    String outputPath = outputRoot + "/flink/twitter_hot_topics";
    String answerFile = root + "/flink/twitter_hot_topics/answer";
    String stopWordsFile = root + "/flink/twitter_hot_topics/stopWord";

    // 删除旧输出
    deleteFolder(outputPath);

    // 执行
    String[] args = {inputFile, outputPath, stopWordsFile};
    TwitterHotTopicsImpl twitterHotTopics = new TwitterHotTopicsImpl();
    twitterHotTopics.run(args);

    // 检验结果
    verifyList(readFolder2String(outputPath), readFile2String(answerFile));

    System.out.println("恭喜通过~");
  }
}
