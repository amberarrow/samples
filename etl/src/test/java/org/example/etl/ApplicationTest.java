package org.example.etl;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import org.apache.apex.malhar.kafka.EmbeddedKafka;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.LocalMode;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ApplicationTest
{
  private final String testTopicData = "dataTopic";
  private final String testTopicResult = "resultTopic";

  private TimeZone defaultTZ;
  private EmbeddedKafka kafka;

  private static String outputFolder = "target/output/";

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() throws Exception
  {
    defaultTZ = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone("GMT"));

    kafka = new EmbeddedKafka();
    kafka.start();
    kafka.createTopic(testTopicData);
    kafka.createTopic(testTopicResult);

    outputFolder += testName.getMethodName() + "/";
  }

  @After
  public void tearDown() throws Exception
  {
    kafka.stop();

    TimeZone.setDefault(defaultTZ);
  }

  @Test
  public void test() throws Exception
  {
    try {
      LocalMode lma = LocalMode.newInstance();
      Configuration conf = new Configuration(false);
      conf.addResource(this.getClass().getResourceAsStream("/META-INF/properties.xml"));

      conf.set("dt.operator.KafkaInput.prop.topics", testTopicData);
      conf.set("dt.operator.KafkaInput.prop.clusters", kafka.getBroker());
      conf.set("folderPath", outputFolder);
      conf.set("fileName", "out.tmp");

      SampleApplication app = new SampleApplication();

      lma.prepareDAG(app, conf);

      LocalMode.Controller lc = lma.getController();

      lc.runAsync();
      kafka.publish(testTopicData,
                    Arrays.asList("15/02/2016 10:15:00 +0000,1,paint1,11",
                                  "15/02/2016 10:16:00 +0000,2,paint2,12",
                                  "15/02/2016 10:17:00 +0000,3,paint3,13",
                                  "15/02/2016 10:18:00 +0000,4,paint4,14",
                                  "15/02/2016 10:19:00 +0000,5,paint5,15",
                                  "15/02/2016 10:10:00 +0000,6,abcde6,16"));

      // timeout after 50 seconds
      final File output = waitForFile(outputFolder, 50 * 1000);
      assertNotNull(output);
      lc.shutdown();


      // compare actual and expected output
      final String[] actualLines = FileUtils.readLines(output).toArray(new String[0]);
      final String[] expectedLines = new String[] {
          "15/02/2016 10:18:00 +0000,15/02/2016 12:00:00 +0000,OILPAINT4",
          "",
          "15/02/2016 10:19:00 +0000,15/02/2016 12:00:00 +0000,OILPAINT5",
          "" };
      assertArrayEquals(expectedLines, actualLines);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }  // test

  // Helper routine to wait for output file to be created; arguments:
  //   dest    : destination directory in which output file is created
  //   timeout : milliseconds to wait before giving up
  //
  // Returns true if the destination file was detected with nonempty content within the timeout
  // interval, false otherwise.
  //
  static File waitForFile(final String dest, final int timeout) throws IOException, InterruptedException {
    final long start = System.currentTimeMillis();
    final File dir = new File(dest);
    do {
      if (dir.exists()) {
        final File[] list = dir.listFiles();
        if (list.length > 0 && 0 != FileUtils.readLines(list[0]).size()) {
            return list[0];
        }
      }
      Thread.sleep(500);
    } while (System.currentTimeMillis() - start < timeout);

    return null;
  }  // waitForFile

}
