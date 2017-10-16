package org.example.etl;

import java.io.File;
import java.io.IOException;
import java.util.TimeZone;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import org.apache.apex.api.EmbeddedAppLauncher;
import org.apache.apex.api.Launcher;
import org.apache.apex.api.Launcher.AppHandle;
import org.apache.apex.api.Launcher.LaunchMode;
import org.apache.apex.api.Launcher.ShutdownMode;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;

import com.datatorrent.api.Attribute;

import info.batey.kafka.unit.KafkaUnit;
import info.batey.kafka.unit.KafkaUnitRule;
import kafka.producer.KeyedMessage;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotNull;

public class SampleApplicationTest
{
  private final String testTopicData = "dataTopic";
  private final String testTopicResult = "resultTopic";

  private TimeZone defaultTZ;

  private final int brokerPort = NetUtils.getFreeSocketPort();
  private final int zkPort = NetUtils.getFreeSocketPort();

  @Rule
  public KafkaUnitRule kafkaUnitRule = new KafkaUnitRule(zkPort, brokerPort);

  {
    // required to avoid 50 partitions auto creation
    this.kafkaUnitRule.getKafkaUnit().setKafkaBrokerConfig("num.partitions", "1");
    this.kafkaUnitRule.getKafkaUnit().setKafkaBrokerConfig("offsets.topic.num.partitions", "1");
  }

  private static String outputFolder = "target/output/";

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() throws Exception
  {
    defaultTZ = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone("GMT"));

    KafkaUnit ku = kafkaUnitRule.getKafkaUnit();
    // topic creation is async and the producer may also auto-create it
    ku.createTopic(testTopicData, 1);
    ku.createTopic(testTopicResult, 1);

    outputFolder += testName.getMethodName() + "/";
  }

  @After
  public void tearDown() throws Exception
  {
    TimeZone.setDefault(defaultTZ);
  }

  @Test
  public void test() throws Exception
  {
    Configuration conf = new Configuration(false);
    conf.addResource(this.getClass().getResourceAsStream("/META-INF/properties.xml"));

    conf.set("apex.operator.KafkaInput.prop.topics", testTopicData);
    conf.set("apex.operator.KafkaInput.prop.clusters", "localhost:"+brokerPort);
    conf.set("apex.operator.KafkaInput.prop.initialOffset", "EARLIEST");
    conf.set("folderPath", outputFolder);
    conf.set("fileName", "out.tmp");

    SampleApplication app = new SampleApplication();

    EmbeddedAppLauncher<?> launcher = Launcher.getLauncher(LaunchMode.EMBEDDED);
    Attribute.AttributeMap launchAttributes = new Attribute.AttributeMap.DefaultAttributeMap();
    launchAttributes.put(EmbeddedAppLauncher.RUN_ASYNC, true); // terminate after results are available
    AppHandle appHandle = launcher.launchApp(app, conf, launchAttributes);

    String[] messages = {"15/02/2016 10:15:00 +0000,1,paint1,11",
        "15/02/2016 10:16:00 +0000,2,paint2,12",
        "15/02/2016 10:17:00 +0000,3,paint3,13",
        "15/02/2016 10:18:00 +0000,4,paint4,14",
        "15/02/2016 10:19:00 +0000,5,paint5,15",
        "15/02/2016 10:10:00 +0000,6,abcde6,16"};
    KafkaUnit ku = kafkaUnitRule.getKafkaUnit();
    for (String msg : messages) {
      ku.sendMessages(new KeyedMessage<String, String>(testTopicData, msg));
    }
    // timeout after 50 seconds
    final File output = waitForFile(outputFolder, 50 * 1000);
    assertNotNull(output);
    appHandle.shutdown(ShutdownMode.KILL);

    // compare actual and expected output
    final String[] actualLines = FileUtils.readLines(output).toArray(new String[0]);
    final String[] expectedLines = new String[] {
    "15/02/2016 10:18:00 +0000,15/02/2016 12:00:00 +0000,OILPAINT4",
    "",
    "15/02/2016 10:19:00 +0000,15/02/2016 12:00:00 +0000,OILPAINT5",
    "" };
    assertArrayEquals(expectedLines, actualLines);

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
