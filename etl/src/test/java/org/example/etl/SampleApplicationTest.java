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
    conf.set("outputDir", outputFolder);
    conf.set("fileName", "out.tmp");

    SampleApplication app = new SampleApplication();

    EmbeddedAppLauncher<?> launcher = Launcher.getLauncher(LaunchMode.EMBEDDED);
    Attribute.AttributeMap launchAttributes = new Attribute.AttributeMap.DefaultAttributeMap();
    launchAttributes.put(EmbeddedAppLauncher.RUN_ASYNC, true); // terminate after results are available
    AppHandle appHandle = launcher.launchApp(app, conf, launchAttributes);

    String[] messages = {
        "13/10/2017 11:45:30 +0000,1,v,111-123-4567,222-987-6543,120",
        "13/10/2017 12:25:30 +0000,2,v,111-123-4567,999-987-6543,600",
        "14/10/2017  9:15:00 +0000,3,d,444-823-4864,555-381-7241,300",
        "15/10/2017 10:15:00 +0000,4,d,111-222-3333,999-187-7654,1845"
    };
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
        "13/10/2017 12:25:30 +0000,Voice,111-123-4567,999-987-6543,$120.00",
        "",
        "15/10/2017 10:15:00 +0000,Data,111-222-3333,999-187-7654,$369.00",
        ""
    };

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
