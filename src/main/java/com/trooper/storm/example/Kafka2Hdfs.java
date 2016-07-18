package com.trooper.storm.example;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import com.alibaba.jstorm.client.ConfigExtension;
import com.trooper.storm.hdfs.DailyRotationPolicy;
import com.trooper.storm.hdfs.FileFolderByDateNameFormat;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import storm.kafka.*;

/**
 * example of kafkaspout and hdfsbolt
 */
public class Kafka2Hdfs {
    public static void main(String[] args)
    {
        Kafka2Hdfs logTopology = new Kafka2Hdfs();
        Config config = new Config();
        config.setDebug(false);
        StormTopology stormTopology;

        Config conf = new Config();
        conf.setDebug(true);
        conf.put("spout.single.thread", true);

        if (args != null && args.length > 0) {
            stormTopology = logTopology.buildTopology(true);
            conf.setNumWorkers(3);
            conf.setNumAckers(3);

            ConfigExtension.setUserDefinedLog4jConf(conf, "File:/opt/app/jstorm/jstorm-2.1.1/conf/jstorm.log4j.properties");
            try {
                StormSubmitter.submitTopology(args[0], conf, stormTopology);
            } catch (AlreadyAliveException e) {
                e.printStackTrace();
            } catch (InvalidTopologyException e) {
                e.printStackTrace();
            }
        } else {
            stormTopology = logTopology.buildTopology(false);
            conf.setMaxTaskParallelism(3);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("kafka2hdfs", conf, stormTopology);
            try {
                Thread.sleep(200000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            cluster.shutdown();
        }
    }

    public StormTopology buildTopology(boolean isLocal) {
        TopologyBuilder builder = new TopologyBuilder();

        //kafkaspout example
        String topic = "TEST_TOPIC"; //kafka topic names
        String zkRoot = "/kafkastorm"; //keep offset in this zookeeper node
        String id = "kafka2hdfs"; // consumer id
        String kafkaZk = "1.1.1.1:2181,1.1.1.2:2181,1.1.1.3:2181"; //zookeeper address that zookeeper uses
        BrokerHosts brokerHosts = new ZkHosts(kafkaZk);
        SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, topic, zkRoot, id);
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        kafkaConfig.ignoreZkOffsets = true;
        kafkaConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime(); //works only when ignoreZkOffsets is true

        builder.setSpout(topic, new KafkaSpout(kafkaConfig), 10);

        //hdfsbolt example
        SyncPolicy syncPolicy = new CountSyncPolicy(100); //hdfs do the sync every 100 messages
        DailyRotationPolicy rotationPolicy = new DailyRotationPolicy(); //rotate file every day
        FileFolderByDateNameFormat fileNameFormat = new FileFolderByDateNameFormat()
                .withPath("/user/walker"); //path in hdfs
        RecordFormat format = new DelimitedRecordFormat()
                .withFieldDelimiter(","); //delimiter of different fields
        HdfsBolt hdfsbolt = new HdfsBolt()
                .withFsUrl("hdfs://1.1.1.1:0000") //hdfs url
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(format)
                .withRotationPolicy(rotationPolicy)
                .withSyncPolicy(syncPolicy);

        builder.setBolt("tohdfs", hdfsbolt, 3).shuffleGrouping("topic");

        return builder.createTopology();
    }
}
