package com.hyr.storm.demo.tick;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import com.hyr.storm.demo.tick.blot.ShowBolt;
import com.hyr.storm.demo.tick.blot.WordCountBolt;
import com.hyr.storm.demo.tick.spout.SourceSpout;


public class TopologyWordCount {

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        //设置数据源 数据源每隔6秒发送一次数据
        builder.setSpout("spout", new SourceSpout(), 1);
        //读取spout，wordcount  定时发送单词统计数据(tick周期3秒)
        builder.setBolt("wordcount", new WordCountBolt(), 1).shuffleGrouping("spout");
        //读取count后的数据，进行缓冲打印 （tick周期3秒，定时3秒，进行打印）
        builder.setBolt("show", new ShowBolt(), 1).shuffleGrouping("wordcount");

        Config config = new Config();
        config.setDebug(false);
        config.setMaxTaskParallelism(1);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("word-count", config, builder.createTopology());
        Thread.sleep(3000000);
        cluster.shutdown();
    }
}
