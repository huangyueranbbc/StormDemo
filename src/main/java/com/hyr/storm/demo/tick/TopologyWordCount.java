package com.hyr.storm.demo.tick;

import com.hyr.storm.demo.tick.blot.ShowBolt;
import com.hyr.storm.demo.tick.blot.WordCountBolt;
import com.hyr.storm.demo.tick.spout.SourceSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.TopologyBuilder;

/**
 * @Title: TopologyWordCount
 * @Package com.hyr.storm.demo.tick
 * @Description: tick 定时统计
 * @author huangyueran
 * @date 2017/11/22 0022 下午 4:49
*/
public class TopologyWordCount {

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        //设置数据源 数据源每隔6秒发送一次数据
        // 会产生一个ComponentCommon(拓扑的基础对象)，包含组件和流的对应关系、分组策略、定义的输出字段、是否是DirectStream、并行度(线程数)、组件配置信息json_conf
        builder.setSpout("spout", new SourceSpout(), 1); // 会返回一个SpoutDecarer对象 --> SpoutGetter(更新默认配置)

        //读取spout，wordcount  定时发送单词统计数据(tick周期3秒)
        builder.setBolt("wordcount", new WordCountBolt(), 1) // 会返回一个BlotDecarer对象 --> BoltGetter(更新默认配置)
                // 会产生一个ComponentCommon(拓扑的基础对象)，包含组件和流的对应关系、分组策略、定义的输出字段、是否是DirectStream、并行度(线程数)、组件配置信息json_conf
                .shuffleGrouping("spout");
                // 通过ConfigGetter替换json_conf组件的配置信息

        //读取count后的数据，进行缓冲打印 （tick周期3秒，定时3秒，进行打印）
        builder.setBolt("show", new ShowBolt(), 1) // 会返回一个BlotDecarer对象 --> BoltGetter(更新默认配置)
                // 会产生一个ComponentCommon(拓扑的基础对象)，包含组件和流的对应关系、分组策略、定义的输出字段、是否是DirectStream、并行度(线程数)、组件配置信息json_conf
                .shuffleGrouping("wordcount");
                // 通过ConfigGetter替换json_conf组件的配置信息

        Config config = new Config();
        config.setDebug(false);
        config.setMaxTaskParallelism(1);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("word-count", config, builder.createTopology());
        Thread.sleep(3000000);
        cluster.shutdown();
    }
}
