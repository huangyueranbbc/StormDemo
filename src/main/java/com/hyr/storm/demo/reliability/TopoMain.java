package com.hyr.storm.demo.reliability;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import com.hyr.storm.demo.reliability.blot.FileWriterBolt;
import com.hyr.storm.demo.reliability.blot.SpliterBolt;
import com.hyr.storm.demo.reliability.spout.MessageSpout;

/**
 * @author huangyueran
 * @Title: TopoMain
 * @Package com.hyr.storm.demo.reliability
 * @Description: Storm Reliability可靠性
 * @date 2017/11/22 0022 上午 11:22
 */
public class TopoMain {

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        TopologyBuilder builder = new TopologyBuilder(); //创建一个构建Topology的对象
        builder.setSpout("spout", new MessageSpout()); // 设置输入数据源
        builder.setBolt("bolt-1", new SpliterBolt()).shuffleGrouping("spout");
        builder.setBolt("bolt-2", new FileWriterBolt()).shuffleGrouping("bolt-1");

        Config conf = new Config();
        conf.setDebug(false);

        LocalCluster cluster = new LocalCluster();

        // 提交Topology任务
        cluster.submitTopology("reliability", conf, builder.createTopology());
    }
}
