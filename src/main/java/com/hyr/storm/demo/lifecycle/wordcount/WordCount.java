package com.hyr.storm.demo.lifecycle.wordcount;

import com.hyr.storm.demo.lifecycle.wordcount.blot.SplitSentenceBlot;
import com.hyr.storm.demo.lifecycle.wordcount.blot.WordCountBlot;
import com.hyr.storm.demo.lifecycle.wordcount.spout.WordCountSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;


/**
 * @Title: WordCount
 * @Package com.hyr.storm.demo.lifecycle.wordcount
 * @Description: WordCount 统计单词个数
 * @author huangyueran
 * @date 2017/11/22 0022 上午 11:21
*/
public class WordCount {

    public static void main(String[] args) throws InterruptedException, InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        TopologyBuilder topologyBuilder = new TopologyBuilder(); // 创建构建Topology的对象
        topologyBuilder.setSpout("wordcountspout", new WordCountSpout(), 2); // 创建2个WordCountSpout实例(task)
        topologyBuilder.setBolt("wordcountsplitblot", new SplitSentenceBlot(), 4) // 创建4个SplitSentenceBlot实例(task)
                .shuffleGrouping("wordcountspout"); // 指定输入数据源 消息随机分发策略
        topologyBuilder.setBolt("wordcountblot", new WordCountBlot(), 4) // 创建4个WordCountBlot实例(task)
                .fieldsGrouping("wordcountsplitblot", new Fields("word")); // 指定输入数据源 消息按字段分发策略

        // 创建配置文件 HashMap
        Config config = new Config();
        //config.setDebug(true);

        // StormSubmitter和LocalCluster 拓扑提交器
        if (args != null && args.length > 0) { // 通过storm jar命令传入配置参数
            config.setNumWorkers(2); //  指定Worker个数 默认在集群节点平分负载
            StormSubmitter.submitTopologyWithProgressBar(args[0], config, topologyBuilder.createTopology());// 提交topology任务
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("word-count", config, topologyBuilder.createTopology());// 提交topology任务
            Thread.sleep(10000);//运行10秒结束
            cluster.shutdown();
        }

    }

}
