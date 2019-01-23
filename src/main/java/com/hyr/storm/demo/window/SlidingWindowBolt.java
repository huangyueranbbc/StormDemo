package com.hyr.storm.demo.window;

import com.hyr.storm.demo.window.spout.WordCountSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author huangyueran
 * @Title: SlidingWindowBolt
 * @Package com.hyr.storm.demo.window
 * @Description: Storm Window窗口
 * @date 2017/11/23 0023 下午 6:28
 */
public class SlidingWindowBolt extends BaseWindowedBolt {

    private static Logger logger = LoggerFactory.getLogger(SlidingWindowBolt.class);

    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(TupleWindow inputWindow) {
        StringBuffer s = new StringBuffer();

        System.out.print("一个窗口内的数据{");

        for (Tuple tuple : inputWindow.get()) { // 遍历滑动窗口内的Tuple
            String str = (String) tuple.getValueByField("words");
            System.out.print(" " + str);
            s.append(str = "\t");
        }
        logger.info("}");

        collector.emit(new Values(s));
    }

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new WordCountSpout(), 1);
        builder.setBolt("slidingwindowbolt",
                // 设置滑动窗口 滑动窗口的长度:3 每当有1个tuple到来后触发一次
                new SlidingWindowBolt().withWindow(new Count(3), new Count(1)),
                1).shuffleGrouping("spout");
        Config conf = new Config();
        conf.setDebug(true);
        conf.setNumWorkers(1);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("Window", conf, builder.createTopology());
    }
}

