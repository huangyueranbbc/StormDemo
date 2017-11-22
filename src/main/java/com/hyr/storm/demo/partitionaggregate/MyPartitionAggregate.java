package com.hyr.storm.demo.partitionaggregate;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.metric.LoggingMetricsConsumer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import storm.trident.TridentTopology;
import storm.trident.testing.FixedBatchSpout;

import java.io.IOException;

/**
 * @Title: MyPartitionAggregate
 * @Package com.hyr.storm.demo.partitionaggregate
 * @Description: Storm的PartitionAggregate
 * @author huangyueran
 * @date 2017/11/22 0022 上午 11:19
*/
public class MyPartitionAggregate {

    public static void main(String[] args) throws AlreadyAliveException,
            InvalidTopologyException, AuthorizationException, IOException {
        FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3,
                new Values("a"), new Values("bb"), new Values("ffasfa"), new Values(
                "gesgc"));
        //设置为true,数据源会源源不断发送
        spout.setCycle(true);
        TridentTopology topology = new TridentTopology();
        topology.newStream("spout", spout)
                .shuffle()
                .partitionAggregate(new Fields("sentence"), new SumWord(),
                        new Fields("sum"))
                /**
                 * 设置3个并发度，可以理解为3个分区操作
                 */
                .parallelismHint(3)
                .each(new Fields("sum"), new PrintFilter_partition());
        Config config = new Config();
        config.setNumWorkers(2);
        config.setNumAckers(1);
        config.setDebug(true);
        config.registerMetricsConsumer(LoggingMetricsConsumer.class, 1);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("trident__partition_aggregate", config,
                topology.build());
        Utils.sleep(10000);
        cluster.killTopology("trident__partition_aggregate");
        cluster.shutdown();
    }

}
