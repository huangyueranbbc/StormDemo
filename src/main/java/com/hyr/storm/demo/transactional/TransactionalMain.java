package com.hyr.storm.demo.transactional;

import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.testing.Split;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 * @Title: TransactionalMain
 * @Package com.hyr.storm.demo.transactional
 * @Description: TODO
 * @author huangyueran
 * @date 2017/11/23 0023 下午 5:33
*/
public class TransactionalMain {

    public static void main(String[] args) {
        // FixedBatchSpout:数据流   Fields fields(标签), int maxBatchSize(每次输出多少Values,也就是输出List<Valuse>的长度), List... outputs(数据源输出的List<Values>)
        FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 4, // 每次输出4个Values
                new Values("the cow jumped over the moon"),
                new Values("the man went to the store and bought some candy"),
                new Values("four score and seven years ago"),
                new Values("how many apples can you eat"));

        spout.setCycle(true); // 循环发送数据

        TridentTopology topology = new TridentTopology();

        /*
        public interface State {
            void beginCommit(Long var1);

            void commit(Long var1);
        }

        */
        TridentState wordCounts;
        wordCounts = topology.newStream("spout1", spout)
                .each(new Fields("sentence"), new Split(), new Fields("word"))
                .groupBy(new Fields("word"))
                .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
                .parallelismHint(6);
    }
}
