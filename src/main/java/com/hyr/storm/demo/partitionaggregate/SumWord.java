package com.hyr.storm.demo.partitionaggregate;


import com.hyr.storm.demo.reliability.spout.MessageSpout;
import org.apache.storm.trident.operation.BaseAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @Title: SumWord
 * @Package com.hyr.storm.demo.partitionaggregate
 * @Description: Storm的PartitionAggregate
 * @author huangyueran
 * @date 2017/11/22 0022 上午 11:21
*/
public class SumWord extends BaseAggregator<Map<String, Integer>> {

    private final static Logger logger = LoggerFactory.getLogger(SumWord.class);

    /**
     *
     */
    private static final long serialVersionUID = 1L;


    /**
     * 属于哪个batch
     */
    private Object batchId;

    /**
     * 属于哪个分区
     */
    private int partitionId;

    /**
     * 分区数量
     */
    private int numPartitions;

    /**
     * 用来统计
     */
    private Map<String, Integer> state;


    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        state = new HashMap<String, Integer>();
        partitionId = context.getPartitionIndex();
        numPartitions = context.numPartitions();

        logger.info("SumWord.prepare" + ";partitionId=" + partitionId + ";partitions=" + numPartitions
                + ",batchId:" + batchId);
    }


    public Map<String, Integer> init(Object batchId, TridentCollector collector) {
        logger.info("SumWord.init" + ";partitionId=" + partitionId + ";partitions=" + numPartitions
                + ",batchId:" + batchId);
        this.batchId = batchId;
        logger.info("state=========="+state);
        return state;
    }


    public void aggregate(Map<String, Integer> val, TridentTuple tuple, TridentCollector collector) {
        logger.info(tuple + ";partitionId=" + partitionId + ";partitions=" + numPartitions
                + ",batchId:" + batchId);
        String word = tuple.getString(0);
        if (null != val.get(word)) {
            val.put(word, val.get(word) + 1);
        } else {
            val.put(word, 0);
        }

        logger.info("sumWord================" + val);
    }

    public void complete(Map<String, Integer> val, TridentCollector collector) {
        collector.emit(new Values(val));
    }
}