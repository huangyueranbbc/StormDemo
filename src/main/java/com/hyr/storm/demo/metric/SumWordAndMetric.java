package com.hyr.storm.demo.metric;


import org.apache.storm.metric.api.CountMetric;
import org.apache.storm.metric.api.MeanReducer;
import org.apache.storm.metric.api.MultiCountMetric;
import org.apache.storm.metric.api.ReducedMetric;
import org.apache.storm.trident.operation.BaseAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * @Title: SumWordAndMetric
 * @Package com.hyr.storm.demo.metric
 * @Description: 统计WordCount的信息
 * @author huangyueran
 * @date 2017/11/22 0022 上午 11:17
*/
public class SumWordAndMetric extends BaseAggregator<Map<String, Integer>> {

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    //定义指标统计对象
    transient CountMetric _countMetric;                 //
    transient MultiCountMetric _wordCountMetric;        //
    transient ReducedMetric _wordLengthMeanMetric;      //


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


    public void prepare(Map conf, TridentOperationContext context) {
        state = new HashMap<String, Integer>();
        partitionId = context.getPartitionIndex();
        numPartitions = context.numPartitions();

        CountMetric countMetric = new CountMetric();

        initMetrics(context);

        System.out.println("SumWord.prepare" + ";partitionId=" + partitionId + ";partitions=" + numPartitions
                + ",batchId:" + batchId);
    }

    /**
     * 初始化Metrics指标统计 计数器
     *
     * @param context
     */
    private void initMetrics(TridentOperationContext context) {
        _countMetric = new CountMetric();
        _wordCountMetric = new MultiCountMetric();
        _wordLengthMeanMetric = new ReducedMetric(new MeanReducer());
        context.registerMetric("execute_count", _countMetric, 5);
        context.registerMetric("word_count", _wordCountMetric, 5);
        context.registerMetric("word_length", _wordLengthMeanMetric, 5);
    }

    //更新计数器
    void updateMetrics(String word) {
        _countMetric.incr();
        _wordCountMetric.scope(word).incr();
        _wordLengthMeanMetric.update(word.length());
    }

    public Map<String, Integer> init(Object batchId, TridentCollector collector) {
        System.out.println("SumWord.init" + ";partitionId=" + partitionId + ";partitions=" + numPartitions
                + ",batchId:" + batchId);
        this.batchId = batchId;
        return state;
    }


    public void aggregate(Map<String, Integer> val, TridentTuple tuple, TridentCollector collector) {
        System.out.println(tuple + ";partitionId=" + partitionId + ";partitions=" + numPartitions
                + ",batchId:" + batchId);
        String word = tuple.getString(0);
        if (null != val.get(word)) {
            val.put(word, val.get(word) + 1);
        } else {
            val.put(word, 0);
        }

        updateMetrics(tuple.getString(0)); // 更新计数器

        System.out.println("sumWord:" + val);
    }

    public void complete(Map<String, Integer> val, TridentCollector collector) {
        collector.emit(new Values(val));
    }
} 