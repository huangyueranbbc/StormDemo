package com.hyr.storm.demo.lifecycle.wordcount.blot;


import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * WordCount处理者 进行Split分词
 * Blot生命周期:
 * 1.构造器 new的时候调用
 * 2.prepare 在worker初始化task的时候调用.
 * 3.declareOutputFields
 * 4.execute(循环执行)
 */
public class SplitSentenceBlot extends BaseBasicBolt {

    /**
     * split分词 每次有tuple进来的时候被调用 进行处理.
     *
     * @param tuple
     * @param basicOutputCollector
     */
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        System.out.println("com.bonree.hyr.storm.blot.SplitSentenceBlot.execute is doing......");
        String sentence = tuple.getString(0); // 获取一行记录
        System.out.println("==> origin sentence : " + sentence);
        String[] words = sentence.split(" "); // 进行split分词
        for (String word : words) {
            basicOutputCollector.emit(new Values(word)); // 发送单词
        }
    }

    /**
     * 定义输出字段
     *
     * @param outputFieldsDeclarer
     */
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        System.out.println("com.bonree.hyr.storm.blot.SplitSentenceBlot.declareOutputFields is doing......");
        outputFieldsDeclarer.declare(new Fields("word")); // 定义字段
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        System.out.println("com.bonree.hyr.storm.blot.SplitSentenceBlot.prepare is doing......");
        super.prepare(stormConf, context);
    }
}
