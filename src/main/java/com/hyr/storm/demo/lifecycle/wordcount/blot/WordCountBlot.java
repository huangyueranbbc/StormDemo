package com.hyr.storm.demo.lifecycle.wordcount.blot;


import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Blot生命周期:
 * 1.构造器 new的时候调用
 * 2.prepare 在worker初始化task的时候调用.
 * 3.declareOutputFields
 * 4.execute(循环执行)
 */
public class WordCountBlot extends BaseBasicBolt {

    private static Logger logger = LoggerFactory.getLogger(WordCountBlot.class);

    private HashMap<String, Integer> counterMapper = new HashMap<String, Integer>();

    /**
     * 统计单词 每次有tuple进来的时候被调用 进行处理.
     *
     * @param tuple
     * @param basicOutputCollector
     */
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        // logger.info("类型===========!!!!!"+tuple.getClass().toString()); // TupleImpl
        logger.info("com.hyr.storm.blot.WordCountBlot.execute is doing......");
        String word = tuple.getString(0); // 获取一个单词word
        if (counterMapper.containsKey(word)) { // 如果统计过这个单词 count++
            Integer count = counterMapper.get(word);
            count++;
            logger.info("wordCount receive " + word + " ------> " + count);
            counterMapper.put(word, count);
        } else { // 如果没有统计过这个单词 增加新词 count为1
            logger.info("wordCount receive " + word + " ------> " + 1);
            counterMapper.put(word, 1);
        }
    }

    /**
     * @param outputFieldsDeclarer
     */
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        logger.info("com.hyr.storm.blot.WordCountBlot.declareOutputFields is doing......");
    }

    /**
     * 实现cleanup接口，在cluster.shutdown()的时候调用，打印单词统计结果
     */
    @Override
    public void cleanup() {
        for (String key : counterMapper.keySet()) {
            logger.info("total ---> " + key + " : " + counterMapper.get(key));
        }
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        logger.info("com.hyr.storm.blot.WordCountBlot.prepare is doing......");
        super.prepare(stormConf, context);
    }
}
