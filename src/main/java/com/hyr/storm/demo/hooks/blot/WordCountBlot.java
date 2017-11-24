package com.hyr.storm.demo.hooks.blot;


import com.hyr.storm.demo.hooks.hook.MyHook;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

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

    private HashMap<String, Integer> counterMapper = new HashMap<String, Integer>();

    /**
     * 统计单词 每次有tuple进来的时候被调用 进行处理.
     *
     * @param tuple
     * @param basicOutputCollector
     */
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String word = tuple.getString(0); // 获取一个单词word
        if (counterMapper.containsKey(word)) { // 如果统计过这个单词 count++
            Integer count = counterMapper.get(word);
            count++;
            System.out.println("wordCount receive " + word + " ------> " + count);
            counterMapper.put(word, count);
        } else { // 如果没有统计过这个单词 增加新词 count为1
            System.out.println("wordCount receive " + word + " ------> " + 1);
            counterMapper.put(word, 1);
        }
    }

    /**
     * @param outputFieldsDeclarer
     */
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }

    /**
     * 实现cleanup接口，在cluster.shutdown()的时候调用，打印单词统计结果
     */
    @Override
    public void cleanup() {
        for (String key : counterMapper.keySet()) {
            System.out.println("total ---> " + key + " : " + counterMapper.get(key));
        }
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
        context.addTaskHook(new MyHook());
    }
}
