package com.hyr.storm.demo.hooks.spout;

import com.hyr.storm.demo.hooks.hook.MyHook;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

/**
 * WordCount数据源
 * spout生命周期:
 * 1.构造方法
 * 2.declareOutputFields
 * 3.open
 * 4.activate
 * 5.nextTuple(反复调用)
 * 6.deactivate
 */
public class WordCountSpout extends BaseRichSpout {
    SpoutOutputCollector _spoutOutputCollector;
    Random _random;

    /**
     * 初始化 调用一次
     *
     * @param map
     * @param topologyContext
     * @param spoutOutputCollector
     */
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        _spoutOutputCollector = spoutOutputCollector;
        _random = new Random();
    }

    /**
     * 反复调用
     */
    public void nextTuple() {
        String[] sentences = new String[]{"hello my name is tuyu", "hi she is Lily", "how do you do",
                "that's great", "how are you"};
        String sentence = sentences[_random.nextInt(sentences.length)];// 随机发送
        _spoutOutputCollector.emit(new Values(sentence)); // 发送Values
        Utils.sleep(100);// 每隔0.1秒发送一个句子
    }

    /**
     * 声明了该spout输出的字段个数，供下游使用。调用一次
     * 定义字段
     *
     * @param outputFieldsDeclarer
     */
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // 定义字段
        outputFieldsDeclarer.declare(new Fields("sentence")); // 定义字段
    }

    @Override
    public void activate() {
        super.activate();
    }

    @Override
    public void deactivate() {
        super.deactivate();
    }
}
