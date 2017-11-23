package com.hyr.storm.demo.lifecycle.wordcount.spout;

import org.apache.storm.shade.org.apache.commons.io.FileUtils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
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
        System.out.println("com.bonree.hyr.storm.spout.WordCountSpout.open doing......");
        _spoutOutputCollector = spoutOutputCollector;
        _random = new Random();
    }

    /**
     * 反复调用
     */
    public void nextTuple() {
        System.out.println("com.bonree.hyr.storm.spout.WordCountSpout.nextTuple doing......");
        // TODO 读取文件
        // 读取文件列表
        Collection<File> listFiles = FileUtils.listFiles(new File("d:/test"), new String[]{"txt"}, true);
        // 循环每个文件
        for (File file : listFiles) {
            // 行格式发送
            try {
                List<String> lines = FileUtils.readLines(file);
                for (String line : lines) {
                    _spoutOutputCollector.emit(new Values(line));
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            // 文件已经处理完成
            try {
                File srcFile = file.getAbsoluteFile();
                File destFile = new File(srcFile + ".done." + System.currentTimeMillis());
                FileUtils.moveFile(srcFile, destFile);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        /*
        String[] sentences = new String[]{"hello my name is tuyu", "hi she is Lily", "how do you do",
                "that's great", "how are you"};
        String sentence = sentences[_random.nextInt(sentences.length)];// 随机发送
        _spoutOutputCollector.emit(new Values(sentence)); // 发送Values
        Utils.sleep(100);// 每隔0.1秒发送一个句子
        */
    }

      /**
     * 声明了该spout输出的字段个数，供下游使用。调用一次
     * 定义字段
     *
     * @param outputFieldsDeclarer
     */
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        System.out.println("com.bonree.hyr.storm.spout.WordCountSpout.declareOutputFields is doing......");
        // 定义字段
        outputFieldsDeclarer.declare(new Fields("sentence")); // 定义字段
    }

    @Override
    public void activate() {
        System.out.println("com.bonree.hyr.storm.spout.WordCountSpout.activate is doing......");
        super.activate();
    }

    @Override
    public void deactivate() {
        System.out.println("com.bonree.hyr.storm.spout.WordCountSpout.deactivate is doing......");
        super.deactivate();
    }
}
