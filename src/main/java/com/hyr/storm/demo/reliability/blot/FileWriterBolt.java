package com.hyr.storm.demo.reliability.blot;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

/**
 * @Title: FileWriterBolt
 * @Package com.hyr.storm.demo.reliability.blot
 * @Description: 写入文件
 * @author huangyueran
 * @date 2017/11/22 0022 上午 11:24
*/
public class FileWriterBolt implements IRichBolt {

    private static final long serialVersionUID = -8619029556495402143L;

    private FileWriter writer;

    private OutputCollector _outputCollector;


    private int count = 0;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this._outputCollector = collector;
        try {
            writer = new FileWriter("e://reliability.txt");
        } catch (IOException e) {
        }
    }

    public void execute(Tuple input) {
        String word = input.getString(0);
        if (count == 5) { // 当count为5 发送失败
            _outputCollector.fail(input);
        } else {
            try {
                // 写入到磁盘
                writer.write(word);
                writer.write("\r\n");
                writer.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
            // // 锚定 如果单词 tuple 在后续处理过程中失败了，作为这棵 tuple 树的根节点的原始 Spout tuple 就会被重新处理。
            _outputCollector.emit(input, new Values(word));
            _outputCollector.ack(input);
        }
        count++;
    }

    public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
