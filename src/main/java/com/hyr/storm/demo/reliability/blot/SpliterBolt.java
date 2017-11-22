package com.hyr.storm.demo.reliability.blot;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * @Title: SpliterBolt
 * @Package com.hyr.storm.demo.reliability.blot
 * @Description: 对记录进行拆分
 * @author huangyueran
 * @date 2017/11/22 0022 上午 11:24
*/
public class SpliterBolt implements IRichBolt {

    private static final long serialVersionUID = 6266473268990329206L;

    private OutputCollector _OutputCollector;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this._OutputCollector = collector;
    }

    /**
     * 声明输出字段
     *
     * @param declarer
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

    /**
     * @param input
     */
    public void execute(Tuple input) {
        String line = input.getString(0); // 获取一行记录
        //input.getStringByField("");
        
        String[] words = line.split(","); // 拆分一行记录 4,four 获取word和MsgId
        for (String word : words) { // 发送
            _OutputCollector.emit(input, new Values(word)); // 发送word和MsgId
        }
        _OutputCollector.ack(input); // ack 确认发送成功
    }

    public void cleanup() {

    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
