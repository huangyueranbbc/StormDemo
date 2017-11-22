package com.hyr.storm.demo.tick.blot;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.TupleUtils;
import org.apache.storm.shade.org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;

public class ShowBolt extends BaseRichBolt {

    Map<String, Integer> _counts = new HashMap<String, Integer>();

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

    }

    public void execute(Tuple tuple) {
        // 每3秒 打印输出一次单词统计结果
        Map<String, Integer> counts = (Map<String, Integer>) tuple.getValue(0);

        // 合并
        for(String key:counts.keySet()){
            if(_counts.containsKey(key)){ // 如果存在Key
                _counts.put(key,_counts.get(key)+counts.get(key));
            }else { // 如果不存在key
                _counts.put(key,counts.get(key));
            }
        }

        // 打印
        for (Map.Entry<String, Integer> kv : _counts.entrySet()) {
            System.out.println(kv.getKey() + "\t" + kv.getValue());
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
