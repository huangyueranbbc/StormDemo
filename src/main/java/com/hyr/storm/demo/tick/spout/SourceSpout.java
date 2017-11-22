package com.hyr.storm.demo.tick.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

public class SourceSpout extends BaseRichSpout {
    SpoutOutputCollector _spoutOutputCollector;
    Random _random;
    String[] _sentences = null;


    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this._spoutOutputCollector=spoutOutputCollector;
        this._random = new Random();
        _sentences=new String[]{"spark","hadoop","hbase","hive","flume","kafka","zookeeper","storm","mahout","solr","redis"};

    }

    public void nextTuple() {
        Utils.sleep(6000); // 6秒发送一次数据
        String word = _sentences[_random.nextInt(_sentences.length)];
        _spoutOutputCollector.emit(new Values(word));
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("words"));
    }
}
