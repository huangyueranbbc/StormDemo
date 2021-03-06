package com.hyr.storm.demo.tick.spout;

import org.apache.storm.Config;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.TupleUtils;
import org.apache.storm.utils.Utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
/**
 * @Title: SourceSpout
 * @Package com.hyr.storm.demo.tick.spout
 * @Description: tick 定时统计 数据源,每隔6秒发送一次数据
 * @author huangyueran
 * @date 2017/11/22 0022 下午 4:49
*/
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
