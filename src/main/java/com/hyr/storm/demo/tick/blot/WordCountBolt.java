package com.hyr.storm.demo.tick.blot;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.TupleUtils;
import org.apache.storm.shade.org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;

/**
 * @Title: WordCountBolt
 * @Package com.hyr.storm.demo.tick.blot
 * @Description: tick 定时统计。 进行WordCount统计，定时每隔3秒发送WordCount统计的结果
 * @author huangyueran
 * @date 2017/11/22 0022 下午 4:50
*/
public class WordCountBolt extends BaseRichBolt {

    Map<String, Integer> _counts = new HashMap<String, Integer>();

    private OutputCollector _outputCollector;

    /**
     * 声明针对当前组件的特殊的Configuration配置
     */
    @Override
    public Map<String, Object> getComponentConfiguration() {
        // 添加tick的config,定时任务
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 3); //加入Tick时间窗口 3秒执行WordCount统计
        return conf;
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this._outputCollector = outputCollector;
    }

    public void execute(Tuple tuple) {
        //时间窗口定义为3s内的统计数据，定时时间到期后，发射到下一阶段的bolt进行处理
        //发射完成后retun结束，开始新一轮的时间窗口计数操作
        if (TupleUtils.isTick(tuple)) { // 如果是定时的Tuple
            System.out.println(new DateTime().toString("yyyy-MM-dd HH:mm:ss") + " 每隔3秒执行一次发送wordcount结果： 结果大小:" + _counts.size());
            _outputCollector.emit(new Values(_counts));//3秒发送一次
            _counts = new HashMap<String, Integer>();//这个地方，不能执行clear方法，可以再new一个对象，否则下游接受的数据，有可能为空 或者深度copy也行，推荐new
            return;
        }

        //如果没到定时时间，就继续统计wordcount
        System.out.println("线程" + Thread.currentThread().getName() + "  map 缓冲统计中......  map size：" + _counts.size());
        String word = tuple.getStringByField("words");
        Integer count = _counts.get(word);
        if (count == null) {
            count = 0;
        }
        count++;
        _counts.put(word, count);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }
}
