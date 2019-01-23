package com.hyr.storm.demo.reliability.spout;

import com.hyr.storm.demo.stream.join.blot.SimpleJoinBolt;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @Title: MessageSpout
 * @Package com.hyr.storm.demo.reliability.spout
 * @Description: 输入数据源
 * @author huangyueran
 * @date 2017/11/22 0022 上午 11:24
*/
public class MessageSpout implements IRichSpout {

    private final static Logger logger = LoggerFactory.getLogger(MessageSpout.class);

    private static final long serialVersionUID = -4664068313075450186L;

    private int index = 0;

    private String[] lines;

    private SpoutOutputCollector _SpoutCollector;

    public MessageSpout() {
        lines = new String[]{
                "0,zero",
                "1,one",
                "2,two",
                "3,three",
                "4,four",
                "5,five",
                "6,six",
                "7,seven",
                "8,eight",
                "9,nine"
        };
    }

    /**
     * 声明输出字段
     *
     * @param declarer
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("line")); // 声明输出字段
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this._SpoutCollector = collector;
    }

    public void nextTuple() {
        // 将一行lines数据中的单词一个一个的进行发送
        if (index < lines.length) {
            String l = lines[index];
            _SpoutCollector.emit(new Values(l), index); // 发送单词 index作为 Message Id
            index++;
        }
    }

    /**
     * 发送成功的处理
     *
     * @param msgId
     */
    public void ack(Object msgId) {
        logger.info("message sends successfully (msgId = " + msgId + ")");
    }

    /**
     * 发送失败的处理
     *
     * @param msgId
     */
    public void fail(Object msgId) {
        logger.info("error : message sends unsuccessfully (msgId = " + msgId + ")");
        logger.info("resending...");
        _SpoutCollector.emit(new Values(lines[(Integer) msgId]), msgId); // 如果发送失败 则重新发送该数据
        logger.info("resend successfully");
    }

    public void close() {

    }

    public void activate() {
    }

    public void deactivate() {

    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
