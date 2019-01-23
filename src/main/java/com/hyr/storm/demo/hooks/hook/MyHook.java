package com.hyr.storm.demo.hooks.hook;

import com.hyr.storm.demo.tick.blot.ShowBolt;
import org.apache.storm.hooks.BaseTaskHook;
import org.apache.storm.hooks.info.BoltAckInfo;
import org.apache.storm.hooks.info.BoltExecuteInfo;
import org.apache.storm.hooks.info.SpoutAckInfo;
import org.apache.storm.task.TopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @Title: MyHook
 * @Package com.hyr.storm.demo.hooks.hook
 * @Description: 自定义Hook
 * @author huangyueran
 * @date 2017/11/24 0024 上午 10:57
*/
public class MyHook extends BaseTaskHook {

    private static Logger logger = LoggerFactory.getLogger(MyHook.class);

    @Override
    public void prepare(Map conf, TopologyContext context) {
        super.prepare(conf, context);
        logger.info("com.hyr.storm.demo.hooks.hook.MyHook.prepare id doing ......");
    }

    @Override
    public void boltExecute(BoltExecuteInfo info) {
        super.boltExecute(info);
        logger.info("boltExecute executingTaskId:" + info.executingTaskId);
        logger.info("boltExecute executedLatencyMs:" + info.executeLatencyMs);
        logger.info("boltExecute execute msg:" + info.tuple.getString(0));
    }


    @Override
    public void boltAck(BoltAckInfo info) {
        super.boltAck(info);
        logger.info("boltAck ackingTaskId:" + info.ackingTaskId);
        logger.info("boltAck processLatencyMs:" + info.processLatencyMs);
        logger.info("boltAck ack msg:" + info.tuple.getString(0));
    }
}
