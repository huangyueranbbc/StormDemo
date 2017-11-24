package com.hyr.storm.demo.hooks.hook;

import org.apache.storm.hooks.BaseTaskHook;
import org.apache.storm.hooks.info.BoltAckInfo;
import org.apache.storm.hooks.info.BoltExecuteInfo;
import org.apache.storm.hooks.info.SpoutAckInfo;
import org.apache.storm.task.TopologyContext;

import java.util.Map;

/**
 * @Title: MyHook
 * @Package com.hyr.storm.demo.hooks.hook
 * @Description: 自定义Hook
 * @author huangyueran
 * @date 2017/11/24 0024 上午 10:57
*/
public class MyHook extends BaseTaskHook {

    @Override
    public void prepare(Map conf, TopologyContext context) {
        super.prepare(conf, context);
        System.out.println("com.hyr.storm.demo.hooks.hook.MyHook.prepare id doing ......");
    }

    @Override
    public void boltExecute(BoltExecuteInfo info) {
        super.boltExecute(info);
        System.out.println("boltExecute executingTaskId:" + info.executingTaskId);
        System.out.println("boltExecute executedLatencyMs:" + info.executeLatencyMs);
        System.out.println("boltExecute execute msg:" + info.tuple.getString(0));
    }


    @Override
    public void boltAck(BoltAckInfo info) {
        super.boltAck(info);
        System.out.println("boltAck ackingTaskId:" + info.ackingTaskId);
        System.out.println("boltAck processLatencyMs:" + info.processLatencyMs);
        System.out.println("boltAck ack msg:" + info.tuple.getString(0));
    }
}
