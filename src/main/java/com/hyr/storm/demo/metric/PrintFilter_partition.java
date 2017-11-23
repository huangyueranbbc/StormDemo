package com.hyr.storm.demo.metric;


import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.tuple.TridentTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Title: PrintFilter_partition
 * @Package com.hyr.storm.demo.metric
 * @Description: 输出打印
 * @author huangyueran
 * @date 2017/11/22 0022 上午 11:18
*/
public class PrintFilter_partition extends BaseFilter {

    private static final Logger LOGGER = LoggerFactory.getLogger(PrintFilter_partition.class);

    private static final long serialVersionUID = 1L;

    public boolean isKeep(TridentTuple tuple) {
        LOGGER.info("打印出来的tuple:" + tuple);
        return true;
    }
}  