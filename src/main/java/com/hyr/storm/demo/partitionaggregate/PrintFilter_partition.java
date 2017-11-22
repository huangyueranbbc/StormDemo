package com.hyr.storm.demo.partitionaggregate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

/**
 * @Title: PrintFilter_partition
 * @Package com.hyr.storm.demo.partitionaggregate
 * @Description: 输出打印
 * @author huangyueran
 * @date 2017/11/22 0022 上午 11:19
*/
public class PrintFilter_partition extends BaseFilter {

    private static final Logger LOGGER = LoggerFactory.getLogger(PrintFilter_partition.class);

    private static final long serialVersionUID = 1L;

    public boolean isKeep(TridentTuple tuple) {
        LOGGER.info("打印出来的tuple:" + tuple);
        return true;
    }
}  