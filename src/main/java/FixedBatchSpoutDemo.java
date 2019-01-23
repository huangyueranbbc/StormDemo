//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

import org.apache.storm.Config;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.IBatchSpout;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class FixedBatchSpoutDemo implements IBatchSpout {

    private final static Logger logger = LoggerFactory.getLogger(FixedBatchSpoutDemo.class);

    Fields fields;
    List<Object>[] outputs;
    int maxBatchSize;
    HashMap<Long, List<List<Object>>> batches = new HashMap();
    int index = 0;
    boolean cycle = false;

    public FixedBatchSpoutDemo(Fields fields, int maxBatchSize, List... outputs) {
        this.fields = fields;
        this.outputs = outputs;
        this.maxBatchSize = maxBatchSize;
    }

    public void setCycle(boolean cycle) {
        this.cycle = cycle;
    }

    public void open(Map conf, TopologyContext context) {
        int thisTaskIndex = context.getThisTaskIndex();// 0 1 2 3 .......
        logger.info("get task id:" + thisTaskIndex);
        this.index = 0;
    }

    public void emitBatch(long batchId, TridentCollector collector) {
        List<List<Object>> batch = (List) this.batches.get(batchId);
        if (batch == null) {
            batch = new ArrayList();
            if (this.index >= this.outputs.length && this.cycle) {
                this.index = 0;
            }

            for (int i = 0; this.index < this.outputs.length && i < this.maxBatchSize; ++i) {
                ((List) batch).add(this.outputs[this.index]);
                ++this.index;
            }

            this.batches.put(batchId, batch);
        }

        Iterator var7 = ((List) batch).iterator();

        int count=1;
        while (var7.hasNext()) {
            logger.info("count:"+count);
            count++;
            List<Object> list = (List) var7.next();
            collector.emit(list);
        }

    }

    public void ack(long batchId) {
        this.batches.remove(batchId);
    }

    public void close() {
    }

    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.setMaxTaskParallelism(1);
        return conf;
    }

    public Fields getOutputFields() {
        return this.fields;
    }
}
