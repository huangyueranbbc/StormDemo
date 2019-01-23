import com.hyr.storm.demo.tick.blot.WordCountBolt;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.FilterNull;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.operation.builtin.Sum;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.testing.Split;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Title: HelloStorm
 * @Package
 * @Description: Storm的HelloWorld。通过Trident实现WordCount。
 * @author huangyueran
 * @date 2017/11/22 0022 上午 11:18
*/
public class HelloStorm {

    private static Logger logger = LoggerFactory.getLogger(HelloStorm.class);

    public static void main(String[] args) {

        // FixedBatchSpout:数据流   Fields fields(标签), int maxBatchSize(每次输出多少Values,也就是输出List<Valuse>的长度), List... outputs(数据源输出的List<Values>)
        FixedBatchSpoutDemo spout = new FixedBatchSpoutDemo(new Fields("sentence"), 4, // 每次输出4个Values
                new Values("the cow jumped over the moon"),
                new Values("the man went to the store and bought some candy"),
                new Values("four score and seven years ago"),
                new Values("how many apples can you eat"));

        spout.setCycle(true); // 循环发送数据

        // 创建FixedBatchSpout 从一个输入数据源中读取数据创建一个新的数据流
        TridentTopology topology = new TridentTopology();

        // 创建wordCount状态
        TridentState wordCounts = topology.newStream("spout1", spout)
                .parallelismHint(3)// 在拓扑中创建一个新的spout1数据流以便从输入源中读取数据
                .shuffle()
                .each(new Fields("sentence"), new Split(), new Fields("word")) // 对每个输入的sentence"字段"，调用Split()函数进行处理。配合运行函数(或过滤器)
                .groupBy(new Fields("word")) // 按特定的字段进行分组
                // 持久化到内存       Count()持久合并的方法,对value进行合并(相加),如果是zero返回0L.
                .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count")) // 聚合函数，persistentAggregate实现的是将数据持久到特定的存储介质中
                .parallelismHint(3); // 设置并行处理的数量

        LocalDRPC client = new LocalDRPC(); // comment in cluster mode
        topology.newDRPCStream("words", client /* don't pass client in cluster mode*/)
                .parallelismHint(3)
                .shuffle()
                .each(new Fields("args"), new Split(), new Fields("word")) // 对于输入参数args，使用Split()方法进行切分,并以word作为字段发送
                .groupBy(new Fields("word")) // 对word字段进行重新分区，保证相同的字段落入同一个分区
                // stateQuery提供对已生成的TridentState对象的查询           MapGet()方法 根据输入Map的Key获取Value
                .stateQuery(wordCounts, new Fields("word"), new MapGet(), new Fields("count")) //状态,输入字段(key),查询的方法,
                .each(new Fields("count"), new FilterNull()) // 使用FilterNull()方法过滤count字段的数据(过滤没有统计到的单词) 过滤器
                .aggregate(new Fields("count"), new Sum(), new Fields("sum")); // 合并 统计count

        LocalCluster cluster = new LocalCluster(); // 创建Local本地运行环境
        Config config = new Config(); // 配置文件Map格式
        config.setMaxSpoutPending(100); // 配置topology.max.spout.pending(同时活跃的batch数量)，设置同时处理的batch数量。默认为1。
        StormTopology stormTopology = topology.build(); // 构建Storm的拓扑Topology
        cluster.submitTopology("test", config, stormTopology); // 在集群环境中提交Topology

        Utils.sleep(10000);

        logger.info("单词统计 cow:" + client.execute("words", "cow")); // 计算key为cat的value(统计cat出现的次数)
        logger.info("单词统计 bought some candy:" + client.execute("words", "bought some candy"));
        logger.info("单词统计 apples:" + client.execute("words", "apples"));
        // 输出JSON编码的结果: "[[5078]]"
        logger.info("============================");

        System.exit(0);

    }

}
