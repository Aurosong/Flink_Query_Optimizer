package com.Aurosong.flink;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.util.*;

public class JobSubmitter {
    public static final OutputTag<Record> lineitemTag = new OutputTag<Record>("lineitem"){};
    public static final OutputTag<Record> ordersTag = new OutputTag<Record>("orders"){};
    public static final OutputTag<Record> customerTag = new OutputTag<Record>("customer"){};

    public static void main(String[] args) throws Exception {
        JobSubmitter submitter = new JobSubmitter();
        submitter.createJob();
    }

    public static void createJob() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> lineitemStream = env.readTextFile("src/main/resources/dataset/csv/lineitem.csv").setParallelism(1);
        DataStreamSource<String> ordersStream = env.readTextFile("src/main/resources/dataset/csv/orders/csv").setParallelism(1);
        DataStreamSource<String> customerStream = env.readTextFile("src/main/resources/dataset/csv/customers.csv").setParallelism(1);
        String outputPath = "/opt/src/outputResult.csv";

        DataStream<Record> orders = processOrdersSource(ordersStream).getSideOutput(ordersTag);
        DataStream<Record> customer = processCustomerSource(customerStream).getSideOutput(customerTag);
        DataStream<Record> lineitem = processLineitemSource(lineitemStream).getSideOutput(lineitemTag);

        DataStream<Record> aliveCustomers = customer.keyBy(i -> i.key).process(new CustomerProcessFunction());
        DataStream<Record> aliveOrders = aliveCustomers.connect(orders).keyBy(i -> i.key, i -> i.key).process(new OrderProcessFunction());
        DataStream<Record> aliveLineitems = aliveOrders.connect(lineitem).keyBy(i -> i.key, i -> i.key).process(new LineitemProcessFunction());
        DataStream<Record> result = aliveLineitems.keyBy(i -> i.key).process(new AggregateProcessFunction());
        DataStreamSink<Record> output = result.writeAsText(outputPath, FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute("Flink streaming Java API Skeleton");
    }

    private static SingleOutputStreamOperator<Record> processLineitemSource(DataStreamSource<String> lineitemStream) throws Exception {

        return lineitemStream.process(new ProcessFunction<String, Record>() {
            @Override
            public void processElement(String str, ProcessFunction<String, Record>.Context context, Collector<Record> collector) throws Exception {
                String content[] = str.split(",");
                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
                context.output(lineitemTag, new Record("lineitem", Long.valueOf(content[0]),
                        new ArrayList<>(Arrays.asList("L_SHIPDATE", "LINENUMBER", "ORDERKEY", "L_EXTENDEDPRICE", "L_DISCOUNT")),
                        new ArrayList<>(Arrays.asList(format.parse(content[10]), Integer.valueOf(content[3]), Long.valueOf(content[0]), Double.valueOf(content[5]), Double.valueOf(content[6])))
                ));
            }
        }).setParallelism(1);
    }

    private static SingleOutputStreamOperator<Record> processOrdersSource(DataStreamSource<String> ordersStream) throws Exception {
        SingleOutputStreamOperator<Record> ordersInputStream = ordersStream.process(new ProcessFunction<String, Record>() {
            @Override
            public void processElement(String str, ProcessFunction<String, Record>.Context context, Collector<Record> collector) throws Exception {
                String content[] = str.split(",");
                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
                context.output(ordersTag, new Record("orders", Long.valueOf(content[1]),
                        new ArrayList<>(Arrays.asList("CUSTKEY", "ORDERKEY", "O_ORDERDATE", "O_SHIPPRIORITY")),
                        new ArrayList<>(Arrays.asList(Long.valueOf(content[1]), Long.valueOf(content[0]), format.parse(content[4]), Integer.valueOf(content[7])))
                ));
            }
        }).setParallelism(1);

        return ordersInputStream;
    }

    public static SingleOutputStreamOperator<Record> processCustomerSource(DataStreamSource<String> customerStream) throws Exception {
        SingleOutputStreamOperator<Record> customerInputStream = customerStream.process(new ProcessFunction<String, Record>() {
            @Override
            public void processElement(String str, ProcessFunction<String, Record>.Context context, Collector<Record> collector) throws Exception {
                String content[] = str.split(",");
                context.output(customerTag, new Record("customer", Long.valueOf(content[0]),
                        new ArrayList<>(Arrays.asList("CUSTKEY", "C_MKTSEGMENT")),
                        new ArrayList<>(Arrays.asList(Long.valueOf(content[0]), content[6]))
                ));
            }
        }).setParallelism(1);

        return customerInputStream;
    }
}
