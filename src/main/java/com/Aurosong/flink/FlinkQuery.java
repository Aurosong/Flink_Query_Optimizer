package com.Aurosong.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.functions.MapFunction;

public class FlinkQuery {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromElements("Hello", "Flink", "World")
                .map(new MapFunction<String, String>() {
                    @Override
                    public String map(String value) throws Exception {
                        return value + "!";
                    }
                })
                .print();

        env.execute("Flink Test Job");
    }
}
