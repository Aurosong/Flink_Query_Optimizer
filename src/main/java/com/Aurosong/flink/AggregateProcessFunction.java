package com.Aurosong.flink;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class AggregateProcessFunction extends KeyedProcessFunction<Object, Record, Record> {
    ValueState<Double> accumulator;
    List<String> outputAttributes = Arrays.asList("L_ORDERKEY", "O_ORDERDATE", "O_SHIPPRIORITY");
    String aggregateFieldName = "revenue";
    String nextKey = "ORDERKEY";

    public void open(Configuration parameters) {
        ValueStateDescriptor<Double> preValueStateDescriptor = new ValueStateDescriptor<>(
                "AggregateProcessFunction" + "PreValue", TypeInformation.of(Double.class));
        accumulator = getRuntimeContext().getState(preValueStateDescriptor);
    }

    @Override
    public void processElement(Record record,
                               KeyedProcessFunction<Object, Record, Record>.Context context,
                               Collector<Record> out) throws Exception {
        if(accumulator.value() == null){
            accumulator.update(0.0);
        }

        double filted = (Double)record.getValueByName("L_EXTENDEDPRICE") * (1.0 - (Double)record.getValueByName("L_DISCOUNT"));
        double newValue = 0.0;

        if(record.type.equals("add")) {
            newValue = filted + accumulator.value();
        }
        accumulator.update(newValue);

        List<Object> value = new ArrayList<>();
        List<String> key = new ArrayList<>();

        for(String attribute : outputAttributes) {
            key.add(attribute);
            value.add(record.getValueByName(attribute));
        }
        key.add(aggregateFieldName);
        value.add(newValue);

        record.attributeKey = key;
        record.attributeValue = value;
        record.setKey(nextKey);
        record.type = "result";
        out.collect(record);
    }
}
