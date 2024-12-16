package com.Aurosong.flink;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class CustomerProcessFunction extends KeyedProcessFunction<Object, Record, Record> {
    ValueState<Set<Record>> aliveSet;
    String nextKey = "CUSTKEY";

    @Override
    public void open(Configuration parameters) throws Exception {
        TypeInformation<Set<Record>> typeInfo = TypeInformation.of(new TypeHint<Set<Record>>() {});
        ValueStateDescriptor<Set<Record>> stateDescriptor = new ValueStateDescriptor<>(
                "CustomerProcessFunction" + "Alive", typeInfo
        );
        aliveSet = getRuntimeContext().getState(stateDescriptor);
    }

    // Corresponding to "c_mktsegment = '[SEGMENT]'" SQL in Query 3
    // Suppose we set '[SEGMENT]' in Query 3 as "BUILDING"
    public boolean isValid(Record record) {
        if (record.getValueByName("C_MKTSEGMENT").equals("BUILDING")) {
            return true;
        }
        else {
            return false;
        }
    }

    public void processElement(Record record, KeyedProcessFunction<Object, Record, Record>.Context ctx, Collector<Record> out) throws Exception {
        if(aliveSet.value() == null) {
            aliveSet.update(Collections.newSetFromMap(new ConcurrentHashMap<>()));
        }

        if(isValid(record)) {
            Record temp = new Record(record);
            temp.type = "temp";

            Set<Record> set = aliveSet.value();
            if(set.add(temp)) {
                record.type = "alive";
                record.setKey(nextKey);
                out.collect(record);
            }
        }
    }

}
