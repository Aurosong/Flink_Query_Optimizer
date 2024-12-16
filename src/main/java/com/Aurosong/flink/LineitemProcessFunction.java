package com.Aurosong.flink;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Set;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;

public class LineitemProcessFunction extends KeyedCoProcessFunction<Object, Record, Record, Record> {
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
    String nextKey = "ORDERKEY";

    ValueState<Integer> aliveCount;
    ValueState<Record> aliveRecord;
    ValueState<Set<Record>> aliveSet;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Integer> countDescriptor = new ValueStateDescriptor<>(
                "LineitemProcessFunction" + "Alive Count", Integer.class);
        aliveCount = getRuntimeContext().getState(countDescriptor);
        TypeInformation<Set<Record>> typeInfo = TypeInformation.of(new TypeHint<Set<Record>>() {});

        ValueStateDescriptor<Set<Record>> aliveSetDescriptor = new ValueStateDescriptor<>(
                "LineitemProcessFunction" + "Alive Set", typeInfo);
        aliveSet = getRuntimeContext().getState(aliveSetDescriptor);

        ValueStateDescriptor<Record> aliveRecordDescriptor = new ValueStateDescriptor<>(
                "LineitemProcessFunction" + "Alive Record", Record.class);
        aliveRecord = getRuntimeContext().getState(aliveRecordDescriptor);
    }

    public boolean isValid(Record record) throws ParseException {
        Date date = format.parse("1993-03-15");
        return ((Date) record.getValueByName("L_SHIPDATE")).compareTo(date) > 0;
    }

    @Override
    public void processElement1(Record record,
                                KeyedCoProcessFunction<Object, Record, Record, Record>.Context context,
                                Collector<Record> out) throws Exception {
        if(aliveSet.value() == null){
            aliveSet.update(Collections.newSetFromMap(new ConcurrentHashMap<>()));
            aliveCount.update(0);
            aliveRecord.update(null);
        }

        if(record.type.equals("alive")) {
            Set<Record> set = aliveSet.value();
            aliveRecord.update(new Record(record));
            aliveCount.update(aliveCount.value() + 1);
            for(Record connectRecord : set) {
                record.type = "add";
                collectRecord(record, connectRecord, out);
            }
        }
    }

    @Override
    public void processElement2(Record record,
                                KeyedCoProcessFunction<Object, Record, Record, Record>.Context context,
                                Collector<Record> out) throws Exception {
        if(aliveSet.value() == null){
            aliveSet.update(Collections.newSetFromMap(new ConcurrentHashMap<>()));
            aliveCount.update(0);
            aliveRecord.update(null);
        }

        Record temp = new Record(record);
        temp.type = "temp";
        temp.key = 0;

        if(isValid(record)) {
            if(aliveCount.value() == 1){
                if(aliveSet.value().add(temp)) {
                    record.type = "add";
                    collectRecord(record, aliveRecord.value(), out);
                }
            } else {
                aliveSet.value().add(temp);
            }
        }
    }

    public void collectRecord(Record record, Record connectRecord, Collector<Record> out) throws IOException {
        Record temp = new Record(record);
        if(connectRecord != null) {
            for(int i = 0; i < connectRecord.attributeKey.size(); i++){
                if(!temp.attributeValue.contains(connectRecord.attributeKey.get(i))) {
                    temp.attributeKey.add(connectRecord.attributeKey.get(i));
                    temp.attributeValue.add(connectRecord.attributeValue.get(i));
                }
            }
        }
        temp.setKey(nextKey);
        out.collect(temp);
    }
}
