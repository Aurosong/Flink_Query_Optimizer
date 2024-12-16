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
import java.util.Date;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class OrderProcessFunction extends KeyedCoProcessFunction<Object, Record, Record, Record> {
    public String nextKey = "ORDERKEY";

    public ValueState<Integer> aliveCount;
    public ValueState<Set<Record>> aliveSet;
    public ValueState<Record> aliveRecord;

    public SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Integer> countDescriptor = new ValueStateDescriptor<>(
                "OrderProcessFunction" + "Alive Count", Integer.class);
        aliveCount = getRuntimeContext().getState(countDescriptor);
        TypeInformation<Set<Record>> typeInfo = TypeInformation.of(new TypeHint<Set<Record>>() {});

        ValueStateDescriptor<Set<Record>> aliveSetDescriptor = new ValueStateDescriptor<>(
                "OrderProcessFunction" + "Alive Set", typeInfo);
        aliveSet = getRuntimeContext().getState(aliveSetDescriptor);

        ValueStateDescriptor<Record> aliveRecordDescriptor = new ValueStateDescriptor<>(
                "OrderProcessFunction" + "Alive Record", Record.class);
        aliveRecord = getRuntimeContext().getState(aliveRecordDescriptor);
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

        if(record.type.equals("alive")){
            Set<Record> recordsSet = aliveSet.value();
            aliveRecord.update(record);
            aliveCount.update(aliveCount.value() + 1);

            for(Record connectRecord : recordsSet) {
                collectRecord(record, connectRecord, out);
            }
        }
    }

    @Override
    public void processElement2(Record record,
                                KeyedCoProcessFunction<Object, Record, Record, Record>.Context context,
                                Collector<Record> out) throws Exception {
        if (aliveSet.value() == null){
            aliveSet.update(Collections.newSetFromMap(new ConcurrentHashMap<>()));
            aliveCount.update(0);
            aliveRecord.update(null);
        }

        Record temp = new Record(record);
        temp.type = "temp";
        temp.key = 0;

        if(isValid(record)) {
            if(aliveCount.value() == 1) {
                record.type = "alive";
                collectRecord(record, aliveRecord.value(), out);
            } else {
                aliveSet.value().add(temp);
            }
        }
    }

    // Corresponding to "and o_orderdate < date '[DATE]'" SQL in Query 3
    // Suppose we set '[DATE]' of o_orderdate in Query 3 as "1995-03-15"
    public boolean isValid(Record record) throws ParseException {
        Date date = format.parse("1995-03-15");
        return ((Date) record.getValueByName("O_ORDERDATE")).compareTo(date) < 0;
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
