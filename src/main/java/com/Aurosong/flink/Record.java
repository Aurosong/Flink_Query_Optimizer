package com.Aurosong.flink;

import java.util.*;

public class Record implements java.io.Serializable {
    public String type;
    public Object key;
    public List<String> attributeKey;
    public List<Object> attributeValue;

    public Record() {}

    public Record(Record record){
        this.type = record.type;
        this.key = record.key;
        this.attributeKey = new ArrayList<>(record.attributeKey);
        this.attributeValue = new ArrayList<>(record.attributeValue);
    }

    public Record(String type, Object key, List<String> attributeKey, List<Object> attributeValue) {
        this.type = type;
        this.key = key;
        this.attributeKey = new ArrayList<>(attributeKey);
        this.attributeValue = new ArrayList<>(attributeValue);
    }

    @Override
    public boolean equals(Object object) {
        if (object.getClass() == this.getClass()) {
            for(int i = 0; i < this.attributeKey.size(); i++){
                int index = Arrays.asList(((Record) object).attributeKey).indexOf(attributeKey.get(i));
                if(index == -1) {
                    return false;
                }
                if(!((Record) object).attributeValue.get(index).equals(attributeValue.get(i))) {
                    return false;
                }
            }
            return true;
        }
        else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(attributeValue);
    }

    public Object getValueByName(String name) {
        int index = attributeKey.indexOf(name);
        if(index == -1) {
            return null;
        } else {
            return attributeValue.get(index);
        }
    }

    public void setKey(String nextKey) {
        this.key = getValueByName(nextKey);
    }

    @Override
    public String toString() {
        return "Record{" +
                "type=" + type +
                ", key=" + key +
                ", attribute key =" + attributeKey +
                ", attribute value =" + attributeValue + '}';
    }
}
