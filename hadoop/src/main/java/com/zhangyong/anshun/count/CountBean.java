package com.zhangyong.anshun.count;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Author zhangyong
 * @Date 2020/4/14 15:06
 * @Version 1.0
 */
public class CountBean implements WritableComparable<CountBean> {
    private String name;
    private int count;

    @Override
    public int compareTo(CountBean o) {
        return o.count - this.count;
    }

    /**
     * 序列化
     *
     * @param dataOutput
     * @throws IOException
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(name);
        dataOutput.writeInt(count);
    }

    /**
     * 反序列化
     *
     * @param dataInput
     * @throws IOException
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.name = dataInput.readUTF();
        this.count = dataInput.readInt();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "CountBean{" +
                "name='" + name + '\'' +
                ", count=" + count +
                '}';
    }
}
