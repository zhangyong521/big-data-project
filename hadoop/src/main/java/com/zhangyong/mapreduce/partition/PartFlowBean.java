package com.zhangyong.mapreduce.partition;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Author zhangyong
 * @Date 2020/4/10 10:01
 * @Version 1.0
 */
public class PartFlowBean implements Writable {
    private String phone;
    private String name;
    private String addr;
    private long flow;

    /**
     * 序列化
     *
     * @param dataOutput
     * @throws IOException
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF (phone);
        dataOutput.writeUTF (name);
        dataOutput.writeUTF (addr);
        dataOutput.writeLong (flow);
    }
    /**
     * 反序列化
     *
     * @param dataInput
     * @throws IOException
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.phone = dataInput.readUTF ();
        this.name = dataInput.readUTF ();
        this.addr = dataInput.readUTF ();
        this.flow = dataInput.readLong ();
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAddr() {
        return addr;
    }

    public void setAddr(String addr) {
        this.addr = addr;
    }

    public long getFlow() {
        return flow;
    }

    public void setFlow(long flow) {
        this.flow = flow;
    }

    @Override
    public String toString() {
        return "FlowBean{" +
                "phone='" + phone + '\'' +
                ", name='" + name + '\'' +
                ", addr='" + addr + '\'' +
                ", flow=" + flow +
                '}';
    }
}
