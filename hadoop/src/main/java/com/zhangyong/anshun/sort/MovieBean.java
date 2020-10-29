package com.zhangyong.anshun.sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Author zhangyong
 * @Date 2020/4/13 8:42
 * @Version 1.0
 */
public class MovieBean implements WritableComparable<MovieBean> {
    private String name;
    private int hot;

    /**
     * 排序方法
     *
     * @param o
     * @return
     */
    @Override
    public int compareTo(MovieBean o) {
        return o.hot - this.hot;
    }

    /**
     * 序列化
     *
     * @param dataOutput
     * @throws IOException
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF (name);
        dataOutput.writeInt (hot);
    }

    /**
     * 反序列化
     *
     * @param dataInput
     * @throws IOException
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.name = dataInput.readUTF ();
        this.hot = dataInput.readInt ();
    }


    public void setName(String name) {
        this.name = name;
    }

    public int getHot() {
        return hot;
    }

    public void setHot(int hot) {
        this.hot = hot;
    }

    @Override
    public String toString() {
        return name +" "+ hot;
    }
}
