package com.zhangyong.mapreduce.max;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author 17616
 */
public class HeightReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    @Override
    public void reduce(Text year, Iterable<LongWritable> t, Context context) throws IOException, InterruptedException {
        long max = 0;
        for (LongWritable data : t) {
            if (max < data.get ()) {
                max = data.get ();
            }
        }
        context.write (year, new LongWritable (max));
    }
}
