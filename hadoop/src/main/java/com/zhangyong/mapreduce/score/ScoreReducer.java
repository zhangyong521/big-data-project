package com.zhangyong.mapreduce.score;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
/**
 * @Author zhangyong
 * @Date 2020/4/10 10:05
 * @Version 1.0
 */
public class ScoreReducer extends Reducer<Text, ScoreBean, Text, ScoreBean> {
    @Override
    protected void reduce(Text key, Iterable<ScoreBean> values, Context context) throws IOException, InterruptedException {

        ScoreBean resultScore = new ScoreBean();
        // 此处key.toSting中只有name一个值，因为在map阶段的输出key只有name
        resultScore.setName(key.toString());

        for (ScoreBean value : values) {
            //  result.setFlow(result.getFlow() + value.getFlow());
            // 语文成绩分数
            resultScore.setChinese(resultScore.getChinese() + value.getChinese());
            // 数学成绩分
            resultScore.setMath(resultScore.getMath() + value.getMath());
            // 英语成绩分
            resultScore.setEnglish(resultScore.getEnglish() + value.getEnglish());
        }
        context.write(key, resultScore);
    }
}
