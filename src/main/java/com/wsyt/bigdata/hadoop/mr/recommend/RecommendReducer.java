package com.wsyt.bigdata.hadoop.mr.recommend;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author ruyin_zh
 * @date 2020-08-30
 * @title
 * @description
 */
public class RecommendReducer extends Reducer<Text, IntWritable,Text,IntWritable> {

    private static IntWritable rVal = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        Iterator<IntWritable> itr = values.iterator();
        IntWritable loopVal;
        boolean flag = false;
        int sum = 0;
        while (itr.hasNext()){
            loopVal = itr.next();


            if (loopVal.get() == 0){
                flag = true;
            }

            sum += loopVal.get();

        }


        if (!flag){
            rVal.set(sum);
            context.write(key,rVal);
        }
    }
}
