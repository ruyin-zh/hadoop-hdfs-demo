package com.wsyt.bigdata.hadoop.mr.recommend;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

/**
 * @author ruyin_zh
 * @date 2020-08-30
 * @title
 * @description
 */
public class RecommendMapper extends Mapper<LongWritable, Text,Text, IntWritable> {


    private static Text mKey = new Text();
    private static IntWritable mVal = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //A	B C E

        String[] splits = StringUtils.split(value.toString(),' ');
        for (int i = 1; i < splits.length; i++){
            //直接关系
            mKey.set(getRelation(splits[0],splits[i]));
            mVal.set(0);
            context.write(mKey,mVal);
            for (int j = i + 1; j < splits.length;j ++){
                //间接关系
                mKey.set(getRelation(splits[i],splits[j]));
                mVal.set(1);
                context.write(mKey,mVal);
            }
        }
    }


    private static String getRelation(String s1, String s2){
        if (s1.compareTo(s2) > 0){
            return s1 + "-" + s2;
        }else {
            return s2 + "-" + s1;
        }
    }
}
