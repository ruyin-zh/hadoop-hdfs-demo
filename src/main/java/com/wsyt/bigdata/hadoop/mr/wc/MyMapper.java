package com.wsyt.bigdata.hadoop.mr.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @author ruyin_zh
 * @date 2020-08-22
 * @title
 * @description
 */
public class MyMapper extends Mapper<Object, Text, Text, IntWritable> {

    private static final IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer tokenizer = new StringTokenizer(value.toString());

        while (tokenizer.hasMoreTokens()){
            word.set(tokenizer.nextToken());
            context.write(word,one);
        }
    }
}
