package com.wsyt.bigdata.hadoop.mr.topn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author ruyin_zh
 * @date 2020-08-27
 * @title
 * @description
 */
public class TopNPartitioner extends Partitioner<TopNKey, IntWritable> {


    @Override
    public int getPartition(TopNKey topNKey, IntWritable intWritable, int numPartitions) {


        //数据可能发生倾斜
        return topNKey.getYear() % numPartitions;
    }
}
