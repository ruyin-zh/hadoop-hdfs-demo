package com.wsyt.bigdata.hadoop.mr.topn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * @author ruyin_zh
 * @date 2020-08-27
 * @title
 * @description
 */
public class MyTopN {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration(true);
        //conf.set("mapreduce.framework.name","local");

        String[] cmdArgs = new GenericOptionsParser(conf,args).getRemainingArgs();

        Job job = Job.getInstance(conf);
        job.setJarByClass(MyTopN.class);
        job.setJar("/Users/ruyin/dev-dir/gitref/bigdata/hadoop-hdfs-demo/target/hadoop-hdfs-demo-1.0.0-SNAPSHOT.jar");
        job.setJobName("TopN");

        job.addCacheFile(new Path("/data/topn/dict/dict.txt").toUri());

        //mapTask
        //input
        TextInputFormat.addInputPath(job,new Path(cmdArgs[0]));

        Path outPath = new Path(cmdArgs[1]);
        FileSystem fs = outPath.getFileSystem(conf);
        if (fs.exists(outPath)){
            fs.delete(outPath,true);
        }
        TextOutputFormat.setOutputPath(job,outPath);


        //map
        job.setMapperClass(TopNMapper.class);
        //key
        job.setMapOutputKeyClass(TopNKey.class);
        job.setMapOutputValueClass(IntWritable.class);
        //partitioner  年、月
        job.setPartitionerClass(TopNPartitioner.class);
        //sortComparator 年、月、温度
        job.setSortComparatorClass(TopNSort.class);
        //combiner




        //reduceTask
        //reduce
        job.setReducerClass(TopNReducer.class);
        //groupingComparator
        job.setGroupingComparatorClass(TopNGrouping.class);



        job.waitForCompletion(true);
    }
}
