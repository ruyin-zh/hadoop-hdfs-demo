package com.wsyt.bigdata.hadoop.mr.recommend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * @author ruyin_zh
 * @date 2020-08-30
 * @title
 * @description
 */
public class MyRecommend {


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration(true);

        conf.set("mapreduce.framework.name","local");

        Job job = Job.getInstance(conf);
        job.setJarByClass(MyRecommend.class);
        job.setJobName("Recommend Friends");


        String[] cmdArgs = new GenericOptionsParser(conf,args).getRemainingArgs();


        TextInputFormat.addInputPath(job,new Path(cmdArgs[0]));

        Path path = new Path(cmdArgs[1]);
        FileSystem fs = path.getFileSystem(conf);
        if (fs.exists(path)){
            fs.delete(path,true);
        }
        TextOutputFormat.setOutputPath(job,path);

        job.setMapperClass(RecommendMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);


        //不执行reduce阶段
        //job.setNumReduceTasks(0);
        job.setReducerClass(RecommendReducer.class);



        job.waitForCompletion(true);
    }
}
