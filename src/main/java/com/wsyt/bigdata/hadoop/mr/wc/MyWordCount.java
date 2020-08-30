package com.wsyt.bigdata.hadoop.mr.wc;

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
 * @date 2020-08-13
 * @title
 * @description
 */
public class MyWordCount {

    /**
     *
     * MR提交方式:
     *      1、jar分发到集群节点-> hadoop jar xxxx.jar
     *      2、嵌入[Linux、windows]开发
     *          集群M、R
     *          client -> RM -> AppMaster
     *          mapreduce.framework.name -> yarn
     *          job.setJar("xxx")
     *      3、local单机自测
     *          mapreduce.framework.name -> local
     *          conf.set("mapreduce.app-submission.cross-platform","true"); windows平台配置,需要兼容window文件系统
     *
     *
     *      //提供的工具类自动将系统设置(-D)设置到conf中,另外可以获取到额外的配置项,通过parser.getRemainingArgs()
     *      GenericOptionsParser parser = new GenericOptionsParser(conf,args);
     *      String[] otherArgs = parser.getRemainingArgs();
     * */

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //加载配置
        Configuration conf = new Configuration(true);

        //Long.MAX_VALUE;
        /***System.out.println(conf.get("mapreduce.framework.name"));
        //设置本地运行模式,不再调用yarn进行资源分配
        conf.set("mapreduce.framework.name","local");
        System.out.println(conf.get("mapreduce.framework.name"));*/


        GenericOptionsParser parser = new GenericOptionsParser(conf,args);
        String[] cmdArgs = parser.getRemainingArgs();

        Job job = Job.getInstance(conf);

        //设置启动主类
        job.setJarByClass(MyWordCount.class);
        //本地机器启动程序运行需要设置jar目录,否则会报ClassNotFound的异常
        job.setJar("/Users/ruyin/dev-dir/gitref/bigdata/hadoop-hdfs-demo/target/hadoop-hdfs-demo-1.0.0-SNAPSHOT.jar");

        //设置输入输出目录

        String inputPath = cmdArgs[0];
        String outputPath = cmdArgs[1];
        Path input = new Path(inputPath);
        TextInputFormat.addInputPath(job,input);
        Path output = new Path(outputPath);

        FileSystem fs = output.getFileSystem(conf);
        if (fs.exists(output)){
            fs.delete(output,true);
        }
        TextOutputFormat.setOutputPath(job,output);

        job.setJobName("wyst");
        //可以设置reduce任务数,可通过命令行动态传入
        //job.setNumReduceTasks(2);

        //设置map/reduce程序
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(MyReducer.class);


        //等待任务执行完成
        job.waitForCompletion(true);
    }



}
