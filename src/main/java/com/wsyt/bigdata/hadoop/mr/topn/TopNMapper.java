package com.wsyt.bigdata.hadoop.mr.topn;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

/**
 * @author ruyin_zh
 * @date 2020-08-27
 * @title
 * @description
 */
public class TopNMapper extends Mapper<LongWritable, Text, TopNKey, IntWritable> {

    //map操作可能调用多次,定义在外面可减少gc
    //实际数据均会序列化成byte,不存在浅拷贝的问题
    private static TopNKey mKey = new TopNKey();
    private static IntWritable mVal = new IntWritable();
    private static DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private Map<String,String> dict = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        URI[] files = context.getCacheFiles();
        String path = files[0].getPath();
        Path file = new Path(path);

        BufferedReader reader = new BufferedReader(new FileReader(new File(file.getName())));

        String line = reader.readLine();
        String[] splits;
        while (line != null){
           splits = line.split("\t");
           dict.put(splits[0],splits[1]);
           line = reader.readLine();
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //2020-08-27 23:17:03   1   31

        String[] splits = StringUtils.split(value.toString(),'\t');
        LocalDateTime ldt = LocalDateTime.parse(splits[0],dtf);
        mKey.setYear(ldt.getYear());
        mKey.setMonth(ldt.getMonthValue());
        mKey.setDay(ldt.getDayOfMonth());
        mKey.setLocation(dict.get(splits[1]));

        Integer climate = Integer.parseInt(splits[2]);
        mKey.setClimate(climate);
        mVal.set(climate);

        context.write(mKey,mVal);
    }
}
