package com.wsyt.bigdata.hadoop.mr.topn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author ruyin_zh
 * @date 2020-08-27
 * @title
 * @description
 */
public class TopNReducer extends Reducer<TopNKey, IntWritable, Text, IntWritable> {

    private static Text rKey = new Text();
    private static IntWritable rVal = new IntWritable();
    private Map<Integer,Integer> uniqueMap;

    @Override
    protected void reduce(TopNKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //2020-08-26 34
        //2020-08-25 33
        //2020-08-27 32
        //2020-08-26 32
        //2020-08-27 31

        uniqueMap = new HashMap<>();

        int day;
        Iterator<IntWritable> itr = values.iterator();

        while (itr.hasNext()){
            //此处一定要调用该方法以让游标移动,否则此处将发生死循环
            IntWritable var = itr.next();    //context.nextKeyValue();

            day = key.getDay();

            Integer climate = uniqueMap.get(day);
            if(climate == null){
                rKey.set(key.getYear() + "-" + key.getMonth() + "-" + key.getDay() + "@" + key.getLocation());
                rVal.set(key.getClimate());
                context.write(rKey,rVal);


                uniqueMap.put(day,key.getClimate());
            }

//            if (flag == 0){
//                rKey.set(key.getYear() + "-" + key.getMonth() + "-" + key.getDay());
//                rVal.set(key.getClimate());
//                context.write(rKey,rVal);
//
//                day = key.getDay();
//                flag++;
//            }
//
//
//            if (flag != 0 && day != key.getDay()){
//                rKey.set(key.getYear() + "-" + key.getMonth() + "-" + key.getDay());
//                rVal.set(key.getClimate());
//                context.write(rKey,rVal);
//                break;
//            }
        }

    }
}
