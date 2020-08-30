package com.wsyt.bigdata.hadoop.mr.topn;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author ruyin_zh
 * @date 2020-08-27
 * @title
 * @description
 */
public class TopNSort extends WritableComparator {


    public TopNSort() {
        super(TopNKey.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TopNKey key1 = (TopNKey) a;
        TopNKey key2 = (TopNKey) b;


        //年、月、温度且温度降序
        int c1 = Integer.compare(key1.getYear(),key2.getYear());
        if (c1 == 0){
            int c2 = Integer.compare(key1.getMonth(),key2.getMonth());
            if (c2 == 0){
                return Integer.compare(key2.getClimate(),key1.getClimate());
            }

            return c2;
        }

        return c1;
    }
}
