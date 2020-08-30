package com.wsyt.bigdata.hadoop.mr.topn;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author ruyin_zh
 * @date 2020-08-27
 * @title
 * @description
 */
public class TopNGrouping extends WritableComparator {

    public TopNGrouping() {
        super(TopNKey.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        TopNKey k1 = (TopNKey) a;
        TopNKey k2 = (TopNKey) b;

        //分组比较 年、月
        int c = Integer.compare(k1.getYear(),k2.getYear());
        if (c == 0){
            return Integer.compare(k1.getMonth(),k2.getMonth());
        }

        return c;
    }
}
