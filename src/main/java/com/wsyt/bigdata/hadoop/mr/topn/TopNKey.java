package com.wsyt.bigdata.hadoop.mr.topn;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author ruyin_zh
 * @date 2020-08-27
 * @title
 * @description
 */
public class TopNKey implements WritableComparable<TopNKey> {

    //年
    private int year;
    //月
    private int month;
    //日
    private int day;
    //温度
    private int climate;

    private String location;

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public int getDay() {
        return day;
    }

    public void setDay(int day) {
        this.day = day;
    }

    public int getClimate() {
        return climate;
    }

    public void setClimate(int climate) {
        this.climate = climate;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    @Override
    public int compareTo(TopNKey o) {
        int c1 = Integer.compare(this.year,o.getYear());
        if (c1 == 0){
            int c2 = Integer.compare(this.month,o.getMonth());
            if (c2 == 0){
                return Integer.compare(this.day,o.getDay());
            }

            return c2;
        }

        return c1;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(year);
        out.writeInt(month);
        out.writeInt(day);
        out.writeInt(climate);
        out.writeUTF(location);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.year    = in.readInt();
        this.month   = in.readInt();
        this.day     = in.readInt();
        this.climate = in.readInt();
        this.location= in.readUTF();
    }


    @Override
    public String toString() {
        return "TopNKey{" +
                "year=" + year +
                ", month=" + month +
                ", day=" + day +
                ", climate=" + climate +
                '}';
    }
}
