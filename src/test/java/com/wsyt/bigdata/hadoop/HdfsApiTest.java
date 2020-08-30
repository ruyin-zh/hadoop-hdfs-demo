package com.wsyt.bigdata.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

/**
 * @author ruyin_zh
 * @date 2020-08-06
 * @title
 * @description hdfs dfs相关操作
 */
public class HdfsApiTest {

    private FileSystem fileSystem;

    @BeforeEach
    public void init() throws IOException {
        Configuration conf = new Configuration();
        conf.set("dfs.blocksize","1m");
        fileSystem = FileSystem.newInstance(conf);
    }


    @Test
    public void testMkdir() throws IOException {
        Path path = new Path("/user/hadoop");
        FSDataOutputStream os = fileSystem.create(path);
        System.out.println(os.getPos());
        System.out.println(os);
    }


    @Test
    public void testPutFile() throws IOException {
        Path src = new Path("/Users/ruyin/Downloads/hadoop/zookeeper-3.6.1.tar.gz");
        Path dest = new Path("/user/hadoop/zk");

        fileSystem.copyFromLocalFile(src,dest);
    }


    @Test
    public void testDeleteFile() throws IOException {
        Path path = new Path("/user/hadoop/zk");
        fileSystem.delete(path,true);
    }


    @Test
    public void testLsDir() throws IOException {
        Path path = new Path("/user/hadoop/zk");
        FileStatus[] fileStatuses = fileSystem.listStatus(path);
        for (FileStatus status : fileStatuses){
            System.out.println(status);
        }

    }


    @AfterEach
    public void close() throws IOException {
        fileSystem.close();
    }
}
