package com.visizen.lijie.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * hdfs测试
 */
public class HdfsApp {

    private FileSystem fs;

    @Before
    public void init() throws IOException {
        Configuration configuration=new Configuration();
        fs=FileSystem.get(configuration);
    }

    /**
     * 读取hdfs文件系统上文件的内容
     * @throws IOException
     */
    @Test
    public void open() throws IOException {
        FSDataInputStream fsDataInputStream = fs.open(new Path("/a.txt"));
        IOUtils.copyBytes(fsDataInputStream,System.out,4096,true);
        IOUtils.closeStream(fsDataInputStream);
    }
}
