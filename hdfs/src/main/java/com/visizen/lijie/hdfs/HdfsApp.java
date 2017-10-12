package com.visizen.lijie.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;

/**
 * hdfs测试
 */
public class HdfsApp {

    private FileSystem fs;

    @Before
    public void init() throws IOException {

        System.setProperty("HADOOP_USER_NAME","jay");

        Configuration configuration=new Configuration();

        fs=FileSystem.get(configuration);
    }

    /**
     * 读取hdfs文件系统上文件的内容
     * @throws IOException
     */
    @Test
    public void open() throws IOException {

        FSDataInputStream fsDataInputStream = fs.open(new Path("/input"));

        IOUtils.copyBytes(fsDataInputStream,System.out,4096,true);

        IOUtils.closeStream(fsDataInputStream);
    }

    /**
     * 上传文件
     * @throws IOException
     */

    @Test
    public void create() throws IOException {

        URL resourceUrl = this.getClass().getResource("/word.txt");

        FSDataOutputStream fsDataOutputStream = fs.create(new Path("/word.txt"));

        FileInputStream fileInputStream=new FileInputStream(new File(resourceUrl.getPath()));

        IOUtils.copyBytes(fileInputStream,fsDataOutputStream,4096,true);


       IOUtils.closeStream(fsDataOutputStream);

        IOUtils.closeStream(fileInputStream);
    }

}
