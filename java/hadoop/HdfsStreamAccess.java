package com.wang.bigdata;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.URI;

/**
 * 用流的方式来操作hdfs的上传
 * 可以实现读取指定偏移量范围的数据
 */
public class HdfsStreamAccess {

    FileSystem fs = null;
    Configuration conf = null;

    @Before
    public void init()throws Exception{
        conf = new Configuration();
        //拿到客户端实例对象
        fs = FileSystem.get(conf);
        //通过参数传递进去用户，hadoop能够识别用户身份，从本地上传到远程的hdfs系统
        fs = FileSystem.get(new URI("hdfs://192.168.52.200:9000"),conf,"root");
    }

    //通过流的方式上传文件
    @Test
    public void testUplod()throws Exception{
        FSDataOutputStream outputStream = fs.create(new Path("/javaMkDir/aaa/bbb/wang.txt"), true);
        FileInputStream inpurStream = new FileInputStream("f:/wang.txt");
        //不用自己关掉数据流，hadoop会帮助我们关闭
        IOUtils.copy(inpurStream,outputStream);
    }

    //通过流的方式获取hdfs测试下载
    @Test
    public void downLoad()throws Exception{
        FSDataInputStream inputStream = fs.open(new Path("/javaMkDir/aaa/bbb/wang.txt"));
        FileOutputStream outputStream = new FileOutputStream("e:/wang.txt");
        IOUtils.copy(inputStream,outputStream);

    }


    //文件的随机读写  指定文件的偏移量进行读写
    @Test
    public void testRandomAccess()throws Exception{

        FSDataInputStream  inputStream = fs.open(new Path("/a.txt"));
        inputStream.seek(12);//从第12字节开始读入数据
        FileOutputStream outputStream = new FileOutputStream("e:/wang.part.txt");
        IOUtils.copyLarge(inputStream,outputStream);

    }

    //读取HDFS上的文件并显示文件的内容到屏幕上
    @Test
    public void testCat()throws Exception{
        FSDataInputStream open = fs.open(new Path("/a.txt.copy"));
        IOUtils.copy(open,System.out);
    }

}
