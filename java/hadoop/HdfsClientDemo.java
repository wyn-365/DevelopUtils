package com.wang.bigdata;

import org.apache.hadoop.fs.*;
import org.junit.Before;
import org.junit.Test;
import org.apache.hadoop.conf.Configuration;

import java.net.URI;

/**
 * 客户端去操作hdfs时候，有一个身份
 * 默认情况下，hdfs客户端api会从jvm中获取一个参数来座位自己的用身份
 * -DHADOOP_USER_NAME=hadoop
 *
 *  也可以利用代码的方式来传入用户的身份
 */

public class HdfsClientDemo {

    FileSystem fs = null;
    Configuration conf = null;

    @Before
    public void init()throws Exception{
        conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://hadoop1:9000");
        //conf.set("dfs.relication","4");
        //拿到客户端实例对象
        //通过参数传递进去用户，hadoop能够识别用户身份，从本地上传到远程的hdfs系统
        fs = FileSystem.get(new URI("hdfs://192.168.52.200:9000"),conf,"root");

    }


    //文件的上传
    @Test
    public void testUpload()throws Exception{
        fs.copyFromLocalFile(new Path("f:/b.txt"),new Path("/b.txt.copy"));
        fs.close();

    }

    //文件的下载
    @Test
    public void testDownLoad() throws Exception{
        fs.copyToLocalFile(new Path("/a.txt.copy"),new Path("f:/"));
        fs.close();
    }

    //创建文件夹
    @Test
    public void testMkDir()throws Exception{
        boolean mkdirs = fs.mkdirs(new Path("/javaMkDir/aaa/bbb"));
        System.out.println(mkdirs);
    }

    //删除文件夹 aaa以及aaa里面的文件夹和文件  提柜删除
    @Test
    public void  deleteDir() throws Exception{
        boolean flag = fs.delete(new Path("/javaMkDir/aaa"),true);
        System.out.println(flag);
    }

    //查看文件目录下文件还有文件夹的属性
    @Test
    public void testLs() throws Exception{
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"),true);
        while(listFiles.hasNext()){
            LocatedFileStatus fileStatus = listFiles.next();
            System.out.println("文件块的大小："+fileStatus.getBlockSize());
            System.out.println("所属的用户："+fileStatus.getOwner());
            System.out.println("副本数量："+fileStatus.getReplication());
            System.out.println("文件权限："+fileStatus.getPermission());
            System.out.println("路径文件名："+fileStatus.getPath().getName());
            System.out.println("-------------------------------");

            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            for (BlockLocation block:blockLocations){
                System.out.println("块起始的偏移量："+ block.getOffset());
                System.out.println("块长度是："+block.getLength());
                System.out.println("位于块的名称是："+ block.getNames());
                //块所在的datanode 节点
                String[] datanodes = block.getHosts();
                for (String dn:datanodes){
                    System.out.println("datanode:"+dn);
                }
            }
        }
    }

    //非递归查看文件夹的目录
    @Test
    public void  testLs2() throws  Exception{
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        for(FileStatus file:fileStatuses){
            System.out.println(file.getAccessTime());
            System.out.println(file.getPath().getName());
            System.out.println(file.isFile()?"是文件":"不是文件");
        }
    }

    public static void main(String[] args) {

    }
}
