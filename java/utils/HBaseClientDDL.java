package com.wang.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.junit.Before;
import org.junit.Test;

public class HBaseClientDDL {

    Connection conn = null;

    @Before
    public void getConn()throws Exception{
        //创建连接对象
        Configuration conf = HBaseConfiguration.create();//家在配合文件
        conn = ConnectionFactory.createConnection(conf);
        conf.set("hbase.zookeeper.quorum","192.168.52.200:2181,192.168.52.201:2181,192.168.52.202:2181");
        System.out.println("000000000000000000");
    }

    /**
     * DDL 定义创建表
     */
    @Test
    public void testCreateTable() throws Exception{

        System.out.println("jjjjjj");
        //连接构造一个DDL操作器
        Admin admin = conn.getAdmin();

        //定义表描述对象
        HTableDescriptor ht = new HTableDescriptor(TableName.valueOf("user_info2"));

        //创建列族描述对象
        HColumnDescriptor hc1 = new HColumnDescriptor("base_info");
        hc1.setMaxVersions(3);//设置列族中存储数据的最大版本数，默认是1
        HColumnDescriptor hc2 = new HColumnDescriptor("extra_info");

        //将列族定义信息放入表定义信息中
        ht.addFamily(hc1);
        ht.addFamily(hc2);


        //DDL对象来建表
        admin.createTable(ht);

        //关闭连接
        admin.close();
        conn.close();

    }

    /**
     * 修改表定义------添加一个列族
     */
    public void testAlterTable()throws Exception{
        Admin admin = conn.getAdmin();

        //取出旧的表定义信息
        HTableDescriptor tableDescriptor = admin.getTableDescriptor(TableName.valueOf("user_info2"));

        //构造一个列族定义
        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("other_info");
        hColumnDescriptor.setBloomFilterType(BloomType.ROWCOL);//设置列族的布隆过滤器

        //列族定义添加到表定义中
        tableDescriptor.addFamily(hColumnDescriptor);

        //修改过的标定仪交给admin执行
        admin.modifyTable(TableName.valueOf("user_info2"),tableDescriptor);

        admin.close();
        conn.close();
    }
    /**
     * 删除表
     */

    @Test
    public void testDropTable()throws Exception{

        Admin admin = conn.getAdmin();

        //先下线表，停用表
        admin.disableTable(TableName.valueOf("user_info2"));

        //删除表
        admin.deleteTable(TableName.valueOf("user_info2"));
    }
}
