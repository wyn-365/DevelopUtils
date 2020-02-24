package com.wang.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;

public class HBaseClientDML {


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
     * 正删改查
     */

    /**
     * 增
     */
    @Test
    public void testPut()throws Exception{
        //获取一个指定表对象
        Table table = conn.getTable(TableName.valueOf("user_info"));

        //构造插入的数据为一个put类型的对象
        Put put = new Put(Bytes.toBytes("001"));
        put.addColumn(Bytes.toBytes("base_info"),Bytes.toBytes("username"),Bytes.toBytes("王一宁"));
        put.addColumn(Bytes.toBytes("base_info"),Bytes.toBytes("age"),Bytes.toBytes("18"));
        put.addColumn(Bytes.toBytes("extra_info"),Bytes.toBytes("addr"),Bytes.toBytes("北京"));


        Put put2 = new Put(Bytes.toBytes("002"));
        put2.addColumn(Bytes.toBytes("base_info"),Bytes.toBytes("username"),Bytes.toBytes("王一宁"));
        put2.addColumn(Bytes.toBytes("base_info"),Bytes.toBytes("age"),Bytes.toBytes("18"));
        put2.addColumn(Bytes.toBytes("extra_info"),Bytes.toBytes("addr"),Bytes.toBytes("北京"));

        ArrayList<Put> puts = new ArrayList<>();

        puts.add(put);
        puts.add(put2);

        //插进去
        table.put(puts);


        //关闭
        table.close();
        conn.close();
    }


    /**
     * 删
     */

    @Test
    public void testDelete()throws Exception{
        Table table = conn.getTable(TableName.valueOf("user_info"));
        //构造要删除的封装对象的集合
        Delete delete1 = new Delete(Bytes.toBytes("001"));

        Delete delete2 = new Delete(Bytes.toBytes("002"));
        delete2.addColumn(Bytes.toBytes("extra_info"),Bytes.toBytes("addr"));

        ArrayList<Delete> deletes = new ArrayList<>();
        deletes.add(delete1);
        deletes.add(delete2);

        table.delete(deletes);

        table.close();
        conn.close();
    }


    /**
     * 查
     */

    @Test
    public void testGet()throws Exception{
        Table table = conn.getTable(TableName.valueOf("user_info"));

        Get get = new Get("002".getBytes());

        Result result = table.get(get);


        CellScanner cellScanner = result.cellScanner();
        while(cellScanner.advance()){
            Cell cell = cellScanner.current();
            byte[] familyArray = cell.getFamilyArray(); //列族字节数组
            byte[] qualifierArray = cell.getQualifierArray(); //列名字节数组
            byte[] valueArray = cell.getValueArray(); //value的字节数组
            System.out.println("列族名："+new String(familyArray));
            System.out.println("列名："+new String(qualifierArray));
            System.out.println("值："+new String(valueArray));

        }
    }

    /**
     * 按照行键的范围查询数据
     */
    @Test
    public void testScan()throws Exception{

        Table table = conn.getTable(TableName.valueOf("user_info"));
        Scan scan = new Scan("001".getBytes(),"005".getBytes());

        //ResultScanner scanner = table.getScanner();

        //Iterator<Result> iterator = scanner.iterator();
        /*while (iterator.hasNext()){

            Result result = iterator.next();
            CellScanner cellScanner = result.cellScanner();
            while(cellScanner.advance()){
                Cell cell = cellScanner.current();
                byte[] familyArray = cell.getFamilyArray(); //列族字节数组
                byte[] qualifierArray = cell.getQualifierArray(); //列名字节数组
                byte[] valueArray = cell.getValueArray(); //value的字节数组
                System.out.println("列族名："+new String(familyArray));
                System.out.println("列名："+new String(qualifierArray));
                System.out.println("值："+new String(valueArray));

            }
        }
*/


        /**1.批量数据如何导入hbase？？？？？？？？  循环put吗？？？ bulkloader工具
         2.hbase的性能调优
          3.MapReduce分析hbase中的数据【数据量大的情况下】maptask reducetask 任务的运行
         4.放入hbase或者hdfs中都可以的
          5.hbase 中的数据建立索引
        6.
        */

    }
}








