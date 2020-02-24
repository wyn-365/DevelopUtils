package com.wang.hadoop.rpc;

public class MyNameNode implements ClientNameNodeProtocol{

    //模拟NameNode业务方法之一，查询获取元数据
    @Override
    public String  getMetaData(String path){

        return path+":2 - {BLK_1,BLK_2} ...";
    }
}
