package com.wang.hadoop.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.net.InetSocketAddress;

public class MyHdfsClient {

    public static void main(String[] args)throws Exception {
        ClientNameNodeProtocol namenode = RPC.getProxy(ClientNameNodeProtocol.class, 1L, new InetSocketAddress("localhost", 8888), new Configuration());
        //调用远程socket请求
        String metaData = namenode.getMetaData("/a.txt.copy");
        System.out.println(metaData);


    }
}
