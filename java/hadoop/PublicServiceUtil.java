package com.wang.hadoop.rpc;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;




public class PublicServiceUtil {

    public static void main(String[] args) throws Exception{
        RPC.Builder builder = new RPC.Builder(new Configuration());
        builder.setBindAddress("localhost")
                .setPort(8888)
                .setProtocol(ClientNameNodeProtocol.class)
                .setInstance(new MyNameNode());

        //吧业务类发布成为服务
        RPC.Server server = builder.build();
        server.start();
    }
}
