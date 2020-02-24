package com.wang.zk;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

public class DistributedServer {
	
	private static final String connectString="192.168.52.200:2181,192.168.52.201:2181,192.168.52.202";
	private static final int sessionTimeout = 2000;
	private ZooKeeper zk = null;
	private static final String parentNode = "/servers";
	
	//创阿金zookeeper的客户端连接
	public void getConnect() throws Exception {
		
		zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				System.out.println(event.getType()+"------"+event.getPath());
				try {
					zk.getChildren("/", true);
					
				}catch(Exception e) {
					
				}
			}
	
		});
	}
	
	//向zookeeper服务器注册服务器
	public void registerServer(String hostname) throws Exception {
		
		String create = zk.create(parentNode+"/server",hostname.getBytes() , Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		System.out.println(hostname + "is online..."+ create);
	}
	
	//处理业务服务
	public void handleBusiness(String hostname) throws InterruptedException {
		System.out.println(hostname + "正在工作！！！...");
		Thread.sleep(Long.MAX_VALUE);
	}
	
	
	public static void main(String[] args) throws Exception {
		//获取zk的连接
		DistributedServer server  = new DistributedServer();
		server.getConnect();
		
		//利用zk注册服务器信息
		server.registerServer(args[0]);
		
		//启动业务功能
		server.handleBusiness(args[0]);
	}

}
