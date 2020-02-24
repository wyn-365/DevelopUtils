package com.wang.zk;

import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class DistributedClient {
	
	private static final String connectString="192.168.52.200:2181,192.168.52.201:2181,192.168.52.202";
	private static final int sessionTimeout = 2000;
	private ZooKeeper zk = null;
	private static final String parentNode = "/servers";
	//多线程程序
	private volatile List<String> serverList;
	
	//创阿金zookeeper的客户端连接
	public void getConnect() throws Exception {
		
		zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				System.out.println(event.getType()+"------"+event.getPath());
				try {
					//更新服务器列表，注册箭筒
					getServerList();
					
				}catch(Exception e) {
					
				}
			}
	
		});
	}
	
	
	//获取服务器信息列表	
	public void getServerList() throws Exception {
		//获取服务器子节点信息，对父节点进行监听
		List<String> children = zk.getChildren(parentNode, true);
		List<String> servers = new ArrayList<String>();
		for(String child:children) {
			
			byte[] data = zk.getData(parentNode+"/child", false, null);
			servers.add(new String(data));
			
		}
		//servers赋值给成员变，已提供给各业务线程的使用
		serverList=servers;
		//打印服务器列表
		System.out.println(serverList);
	}
	
	//处理业务服务
	public void handleBusiness() throws InterruptedException {
		System.out.println("Client正在工作！！！...");
		Thread.sleep(Long.MAX_VALUE);
	}
	
	
	public static void main(String[] args) throws Exception {
		
		//获取zookeeper连接
		DistributedClient client = new DistributedClient();
		client.getConnect();
		//获取servers的子节点信息【监听】，获取服务器信息列表
		client.getServerList();
		
		//业务线程启动
		client.handleBusiness();
		
	}
}
