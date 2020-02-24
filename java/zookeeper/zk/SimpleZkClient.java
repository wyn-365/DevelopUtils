package com.wang.zk;

import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

public class SimpleZkClient {
	
	private static final String connectString="192.168.52.200:2181,192.168.52.201:2181,192.168.52.202";
	private static final int sessionTimeout = 2000;
	ZooKeeper zkClient = null;
	
	
	public static void main(String[] args){
		
	
	}
	
	@Before
	public void init() throws Exception{
		zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				System.out.println(event.getType()+"------"+event.getPath());
				try {
					zkClient.getChildren("/", true);
					
				}catch(Exception e) {
					
				}
			}
	
		});
	}
	
	//数据的增删改查
	
	//创阿金节点到zk中
	//数据类型神魔都可以，但是要转成byte
	@Test
	public void testCreate() throws Exception{
		String nodeCreated = zkClient.create("/eclipse", "hellozk".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
	}
	
	//判断节点是否存在
	@Test
	public void testExist() throws Exception{
		Stat stat = zkClient.exists("/eclipse", false);
		System.out.println(stat==null?"not exist":"exist");
	}
	
	//获取子节点
	@Test
	public void getChildren() throws Exception{
		List<String> children = zkClient.getChildren("/", true);
		for (String child:children) {
			System.out.println(child);
		}
		Thread.sleep(Long.MAX_VALUE); //主线程一直存在
	}
	
	//获取znode的数据
	@Test
	public void getData() throws Exception{
		byte[] data = zkClient.getData("/eclipse", false, null);
		System.out.println(new String(data));
	}
	
	//删除节点
	@Test
	public void deleteZnode() throws Exception{
		//指定删除的版本  -1  删除所有
		zkClient.delete("/eclipse", -1);
		
	}
	
	//改节点
	@Test
	public void setData() throws Exception{
		//改节点
		zkClient.setData("eclipse", "i miss you".getBytes(), -1);
		
		//获取一下试试看
		byte[] data = zkClient.getData("/eclipse", false, null);
		System.out.println(new String(data));
	}
	
}
