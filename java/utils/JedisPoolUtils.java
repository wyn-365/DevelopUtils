package com.jedis.util;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import sun.security.krb5.Config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * 连接池的工具类
 *   加载配置文件  连接池的参数
 *   提供获取连接的方法
 *
 */
public class JedisPoolUtils {

    private static JedisPool jedisPool;

    static{
        //静态代码快，加载的时候 读取配置文件  只有一次
        InputStream is = JedisPoolUtils.class.getClassLoader().getResourceAsStream("jedis.properties");

        //创建对象
        Properties pro = new Properties();

        //关联文件
        try {
            pro.load(is);
        } catch (IOException e) {
            e.printStackTrace();
        }

        //获取数据，设置到配置文件中
        JedisPoolConfig config = new JedisPoolConfig();

        //读取并设置
        config.setMaxTotal(Integer.parseInt(pro.getProperty("maxTotal")));

        config.setMaxIdle(Integer.parseInt(pro.getProperty("maxIdle")));

        //初始化pool
        jedisPool  =  new JedisPool(config,pro.getProperty("host"),Integer.parseInt(pro.getProperty("port")));
    }

    //获取连接的方法
    public static Jedis getJedis(){

        return jedisPool.getResource();
    }
}
