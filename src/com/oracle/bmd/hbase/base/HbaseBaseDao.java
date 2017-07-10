package com.oracle.bmd.hbase.base;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseBaseDao {
	
	public Configuration conf = HBaseConfiguration.create();
	public HbaseBaseDao(){
		//¥Ê¥¢zookeeperµÿ÷∑
		conf.set("hbase.zookeeper.quorum", "yunfei3:2181,yunfei2:2181,yunfei1:2181");
	}
	
	public byte [] toBytes(String info){
		return Bytes.toBytes(info);
	}
	
	public String toStringBytes(byte content[]) {
		return Bytes.toString(content);
	}
}
