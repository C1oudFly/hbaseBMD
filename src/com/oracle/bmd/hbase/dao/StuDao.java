package com.oracle.bmd.hbase.dao;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ColumnCountGetFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.InclusiveStopFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RandomRowFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueExcludeFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;

import com.oracle.bmd.hbase.base.HbaseBaseDao;
import com.oracle.bmd.hbase.common.HBaseConstants;

public class StuDao extends HbaseBaseDao {
	
	public void addStu(){
		try {
			HTable table = new HTable(conf, toBytes(HBaseConstants.TABLE_NAME));
			Put put = new Put(toBytes("rk001"));
			put.add(toBytes(HBaseConstants.FIMALY), toBytes(HBaseConstants.COLUMN_NAME), toBytes("zhangsan"));
			put.add(toBytes(HBaseConstants.FIMALY), toBytes(HBaseConstants.COLUMN_SEX), toBytes("nan"));
			put.add(toBytes(HBaseConstants.FIMALY), toBytes(HBaseConstants.COLUMN_AGE), toBytes("20"));
			table.put(put);
			table.close();
		
		} catch (Exception e) {
			// TODO: handle exception
		}
	}
	
	public void getStu(){
		try {
			HTable table = new HTable(conf, toBytes(HBaseConstants.TABLE_NAME));
			Get get = new Get(toBytes("rk001"));
			Result result = table.get(get);
			byte name [] = result.getValue(toBytes(HBaseConstants.FIMALY), toBytes(HBaseConstants.COLUMN_NAME));
			byte sex [] = result.getValue(toBytes(HBaseConstants.FIMALY), toBytes(HBaseConstants.COLUMN_SEX));
			System.out.println(toStringBytes(name) + "\t" + toStringBytes(sex));
			table.close();
		
		} catch (Exception e) {
			// TODO: handle exception
		}
	}
	
	public void scanStu(){
		try {
			HTable table = new HTable(conf, toBytes(HBaseConstants.TABLE_NAME));
			Scan scan = new Scan();
			ResultScanner rs = table.getScanner(scan);
			for(Result result : rs){
				byte name [] = result.getValue(toBytes(HBaseConstants.FIMALY), toBytes(HBaseConstants.COLUMN_NAME));
				byte sex [] = result.getValue(toBytes(HBaseConstants.FIMALY), toBytes(HBaseConstants.COLUMN_SEX));
				System.out.println(toStringBytes(name) + "\t" + toStringBytes(sex));
			}
			table.close();
		
		} catch (Exception e) {
			// TODO: handle exception
		}
	}
	
	public void scanFilterStu(){
		try {
			HTable table = new HTable(conf, toBytes(HBaseConstants.TABLE_NAME));
			Scan scan = new Scan();
			//前缀过滤器
			PrefixFilter pre = new PrefixFilter(toBytes("rk"));
			scan.setFilter(pre);
			
			//RowFilter rowFilter = new RowFilter(CompareOp.GREATER_OR_EQUAL, new RegexStringComparator("^[a-z A-Z 0-9]{2,}$"));
			
			
			//比较过滤器
			RowFilter rowFilter = new RowFilter(CompareOp.EQUAL, new BinaryComparator(toBytes("rk001")));
			scan.setFilter(rowFilter);
			
			//单列值过滤器
			SingleColumnValueFilter scf = new SingleColumnValueFilter(toBytes(HBaseConstants.FIMALY),
					toBytes(HBaseConstants.COLUMN_SEX), CompareOp.EQUAL, new SubstringComparator("nan"));

			scan.setFilter(scf);
			
			//多个过滤器
			SingleColumnValueFilter scf1 = new SingleColumnValueFilter(toBytes(HBaseConstants.FIMALY),
					toBytes(HBaseConstants.COLUMN_NAME), CompareOp.EQUAL, new SubstringComparator("马"));
			
			FilterList list=new FilterList();
			list.addFilter(scf);
			list.addFilter(scf1);
			
			scan.setFilter(list);

			//不包含的单列值过滤器
			SingleColumnValueExcludeFilter filter = new SingleColumnValueExcludeFilter(toBytes(HBaseConstants.FIMALY),
					toBytes(HBaseConstants.COLUMN_SEX), CompareOp.EQUAL, new SubstringComparator("nan"));

			scan.setFilter(filter);
			
			//随机值过滤器
			RandomRowFilter filter2=new RandomRowFilter((float)0.2);
			scan.setFilter(filter2);
			
			
			scan.setStartRow(toBytes("rk001"));
			//scan.setStopRow(toBytes("rk006"));
			InclusiveStopFilter filter3=new InclusiveStopFilter(toBytes("rk002"));			
			scan.setFilter(filter3);
			
			//
			Filter ccf = new ColumnCountGetFilter(3);			
			scan.setFilter(ccf);
			
			//
			Filter ccf1 = new ValueFilter(CompareOp.EQUAL,new SubstringComparator("nan"));			
			scan.setFilter(ccf1);
			
			//
			Filter ccf2 = new ValueFilter(CompareOp.EQUAL,new SubstringComparator("na"));			
			scan.setFilter(ccf2);
			
			ResultScanner rs = table.getScanner(scan);
			for(Result result : rs){
				byte name [] = result.getValue(toBytes(HBaseConstants.FIMALY), toBytes(HBaseConstants.COLUMN_NAME));
				byte sex [] = result.getValue(toBytes(HBaseConstants.FIMALY), toBytes(HBaseConstants.COLUMN_SEX));
				System.out.println(toStringBytes(name) + "\t" + toStringBytes(sex));
			}
			table.close();
		
		} catch (Exception e) {
			// TODO: handle exception
		}
	}
}
