package com.oracle.bmd.hbase.test;

import com.oracle.bmd.hbase.dao.StuDao;

public class Test {

	public static void main(String[] args) {
		
		StuDao stuDao = new StuDao();
		stuDao.addStu();
		
		stuDao.getStu();
		
		stuDao.scanStu();
		
		stuDao.scanFilterStu();
	}

}
