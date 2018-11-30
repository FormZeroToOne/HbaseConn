package com.hbase.dao;

import com.hbase.tool.connHb;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.TableDescriptor;

/**
 * 查看Hbase 中有多少张表 list  扫描表 admin
 */
public class listTable {

    public  static Configuration conf = connHb.getCon();
    public  static void  listTables(){
        Connection conn = null;
        HBaseAdmin admin = null;
        try {
            conn=ConnectionFactory.createConnection(conf);
            admin = (HBaseAdmin)conn.getAdmin();
            TableDescriptor tableDesc []  = admin.listTables();
        /*for (int i = 0 ; i< tableDesc.length; i++){
            System.out.println(tableDesc[i].getTableName());
        }*/
        //list
            for (TableDescriptor table:
                  tableDesc) {
                System.out.println(table.getTableName());
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        finally {
            try {
                if(conn!=null && !conn.isClosed()){
                    conn.close();
                }

            }catch (Exception e)
            {
                e.printStackTrace();
            }
        }

    }
}
