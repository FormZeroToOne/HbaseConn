package com.hbase.dao;

import com.hbase.tool.connHb;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

/**
 * 创建表 admin
 */
public class CreateTable {

     public static Configuration conf = connHb.getCon();
    public static boolean createTable(String tableName,String [] columnFamilys) throws IOException{
        boolean flag = false;
        Connection conn = null;
        try {
            conn = ConnectionFactory.createConnection(conf);
            //创建数据库管理员
            HBaseAdmin admin=(HBaseAdmin)conn.getAdmin();
            TableName tn = TableName.valueOf(tableName);
            if (admin.tableExists(tn)){
                System.out.println(tableName+"表已经存在");
                conn.close();
            }else {
                //新建一个表的描述
                TableDescriptorBuilder tableDesc = TableDescriptorBuilder.newBuilder(tn);
                //在表的描述里添加列族
                for (String columnFamily : columnFamilys   ) {
                    byte[] cfn = columnFamily.getBytes();
                    ColumnFamilyDescriptorBuilder cfdb = ColumnFamilyDescriptorBuilder.newBuilder(cfn);
                    cfdb.setMaxVersions(3);
                    tableDesc.setColumnFamily(cfdb.build());

                }
                  admin.createTable(tableDesc.build());
                System.out.println("创建表:"+tableName+"成功");
                admin.close();
                flag = true;
            }
        }catch (Exception e){
             e.printStackTrace();
        }finally {

            if (conn!=null && !conn.isClosed()){
            conn.close();
            }
        }


        return flag;
    }



}
