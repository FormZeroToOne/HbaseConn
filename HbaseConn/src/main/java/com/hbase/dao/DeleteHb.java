package com.hbase.dao;

import com.hbase.tool.connHb;
import javafx.scene.control.Tab;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 删除一系列操作 delete
 * 删除表 admin
 * 删除数据 delete
 */
public class DeleteHb {

    public  static Configuration conf = connHb.getCon();

    //删除数据库的表
    public  static  void  deleteTable (String tableName) throws IOException{
        //建立数据库的连接
        Connection  conn  = ConnectionFactory.createConnection(conf);
        //创建一个数据库的管理员
        HBaseAdmin admin = (HBaseAdmin)conn.getAdmin();
        TableName tn = TableName.valueOf(tableName);
        try {
            if (admin.tableExists(tn)) {
                //删除表之前先禁用表，然后才能进行删除
                admin.disableTable(tn);
                admin.deleteTable(tn);
                System.out.println("删除表:"+tableName+"成功");
                admin.close();

            }else {
                System.out.println("需要删除的表："+tableName+"不存在");
                System.exit(0);
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if (conn!=null && !conn.isClosed()){
                conn.close();
            }
        }

    }

    /**
     * 删除一条数据
     */
    public  static void delRow(String tableName,String RowKey)throws  Exception{
        //建立连接
        Configuration conf = connHb.getCon();
        Connection conn =  ConnectionFactory.createConnection(conf);
        //获取表
        HTable table =(HTable)conn.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(RowKey));
        table.delete(delete);
        //关闭资源
        table.close();
        conn.close();
    }
    /**
     * 删除多条数据
     */
     public  static  void  delRows(String tableName,String [] rows) throws  Exception{
         //建立数据库的连接
         Configuration conf = connHb.getCon();
         Connection conn = ConnectionFactory.createConnection(conf);
         //获取表
          HTable table =(HTable)conn.getTable(TableName.valueOf(tableName));
          //删除多条数据
         List<Delete> deletes = new ArrayList<Delete>();
         for (String row:
              rows) {
               Delete delete = new Delete(Bytes.toBytes(row));
               deletes.add(delete);
         }
          table.delete(deletes);
          table.close();
          conn.close();
     }

     public  static void delRowFamily(String tableName,String columnFamily) throws  Exception{
      //建立数据的连接
         Configuration conf = connHb.getCon();
         Connection conn = ConnectionFactory.createConnection(conf);
         //创建数据的管理员
         HBaseAdmin admin = (HBaseAdmin)conn.getAdmin();
         TableName tn = TableName.valueOf(tableName);

         //删除一个指定表的列族
         admin.deleteColumnFamily(tn,Bytes.toBytes(columnFamily));
         admin.close();
         conn.close();


     }

}
