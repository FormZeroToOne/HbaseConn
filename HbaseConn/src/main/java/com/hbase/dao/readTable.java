package com.hbase.dao;

import com.hbase.tool.connHb;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * 读取表中的数据 table
 */
public class readTable {

    public  static  void ReadTable(String tableName,String rowKey){
        //获取配置
        Configuration  conf = connHb.getCon();
        //建立连接
        Connection conn = null;
        TableName tn = TableName.valueOf(tableName);
        Table table = null;
         try{
             conn = ConnectionFactory.createConnection(conf);
             table=conn.getTable(tn);
             Get get = new Get(Bytes.toBytes(rowKey));
             //读数据
             Result result =table.get(get);

             byte [] one = result.getValue(Bytes.toBytes("cf1"),Bytes.toBytes("count"));
             byte [] two = result.getValue(Bytes.toBytes("cf1"),Bytes.toBytes("gname"));
             byte [] three = result.getValue(Bytes.toBytes("cf1"),Bytes.toBytes("oid"));
             byte [] four = result.getValue(Bytes.toBytes("cf2"),Bytes.toBytes("address"));

                 String one1 = Bytes.toString(one);
                 String two2 = Bytes.toString(two);
                 String three3 = Bytes.toString(three);
                 String four4 = Bytes.toString(four);
                 System.out.println("one1:\t"+one1+",two2:\t"+two2+",three3:\t"+three3+",four4:\t"+four4);


         }catch (Exception e){
             e.printStackTrace();
         }
              finally {
             try {
                 if (conn!=null && !conn.isClosed()){
                     conn.close();
                 }
             } catch (IOException e) {
                 e.printStackTrace();
             }

         }


    }
}
