package com.hbase.dao;

import com.hbase.tool.connHb;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

/**
 * 全表扫描  table
 */
public class scanHb {

    public  static  void scanTable(String tableName)throws  Exception{
       //建立一个数据库的连接
        Configuration conf = connHb.getCon();
        Connection conn = ConnectionFactory.createConnection(conf);
        //获取表
        HTable table = (HTable)conn.getTable(TableName.valueOf(tableName));
        //创建一个扫描对象
        Scan scan = new Scan();
        //扫描全表输出结果
        ResultScanner rs = table.getScanner(scan);
        for (Result result:
             rs) {
            for (Cell cell:
                 result.rawCells()) {
                System.out.println("行键:"+new String(CellUtil.cloneRow(cell))+"\t"+"列族:"
                                   +new String(CellUtil.cloneFamily(cell))+"\t"+"列名:"
                                   +new String(CellUtil.cloneQualifier(cell))+"\t"+"值:"
                                   +new String(CellUtil.cloneValue(cell)));
            }
        }
      //关闭资源
         rs.close();
        table.close();
        conn.close();


    }

}
