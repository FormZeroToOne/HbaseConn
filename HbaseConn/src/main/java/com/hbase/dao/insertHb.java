package com.hbase.dao;

import com.hbase.pojo.PrjDesc;
import com.hbase.tool.connHb;
import org.apache.commons.configuration.ConfigurationFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

import java.util.ArrayList;
import java.util.List;

/**
 * 添加数据 table
 */
public class insertHb {

    public static  void  insertPutList(String tableName, List<Put> puts) throws Exception{
        //建立一个数据库的连接
        Configuration conf = connHb.getCon();
        Connection conn  = ConnectionFactory.createConnection(conf);
        //获取表
        TableName table = TableName.valueOf(tableName);
        HTable hTable =(HTable)conn.getTable(table);
        //关闭资源
        hTable.close();
        conn.close();


    }
   public static void testAddPuts() {
        PrjDesc p = new PrjDesc();
        p.idx = "44";
        p.name = "科源大厦热水供应项目";
        p.area = "1000";
        p.type = "2";
        p.power = "50";
        p.addr = "北京市海淀区西三环北路105号";
        p.lng = "116.309400";
        p.lat = "39.928940";
        PrjDesc q = new PrjDesc();
        q.idx = "45";
        q.name = "裕惠大厦供暖项目";
        q.type = "1";
        q.area = "80000";
        q.power = "800";
        q.addr = "北京市海淀区阜成路73号";
        q.lng = "116.298790";
        q.lat = "39.925620";
        List<Put> puts = new ArrayList<>();
        puts.add(p.getHbasePut());
        puts.add(q.getHbasePut());
        try {
            insertPutList(PrjDesc.tbName, puts);
            System.out.println("表加入数据"+PrjDesc.tbName+"成功");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
