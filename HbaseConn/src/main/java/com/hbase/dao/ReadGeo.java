package com.hbase.dao;

import com.hbase.tool.connHb;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import javax.sound.midi.Soundbank;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLOutput;

/**
 * Table  以读取文件的方式 添加数据
 */
public class ReadGeo {

   private static HTable  table = null;
    public  static  void  readGsv(String fileName,String tableName)throws Exception{
        //使用字节流读取文件
        FileReader fr  =new  FileReader(fileName);
        BufferedReader br = new BufferedReader(fr);
        Long RowNo = 0L;
        //先读取一行
        br.readLine();
        //建立连接
        Configuration conf = connHb.getCon();
        Connection conn = ConnectionFactory.createConnection(conf);
        //获取表
          table = (HTable)conn.getTable(TableName.valueOf(tableName));

         //循环读取每一行
        while (br.ready()){
            String line= br.readLine();
            insertIntoHb(RowNo,line);
            RowNo++;
            if (RowNo%10000==0){
                System.out.println(RowNo);
            }
        }




    }

    private static void insertIntoHb(Long RowNo, String lines)  throws IOException {
        //用逗号进行拆分
         String[] fields = lines.split(",");
        //列族type:name,cat,type,kind,
        //列族location:addr,prov,city,district,lng,lat
        byte [] cf1= Bytes.toBytes("type");
        byte [] cf2= Bytes.toBytes("location");
        //逐字段读取
        byte[] rk = Bytes.toBytes(RowNo);
        Put p = new  Put(rk);
        if (fields.length==10){
            //add 方法，第一个列族，列名，值
            p.addColumn(cf1,Bytes.toBytes("name"),Bytes.toBytes(fields[0]));
            p.addColumn(cf1, Bytes.toBytes("cat"), Bytes.toBytes(fields[1]));
            p.addColumn(cf1, Bytes.toBytes("type"), Bytes.toBytes(fields[2]));
            p.addColumn(cf1, Bytes.toBytes("kind"), Bytes.toBytes(fields[3]));
            p.addColumn(cf2, Bytes.toBytes("addr"), Bytes.toBytes(fields[4]));
            p.addColumn(cf2, Bytes.toBytes("prov"), Bytes.toBytes(fields[5]));
            p.addColumn(cf2, Bytes.toBytes("city"), Bytes.toBytes(fields[6]));
            p.addColumn(cf2, Bytes.toBytes("district"), Bytes.toBytes(fields[7]));
            p.addColumn(cf2, Bytes.toBytes("lng"), Bytes.toBytes(fields[8]));
            p.addColumn(cf2, Bytes.toBytes("lat"), Bytes.toBytes(fields[9]));

           table.put(p);
        }else {
            System.out.println("数据格式错误:"+RowNo);
        }



























    }
}
