package com.hbase.pojo;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class PrjDesc {

    public static String tbName = "geo_info";
    public String idx;
    public String name;
    public String type;
    public String area;
    public String power;
    public String addr;
    public String lng;
    public String lat;
    public Put getHbasePut() {
        Put p = new Put(Bytes.toBytes(idx));//rowkey
        // 指定列族名，列描述符，值
        p.addColumn(Bytes.toBytes("Prj_attr"), Bytes.toBytes("name"), Bytes.toBytes(this.name));
        p.addColumn(Bytes.toBytes("Prj_attr"), Bytes.toBytes("area"), Bytes.toBytes(this.area));
        p.addColumn(Bytes.toBytes("Prj_attr"), Bytes.toBytes("type"), Bytes.toBytes(this.type));
        p.addColumn(Bytes.toBytes("Prj_attr"), Bytes.toBytes("power"), Bytes.toBytes(this.power));
        p.addColumn(Bytes.toBytes("location"), Bytes.toBytes("addr"), Bytes.toBytes(this.addr));
        p.addColumn(Bytes.toBytes("location"), Bytes.toBytes("lng"), Bytes.toBytes(this.lng));
        p.addColumn(Bytes.toBytes("location"), Bytes.toBytes("lat"), Bytes.toBytes(this.lat));
        return p;
    }
}
