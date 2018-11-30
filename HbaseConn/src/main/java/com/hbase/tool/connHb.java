package com.hbase.tool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class connHb {

     public static Configuration getCon(){
      Configuration config  = HBaseConfiguration.create();
      config.set("hbase.master","192.168.43.131");
    config.set("hbase.zookeeper.quorum","192.168.43.131,192.168.43.130,192.168.43.132");
    config.set("hbase.zookeeper.property.clientPort","2181");
    //不可用
   /*config.addResource(new Path("/opt/module/hbase/conf/hbase-site.xml"));
    config.addResource(new Path("/opt/module/hadoop/conf/core-site.xml"));*/


    return  config;
     }

}
