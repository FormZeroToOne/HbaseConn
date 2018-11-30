package com.hbase.Test;


import com.hbase.dao.*;

public class testHb {

    public static void main(String[] args) {

       try {
         /*  int num = 0;

               if (num==0) {
                   String[] cf = {"type", "location"};
                   CreateTable.createTable("compy", cf);
                     num=1;
                   if (num==1){
                       ReadGeo.readGsv("C:\\Users\\Lenovo\\Desktop\\TXT\\hive\\建表语句\\company.csv","compy");
                   }
               }*/

           ReadGeo.readGsv("C:\\Users\\Lenovo\\Desktop\\TXT\\hive\\建表语句\\company.csv","compy");


        /*   DeleteHb.deleteTable("TB");*/
        /*   listTable.listTables();*/
        /*   readTable.ReadTable("t1","laobai");*/
        /*   insertHb.testAddPuts();*/
          /* scanHb.scanTable("geo_info");*/

       }catch (Exception e){
           e.printStackTrace();
       }

    }



}