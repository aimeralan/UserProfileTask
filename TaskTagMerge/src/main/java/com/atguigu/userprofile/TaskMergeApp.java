package com.atguigu.userprofile;

import com.atguigu.userprofile.dao.TagInfoDAO;
import com.atguigu.userprofile.util.MyPropertiesUtil;
import com.atguigu.userprofile.bean.TagInfo;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class TaskMergeApp {

    //1  如何得到多个标签单表？？
    // 通过查询mysql 得到  已启用的标签  通过tag_code就可以得到表名
    //
    //2 标签宽表 怎么建立
    // 1  手动建立 2 程序建立
    // 1  每天建立一张 可以用日期当后缀     2 一共建立一张表 每天一个分区
    //create table xxxxx
    //( uid , ...... )
    //不分区
    // 文本格式
    //location ...
    //
    //
    // 3 通过一条pivot SQL语句把多个标签单表合并为一个标签宽表

    public static void main(String[] args) {
        String taskId = args[0];
        String busiDate = args[1];

        //spark环境
//        SparkConf conf = new SparkConf().setAppName("task_merge_app").setMaster("local[*]");
        SparkConf conf = new SparkConf().setAppName("task_merge_app");
        SparkSession sparkSession = SparkSession.builder().config(conf)
                .enableHiveSupport().getOrCreate();


        //1 通过查询mysql 得到  已启用的标签list  通过tag_code就可以得到表名    tag_info

        List<TagInfo> tagInfoList  = TagInfoDAO.getTagInfoListWithOn();


        //2 标签宽表   每天建立一张 可以用日期当后缀
        //create table xxxxx      --> up_tag_merge_$busiDate
        //( uid string, ...... )   --> tagCode1 string ,tagCode2 string ....
        //不分区
        // 文本格式
        //location ..
        String  tableName= "up_tag_merge_"+ busiDate.replace("-","") ;
        List<String>  filedNameList = tagInfoList.stream()
                .map(tagInfo ->  tagInfo.getTagCode().toLowerCase()  +" string")
                .collect(Collectors.toList());
        String filedNameSQL   = StringUtils.join(filedNameList ,",");

        Properties properties = MyPropertiesUtil.load("config.properties");
        String hdfsPath = properties.getProperty("hdfs-store.path");
        String upDbName = properties.getProperty("user-profile.dbname");
        String dwDbName = properties.getProperty("data-warehouse.dbname");

        sparkSession.sql( "use "+ upDbName);
        String dropSQL=  "drop table if exists "+upDbName+"."+tableName;
        System.out.println(dropSQL);
        sparkSession.sql(dropSQL);



        String createTableSQL = "create table if not exists " + upDbName + "." + tableName +
                " (uid string ,   "+  filedNameSQL + " ) " +
                " ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'" +
                " location '"+hdfsPath+"/"+upDbName+"/"+tableName+"' ";
        System.out.println(createTableSQL);
        sparkSession.sql(createTableSQL);


        // 3 通过一条pivot SQL语句把多个标签单表合并为一个标签宽表
        ////维度列 uid 旋转列 tag_code  聚合列 tag_value  max(xx)
        // select * from (   select uid,tag_code,tag_value from xxx_gender where dt='xxx'
        //                    union all
        //                  select * from xxx_agegroup where dt='xxx'
        //                    union all
        //                  .....
        // ) pivot (  max(tag_value) tv   for  tag_code in ('xxx_gender','xxx_agegroup') )
        //
        //每个标签产生一条语句
        List<String>  tagSQLList  = tagInfoList.stream().map(tagInfo ->
                "select uid,'"+tagInfo.getTagCode().toLowerCase()
                        +"' tag_code,tag_value from "+upDbName+"."+ tagInfo.getTagCode().toLowerCase() +" where dt='"+busiDate+"' ").collect(Collectors.toList());

        String unionSQL=StringUtils.join(tagSQLList ," union all ") ;

        List<String> tagCodeList = tagInfoList.stream()
                .map(tagInfo ->  "'"+ tagInfo.getTagCode().toLowerCase()+"'")
                .collect(Collectors.toList());
        String tagCodeSQL  = StringUtils.join(tagCodeList,",");

        String pivotSQL=
                " select * from (  "+unionSQL+" )"+
                        " pivot (  max(tag_value) tv   for  tag_code in ("+tagCodeSQL+") )";


        // println(pivotSQL)

        sparkSession.sql( " use "+  upDbName );
        String  insertSQL= " insert overwrite table  "+upDbName +  "." + tableName+ " " + pivotSQL ;

        System.out.println(insertSQL);

        sparkSession.sql(insertSQL);

    }

}
