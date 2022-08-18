package com.atguiu.profile;

import com.atguigu.userprofile.bean.TagInfo;
import com.atguigu.userprofile.dao.TagInfoDAO;
import com.atguigu.userprofile.util.MyPropertiesUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import com.atguigu.userprofile.util.MyClickHouseUtil;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class TaskExportClickHouse {
    public static void main(String[] args) {
        //获得业务参数
        String taskId = args[0];
        String busiDate = args[1];

        //spark环境
        SparkConf conf = new SparkConf().setAppName("task_exprot_clickhouse_app").setMaster("local[*]");
        SparkSession sparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();

        //1 查询已启用的标签列表
        List<TagInfo> tagInfoList = TagInfoDAO.getTagInfoListWithOn();

        //2 clickhouse的建表
        //同任务2 的建表逻辑大致相同 1 程序建表 2 每天一张表
        // create table xxx
        //(uid String , $fieldSQL )
        //engine=  MergeTree
        //分区？不分区
        //primary key uid     可省
        //order by  uid
        String tableName = "up_tag_merge_" + busiDate.replace("-", "");
        List<String> filedNameList = tagInfoList.stream()
                .map(tagInfo -> tagInfo.getTagCode().toLowerCase() + " String").collect(Collectors.toList());
        String filedNameSQL = StringUtils.join(filedNameList, ",");

        Properties properties = MyPropertiesUtil.load("config.properties");
        String upDbName = properties.getProperty("user-profile.dbname");

        String dropSQL = "drop table if exists  " + tableName;
        System.out.println(dropSQL);
        MyClickHouseUtil.executeSql(dropSQL);
        //3 读取hive的宽表
        //rdd dataframe dataset
        String createTableSQL = "create table if not exists  " + tableName +
                " (uid String ,   " + filedNameSQL + " ) " +
                " engine=  MergeTree" +
                " order by uid ";
        MyClickHouseUtil.executeSql(createTableSQL);


        Dataset<Row> dataset = sparkSession.sql("select * from " + upDbName + "." + tableName);

        //4 写入clickhouse
        String clickhouseUrl = properties.getProperty("clickhouse.url");

        dataset.write().mode(SaveMode.Append)
                .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
                .option("batchsize", 500) //批量提交1.减少连接 网络IO次数 2.减少磁盘碎片

                .option("isolationLevel", "NONE")   //事务关闭
                .option("numPartitions", "4") // 设置并发
                .jdbc(clickhouseUrl, tableName, new Properties());
    }

}
