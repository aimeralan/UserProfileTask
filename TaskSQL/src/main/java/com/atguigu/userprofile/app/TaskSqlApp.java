package com.atguigu.userprofile.app;

import com.atguigu.userprofile.bean.TagInfo;
import com.atguigu.userprofile.bean.TaskInfo;
import com.atguigu.userprofile.bean.TaskTagRule;
import com.atguigu.userprofile.constant.ConstCodes;
import com.atguigu.userprofile.dao.TagInfoDAO;
import com.atguigu.userprofile.dao.TaskInfoDAO;
import com.atguigu.userprofile.dao.TaskTagRuleDAO;
import com.atguigu.userprofile.util.MyPropertiesUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class TaskSqlApp {


    public static void main(String[] args) {
        //获得业务参数
        String taskId = args[0];
        String busiDate = args[1];

        //spark环境
//        SparkConf conf = new SparkConf().setAppName("task_sql_app").setMaster("local[*]");
        SparkConf conf = new SparkConf().setAppName("task_sql_app");
        SparkSession sparkSession = SparkSession.builder()
                .config(conf).enableHiveSupport().getOrCreate();


        //  1  得到标签的定义、标签任务、标签的值映射
        //     读取mysql     tag_info ,task_info , task_tag_rule
        //    利用jdbc读取
        TagInfo tagInfo = TagInfoDAO.getTagInfoByTaskId(taskId);
        TaskInfo taskInfo = TaskInfoDAO.getTaskInfo(taskId);
        List<TaskTagRule> taskTagRuleList =
                TaskTagRuleDAO.getTaskTagRuleList(taskId);

        //2 程序自动建表，根据标签的各种定义，摘取建表要素
        //create table表名         --》 标签编码
        //（字段名，字段类型）   （uid   string ,  tag_value   $tagValueType判断 ）     通过tag_info 的 标签值类型
        //分区                           每天一份数据   partitioned by (dt string)
        //comment                    标签的中文名称
        //格式  压缩                   不压缩  普通文本格式 便于计算导出
        //存储位置                     location    hdfsPath/库名/表名

        String tableName = tagInfo.getTagCode().toLowerCase();
        String comment = tagInfo.getTagName();

        String fieldType = null;
        if (ConstCodes.TAG_VALUE_TYPE_LONG.equals(tagInfo.getTagValueType())) {
            fieldType = "bigint";
        } else if (ConstCodes.TAG_VALUE_TYPE_DECIMAL.equals(tagInfo.getTagValueType())) {
            fieldType = "decimal";
        } else if (ConstCodes.TAG_VALUE_TYPE_STRING.equals(tagInfo.getTagValueType())) {
            fieldType = "string";
        } else if (ConstCodes.TAG_VALUE_TYPE_DATE.equals(tagInfo.getTagValueType())) {
            fieldType = "string";
        }

        Properties properties = MyPropertiesUtil.load("config.properties");
        String hdfsPath = properties.getProperty("hdfs-store.path");
        String upDbName = properties.getProperty("user-profile.dbname");
        String dwDbName = properties.getProperty("data-warehouse.dbname");

        String createTableSQL = "create table if not exists "
                + upDbName + "." + tableName +
                " (uid string ,  tag_value  " + fieldType + " ) " +
                " partitioned by (dt string) " +
                " comment '" + comment + "'" +
                " ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'" +
                " location '"+hdfsPath+"/"+upDbName+"/"+tableName+"' ";

        System.out.println(createTableSQL);
        sparkSession.sql(createTableSQL);

        //3 根据把查询值转换为四级标签
        //    //作业兼容问题：   如果没有四级标签的映射，
        //    则不用casewhen  直接用queryvalue作为查询结果即可

        String taskSQL = taskInfo.getTaskSql();

        String tagValueSQL;
        if (taskTagRuleList.size() > 0) {

            List<String> whenThenList = taskTagRuleList.stream().map(taskTagRule ->
                    " when '" + taskTagRule.getQueryValue() + "' then '" + taskTagRule.getSubTagValue() + "' ").collect(Collectors.toList());

            String whenThenSQL = StringUtils.join(whenThenList, " ");

            tagValueSQL = "case query_value" + whenThenSQL + " end as  tag_value";
        } else {
            tagValueSQL = " query_value as tag_value";
        }


        taskSQL = taskSQL.replace("$dt", busiDate);

        // 把 sql 中的$dt 替换成业务日期
        String selectSQL = " select uid, " + tagValueSQL + " from (" + taskSQL + ") tt";
        System.out.println(selectSQL);

        // 4 生成insert 语句

        String insertSQL =  " insert overwrite table  "+upDbName+"."+tableName+
                " partition (dt='"+busiDate+"') "+
                selectSQL;

        System.out.println(insertSQL);
        sparkSession.sql( "use "+dwDbName);
        sparkSession.sql(insertSQL) ;

    }
}
