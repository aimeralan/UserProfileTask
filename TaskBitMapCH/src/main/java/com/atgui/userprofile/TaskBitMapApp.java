package com.atgui.userprofile;

import com.atguigu.userprofile.constant.ConstCodes;
import com.atguigu.userprofile.dao.TagInfoDAO;
import com.atguigu.userprofile.util.MyClickHouseUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import com.atguigu.userprofile.bean.*;

import java.util.ArrayList;

import java.util.List;
import java.util.stream.Collectors;

public class TaskBitMapApp {
    public static void main(String[] args) {

        //获得业务参数
        String taskId = args[0];
        String busiDate = args[1];

        //spark环境
//        SparkConf conf = new SparkConf().setAppName("task_bitmap").setMaster("local[*]");
        SparkConf conf = new SparkConf().setAppName("task_bitmap");
        SparkContext sparkContext = new SparkContext(conf);

        //1 查询已启用的标签列表
        List<TagInfo> tagInfoList = TagInfoDAO.getTagInfoListWithOn();


        //
        //2 因为要按标签值的类型不同，分成四份，分别插入4种bitmap表中
        List<TagInfo> tagInfoStringList = new ArrayList();
        List<TagInfo> tagInfoLongList = new ArrayList();
        List<TagInfo> tagInfoDecimalList = new ArrayList();
        List<TagInfo> tagInfoDateList = new ArrayList();

        //2.1分成四份
        for (TagInfo tagInfo : tagInfoList) {
            if (ConstCodes.TAG_VALUE_TYPE_STRING.equals(tagInfo.getTagValueType())) {
                tagInfoStringList.add(tagInfo);
            } else if (ConstCodes.TAG_VALUE_TYPE_LONG.equals(tagInfo.getTagValueType())) {
                tagInfoLongList.add(tagInfo);
            } else if (ConstCodes.TAG_VALUE_TYPE_DECIMAL.equals(tagInfo.getTagValueType())) {
                tagInfoDecimalList.add(tagInfo);
            } else if (ConstCodes.TAG_VALUE_TYPE_DATE.equals(tagInfo.getTagValueType())) {
                tagInfoDateList.add(tagInfo);
            }
        }

        //2.2 把4个list 转换为四条sql 分别执行
        insertBitmap(tagInfoStringList, "user_tag_value_string", busiDate);
        insertBitmap(tagInfoLongList, "user_tag_value_long", busiDate);
        insertBitmap(tagInfoDecimalList, "user_tag_value_decimal", busiDate);
        insertBitmap(tagInfoDateList, "user_tag_value_date", busiDate);


    }


    //跟list插入到目标bitmap表
    private static void insertBitmap(List<TagInfo> tagInfoList, String targetTableName, String busiDate) {

        if (tagInfoList.size() > 0) {
            List<String> tagCodeList = tagInfoList.stream()
                    .map(tagInfo ->
                            "('" + tagInfo.getTagCode().toLowerCase() + "'," + tagInfo.getTagCode().toLowerCase() + ")")
                    .collect(Collectors.toList());
            String tagCodeSQL = StringUtils.join(tagCodeList, ",");

            //幂等性处理
            String deleteSQL = " alter table " + targetTableName + " delete where dt='" + busiDate + "'";

            MyClickHouseUtil.executeSql(deleteSQL);

            String sourceTableName = "up_tag_merge_" + busiDate.replace("-", "");

            String sql =
                    " insert into  " + targetTableName +
                            "   select  tt.tg.1 as tag_code,if(tt.tg.2='','0',tt.tg.2) as tag_value , " +
                            " groupBitmapState(cast (uid as UInt64)  ) ,'" + busiDate + "'" +
                            " from ( " +
                            " select uid ,arrayJoin([" + tagCodeSQL + "]) tg " +
                            " from " + sourceTableName +
                            " ) tt" +
                            "  group by  tt.tg.1 as tag_code,tt.tg.2 ";


            System.out.println(sql);

            MyClickHouseUtil.executeSql(sql);
        }

    }


}
