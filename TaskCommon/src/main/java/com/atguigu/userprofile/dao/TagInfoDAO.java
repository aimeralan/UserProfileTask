package com.atguigu.userprofile.dao;

import com.atguigu.userprofile.bean.TagInfo;
import com.atguigu.userprofile.util.MySQLUtil;

import java.util.List;

public class TagInfoDAO {

    public static TagInfo getTagInfoByTaskId(String taskId) {
        String sql = "select * from tag_info ti where ti.tag_task_id='" + taskId + "'";
        TagInfo tagInfo = MySQLUtil.queryOne(sql, TagInfo.class, true);
        return tagInfo;
    }


    //得到启用状态的标签list
    public static List<TagInfo> getTagInfoListWithOn() {
        String sql = "  select ti.* from tag_info ti join task_info tk on ti.tag_task_id = tk.id"
                + " where tk.task_status='1'";

        List<TagInfo> tagInfoList = MySQLUtil.queryList(sql, TagInfo.class, true);

        return tagInfoList;

    }
}
