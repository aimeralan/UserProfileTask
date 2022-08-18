package com.atguigu.userprofile.dao;

import com.atguigu.userprofile.bean.TaskInfo;
import com.atguigu.userprofile.util.MySQLUtil;

public class TaskInfoDAO {

    public static TaskInfo getTaskInfo(String taskId ) {
        String sql= "select * from task_info ti where ti.id='"+taskId+"'";
        TaskInfo taskInfo  = MySQLUtil.queryOne(sql,TaskInfo.class, true);
        return taskInfo;

    }
}
