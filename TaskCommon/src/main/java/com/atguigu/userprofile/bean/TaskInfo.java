package com.atguigu.userprofile.bean;

import lombok.Data;

import java.util.Date;

@Data
public class TaskInfo {

    Long id;

    String taskName;

    String taskStatus;

    String taskComment;

    String taskType;

    String execType;

    String mainClass;

    Long fileId;

    String taskArgs;

    String taskSql;

    Long taskExecLevel;

    Date createTime;
}
