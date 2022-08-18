package com.atguigu.userprofile.bean;

import lombok.Data;
import java.util.Date;

@Data
public class TagInfo {

    Long id;

    String tagCode;

    String tagName;

    Long tagLevel;

    Long parentTagId;

    String tagType;

    String tagValueType;

    Long tagTaskId;

    String tagComment;

    Date createTime;

    Date updateTime;

}
