package com.pixelengine.DataModel;
/// 对应 tbofftask 中的一条记录
/// 2022-4-7 created


import java.sql.Date;
import java.sql.Time;

public class JOfftask {

    public int ofid ;
    public int mode ;//1-stat 1-skserial 2-lsserial 4-tc2hb 5-export
    public int uid ;
    public String orderfile ;//relative path under pedir
    public String resultfile ;//relative path under pedir
    public String ctime ;//create time
    public String utime ;//update time
    public int status ;//0-not started , 1-running , 2-succ , 3-failed
    public String tag ;//user tag
    public String msg ;//not using
}
