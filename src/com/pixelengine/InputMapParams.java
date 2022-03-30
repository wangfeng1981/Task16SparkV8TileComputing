package com.pixelengine;

import java.io.Serializable;

public class InputMapParams implements Serializable{
    public int oftid ;//off task id
    public String scriptfilename ;//脚本路径
    public String outhtable;
    public String outhfami ;
    public int outhpid ;
    public int outhpidblen;
    public int outyxblen ;
    public long outhcol ;
    public int usebound ;//是否使用矩形区域限制 0-不使用   1-使用
    public double left,right,top,bottom ;//wgs84 long/lat
    public int zmin , zmax ;
}
