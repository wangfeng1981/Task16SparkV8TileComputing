package com.pixelengine.DataModel;
//2022-01-01
public class JExportParams {
    public int inpid ;//mysql pid
    public long dt ;//hcol
    public String htable ;
    public String hfami ;
    public int hpid ;
    public int hpidblen ;
    public int yxblen ;
    public double left,right,top,bottom ;
    public int level ;
    public int filldata ;
    public String outfilename;//输出绝对路径 /var/www/html/pe/export/2021/20210421/export-uid-tid.tif
    public String outfilenamedb;//输出相对路，用于数据库保存 /export/2021/20210421/export-uid-tid.tif
    public String zookeeper ;
    public int datatype ;
}
