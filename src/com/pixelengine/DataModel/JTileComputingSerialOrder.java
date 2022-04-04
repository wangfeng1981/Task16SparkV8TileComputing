package com.pixelengine.DataModel;
//serial analyse order
//2022-3-27 created
//2022-4-4


//记得把这个说明添加到设计文档中
//
public class JTileComputingSerialOrder {
    public String dsname;//dsname不为空时，代码自动生产脚本内容，同时jsfile，sdui不生效; datetime collections相关参数生效；
    public String jsfile ;//脚本绝对路径，与dsname互斥，优先看dsname，如果dsname等于空字符串才看jsfile
    public String roi ;//必填，不能为空，或者是roi2的id（user:1,sys:2），或者是tlv文件绝对路径
    public Double filldata ;
    public Double validMinInc ;
    public Double validMaxInc ;
    public Long dt ; //用于生成 pe.extraData={"datetime":{dt}}
    public String sdui ;
    public String method ;//min,max,ave,sum

    /// datetime collections params start 仅在 dsname 不为空的时候生效
    public Long whole_start,whole_stop,repeat_start,repeat_stop;
    public Integer whole_start_inc,whole_stop_inc,repeat_start_inc,repeat_stop_inc,repeat_stopnextyear;
    public String repeat_type="" ;// '' , 'm' , 'y'
    /// datetime collections params end.
}
