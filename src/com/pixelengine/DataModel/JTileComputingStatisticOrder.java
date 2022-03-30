package com.pixelengine.DataModel;
// 离线计算，区域统计订单
//2022-3-27 created

public class JTileComputingStatisticOrder {
    public String dsname;//dsname不为空时，代码自动生产脚本内容，同时jsfile，sdui不生效
    public String jsfile ;//脚本绝对路径，与dsname互斥，优先看dsname，如果dsname等于空字符串才看jsfile
    public String roi ;//必填，不能为空，或者是roi2的id（user:1,sys:2），或者是tlv文件绝对路径
    public Double filldata ;
    public Double validMinInc ;
    public Double validMaxInc ;
    public Long dt ;
    public String sdui ;
}
