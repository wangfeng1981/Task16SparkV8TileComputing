package com.pixelengine.DataModel;
//日期时间几何构造类
//2022-3-28 0519
//2022-3-29

/// global 表示整体的时间范围
/// repeater 表示重复周期，
// 如果为空表示不重复，仅仅返回开始日期
// repeater=d，表示每天重复，比如startstop 为20010101和20010110时，会生产 20010101，20010102，。。。，20010110
// repeater=m，表示每月重复，比如startstop 为20010101和20020110时，会生成 20010101，20010201，20010301，。。。20020101
// repeater=y，表示每年重复，比如startstop 为20010101和20030110时，会生产20010101，20020101，20030101
// filters，表示进一步筛选，只有在repeater生效的时候才生效，filters会对repeater生产的数组进行二次筛选，
// 筛选后的数据才会作为最后返回的数组。




public class JDtCollectionBuilder {
    public JDtPair wholePeriod = new JDtPair();
    public String  repeatType ;// d for daily, m for monthly, y for yearly
    public JDtPair repeatPeriod = new JDtPair();

}
