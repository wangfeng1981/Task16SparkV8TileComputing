package com.pixelengine.DataModel;
//2022-9-4

public class JTileComputingScriptSerialOrder {
    public String baseddsname="";//用于生成 dtcollection 和 maxZoom
    public String jsfile ="";//脚本绝对路径，Map函数会执行这个文件中的特定函数，这个特定函数名在caller中定义
    public String caller ="";//针对每个dtcollection调用的函数，返回dataset，一个完整的jsfile样例如下：
    /*
    // jsfile for script product serial analyse.
    var sdui={ ... };
    const maskfunc = function(tiledata) {
        let newdata = new Int16Array(65536);
        for(let i = 0 ; i<65536;++i) {
            if(
        }
    }
    const serialcaller = function() {
        let maskds = pe.Dataset("etc/soyb", 20100101000000) ;

        let dscoll = pe.DatasetCollection('some/name', pe.extraData) ;
        dscoll.mask( maskTileData , filldata ) ;
        return dscoll.compose(2, 0 ,10000, -19999) ;
    }

    function main() {
        ... some main function codes....
    }
     */
    public Double filldata ;      //下面三个值需要与dscollection.compose(...)一至
    public Double validMinInc ;
    public Double validMaxInc ;
    
    public String roi ="";//必填，不能为空，或者是roi2的id（user:1,sys:2），或者是tlv文件绝对路径

    /// datetime collections params start 仅在 dsname 不为空的时候生效
    public Long whole_start,whole_stop,repeat_start,repeat_stop;
    public Integer whole_start_inc,whole_stop_inc,repeat_start_inc,repeat_stop_inc,repeat_stopnextyear;
    public String repeat_type="" ;// '' , 'm' , 'y'
}
