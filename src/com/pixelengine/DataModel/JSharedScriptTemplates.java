package com.pixelengine.DataModel;

//Task17, Task16, Task15 共用的脚本模版
//2022-7-27 created



public class JSharedScriptTemplates {

    // {{{name}}}
    public static String scriptTemplate_name = "function main(){"
            +"let nearestdt=pe.NearestDatetimeBefore('{{{name}}}',pe.extraData.datetime) ;"
            +"if(typeof nearestdt==='undefined'){pe.log('[ERROR]该日期没有数据[/ERROR]'); return null;}"
            +"if( nearestdt.dt0<=pe.extraData.datetime && pe.extraData.datetime<nearestdt.dt1){pe.log('[INFO]' + nearestdt.display + '[/INFO]');}"
            +"else {pe.log('[WARN]最近一期' + nearestdt.display + '[/WARN]');} "
            +"var ds=pe.Dataset('{{{name}}}', nearestdt.dt );"
            +"return ds; } " ;


    // {{{name}}} {{{roiid}}} {{{nodata}}}
    public static String scriptTemplate_name_roiid_nodata = "function main(){"
            +"let nearestdt=pe.NearestDatetimeBefore('{{{name}}}',pe.extraData.datetime) ;"
            +"if(typeof nearestdt==='undefined'){pe.log('[ERROR]该日期没有数据[/ERROR]'); return null;}"
            +"if( nearestdt.dt0<=pe.extraData.datetime && pe.extraData.datetime<nearestdt.dt1){pe.log('[INFO]' + nearestdt.display + '[/INFO]');}"
            +"else {pe.log('[WARN]最近一期' + nearestdt.display + '[/WARN]');} "
            +"var ds=pe.Dataset('{{{name}}}', nearestdt.dt );"
            +"return ds.clip2('{{{roiid}}}',{{{nodata}}}); } " ;
}
