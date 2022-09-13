//com_pixelengine_HBasePeHelperCppConnector
package com.pixelengine;
////////////////////////////////////////////////////////
//
//
//这个接口是java主动调用C++ so的
//update 2022-2-13 1020
//update 2022-3-22 2155
//update 2022-3-23 1127 增加hsegtlv 计算经纬度范围的方法 UtilsComputeHsegTlvExtent
//update 2022-3-24 0352 增加tlv直接裁剪功能，不依赖v8和js
//update 2022-3-27 1029 增加tlv对瓦片计算统计值功能，不依赖v8和js
//update 2022-4-4 这个类名称已经写在C++程序里了，所以这里这个参数没啥用 com/pixelengine/HBasePixelEngineHelper
//update 2022-4-17
//update 2022-7-17
//2022-9-9
/////////////////////////////////////////////////////////

//Java离线计算与C++接口。
//// -Djava.library.path="/home/hadoop/IdeaProjects/PixelEngine_TileCompute/pe_tilecompute/HBasePeHelperCppConnector"

public class HBasePeHelperCppConnector {
    static {
        System.loadLibrary("HBasePeHelperCppConnector") ;
    }

    public native String ParseScriptForDsDt(String javaHelperClassName, String script ) ;

    //2022-2-12 Java_com_pixelengine_HBasePeHelperCppConnector_GetDatasetNameArray
    //2022-4-17 output is JDsNameArrayResult json {"state":0,"error":"...","data":[...]}
    public native String GetDatasetNameArray(String javaHelperClassName, String script ) ;

    /// javaHelperClassName='com/pixelengine/HBasePixelEngineHelper' 下面一样 2022-4-4
    public native TileComputeResult RunScriptForTileWithoutRender(String javaHelperClassName, String script, long datetime, int z , int y , int x) ;
    public native TileComputeResult RunScriptForTileWithRender(String javaHelperClassName,String script,String styleJson,long datetime,int z,int y,int x) ;

    public native TileComputeResult RunScriptForTileWithoutRenderWithExtra(String javaHelperClassName, String script, String extraJsonText , int z , int y , int x) ;
    public native TileComputeResult RunScriptForTileWithRenderWithExtra(String javaHelperClassName,String script,String styleJson,String extraJsonText ,int z,int y,int x) ;
    //2022-9-8
    public native TileComputeResult RunScriptFunctionForTileResult(
            String javaHelperClassName,String script,String caller,int z,int y,int x) ;
    //2022-9-9
    public native String RunScriptFunctionForTextResultOrNothing(
            String javaHelperClassName,String script,String caller,int z,int y,int x) ;

    //2022-3-22
    public native TileComputeResultWithRunAfterInfo RunScriptForTileWithoutRenderWithExtraWithRunAfterInfo(
            String javaHelperClassName, String script, String extraJsonText , int z , int y , int x) ;

    //2022-7-17
    public native String RunScriptForTextResultWithExtra(String javaHelperClassName, String script,String extraJsonText ) ;


    /// 通过检查返回空字符串 “”
    /// if not pass check , return the error message string
    public native String CheckScriptOk( String javaHelperClassName, String script ) ;

    public native String GetVersion() ;

    /// if get style , return the style json string
    /// else return the "" empty string
    public native String RunToGetStyleFromScript(String javaHelperClassName,String script) ;

    //2022-3-23 计算hsegtlv第0级的四角经纬度范围，注意该范围可能比实际范围略小，可以向四个方向增加一个像素距离即可解决
    //Java_com_pixelengine_HBasePeHelperCppConnector_UtilsComputeHsegTlvExtent
    public native String UtilsComputeHsegTlvExtent(byte[] hsegtlvdata) ;

    //2022-3-24 使用tlv直接裁剪dataset功能，不依赖v8和js代码
    public native TileComputeResult ClipTileComputeResultByHsegTlv(String javaHelperClassName,TileComputeResult srcTCR,byte[] tlvData,double fillData);

    //2022-3-27 use tlv to compute statistic data without using v8 or js
    //returned values are bands.
    public native JStatisticData[] ComputeStatisticTileComputeResultByHsegTlv(String javaHelperClassName,
                                                                              TileComputeResult tcr,
                                                                              byte[] tlvdata,
                                                                              double filldata,
                                                                              double validminInc,
                                                                              double validmaxInc) ;
}



