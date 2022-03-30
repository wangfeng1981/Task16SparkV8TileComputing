package com.pixelengine.DataModel;
//2022-3-22





//计算0，0，0号瓦片，得到dsname数组，时间hcol数组，输出数据类型，输出波段数量，log信息

import com.pixelengine.HBasePeHelperCppConnector;
import com.pixelengine.TileComputeResult;
import com.pixelengine.TileComputeResultWithRunAfterInfo;
import com.pixelengine.tools.JScriptTools;


import java.util.ArrayList;

public class WComputeZeroTile {
    //用于描述DatasetArray的时间集合
    public class DtCollection {
        public long startdt=0;
        public long stopdt =0;
        public Boolean startInclusive=true;
        public Boolean stopInclusive=true ;
        public int everyNYear = 0 ;
        public int everyNMonth = 0 ;
        public int everyNDay = 0 ;
        public int everyNHour = 0 ;
        public int everyNMinu = 0 ;
        public int everyNSeco = 0 ;
    }


    public TileComputeResultWithRunAfterInfo computeZeroTile(String scriptText, long dt, String sduiText)
    {
        try{
            HBasePeHelperCppConnector cc = new HBasePeHelperCppConnector();

            String extraText = "{\"datetime\":"+String.valueOf(dt)+"}" ;
            String scriptWithSDUI = JScriptTools.assembleScriptWithSDUI( scriptText , sduiText ) ;
            TileComputeResultWithRunAfterInfo tileResult=
                    cc.RunScriptForTileWithoutRenderWithExtraWithRunAfterInfo(
                            "com/pixelengine/HBasePixelEngineHelper",
                            scriptWithSDUI,
                            extraText,
                            0,0,0
                    ) ;
            if(tileResult==null ){
                throw new Exception("zeroTileResult is null") ;
            }
            if( tileResult.status!=0 ){
                throw new Exception("compute zeroTileResult failed") ;
            }
            System.out.println("zeroTileResult ok, dataType:"+tileResult.outType+
                    ", nband:"+tileResult.nbands);
            return tileResult ;
        }catch (Exception ex){
            System.out.println("WComputeZeroTile.computeZeroTile exception:"+ex.getMessage());
            return null ;
        }
    }
}
