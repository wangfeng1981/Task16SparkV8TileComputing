package com.pixelengine.tools;
//2022-3-24 created
//2022-3-27 roi2 use absfilepath

import com.pixelengine.DataModel.JRoi2;
import com.pixelengine.DataModel.WConfig;
import com.pixelengine.HBasePixelEngineHelper;

//读取二进制roi2数据
public class JRoi2Loader {
    public static byte[] loadData( String roi2IdOrAbsFilepath  ){
        if( roi2IdOrAbsFilepath.length()==0 ){
            System.out.println("JRoi2Loader.loadData error: null roi2IdOrRelFilepath");
            return null ;
        }else{
            byte[] roi2data = null ;
            if( roi2IdOrAbsFilepath.contains("user:") ){
                JRoi2.Roi2ID roi2id= JRoi2.parseRoi2ID(roi2IdOrAbsFilepath) ;
                if( roi2id==null ){
                    System.out.println("JRoi2Loader.loadData error:bad roi2id "+ roi2IdOrAbsFilepath);
                    return null ;
                }
                HBasePixelEngineHelper hbaseHelper = new HBasePixelEngineHelper() ;
                roi2data = hbaseHelper.getRoiHsegTlv(1 , roi2id.id) ;
            }else if( roi2IdOrAbsFilepath.contains("sys:") ){
                JRoi2.Roi2ID roi2id= JRoi2.parseRoi2ID(roi2IdOrAbsFilepath) ;
                if( roi2id==null ){
                    System.out.println("JRoi2Loader.loadData error:bad roi2id "+ roi2IdOrAbsFilepath);
                    return null ;
                }
                HBasePixelEngineHelper hbaseHelper = new HBasePixelEngineHelper() ;
                roi2data = hbaseHelper.getRoiHsegTlv(0 , roi2id.id) ;
            }else {
                //relative filepath of hseg.tlv file
                String absfilepath =  roi2IdOrAbsFilepath;
                roi2data = FileDirTool.readFileAsBytes(absfilepath) ;
            }
            if( roi2data==null ){
                System.out.println("JRoi2Loader.loadData error:null roi2data for "+roi2IdOrAbsFilepath);
                return null ;
            }
            return roi2data ;
        }
    }
}
