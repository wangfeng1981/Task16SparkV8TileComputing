package com.pixelengine.tools;
//2022-3-23 created
//2022-3-24 update use JRoi2Loader
//2022-3-27

import com.pixelengine.DataModel.JRoi2;
import com.pixelengine.DataModel.WConfig;
import com.pixelengine.HBasePeHelperCppConnector;
import com.pixelengine.HBasePixelEngineHelper;
import com.pixelengine.JRDBHelperForWebservice;
//import scala.reflect.io.File;

//传入 roi2id 或者 相对路径 计算经纬度四角范围
public class Roi2HsegTlv2LonLatExtent {
    public static class Extent {
        public double left,right,top,bottom ;
        public void print(){
            System.out.println(
                    String.valueOf(left)+","+
                            String.valueOf(right)+","+
                            String.valueOf(top)+","+
                            String.valueOf(bottom)
            );
        }
    }

    public static double[] parseLeftRightTopBottomString(String str){
        String[] strarr = str.split(",") ;
        if( strarr.length != 4 ){
            System.out.println("bad extent str:"+str);
            return null ;
        }else{
            double[] vals = new double[4] ;
            vals[0] = Double.valueOf(strarr[0]) ;
            vals[1] = Double.valueOf(strarr[1]) ;
            vals[2] = Double.valueOf(strarr[2]) ;
            vals[3] = Double.valueOf(strarr[3]) ;
            return vals ;
        }
    }


    public static Extent computeExtent( String[] roi2array ){
        Extent extent = new Extent() ;
        extent.left = -180.0;
        extent.right = 180.0 ;
        extent.top = 90.0;
        extent.bottom = -90.0 ;
        //JRDBHelperForWebservice rdb = new JRDBHelperForWebservice() ;
        HBasePixelEngineHelper hbaseHelper = new HBasePixelEngineHelper() ;
        HBasePeHelperCppConnector cc = new HBasePeHelperCppConnector() ;
        for(int i = 0 ; i<roi2array.length;++i ){
            if( roi2array[i].length()==0 ){
                continue; //empty string
            }else{
                byte[] roi2data = JRoi2Loader.loadData(roi2array[i]) ;//2022-3-27
                if( roi2data==null ){
                    System.out.println("bad roi2data for "+roi2array[i]);
                    return null ;
                }
                double[] lrtb = parseLeftRightTopBottomString(cc.UtilsComputeHsegTlvExtent(roi2data)) ;
                if( lrtb==null ){
                    System.out.println("bad parseLeftRightTopBottomString");
                    return null ;
                }else{
                    extent.left = Math.max(extent.left  , lrtb[0]) ;
                    extent.right = Math.min(extent.right , lrtb[1]) ;
                    extent.top = Math.min(extent.top   , lrtb[2]) ;
                    extent.bottom = Math.max(extent.bottom, lrtb[3]) ;
                    extent.print();
                }
            }
        }
        return extent ;
    }
    public static final double level0Reso = 1.406250000 ;// 360.0/256

    //上下左右各外扩半个level0像素
    public static Extent expandHalfPixel( Extent oldExtent){
        Extent newExtent = new Extent() ;
        newExtent.left = Math.max(-180.0 , oldExtent.left - level0Reso/2 ) ;
        newExtent.right = Math.min(180.0 , oldExtent.right + level0Reso/2 ) ;
        newExtent.top = Math.min(   90.0 , oldExtent.top + level0Reso/2 ) ;
        newExtent.bottom = Math.max( -90.0 , oldExtent.bottom - level0Reso/2 ) ;
        return newExtent ;
    }
}
