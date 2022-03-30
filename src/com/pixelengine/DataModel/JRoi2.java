package com.pixelengine.DataModel;
//2022-01-01
//2022-3-22

import java.util.Date;

/// second version ROI
/// support both system roi and user roi
public class JRoi2 {
    public static class Roi2ID{
        public String prefix;
        public int    id ;
    }

    public int rid = 0 ;//both
    public String name ;//both
    public String geojson ;//both

    public int rcid = 0 ;//sys
    public String name2;//sys
    public String shp;//user
    public int uid;//user
    public Date ctime;//user

    //2022-3-22
    public static Roi2ID parseRoi2ID(String roi2id){
        String[] strarr = roi2id.split(":") ;
        if( strarr.length == 2 ){
            Roi2ID roi2 = new Roi2ID() ;
            roi2.prefix = strarr[0] ;
            roi2.id = Integer.valueOf(strarr[1]) ;
            return roi2 ;
        }else{
            return null ;
        }
    }

}
