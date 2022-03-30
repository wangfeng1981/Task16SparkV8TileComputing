package com.pixelengine;


import java.awt.*;

public class JLongLat2TileXY {

    public static int[] computeTileXYByLonglat(double lon,double lat,int zlevel, int tilesize)
    {
        int[] res = new int[2] ;
        int numxtiles = (int)Math.pow(2 , zlevel) ;
        int numytiles = Math.max(1, numxtiles/2) ;
        double resolution = 360.0 / numxtiles / tilesize ;
        int ixtile = (int) ((lon + 180) / resolution / tilesize) ;
        int iytile = (int) ( (90-lat) / resolution / tilesize ) ;
        if( ixtile < 0 ) ixtile = 0 ;
        else if( ixtile >= numxtiles ) ixtile = numxtiles-1 ;
        if( iytile < 0 ) iytile = 0 ;
        else if( iytile >= numytiles ) iytile = numytiles -1 ;
        res[0] = ixtile ;
        res[1] = iytile ;
        return res ;
    }
}
