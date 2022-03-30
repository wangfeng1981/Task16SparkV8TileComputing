package com.pixelengine.tools;
//2022-3-22

public class JTileResolutionTool {
    public static double computeResolution(int level,int tileSize){
        int ntile = (int)Math.pow(2,level) ;
        int xpixelcount = ntile*tileSize ;
        return 360.0/xpixelcount ;
    }
}
