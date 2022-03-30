package com.pixelengine.tools;
//2022-2-13
//2022-3-23 1716 bugfixed for compute ytilenum


//maxZoom constrained into 21
//use long lat to compute tile range
public class JTileRangeTool {
    public static class TileXY {
        public int x=0;
        public int y=0;
        public double left,right,top,bottom ;
    }

    public static class TileXYRange {
        public int xmin = 0 ;//include
        public int xmax = 0 ;//include
        public int ymin = 0 ;//include
        public int ymax = 0 ;//include
        public double left,right,top,bottom ;
    }

    public static TileXY computeTileXYByLonglat(double lon,double lat,int zlevel,int tileSize)
    {
        //long -180 ~ +180
        //lat -90 ~ +90
        //zlevel 0-...
        //tileSize should be 256 or 512
        if( lon>=-180 && lon<=180 && lat>=-90 && lat <= 90 && zlevel >=0 && zlevel < 22 && tileSize>0 )
        {
            TileXY tilexy=new TileXY() ;
            double theReso = 360.0/tileSize ;
            for(int iz = 1 ; iz<=zlevel;++iz ){
                theReso = theReso/2 ;
            }
            int xtilenum = (int)Math.pow(2,zlevel) ;
            int ytilenum = Math.max(1, xtilenum/2) ;//bugfixed 2022-3-23

            int pixelx = (int)((lon + 180.0) / theReso) ;
            int pixely = (int)((90.0-lat)/theReso) ;

            int xtile = pixelx / tileSize ;
            int ytile = pixely / tileSize ;

            if( xtile > xtilenum ){
                System.out.println("xtile exceed xtilenum for zlevel:"+xtile+","+xtilenum+","+zlevel) ;
                return null ;
            }else if( xtile==xtilenum ){
                xtile = xtilenum-1 ;
            }

            if( ytile > ytilenum ){
                System.out.println("ytile exceed ytilenum for zlevel:"+ytile+","+ytilenum+","+zlevel) ;
                return null ;
            }else if( ytile==ytilenum ){
                ytile = ytilenum-1 ;
            }

            tilexy.x = xtile ;
            tilexy.y = ytile ;
            tilexy.left = xtile * tileSize * theReso - 180.0 ;
            tilexy.right = tilexy.left + tileSize * theReso ;
            tilexy.top = 90.0 - ytile * tileSize * theReso ;
            tilexy.bottom = tilexy.top - tileSize * theReso ;

            return tilexy ;
        }else{
            return null ;
        }
    }

    public static TileXYRange computeTileRangeByLonglatExtent(
            double left,double right,double top,double bottom , int zlevel ,int tileSize )
    {
        TileXY tileTopLeft = computeTileXYByLonglat(left , top , zlevel ,tileSize) ;
        TileXY tileBottomRight = computeTileXYByLonglat(right , bottom , zlevel ,tileSize) ;
        if( tileTopLeft==null ){
            System.out.println("bad tileTopLeft");
            return null ;
        }
        if( tileBottomRight==null ){
            System.out.println("bad tileBottomRight");
            return null ;
        }
        TileXYRange tileRange = new TileXYRange() ;
        tileRange.xmax = tileBottomRight.x ;
        tileRange.xmin = tileTopLeft.x ;
        tileRange.ymin = tileTopLeft.y ;
        tileRange.ymax = tileBottomRight.y ;
        tileRange.left =tileTopLeft.left ;
        tileRange.right = tileBottomRight.right ;
        tileRange.top = tileTopLeft.top ;
        tileRange.bottom = tileBottomRight.bottom ;
        return tileRange ;
    }

}
