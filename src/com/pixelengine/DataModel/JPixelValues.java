package com.pixelengine.DataModel;
//2022-01-01
public class JPixelValues {
    public double longitude,latitude;
    public int tilez,tiley,tilex,col,row;
    public double[] values ;
    public static JPixelValues CreateByLongLat(double lon1,double lat1,int z,int tilewid,int tilehei)
    {
        JPixelValues pv = new JPixelValues();
        pv.longitude = lon1;
        pv.latitude = lat1;
        pv.tilez = z;
        int tilexnum = (int) Math.pow(2.0,(double)z) ;
        double resox = 360.0 / tilexnum / tilewid;
        double resoy = 180.0 / (tilexnum/2.0) / tilehei ;
        int fully = (int)((90.0-pv.latitude)/resoy+0.5) ;
        int fullx = (int)((pv.longitude+180.0)/resox+0.5) ;
        pv.tiley = (int)(fully/tilehei) ;
        pv.tilex = (int)(fullx/tilewid) ;
        pv.col = fullx%tilewid;
        pv.row = fully%tilehei;
        return pv;
    }
}
