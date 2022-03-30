package com.pixelengine.DataModel;
//2022-01-01

import java.util.Date;

public class ROI {
    //
    public String rtype = "roi";// roi / area
    public int children = 0;//是否有子节点，0-没有，1-有
    public String code = "0" ;//仅针对预定于区域
    public String parentCode="0" ;//仅针对预定于区域
    public int rid ; //主键
    public String name ;
    public String shp ;
    public String geojson ;
    public int uid ;
    public Date createtime ;
    public Date updatetime;


    public static ROI convertArea2ROI(Area tarea)
    {
        ROI roi = new ROI();
        roi.name = tarea.name ;
        roi.uid = 0;
        roi.shp = "" ;
        roi.geojson =  tarea.path;
        roi.rid = tarea.id ;
        roi.rtype = "area" ;
        roi.children = tarea.children ;
        roi.code = tarea.code ;
        roi.parentCode = tarea.parentCode ;
        return roi ;
    }


    public static ROI convertRegionDTO2ROI(JRegionDbObject region )
    {
        ROI roi = new ROI() ;
        roi.rid =   region.rid ;
        roi.shp = region.shp ;
        roi.uid = region.uid ;
        roi.children = 0 ;
        roi.code = "0" ;
        roi.parentCode = "0" ;
        roi.geojson = region.geojson  ;
        roi.name = region.name ;
        roi.rtype = "roi" ;
        roi.createtime = region.createtime ;
        roi.updatetime = region.updatetime ;
        return roi ;
    }
}