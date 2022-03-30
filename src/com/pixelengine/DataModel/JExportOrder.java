package com.pixelengine.DataModel;

//新街口 2022-2-8
public class JExportOrder {
    public String mode ;//pe or script
    public Long datetime ; //yyyyMMddHHmmss
    public String geojsonRelFilepath ; //relative geojson file path of nginx-pedir
    public Integer pid ;//for pe
    public String scriptRelFilepath ;//relative script file path of nginx-pedir, for script
    public String sdui ;//json string or 'null', for script
    public Double fillvalue ;
    public String resultRelFilepath;//result relative path

}
