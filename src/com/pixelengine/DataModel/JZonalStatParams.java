package com.pixelengine.DataModel;
//2022-01-01

import com.google.gson.Gson;

public class JZonalStatParams {
    public String rtype;
    public Long rid ;
    public int pid,bandindex ;
    public double vmin,vmax;//统计区间
    public Long fromdt ;//datetime
    public Long todt ;
    //extra params by produt info
    public String regfile ;
    public int zlevel ;
    public String hTableName;
    public Integer hPid ;
    public int bsqIndex ;
    public int dataType ;
    public String hFamily ;
    public int hpidblen ;//hbase pid byte length
    public int yxblen ; //hbase tile yx byte length
    public double bandValidMin ;
    public double bandValidMax ;
    public double bandNodata ;
    public String method; //min,max,ave
    public String offsetdt ;

    public String outfilename,outfilenamedb ;//new

    public String toJson() {
        Gson gson = new Gson();
        String jsonstr = gson.toJson(this);
        return jsonstr ;
    }
    public static JZonalStatParams fromJson(String jsonstr){
        Gson gson = new Gson();
        JZonalStatParams res = gson.fromJson(jsonstr,JZonalStatParams.class) ;
        return res ;
    }
}
