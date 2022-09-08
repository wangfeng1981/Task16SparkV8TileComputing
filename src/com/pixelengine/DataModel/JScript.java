package com.pixelengine.DataModel;
//2022-01-01
//2022-5-11
import java.sql.Date;

public class JScript {
    public int sid;
    public String title;
    public String jsfile ;//new
    public String scriptContent;//old
    public String utime;//更新时间
    public int uid;
    public int state;//new

    public JProduct convert2UsJProductWithDisplay() {
        JProduct pdt = new JProduct() ;
        pdt.displayid = "us"+String.valueOf(sid) ;
        pdt.maxZoom=12;
        pdt.proj = "" ;
        pdt.name = "" ;
        pdt.source = "";
        pdt.styleid = 0 ;
        pdt.pid = sid ;
        pdt.hTableName = "" ;
        pdt.userid = uid ;
        pdt.timeType = 0 ;
        pdt.dataType=0;
        pdt.tileHei=256;
        pdt.tileWid = 256 ;
        pdt.minZoom = 0 ;
        pdt.compress="" ;
        pdt.productDisplay.productname = title ;
        pdt.productDisplay.subtitle = utime ;
        pdt.productDisplay.params = "{\"sid\":"+String.valueOf(sid)+"}" ;
        pdt.productDisplay.type = "us" ;
        pdt.productDisplay.iorder = 0 ;
        pdt.productDisplay.cat = 0 ;
        pdt.productDisplay.dpid = sid ;
        pdt.productDisplay.pid = sid ;
        return pdt ;
    }
}
