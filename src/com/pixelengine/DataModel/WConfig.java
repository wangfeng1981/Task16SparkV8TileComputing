package com.pixelengine.DataModel;
//update 2022-2-13 1118
//update 2022-2-13 1854
//update 2022-2-13 2116
//update 2022-3-5 2121
//update 2022-3-18 0930 add initWithInStream method
//update 2022-3-23 1647 add initWithString method


import com.google.gson.Gson;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;


public class WConfig {
    public static WConfig getSharedInstance() {
        if( sharedInstance==null ){
            System.out.println("WConfig.sharedInstance is null. WConfig.init must be called before use WConfig.");
            System.exit(11);
        }
        return sharedInstance ;
    }
    private static WConfig sharedInstance = null ;
    public static void init(String jsonfile)
    {
        try
        {
            FileInputStream ins = new FileInputStream(jsonfile) ;
            Reader reader0 = new InputStreamReader(ins, "UTF-8");
            Gson gson0 = new Gson() ;
            sharedInstance = new WConfig() ;
            sharedInstance = gson0.fromJson(reader0 , WConfig.class) ;
        }catch(Exception ex )
        {
            System.out.println("WConfig.init Error : failed to init wconfig:"+ex.getMessage() );
            System.exit(11);
        }
    }

    public static void initWithString(String jsontext)
    {
        try
        {
            Gson gson0 = new Gson() ;
            sharedInstance = new WConfig() ;
            sharedInstance = gson0.fromJson(jsontext , WConfig.class) ;
        }catch(Exception ex )
        {
            System.out.println("WConfig.init Error : failed to initWithString wconfig:"+ex.getMessage() );
            System.exit(11);
        }
    }

    //2022-3-18
    public static void initWithInStream(InputStream ins){
        try
        {
            Reader reader0 = new InputStreamReader(ins, "UTF-8");
            Gson gson0 = new Gson() ;
            sharedInstance = new WConfig() ;
            sharedInstance = gson0.fromJson(reader0 , WConfig.class) ;
        }catch(Exception ex )
        {
            System.out.println("WConfig.init Error : failed to init wconfig:"+ex.getMessage() );
            System.exit(11);
        }
    }


    public String zookeeper,sparkmaster;
    public String connstr,user,pwd;//mysql
    public String hbaseuserfiletable;
    public String productwmts ;
    public String host , port ;//task17
    public String pedir ;//nginx-pedir
    public String userhtable, userhfami ;
    public int    userhpidblen , useryxblen ;
    public String tilelocalrootdir ;//本地瓦片数据根目录

    //2022-2-6
    public String scriptwmts ;

    //2022-2-13 1854
    public String gdaltranslate;
    public String gdalwarp ;

    //2022-2-13 2116
    public String offtask_result_collector ;
    public String offtask_export_producer ;

    //2022-3-5 2121 ROI geojson和shp文件转换成hseg.tlv格式数据，然后写入HBase
    public String shpgeojson2hsegtlv ;

}
