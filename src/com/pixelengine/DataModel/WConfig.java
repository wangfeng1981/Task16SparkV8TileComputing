package com.pixelengine.DataModel;
//update 2022-2-13 1118
//update 2022-2-13 1854
//update 2022-2-13 2116
//update 2022-3-5 2121
//update 2022-3-18 0930 add initWithInStream method
//update 2022-3-23 1647 add initWithString method
//update 2022-4-5 offtask_producer_for_cppspark
//update 2022-4-5 1618 offtask_cppspark_order_recv_socket
//2022-9-9

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


    public String binaryfile2hbase;
    public String connstr;
    public String gdaltranslate;
    public String gdalwarp ;
    public String hbaseuserfiletable;
    public String host ;

    public String offtask_cppspark_0mq_service;
    public String offtask_cppspark_order_recv_socket ;//task17 离线spark瓦片计算命令外发 0mq-socket "tcp://127.0.0.1:5520"
    public String offtask_export_0mq_service ;
    public String offtask_export_producer ;//task17 数据导出命令外发 0mq-socket "tcp://127.0.0.1:5510" ,
    public String offtask_pe_gots_worker ;//thsi program will be called by
    public String offtask_result_collector ;//全部离线任务结果收集器 0mq-socket "tcp://127.0.0.1:5500" ,
    public String offtask_sparkv8tilecompute_jar;

    public String omc_localhost_api  ;//http://localhost:15911/
    public String omc_port; // "15911",
    public String omc_resdir ;// "/var/www/html/pe/omc_res/" ,
    public String omc_service ;// pe_mapcomposer_service ,
    public String omc_zmqport ;// "15922" ,

    public String pedir ;//nginx-pedir /var/www/html/pe/
    public String http_pedir;//http://192.168.56.103:15980/
    public String port ;//task17 "15900"
    public String productwmts ; //productwmts-template.xml
    public String pwd;//mysql pwd
    public String scriptwmts ; //scriptwmts-template.xml
    public String shpgeojson2hsegtlv ;
    public String sparkmaster;     //spark://localhost:7077
    public String sparksubmit;     // /usr/local/spark/bin/spark-submit
    public String task17_api_root ;//"http://192.168.56.103:15900/pe/",
    public String tilelocalrootdir ;//本地瓦片数据根目录 will deprecated

    public String user;// mysql username
    public String userhfami ; //tiles fixed
    public String userhtable; //sparkv8out
    public int userhpidblen ; //4
    public int useryxblen ;   //4

    public String zookeeper;

}
