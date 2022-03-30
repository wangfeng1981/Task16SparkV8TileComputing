package com.pixelengine;
// 区域统计
// 2022-3-27 created

import com.google.gson.Gson;
import com.pixelengine.DataModel.*;
import com.pixelengine.tools.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;
import scala.Serializable;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;

public class WTileComputingStatisticProcessor implements Serializable {
    public String task17configFile ;
    public String orderJsonFile ;
    public String resultJsonFile ;


    protected boolean writeResultJson(int state, JStatisticData[] statdatas, String msg)
    {
        if (resultJsonFile.isEmpty()) {
            System.out.println("resultJsonFile is empty.");
            return false ;
        } else {
            try {
                Gson gson = new Gson() ;
                String datastr = gson.toJson(statdatas , JStatisticData[].class) ;

                OutputStream outputStream = new FileOutputStream(resultJsonFile);
                String content = "{\"state\":" + String.valueOf(state)
                        + ",\"data\":" + datastr
                        + ",\"message\":\"" + msg + "\"}";
                outputStream.write(content.getBytes());
                outputStream.close();
                return true ;
            } catch (Exception ex) {
                System.out.println("WTileComputingStatisticProcessor.writeResultJson exception:" + ex.getMessage());
                return false ;
            }
        }
    }

    public int runit()
    {
        //load resource from config.json in jar
        System.out.println("WTileComputingStatisticProcessor running. parsing task17config.json");
        String task17configJsonText = WTextFile.readFileAsString(task17configFile) ;
        WConfig wconfig = null ;
        {
            WConfig.initWithString(task17configJsonText);
            wconfig = WConfig.getSharedInstance() ;
        }
        System.out.println("**************** Check zookeeper, spark, mysql info**************");
        System.out.println("zookeeper:" + wconfig.zookeeper);
        System.out.println("sparkmaster:"+wconfig.sparkmaster);
        System.out.println("connstr:" + wconfig.connstr);
        System.out.println("user:" + wconfig.user);
        System.out.println("pwd:" + wconfig.pwd);
        System.out.println("**************** *************************** *******************");
        String sparkmaster = wconfig.sparkmaster ;
        String zookeeper = wconfig.zookeeper ;

        //init mysql
        JRDBHelperForWebservice.init(wconfig.connstr,wconfig.user,wconfig.pwd);
        JRDBHelperForWebservice rdb = new JRDBHelperForWebservice() ;

        //init spark+hbase
        String appname = new File(orderJsonFile).getName() ;
        System.out.println("use filename as appname:" + appname);
        SparkConf sparkconf = new SparkConf().
                setAppName(appname).setMaster(sparkmaster);
        JavaSparkContext jsc = new JavaSparkContext(sparkconf);

        //show pixelengine core version
        HBasePeHelperCppConnector cc = new HBasePeHelperCppConnector();
        System.out.println("pe core version: " + cc.GetVersion() );

        //load input order.json
        System.out.println("load task order jsonfile "+orderJsonFile);
        Gson gson = new Gson() ;
        JTileComputingStatisticOrder order = gson.fromJson(
                WTextFile.readFileAsString(orderJsonFile),JTileComputingStatisticOrder.class) ;

        String scriptText = "" ;
        int targetLevel = 0 ;
        if( order.dsname.compareTo("") != 0 ){
            scriptText = "function main(){return pe.Dataset('"
                    + order.dsname
                    +"',"
                    + order.dt +");}" ;
            JProduct productInfo = rdb.rdbGetProductInfoByName(order.dsname) ;
            if( productInfo==null ){
                writeResultJson(19,null,"not find product of "+ order.dsname);
                return 19 ;
            }
            targetLevel = productInfo.maxZoom ;
        }else if( order.jsfile.compareTo("")!=0 ) {
            scriptText = WTextFile.readFileAsString(order.jsfile) ;
            //计算0，0，0号瓦片，获取 dsnameArr 列表，与时间列表 dtArr ，输出类型 outdatatype
            WComputeZeroTile computerForZeroTile = new WComputeZeroTile() ;
            TileComputeResultWithRunAfterInfo tileResultWithRunAfterInfo =
                    computerForZeroTile.computeZeroTile(
                            scriptText,
                            order.dt,
                            order.sdui
                    ) ;
            if( computerForZeroTile==null ){
                writeResultJson(20,null,"bad computerForZeroTile result.");
                return 20 ;
            }

            //output info for zero tile computing
            ArrayList<JDsnameDts> dsnameDtsArr = tileResultWithRunAfterInfo.getDsnameDtsArray() ;
            if( dsnameDtsArr.size()==0){
                writeResultJson(21,null,"the script should use one dataset at least.");
                return 21 ;
            }
            //dsnameArr proj属性必须一致，否则瓦片对不上,通过dsname计算 最小值maxlevel
            String projStr = "" ;
            for( int ids = 0 ; ids< dsnameDtsArr.size() ; ++ ids ){
                JProduct pinfo = rdb.rdbGetProductInfoByName(dsnameDtsArr.get(ids).dsname) ;
                if( ids==0 ){
                    projStr = pinfo.proj ;
                    targetLevel = pinfo.maxZoom ;
                }else
                {
                    if( projStr!=pinfo.proj){
                        writeResultJson(22, null,
                                "bad proj0 "+projStr + " and proj1 "+pinfo.proj);
                        return 22;
                    }
                    targetLevel = Math.min(targetLevel,pinfo.maxZoom) ;//取值最小的level
                }
            }
        }else{
            writeResultJson(11, null , "both dsname or jsfile is empty.") ;
            return 11 ;
        }

        //通过roi计算经纬度范围 extent，如果没有roi使用-180~+180 -90~+90 全球范围
        byte[] orderRoi2TlvData = JRoi2Loader.loadData(order.roi) ;//
        if( orderRoi2TlvData==null ){
            writeResultJson(23,null,
                    "bad order roi data: user add a roi in order but we load a null data, the roi input is "
                            +order.roi);
            return 23 ;
        }
        String[] roiarr = new String[1] ;
        roiarr[0] = order.roi ;
        Roi2HsegTlv2LonLatExtent.Extent tileExtent = Roi2HsegTlv2LonLatExtent.expandHalfPixel(
                Roi2HsegTlv2LonLatExtent.computeExtent(roiarr)
        ) ;
        double outExtentLeft = tileExtent.left ;
        double outExtentRight = tileExtent.right ;
        double outExtentTop = tileExtent.top ;
        double outExtentBottom = tileExtent.bottom ;

        System.out.println("use output lonlat extent:" + outExtentLeft+
                ","+outExtentRight+","+outExtentTop+","+outExtentBottom);


        //通过 extent 和 0-maxlevel  计算每个level的 tile_extentArray
        ArrayList<TileXYZ> tileXyzArray = new ArrayList<>() ;
        JTileRangeTool.TileXYRange range1 = JTileRangeTool.computeTileRangeByLonglatExtent(
                outExtentLeft,outExtentRight,
                outExtentTop,outExtentBottom,
                targetLevel,256) ;
        for(int ity = range1.ymin ; ity < range1.ymax+1; ++ ity ){
            for(int itx = range1.xmin ; itx < range1.xmax+1; ++ itx ){
                tileXyzArray.add(new TileXYZ(targetLevel,ity,itx)) ;
            }
        }

        System.out.println("use tile count will be :"+ tileXyzArray.size() );

        //shared data
        String scriptWithSDUI = JScriptTools.assembleScriptWithSDUI(scriptText,order.sdui) ;
        Broadcast<String> broadcastScriptWithSDUI = jsc.sc().broadcast(
                scriptWithSDUI, scala.reflect.ClassManifestFactory.fromClass(String.class)
        ) ;
        String extraText = "{\"datetime\":"+String.valueOf(order.dt)+"}" ;
        Broadcast<String> broadcastExtraText = jsc.sc().broadcast(
                extraText ,
                scala.reflect.ClassManifestFactory.fromClass(String.class)) ;

        Broadcast<byte[]> broadcastOrderRoiData = jsc.sc().broadcast(
                orderRoi2TlvData ,
                scala.reflect.ClassManifestFactory.fromClass(byte[].class)) ;
        Broadcast<Double> broadcastFillData = jsc.sc().broadcast(
                order.filldata ,
                scala.reflect.ClassManifestFactory.fromClass(Double.class)) ;
        Broadcast<Double> broadcastValidMinInc = jsc.sc().broadcast(
                order.validMinInc ,
                scala.reflect.ClassManifestFactory.fromClass(Double.class)) ;
        Broadcast<Double> broadcastValidMaxInc = jsc.sc().broadcast(
                order.validMaxInc ,
                scala.reflect.ClassManifestFactory.fromClass(Double.class)) ;
        Broadcast<String> broadcastTask17ConfigText = jsc.sc().broadcast(
                task17configJsonText ,
                scala.reflect.ClassManifestFactory.fromClass(String.class)) ;

        LongAccumulator accGoodTc = jsc.sc().longAccumulator() ;
        LongAccumulator accBadTc  = jsc.sc().longAccumulator() ;
        LongAccumulator accBadReduce = jsc.sc().longAccumulator() ;

        //build RDD for tile computing
        JavaRDD<TileXYZ> tilexyzRdds = jsc.parallelize(tileXyzArray , 10) ;
        //every tile do script computing, with a JStatisticData return value
        //if compute return null result, use a new JStatisticData to return
        JavaRDD<JStatisticData[]> statResRdd = tilexyzRdds.map(
                new Function<TileXYZ, JStatisticData[]>() {
                    @Override
                    public JStatisticData[] call(TileXYZ tileXYZ) throws Exception {
                        String brtask17configJsonText=broadcastTask17ConfigText.value() ;
                        WConfig.initWithString(brtask17configJsonText);
                        String myconn = WConfig.getSharedInstance().connstr;
                        String myuser = WConfig.getSharedInstance().user;
                        String mypwd =  WConfig.getSharedInstance().pwd ;
                        byte[] brOrderRoiData = broadcastOrderRoiData.value() ;
                        double brFillData = broadcastFillData.value() ;
                        double brValidMin = broadcastValidMinInc.value() ;
                        double brValidMax = broadcastValidMaxInc.value() ;

                        JRDBHelperForWebservice.init(myconn,myuser,mypwd);

                        String scirptWithSdui = broadcastScriptWithSDUI.value();
                        String extraText = broadcastExtraText.value() ;
                        HBasePeHelperCppConnector cc1 = new HBasePeHelperCppConnector();
                        TileComputeResult tileResult1=
                                cc1.RunScriptForTileWithoutRenderWithExtra(
                                        "com/pixelengine/HBasePixelEngineHelper",
                                        scirptWithSdui,
                                        extraText,
                                        tileXYZ.z,tileXYZ.y ,tileXYZ.x
                                ) ;
                        if(tileResult1==null ){
                            accBadTc.add(1);
                            return null;
                        }
                        if( tileResult1.status!=0 ){
                            accBadTc.add(1) ;
                            return null;
                        }
                        JStatisticData[] statDatas = cc1.ComputeStatisticTileComputeResultByHsegTlv(
                                "com/pixelengine/HBasePixelEngineHelper",
                                tileResult1,
                                brOrderRoiData,
                                brFillData,brValidMin,brValidMax
                        ) ;
                        accGoodTc.add(1) ;
                        return statDatas ;
                    }
                }) ;
        JavaRDD<JStatisticData[]> filteredRdd = statResRdd.filter(new Function<JStatisticData[], Boolean>() {
            @Override
            public Boolean call(JStatisticData[] jStatisticData) throws Exception {
                if( jStatisticData==null ) return false ;
                else return true ;
            }
        }) ;

        JStatisticData[] reducedStatData = filteredRdd.reduce(new Function2<JStatisticData[], JStatisticData[], JStatisticData[]>() {
            @Override
            public JStatisticData[] call(JStatisticData[] data1, JStatisticData[] data2) throws Exception {
                if( data1.length==data2.length )
                {
                    for(int ib = 0 ; ib < data1.length;++ib ){
                        if( data1[ib].validCnt > 0 && data2[ib].validCnt> 0 ){
                            data1[ib].validCnt+=data2[ib].validCnt ;
                            data1[ib].allCnt += data2[ib].allCnt ;
                            data1[ib].areakm2 += data2[ib].areakm2;
                            data1[ib].fillCnt += data2[ib].fillCnt ;
                            data1[ib].sq_sum += data2[ib].sq_sum;
                            data1[ib].sum += data2[ib].sum ;
                            data1[ib].validMax = Math.max(data1[ib].validMax,data2[ib].validMax) ;
                            data1[ib].validMin = Math.min(data1[ib].validMin,data2[ib].validMin) ;
                        }else if( data2[ib].validCnt > 0 ){
                            data1[ib] = data2[ib] ;
                        }
                    }
                    return data1 ;
                }else{
                    accBadReduce.add(1) ;
                    System.out.println("Exception data1 data2 have different result bands num.");
                    return new JStatisticData[0] ;
                }
            }
        }) ;

        writeResultJson(0 , reducedStatData , "success" ) ;
        System.out.println("*\n*\n*") ;
        System.out.println("good tc:" + accGoodTc.value()) ;
        System.out.println("bad tc:"+accBadTc.value());
        System.out.println("bad reduce:"+accBadReduce.value());
        System.out.println("*\n*\n*") ;
        System.out.println("done");
        return 0 ;
    }


}
