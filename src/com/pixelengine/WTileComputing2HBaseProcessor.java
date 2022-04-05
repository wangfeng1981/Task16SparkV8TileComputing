package com.pixelengine;
// 使用js进行瓦片计算，结果写入HBase
// 2022-3-27 created


import com.google.gson.Gson;
import com.pixelengine.DataModel.*;
import com.pixelengine.tools.JRoi2Loader;
import com.pixelengine.tools.JScriptTools;
import com.pixelengine.tools.JTileRangeTool;
import com.pixelengine.tools.Roi2HsegTlv2LonLatExtent;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import scala.Serializable;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

public class WTileComputing2HBaseProcessor implements Serializable {


    public String task17configFile ;
    public String orderJsonFile ;
    public String resultJsonFile ;

    protected boolean writeResultJson(int state, int mypid, String msg)
    {
        if (resultJsonFile.isEmpty()) {
            System.out.println("resultJsonFile is empty.");
            return false ;
        } else {
            try {
                OutputStream outputStream = new FileOutputStream(resultJsonFile);
                String content = "{\"state\":" + String.valueOf(state)
                        + ",\"mypid\":" + String.valueOf(mypid)
                        + ",\"message\":\"" + msg + "\"}";
                outputStream.write(content.getBytes());
                outputStream.close();
                return true ;
            } catch (Exception ex) {
                System.out.println("WTileComputing2HBaseProcessor.writeResultJson exception:" + ex.getMessage());
                return false ;
            }
        }
    }

    public int runit()
    {
        //load resource from config.json in jar
        System.out.println("WTileComputing2HBaseProcessor running. parsing task17config.json");
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
        //Configuration conf = HBaseConfiguration.create();
        //conf.set("hbase.zookeeper.quorum", zookeeper );//must has this code for hbase.
        SparkConf sparkconf = new SparkConf().
                setAppName(appname).setMaster(sparkmaster);
        JavaSparkContext jsc = new JavaSparkContext(sparkconf);
        //JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);

        //show pixelengine core version
        HBasePeHelperCppConnector cc = new HBasePeHelperCppConnector();
        System.out.println("pe core version: " + cc.GetVersion() );

        //load input order.json
        System.out.println("load task order jsonfile "+orderJsonFile);
        Gson gson = new Gson() ;
        JTileComputing2HBaseOrder tcHbOrder = gson.fromJson(
                WTextFile.readFileAsString(orderJsonFile),JTileComputing2HBaseOrder.class) ;

        //计算0，0，0号瓦片，获取 dsnameArr 列表，与时间列表 dtArr ，输出类型 outdatatype
        String scriptText = WTextFile.readFileAsString(tcHbOrder.jsfile) ;
        WComputeZeroTile computerForZeroTile = new WComputeZeroTile() ;
        TileComputeResultWithRunAfterInfo tileResultWithRunAfterInfo =
                computerForZeroTile.computeZeroTile(
                        scriptText,
                        tcHbOrder.dt,
                        tcHbOrder.sdui
                ) ;
        if( computerForZeroTile==null ){
            writeResultJson(20,0,"bad computerForZeroTile result.");
            return 20 ;
        }

        //output info for zero tile computing
        ArrayList<JDsnameDts> dsnameDtsArr = tileResultWithRunAfterInfo.getDsnameDtsArray() ;
        if( dsnameDtsArr.size()==0){
            writeResultJson(21,0,"the script should use one dataset at least.");
            return 21 ;
        }
        ArrayList<String> roi2Arr = tileResultWithRunAfterInfo.getRoi2Array() ;
        System.out.println("dsname0,dt0:" + dsnameDtsArr.get(0).dsname+","+dsnameDtsArr.get(0).dtarr.get(0));
        if( roi2Arr.size()>0) System.out.println("roi2-0:"+roi2Arr.get(0)) ;
        System.out.println("outDatatype:"+tileResultWithRunAfterInfo.dataType);
        System.out.println("outNumband:" + tileResultWithRunAfterInfo.nbands);

        //dsnameArr proj属性必须一致，否则瓦片对不上,通过dsname计算 最小值maxlevel
        String projStr = "" ;
        Long hcolForDs0Dt0 =  dsnameDtsArr.get(0).dtarr.get(0) ;
        int maxZoom = 12;
        for( int ids = 0 ; ids< dsnameDtsArr.size() ; ++ ids ){
            JProduct pinfo = rdb.rdbGetProductInfoByName(dsnameDtsArr.get(ids).dsname) ;
            if( ids==0 ){
                projStr = pinfo.proj ;
                maxZoom = pinfo.maxZoom ;
            }else
            {
                if( projStr!=pinfo.proj){
                    writeResultJson(22, 0,"bad proj0 "+projStr + " and proj1 "+pinfo.proj);
                    return 22;
                }
                if( pinfo.maxZoom < maxZoom) maxZoom = pinfo.maxZoom ;//取值最小的level
            }
        }

        //通过roi计算经纬度范围 extent，如果没有roi使用-180~+180 -90~+90 全球范围
        double outExtentLeft = -180 ;
        double outExtentRight = 180 ;
        double outExtentBottom = -90 ;
        double outExtentTop = 90 ;
        boolean useOrderRoi = false ;
        byte[] orderRoi2TlvData = new byte[1] ;//a nearly empty byte array for broadcast if no order roi
        if(tcHbOrder.roi.length()>0) {
            roi2Arr.add( tcHbOrder.roi) ;
            useOrderRoi = true ;
            orderRoi2TlvData = JRoi2Loader.loadData(tcHbOrder.roi) ;
            if( orderRoi2TlvData==null ){
                writeResultJson(23,0,"bad order roi data: user add a roi in order but we load a null data, the roi input is "+tcHbOrder.roi);
                return 23 ;
            }
        }
        if( roi2Arr.size()> 0 ){
            Roi2HsegTlv2LonLatExtent.Extent tileExtent = Roi2HsegTlv2LonLatExtent.expandHalfPixel(
                    Roi2HsegTlv2LonLatExtent.computeExtent(
                            roi2Arr.toArray(new String[0])
                    )
            ) ;
            outExtentLeft = tileExtent.left ;
            outExtentRight = tileExtent.right ;
            outExtentTop = tileExtent.top ;
            outExtentBottom = tileExtent.bottom ;
        }
        System.out.println("use output lonlat extent:" + outExtentLeft+","+outExtentRight+","+outExtentTop+","+outExtentBottom);


        //通过 extent 和 0-maxlevel  计算每个level的 tile_extentArray
        ArrayList<TileXYZ> tileXyzArray = new ArrayList<>() ;
        for(int iz = 0 ; iz <= maxZoom ; ++ iz )
        {
            JTileRangeTool.TileXYRange range1 = JTileRangeTool.computeTileRangeByLonglatExtent(
                    outExtentLeft,outExtentRight,
                    outExtentTop,outExtentBottom,
                    iz,256) ;
            for(int ity = range1.ymin ; ity < range1.ymax+1; ++ ity ){
                for(int itx = range1.xmin ; itx < range1.xmax+1; ++ itx ){
                    tileXyzArray.add(new TileXYZ(iz,ity,itx)) ;
                }
            }
        }
        System.out.println("output tile count will be :"+ tileXyzArray.size() );

        //shared data
        String scriptWithSDUI = JScriptTools.assembleScriptWithSDUI(scriptText,tcHbOrder.sdui) ;
        Broadcast<String> broadcastScriptWithSDUI = jsc.sc().broadcast(
                scriptWithSDUI, scala.reflect.ClassManifestFactory.fromClass(String.class)
        ) ;
        String extraText = "{\"datetime\":"+String.valueOf(tcHbOrder.dt)+"}" ;
        Broadcast<String> broadcastExtraText = jsc.sc().broadcast(
                extraText ,
                scala.reflect.ClassManifestFactory.fromClass(String.class)) ;
        Broadcast<Integer> broadcastHpid = jsc.sc().broadcast(
                tcHbOrder.mpid_hpid ,
                scala.reflect.ClassManifestFactory.fromClass(Integer.class)) ;
        Broadcast<Integer> broadcastHpidLen = jsc.sc().broadcast(
                tcHbOrder.out_hpidlen ,
                scala.reflect.ClassManifestFactory.fromClass(Integer.class)) ;
        Broadcast<Integer> broadcastYxLen = jsc.sc().broadcast(
                tcHbOrder.out_xylen ,
                scala.reflect.ClassManifestFactory.fromClass(Integer.class)) ;
        Broadcast<Long> broadcastHcol = jsc.sc().broadcast(
                tcHbOrder.out_hcol ,
                scala.reflect.ClassManifestFactory.fromClass(Long.class)) ;
        Broadcast<String> broadcastHtable = jsc.sc().broadcast(
                tcHbOrder.out_htable ,
                scala.reflect.ClassManifestFactory.fromClass(String.class)) ;
        Broadcast<String> broadcastZk = jsc.sc().broadcast(
                wconfig.zookeeper ,
                scala.reflect.ClassManifestFactory.fromClass(String.class)) ;
        Broadcast<String> broadcastMyConnStr = jsc.sc().broadcast(
                wconfig.connstr ,
                scala.reflect.ClassManifestFactory.fromClass(String.class)) ;
        Broadcast<String> broadcastMyUser = jsc.sc().broadcast(
                wconfig.user ,
                scala.reflect.ClassManifestFactory.fromClass(String.class)) ;
        Broadcast<String> broadcastMyPwd = jsc.sc().broadcast(
                wconfig.pwd ,
                scala.reflect.ClassManifestFactory.fromClass(String.class)) ;
        Broadcast<String> broadcastTask17ConfigText = jsc.sc().broadcast(
                task17configJsonText ,
                scala.reflect.ClassManifestFactory.fromClass(String.class)) ;
        Broadcast<Boolean> broadcastUseOrderRoi = jsc.sc().broadcast(
                useOrderRoi ,
                scala.reflect.ClassManifestFactory.fromClass(Boolean.class)) ;
        Broadcast<byte[]> broadcastOrderRoiData = jsc.sc().broadcast(
                orderRoi2TlvData ,
                scala.reflect.ClassManifestFactory.fromClass(byte[].class)) ;
        Broadcast<Double> broadcastFillData = jsc.sc().broadcast(
                tcHbOrder.filldata ,
                scala.reflect.ClassManifestFactory.fromClass(Double.class)) ;

        System.out.println("debug 1");
        //build RDD for tile computing
        JavaRDD<TileXYZ> tilexyzRdds = jsc.parallelize(tileXyzArray , 10) ;
        //every tile do script computing, with a Integer return value
        //return value 0 good , 1 bad.
        System.out.println("debug 2");
        JavaRDD<Integer> tileResRdd = tilexyzRdds.map(
                new Function<TileXYZ, Integer>() {
                    @Override
                    public Integer call(TileXYZ tileXYZ) throws Exception {
                        String brtask17configJsonText=broadcastTask17ConfigText.value() ;
                        WConfig.initWithString(brtask17configJsonText);
                        String myconn = broadcastMyConnStr.value();
                        String myuser = broadcastMyUser.value();
                        String mypwd = broadcastMyPwd.value() ;
                        Boolean brUseOrderRoi = broadcastUseOrderRoi.value() ;
                        byte[] brOrderRoiData = broadcastOrderRoiData.value() ;
                        double brFillData = broadcastFillData.value() ;

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
                            return 1;
                        }
                        if( tileResult1.status!=0 ){
                            return tileResult1.status ;
                        }

                        TileComputeResult newTCR = null ;
                        if( brUseOrderRoi==true ){
                            newTCR = cc1.ClipTileComputeResultByHsegTlv(
                                    "com/pixelengine/HBasePixelEngineHelper",
                                    tileResult1 ,
                                    brOrderRoiData,
                                    brFillData
                            ) ;
                        }else{
                            newTCR = tileResult1 ;
                        }

                        Integer brhpidblen = broadcastHpidLen.value() ;
                        Integer brhpid = broadcastHpid.value() ;
                        Integer bryxblen = broadcastYxLen.value() ;
                        long brhcol = broadcastHcol.value() ;
                        String brHtable = broadcastHtable.value() ;
                        String brzookeeper = broadcastZk.value() ;

                        byte[] outRowkey = WHBaseUtil.GenerateRowkey(
                                brhpidblen ,
                                brhpid ,
                                bryxblen ,
                                tileXYZ.z,
                                tileXYZ.y,
                                tileXYZ.x) ;
                        Put put1 = new Put( outRowkey ) ;
                        put1.addColumn(
                                "tiles".getBytes() ,
                                Bytes.toBytes(brhcol) ,
                                newTCR.binaryData ) ;

                        Configuration conf = HBaseConfiguration.create();
                        conf.set("hbase.zookeeper.quorum",brzookeeper);
                        Connection hbaseConnection = ConnectionFactory.createConnection(conf) ;
                        Table outputTable = hbaseConnection.getTable( TableName.valueOf(brHtable) ) ;
                        outputTable.put( put1) ;
                        return 0 ;
                    }
                }) ;

        //trigger spark to run
        List<Integer> allTcStatus = tileResRdd.collect() ;
        ListIterator<Integer> tcStatusIter = allTcStatus.listIterator() ;
        int goodTcCount = 0 ;
        int badTcCount =0 ;
        while(tcStatusIter.hasNext()){
            if( 0 == tcStatusIter.next() ) ++ goodTcCount ;
            else ++ badTcCount ;
        }

        //add band records
        boolean bandsok = rdb.writeProductBandRecord(tcHbOrder.mpid_hpid,tcHbOrder.mpid_hpid,
                tileResultWithRunAfterInfo.nbands,0,255,tcHbOrder.filldata) ;
        if( bandsok==false ){
            System.out.println("failed to write band infos into mysql.");
            writeResultJson(24,0,"failed to write band infos into mysql.");
            return 24 ;
        }
        System.out.println("write bands records ok.");

        //add dataitem record
        int dataitemId = rdb.writeProductDataItem(tcHbOrder.mpid_hpid,tcHbOrder.out_hcol,outExtentLeft,
                outExtentRight,outExtentTop,outExtentBottom) ;
        if( dataitemId<0 ){
            System.out.println("failed to write data item into mysql.");
            writeResultJson(25,0,"failed to write data item into mysql.");
            return 25 ;
        }
        System.out.println("write data item records ok:" + dataitemId);

        //update mysql tbproduct record
        String pdtName = "user/" + tcHbOrder.mpid_hpid ;
        boolean updatepdtok = rdb.updateProductInfo(tcHbOrder.mpid_hpid,
                projStr,
                0,
                maxZoom,
                tileResultWithRunAfterInfo.dataType,
                0,
                tcHbOrder.out_htable,
                256,256,
                "deflate" ,
                0
        ) ;
        if( updatepdtok==false){
            System.out.println("failed to update pdt info.");
            writeResultJson(26,0,"failed to update pdt info.");
            return 26 ;
        }
        System.out.println("update pdt name and info ok:" + pdtName);


        //done.
        writeResultJson(0,tcHbOrder.mpid_hpid,"success done, good:"+goodTcCount+", bad:"+badTcCount);
        System.out.println("*\n*\n*");
        System.out.println("good:"+goodTcCount);
        System.out.println("bad:"+badTcCount);
        System.out.println("*\n*\n*\n");
        System.out.println("done");
        return 0 ;
    }
}
