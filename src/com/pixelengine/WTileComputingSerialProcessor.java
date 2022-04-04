package com.pixelengine;
//spark序列分析
//2022-3-27 created
//2022-4-4  first working version


import com.google.gson.Gson;
import com.pixelengine.DataModel.*;
import com.pixelengine.tools.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;
import scala.Serializable;
import scala.Tuple2;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

public class WTileComputingSerialProcessor implements Serializable {
    public String task17configFile ;
    public String orderJsonFile ;
    public String resultJsonFile ;

    protected boolean writeResultJson(int state, JDtStatisticData[] statdatas, String msg)
    {
        if (resultJsonFile.isEmpty()) {
            System.out.println("resultJsonFile is empty.");
            return false ;
        } else {
            try {
                Gson gson = new Gson() ;
                String datastr = gson.toJson(statdatas , JDtStatisticData[].class) ;

                OutputStream outputStream = new FileOutputStream(resultJsonFile);
                String content = "{\"state\":" + String.valueOf(state)
                        + ",\"data\":" + datastr
                        + ",\"message\":\"" + msg + "\"}";
                outputStream.write(content.getBytes());
                outputStream.close();
                return true ;
            } catch (Exception ex) {
                System.out.println("WTileComputingSerialProcessor.writeResultJson exception:" + ex.getMessage());
                return false ;
            }
        }
    }

    protected boolean writeResultCsv(JDtStatisticData[] statdatas)
    {
        if (resultJsonFile.isEmpty()) {
            System.out.println("resultJsonFile is empty.");
            return false ;
        } else {
            try {
                String csvfile = resultJsonFile + ".csv" ;
                FileWriter writer = new FileWriter(csvfile) ;
                writer.write("key,areakm2,acnt,fcnt,vcnt,vmin,vmax,sum,sqsum,mean,stddev,...\n") ;
                for(int idt=0;idt < statdatas.length; ++ idt )
                {
                    writer.write(statdatas[idt].key) ;
                    for(int iband = 0 ; iband < statdatas[idt].data.length ;  ++ iband )
                    {
                        writer.write(
                                "," + statdatas[idt].data[iband].areakm2
                                    + "," + statdatas[idt].data[iband].allCnt
                                    + "," + statdatas[idt].data[iband].fillCnt
                                    + "," + statdatas[idt].data[iband].validCnt
                                    + "," + statdatas[idt].data[iband].validMin
                                    + "," + statdatas[idt].data[iband].validMax
                                    + "," + statdatas[idt].data[iband].sum
                                    + "," + statdatas[idt].data[iband].sq_sum
                                    + "," + statdatas[idt].data[iband].computeMean()
                                    + "," + statdatas[idt].data[iband].computeStdev()
                        );
                    }
                    writer.write(",\n") ;
                }
                writer.close();
                return true ;
            } catch (Exception ex) {
                System.out.println("WTileComputingSerialProcessor.writeResultCsv exception:" + ex.getMessage());
                return false ;
            }
        }
    }

    public class SparkTaskParams implements Serializable  {
        public int z,y,x ;
        long[] dtCollection;
    }

    public int runit()
    {
        //load resource from config.json in jar
        System.out.println("WTileComputingSerialProcessor running. parsing task17config.json");
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
        JTileComputingSerialOrder order = gson.fromJson(
                WTextFile.readFileAsString(orderJsonFile),JTileComputingSerialOrder.class) ;


        String scriptText = "" ;
        int targetLevel = 0 ;
        if( order.dsname.compareTo("") != 0 ){
            JDtCollectionBuilder dtcollBuilder = new JDtCollectionBuilder();
            dtcollBuilder.wholePeriod.startDt = order.whole_start ;
            dtcollBuilder.wholePeriod.startInclusive = order.whole_start_inc==1 ;
            dtcollBuilder.wholePeriod.stopDt = order.whole_stop ;
            dtcollBuilder.wholePeriod.stopInclusive = order.whole_stop_inc==1 ;
            dtcollBuilder.repeatType = order.repeat_type ;
            dtcollBuilder.repeatPeriod.startDt = order.repeat_start ;
            dtcollBuilder.repeatPeriod.startInclusive = order.repeat_start_inc==1 ;
            dtcollBuilder.repeatPeriod.stopDt = order.repeat_stop ;
            dtcollBuilder.repeatPeriod.stopInclusive = order.repeat_stop_inc==1 ;
            dtcollBuilder.repeatPeriod.stopInNextYear = order.repeat_stopnextyear;

            JDtCollection[] dtcollectionArray = rdb.buildDtCollection(order.dsname,
                        dtcollBuilder
                    ) ;
            if( dtcollectionArray==null || dtcollectionArray.length==0 )
            {
                writeResultJson(30 , null , "dtcollectionArray is null or empty") ;
                return 30 ;
            }

            //compute maxZoom
            JProduct productInfo = rdb.rdbGetProductInfoByName( order.dsname ) ;
            if( productInfo==null )
            {
                writeResultJson(31 , null , "no product for '" + order.dsname  + "'") ;
                return 31 ;
            }

            //通过roi计算经纬度范围 extent
            byte[] orderRoi2TlvData = JRoi2Loader.loadData(order.roi) ;//
            if( orderRoi2TlvData==null ){
                writeResultJson(32,null,"bad roi:'" + order.roi +"'" );
                return 32 ;
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
            System.out.println("roi extent:" + outExtentLeft+
                    ","+outExtentRight+","+outExtentTop+","+outExtentBottom);

            //通过 extent 和 0-maxlevel  计算每个level的 tile_extentArray
            ArrayList<TileXYZ> tileXyzArray = new ArrayList<>() ;
            JTileRangeTool.TileXYRange range1 = JTileRangeTool.computeTileRangeByLonglatExtent(
                    outExtentLeft,outExtentRight,
                    outExtentTop,outExtentBottom,
                    productInfo.maxZoom,256) ;
            for(int ity = range1.ymin ; ity < range1.ymax+1; ++ ity ){
                for(int itx = range1.xmin ; itx < range1.xmax+1; ++ itx ){
                    tileXyzArray.add(new TileXYZ(productInfo.maxZoom,ity,itx)) ;
                }
            }
            System.out.println("use tile count will be :"+ tileXyzArray.size() );

            //make all task params
            ArrayList< Tuple2<String,SparkTaskParams> > paramsList = new ArrayList<>() ;
            for(int icoll = 0 ; icoll < dtcollectionArray.length ; ++ icoll  )
            {
                for( TileXYZ tilexyz : tileXyzArray)
                {
                    SparkTaskParams oneParams = new SparkTaskParams();
                    oneParams.x = tilexyz.x ;
                    oneParams.y = tilexyz.y ;
                    oneParams.z = tilexyz.z ;
                    oneParams.dtCollection = dtcollectionArray[icoll].datetimes ;
                    Tuple2<String,SparkTaskParams> tuple = new Tuple2<>( dtcollectionArray[icoll].key , oneParams) ;
                    paramsList.add(tuple) ;
                }
            }
            System.out.println("total task count is :"+ paramsList.size() );

            Integer compositeMethod = 1;//min-1,max-2,ave-3,sum-4
            if( order.method.equals("min") ){
                compositeMethod = 1 ;
            }else if(order.method.equals("max") ){
                compositeMethod=2 ;
            }else if( order.method.equals("ave") ){
                compositeMethod = 3 ;
            }else if( order.method.equals("sum") ){
                compositeMethod = 4 ;
            }else{
                writeResultJson(33,null,"unsupported method:'" + order.method +"'" );
                return 33 ;
            }


            //shared data
            Broadcast<String> broadcastDsname = jsc.sc().broadcast(
                    order.dsname ,
                    scala.reflect.ClassManifestFactory.fromClass(String.class)) ;
            Broadcast<Integer> broadcastMethod = jsc.sc().broadcast(
                    compositeMethod ,
                    scala.reflect.ClassManifestFactory.fromClass(Integer.class)) ;
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
            JavaPairRDD<String,SparkTaskParams> paramsRdds = jsc.parallelizePairs( paramsList , 10);
            //every tile do script computing, with a JStatisticData return value
            //if compute return null result, use a new JStatisticData to return
            JavaPairRDD<String,JStatisticData[]> statRdds = paramsRdds.mapToPair(
                    new PairFunction<Tuple2<String, SparkTaskParams>, String, JStatisticData[]>() {
                @Override
                public Tuple2<String, JStatisticData[]> call(Tuple2<String, SparkTaskParams> intuple) throws Exception {
                    String brtask17configJsonText=broadcastTask17ConfigText.value() ;
                    WConfig.initWithString(brtask17configJsonText);
                    String myconn = WConfig.getSharedInstance().connstr;
                    String myuser = WConfig.getSharedInstance().user;
                    String mypwd =  WConfig.getSharedInstance().pwd ;
                    String dsname = broadcastDsname.value() ;
                    int imethod =    broadcastMethod.value() ;//min-1,max-2,ave-3,sum-4 also in c++ and javascript
                    byte[] brOrderRoiData = broadcastOrderRoiData.value() ;
                    double brFillData = broadcastFillData.value() ;
                    double brValidMin = broadcastValidMinInc.value() ;
                    double brValidMax = broadcastValidMaxInc.value() ;

                    JRDBHelperForWebservice.init(myconn,myuser,mypwd);

                    String dtarrstr = "[" ;
                    long[] dtarr = intuple._2.dtCollection ;
                    for(int idt = 0 ; idt < dtarr.length;++idt ) {
                        if( idt==0 ) dtarrstr += dtarr[idt] ;
                        else dtarrstr += ","+dtarr[idt] ;
                    }
                    dtarrstr+="]" ;
                    String outTypeStr = "" ;
                    if( imethod==3 || imethod==4 ){
                        outTypeStr = ",6"  ;//use float32
                    }
                    String scriptText =
                            "function main(){" +
                                    "let dtarr="+dtarrstr+";" +
                                    "let imethod="+imethod+";" +
                                    "let dscoll=pe.DatasetCollection('"+dsname+"',dtarr);" +
                                    "let ds=pe.CompositeDsCollection(dscoll,imethod,"+brValidMin+","+brValidMax + "," +brFillData + outTypeStr +");" +
                                    "return ds;" +
                            "}";
                    HBasePeHelperCppConnector cc1 = new HBasePeHelperCppConnector();
                    TileComputeResult tileResult1=
                            cc1.RunScriptForTileWithoutRenderWithExtra(
                                    "com/pixelengine/HBasePixelEngineHelper",
                                    scriptText,
                                    "",
                                    intuple._2.z,intuple._2.y ,intuple._2.x
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
                    Tuple2<String, JStatisticData[]> outTuple = new Tuple2<String, JStatisticData[]>(intuple._1, statDatas) ;
                    return outTuple ;
                }
            }) ;

            JavaPairRDD<String,JStatisticData[]> filteredRdd = statRdds.filter(
                    new Function<Tuple2<String, JStatisticData[]>, Boolean>() {
                @Override
                public Boolean call(Tuple2<String, JStatisticData[]> tup) throws Exception {
                    if( tup==null ) return false ;
                    else return true ;
                }
            }) ;

            JavaPairRDD<String,JStatisticData[]> reducedRdd = filteredRdd.reduceByKey(
                    new Function2<JStatisticData[], JStatisticData[], JStatisticData[]>() {
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

            List< Tuple2<String, JStatisticData[]> > serialStatResults = reducedRdd.collect() ;
            System.out.println("do collection with size " + serialStatResults.size() );
            JDtStatisticData[] dtstatDataArray = new  JDtStatisticData[serialStatResults.size()] ;
            for(int idt =0 ; idt < dtstatDataArray.length; ++ idt )
            {
                dtstatDataArray[idt] = new JDtStatisticData() ;
                dtstatDataArray[idt].key = serialStatResults.get(idt)._1 ;
                dtstatDataArray[idt].data = serialStatResults.get(idt)._2 ;
            }

            System.out.println("write out json ");
            writeResultJson(0 , dtstatDataArray , "success" ) ;
            System.out.println("write csv ");
            writeResultCsv( dtstatDataArray ) ;
            System.out.println("*\n*\n*") ;
            System.out.println("good tc:" + accGoodTc.value()) ;
            System.out.println("bad tc:"+accBadTc.value());
            System.out.println("bad reduce:"+accBadReduce.value());
            System.out.println("*\n*\n*") ;
            System.out.println("done");
            return 0 ;

        }else if( order.jsfile.compareTo("")!=0 ) {
            //目前看仅仅通过脚本没法计算出序列值，以后有时间在完善 2022-4-4
            writeResultJson(40,null,"no support jsfile mode yet 2022-4-4." );
            return 40 ;
        }else{
            writeResultJson(11, null , "both dsname or jsfile is empty.") ;
            return 11 ;
        }
    }
}
