package com.pixelengine;
////////////////////////////////////////////////////////
//
//
// 这个接口是C++回调Java进程的
//update 2022-2-13 1020
//update 2022-3-19
//update 2022-4-3 0800
//update 2022-6-6 1027 add debug infos output for finding bugs in getDataCollection
//
/////////////////////////////////////////////////////////

import com.pixelengine.DataModel.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

//import org.apache.spark.sql.catalyst.plans.logical.Except;
//import shapeless.ops.nat;


public class HBasePixelEngineHelper {
    public String errorMessage;
//    private static int MaxDatetimeRecords = 30 ;//这个地方必须是static，否则c++调用的时候不会初始化这个值。static出现新的bug了
//    private static int PEIgnore = -1 ;//这个地方必须是static，否则c++调用的时候不会初始化这个值。

    //返回最近一次调用的错误信息
    public String getErrorMessage() {
        return errorMessage;
    }

    public static Connection hbaseConn = null ;
    public static String zookeeper = null ;

    public static Connection getHBaseConnection() throws IOException {
        if( zookeeper==null )
        {
            zookeeper = WConfig.getSharedInstance().zookeeper;
        }
        if( hbaseConn==null || hbaseConn.isClosed() || hbaseConn.isAborted() )
        {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", zookeeper );//must has this code for hbase.
            hbaseConn = ConnectionFactory.createConnection(conf);
        }
        return hbaseConn ;
    }

    //get one tile data
    public TileData getTileData( long dt, String dsName,int[] bandindices,int z,int y,int x)
    {
        System.out.println("in java getTileData z,y,x,dt: "+z + "," + y+","+x+","+dt ) ;//2022-6-6
        TileData tiledata = new TileData(1) ;

        tiledata.datetimeArray[0] = dt ;
        tiledata.numds = 1;
        tiledata.x = x ;
        tiledata.y = y ;
        tiledata.z = z ;

        boolean useFilekey = false;
        if( dsName.length() == 0 )
        {
            errorMessage="Error : dsName is empty" ;
            System.out.println("debug " + errorMessage);//2022-6-6
            return null;
        }

        if( dsName.getBytes()[0] == '/'){
            useFilekey = true;
        }

        //get product by name
        JRDBHelperForWebservice rdb = new JRDBHelperForWebservice() ;
        JProduct pdt = rdb.rdbGetProductInfoByName(dsName) ;
        if( pdt==null ){
            errorMessage="Error : not find product of "+dsName ;
            System.out.println("debug " + errorMessage);//2022-6-6
            return null ;
        }
        //build usebandlist by input bandindices
        ArrayList<JProductBand> usebandlist = new ArrayList<>() ;
        if( bandindices.length == 0 ){
            usebandlist = pdt.bandList ;
        }else{
            //get band list by bandindices
            for(int i = 0 ; i < bandindices.length; ++ i ){
                //get the JProductBand by bandindices and pid
                int bandindex = bandindices[i] ;
                if( bandindex>=0 && bandindex < pdt.bandList.size() ){
                    usebandlist.add(pdt.bandList.get(bandindex)) ;
                }else{
                    System.out.println("bandindex is out of range.");
                    return null ;
                }
            }
        }
        if( useFilekey==false ){
            //this is Dataset
            try {
                tiledata.width = pdt.tileWid;
                tiledata.height = pdt.tileHei ;
                tiledata.nband = usebandlist.size() ;//
                tiledata.dataType = pdt.dataType ;
                int pixelLen = JProduct.getDataByteLenByDataType( pdt.dataType) ;
                System.out.println("dataType:" + tiledata.dataType + "; pixelLen:"+pixelLen);
                tiledata.tiledataArray[0] = new byte[
                        tiledata.width
                                *tiledata.height
                                *tiledata.nband
                                *pixelLen]  ;
                int bandbytesize = tiledata.width*tiledata.height * pixelLen ;
                int lasthpid = -1;
                byte[] lastCellData = null ;
                for(int iband = 0 ; iband < usebandlist.size() ; ++ iband )
                {
                    int newhpid = usebandlist.get(iband).hPid ;
                    if( newhpid == lasthpid && lastCellData!=null ){
                        copyBytes2Bytes(lastCellData, usebandlist.get(iband).bsqIndex ,
                                bandbytesize , tiledata.tiledataArray[0], iband );
                    }else{
                        //2021-8-22
                        if( pdt.source.equals("hbase") )
                        {
                            lastCellData = hbaseGetCellData(
                                    pdt.hbaseTable.hTableName,
                                    pdt.hbaseTable.hFamily,
                                    dt,
                                    pdt.hbaseTable.hPidByteNum,
                                    newhpid,
                                    pdt.hbaseTable.hYXByteNum ,
                                    z,y,x) ;
                        }else if( pdt.source.equals("file") )
                        {
                            lastCellData = filesystemGetCellData(
                                    pdt.hbaseTable.hTableName,dt,newhpid,z,y,x) ;

                        }else{
                            errorMessage = "Error : pdt.source is not supported for '" + pdt.source+"'. " ;
                            System.out.println("debug " + errorMessage);//2022-6-6
                            return null ;
                        }


                        if( lastCellData==null ){
                            errorMessage = "get emtpty cell data.";
                            System.out.println("debug " + errorMessage);//2022-6-6
                            return null ;
                        }
                        lasthpid = newhpid ;
                        copyBytes2Bytes(lastCellData, usebandlist.get(iband).bsqIndex ,
                                bandbytesize , tiledata.tiledataArray[0], iband );
                    }

                }
                return tiledata;
            } catch (Exception e) {
                // TODO Auto-generated catch block
                errorMessage = "Error : getTileData exception of " + e.getMessage() ;
                System.out.println("debug " + errorMessage);//2022-6-6
                return null ;
            }
        }else{
            //user file only for hbase
            //use filekey or filepath
            try {
                //get the hcol value in dataItem
                ArrayList<JProductDataItem> dataItems = rdb.rdbGetProductDataItemList(
                        pdt.pid,0,1,"ASC") ;
                //for user's file , this is the only one dataitem, only for hcol.
                if( dataItems==null || dataItems.size()==0 ){
                    System.out.println("no dataitem for user file:"+ pdt.name);
                    return null ;
                }
                JProductDataItem onlyDataItem = dataItems.get(0) ;

                tiledata.width = pdt.tileWid;
                tiledata.height = pdt.tileHei ;
                tiledata.nband = pdt.bandList.size();//different with dataset
                tiledata.dataType = pdt.dataType;
                int pixelLen = pdt.getDataByteLen() ;
                System.out.println("dataType:" + tiledata.dataType + ";; pixelLen:"+pixelLen);
                tiledata.tiledataArray[0] = new byte[
                        tiledata.width
                                *tiledata.height
                                *tiledata.nband
                                *pixelLen] ;
                byte[] lastCellData = hbaseGetCellData(
                        pdt.hbaseTable.hTableName,
                        pdt.hbaseTable.hFamily,
                        onlyDataItem.hcol ,//different with dataset
                        pdt.hbaseTable.hPidByteNum,
                        pdt.bandList.get(0).hPid ,
                        pdt.hbaseTable.hYXByteNum ,
                        z,y,x) ;
                if( lastCellData==null ){
                    errorMessage = "get emtpty cell data hcol:" + onlyDataItem.hcol;
                    System.out.println("debug " + errorMessage);//2022-6-6
                    return null ;
                }
                System.arraycopy(lastCellData, 0 ,
                        tiledata.tiledataArray[0] , 0 ,
                        lastCellData.length);//full copy

                return tiledata;
            } catch (Exception e) {
                // TODO Auto-generated catch block
                errorMessage = "Error : getTileData exception of " + e.getMessage() ;
                System.out.println("debug " + errorMessage);//2022-6-6
                return null ;
            }
        }
    }


    //hbase get cell data
    private byte[] hbaseGetCellData( String htablename,String hfami,
                                     long datetime ,
                                     int hpidlen, int hpid ,
                                     int yxlen , int z,int y,int x )
    {
        try{
            Connection conn = getHBaseConnection();
            Table table = conn.getTable(TableName.valueOf(htablename)) ;
            byte[] rowkey1 = WHBaseUtil.GenerateRowkey( hpidlen ,hpid ,yxlen ,z,y,x) ;
            Get get = new Get(rowkey1);
            get.readVersions(1) ;
            get.addColumn(hfami.getBytes() , Bytes.toBytes(datetime) ) ;
            Result result = table.get(get);
            if(null ==result)
            {
                errorMessage = "not find the cell in step1." ;
                return null ;
            }
            if(result.listCells()==null || result.listCells().size()==0)
            {
                errorMessage = "not find the cell in step2." ;
                return null ;
            }
            Cell cell1 = result.listCells().get(0) ;

            byte[] outByteData = CellUtil.cloneValue(cell1) ;
            System.out.println("java debug listCells.get(0) cellvalue.bytesize : " + outByteData.length ) ;
            return outByteData;
        }catch (Exception ex)
        {
            errorMessage = "Error : get cell data exception :" + ex.getMessage() ;
            return null;
        }
    }


    //get cell data from filesystem at tileroot
    private byte[] filesystemGetCellData( String htablename,
                                          long datetime ,
                                          int hpid ,
                                          int z,int y,int x )
    {
        try{
            String cellfilepath = WConfig.getSharedInstance().tilelocalrootdir
                    + "/"
                    + htablename + "/"
                    + String.valueOf(hpid) + "/"
                    + String.valueOf(datetime) + "/"
                    + "tile_" + String.valueOf(z) + "_" + String.valueOf(y) + "_" + String.valueOf(x) ;
            File cellfile = new File(cellfilepath) ;
            if( cellfile.exists() == true )
            {
                InputStream instream = new FileInputStream(cellfilepath) ;
                long filesize = cellfile.length();
                byte[] allbytes = new byte[(int)filesize] ;
                instream.read(allbytes) ;
                return allbytes ;
            }else{
                errorMessage = "not find the cell in step1." ;
                return null ;
            }
        }catch (Exception ex)
        {
            errorMessage = "Error : filesystem get cell data exception :" + ex.getMessage() ;
            return null;
        }
    }


    //copy band data into new container
    private void copyBytes2Bytes(byte[] source,
                                 int sourceBandIndex,
                                 int bandbytelen,
                                 byte[] target,
                                 int targetbandindex )
    {
        System.arraycopy(source, sourceBandIndex*bandbytelen ,
                target , targetbandindex * bandbytelen ,
                bandbytelen);
    }

    //get filtered cell data from hbase
    private ArrayList<DatetimeCellData> getFilteredDatetimeCellData(
            String tableName,
            String fami,
            int pidlen,int hpid,long startdt,long stopdt,
            int filterMon,
            int filterDay,
            int filterHour,
            int filterMinu,
            int filterSec,
            int yxlen,
            int z,
            int y,
            int x) throws IOException {
        final int MaxDatetimeRecords = 30 ;

        ArrayList<DatetimeCellData> dtdatalist = new ArrayList<DatetimeCellData>() ;

        byte[] rowkey1 = WHBaseUtil.GenerateRowkey(pidlen,hpid,yxlen,z,y,x) ;
        FilterList filterList = new FilterList() ;//default must pass all.
        ColumnRangeFilter rangeFilter = new ColumnRangeFilter( Bytes.toBytes(startdt),
                true ,
                Bytes.toBytes(stopdt) ,
                true ) ;
        filterList.addFilter(rangeFilter);
        KeyOnlyFilter keyfilter = new KeyOnlyFilter() ;
        filterList.addFilter(keyfilter);
        Get get = new Get(rowkey1);
        get.readVersions(1) ;
        get.addFamily(fami.getBytes()) ;
        get.setFilter(filterList) ;

        Connection conn = getHBaseConnection();
        Table table = conn.getTable( TableName.valueOf(tableName) ) ;
        Result result = table.get(get);

        if(result != null )
        {
            int findcount = 0;
            while( result.advance() )
            {
                ++findcount;
                if( dtdatalist.size()>= MaxDatetimeRecords ) break ;
                Cell cell1 = result.current() ;
                ByteBuffer bbuff = ByteBuffer.allocate(8) ;
                byte[] qualifier = CellUtil.cloneQualifier(cell1);
                bbuff.put(qualifier) ;
                long datetime1 = bbuff.getLong(0) ;
                if( isPassAllDatetimeFilter(datetime1,filterMon,filterDay,filterHour,
                        filterMinu,filterSec) )
                {
                    Get newget = new Get(rowkey1) ;
                    newget.addColumn(fami.getBytes() ,qualifier) ;
                    newget.readVersions(1);
                    Result newresult = table.get(newget) ;
                    if( newresult.value() != null )
                    {
                        DatetimeCellData dtdata = new DatetimeCellData();
                        dtdata.dt = datetime1;
                        dtdata.data = CellUtil.cloneValue(newresult.listCells().get(0)) ;
                        dtdatalist.add(dtdata) ;
                    }
                }

            }
            System.out.println("getFilteredDatetimeCellData max records:"+MaxDatetimeRecords) ;
            System.out.println("getFilteredDatetimeCellData findcount:" + findcount + " filtered:"+dtdatalist.size() ) ;
        }
        //sort
        if( dtdatalist.size() > 0)
        {
            dtdatalist.sort(new Comparator<DatetimeCellData>() {
                @Override
                public int compare(DatetimeCellData o1, DatetimeCellData o2) {
                    if( o1.dt < o2.dt ) {
                        return -1;
                    }else if( o1.dt > o2.dt ){
                        return 1;
                    }else{
                        return 0;
                    }
                }
            });
        }
        return dtdatalist;
    }

    //通过过滤器和开始和结束日期，生成datetime数组，用在本地瓦片数据筛选中
    ArrayList<Long> generateDatetimeArray(long startdt,long stopdt,
                                          int filtermon,//-1,1-12
                                          int filterday,//-1,1-31
                                          int filterhour,//-1,0-23
                                          int filterminu,//-1,0-60
                                          int filtersec )//-1,0-60  这五个过滤器只能有一个和0个-1，其他过滤器必须是确定的数字
    {
        int numNeg1 = 0 ;
        if( filtermon==-1 ) ++numNeg1;
        if( filterday==-1 ) ++numNeg1;
        if( filterhour==-1 ) ++ numNeg1;
        if( filterminu==-1 ) ++ numNeg1;
        if( filtersec==-1 ) ++ numNeg1;

        if( numNeg1>1 ){
            return null ;
        }

        long year0 = startdt / 10000000000L ;
        long year1 = stopdt / 10000000000L ;

        ArrayList<Long> yearlist = new ArrayList<>();
        for(long iyear = year0; iyear < year1+1; ++ iyear )
        {
            yearlist.add(iyear) ;
        }

        ArrayList<Long> monlist = new ArrayList<>() ;
        if( filtermon==-1 ){
            for(long imon=1;imon<13;++imon) monlist.add(imon) ;
        }else{
            monlist.add((long)filtermon) ;
        }

        ArrayList<Long> daylist =new ArrayList<>() ;
        if( filterday == -1 ){
            for(long iday = 1; iday < 32 ;++ iday ) daylist.add(iday) ;
        }else{
            daylist.add((long)filterday) ;
        }

        ArrayList<Long> hourlist = new ArrayList<>() ;
        if( filterhour==-1 ){
            for(long ih = 0 ; ih < 24;++ih ) hourlist.add(ih) ;
        }else{
            hourlist.add((long)filterhour) ;
        }

        ArrayList<Long> minulist = new ArrayList<>() ;
        if( filterminu==-1 ){
            for(long im = 0 ; im < 60 ;++im ) minulist.add(im) ;
        }else{
            minulist.add((long)filterminu) ;
        }

        ArrayList<Long> seclist = new ArrayList<>() ;
        if( filtersec==-1 ){
            for(long is = 0 ; is < 60;++is ) seclist.add(is) ;
        }else{
            seclist.add((long)filtersec) ;
        }

        ArrayList<Long> res =new ArrayList<Long>() ;
        for(long year : yearlist)
        {
            for(long mon: monlist)
            {
                for(long day: daylist)
                {
                    for(long hour:hourlist)
                    {
                        for(long minu:minulist)
                        {
                            for(long sec:seclist)
                            {
                                long dt1 = (year*10000+mon*100+day)*1000000
                                        + hour*10000+minu*100+sec ;
                                if( dt1>=startdt && dt1 <=stopdt ){
                                    res.add(dt1) ;
                                }
                            }
                        }
                    }
                }
            }
        }

        return res ;
    }

    //get filtered cell data from filesystem
    private ArrayList<DatetimeCellData> getFilteredDatetimeCellDataFromFileSystem(
            String tableName,
            int hpid,long startdt,long stopdt,
            int filterMon,
            int filterDay,
            int filterHour,
            int filterMinu,
            int filterSec,
            int z,
            int y,
            int x) throws IOException {
        final int MaxDatetimeRecords = 30 ;
        ArrayList<DatetimeCellData> dtdatalist = new ArrayList<DatetimeCellData>() ;

        ArrayList<Long> dtarr = generateDatetimeArray(startdt,stopdt,filterMon,filterDay,filterHour,filterMinu,filterSec) ;
        if( dtarr==null ){
            System.out.println("getFilteredDatetimeCellDataFromFileSystem generateDatetimeArray failed.") ;
            return null ;
        }

        for(long dt1 : dtarr)
        {
            byte[] celldata = filesystemGetCellData(tableName , dt1 , hpid,z,y,x) ;
            if( celldata!= null )
            {
                DatetimeCellData dtdata = new DatetimeCellData();
                dtdata.dt = dt1;
                dtdata.data = celldata ;
                dtdatalist.add(dtdata) ;
            }
        }
        System.out.println("getFilteredDatetimeCellDataFromFileSystem max records:"+MaxDatetimeRecords) ;
        System.out.println("getFilteredDatetimeCellDataFromFileSystem findcount:" + dtarr.size() + " filtered:"+dtdatalist.size() ) ;
        return dtdatalist;
    }

    private boolean isPassAllDatetimeFilter(long dtx, int monf, int dayf, int hourf, int minuf, int secf)
    {
        final int PEIgnore = -1 ;
        if( monf!=PEIgnore )
        {
            int monx = (int) ((dtx / 100000000L)%100) ;
            if( monx != monf )
            {
                return false ;
            }
        }
        if( dayf!=PEIgnore )
        {
            int dayx = (int) ((dtx / 1000000L)%100) ;
            if( dayx != dayf )
            {
                return false ;
            }
        }
        if( hourf!=PEIgnore )
        {
            int hx = (int) ((dtx / 10000L)%100) ;
            if( hx != hourf )
            {
                return false ;
            }
        }
        if( minuf!=PEIgnore )
        {
            int minux = (int) ((dtx / 100L)%100) ;
            if( minux != minuf )
            {
                return false ;
            }
        }
        if( secf !=PEIgnore )
        {
            int secx = (int) (dtx % 100L) ;
            if( secx != secf )
            {
                return false ;
            }
        }
        return true ;
    }



    //get tile data array
    //deprecated 2022-4-3
    public TileData getTileDataArray(long fromdtInclude,long todtInclude ,
                                     String dsName, int[] bandindices ,
                                     int z,int y,int x,
                                     int filterMon,int filterDay,int filterHour,
                                     int filterMinu,int filterSec
                                     )
    {
        System.out.println("in java getTileDataArray zyx: "+z + "," + y+","+x ) ;

        try {
            JRDBHelperForWebservice rdb = new JRDBHelperForWebservice() ;
            JProduct pdt = rdb.rdbGetProductInfoByName(dsName) ;
            if( pdt==null ){
                errorMessage="Error : not find info of "+dsName ;
                return null ;
            }
            TileData resultTileData = null ;
            int last_hPid = -1;

            //一个波段瓦片数据的byte数量
            int bandbytesize = pdt.tileWid*pdt.tileHei*pdt.getDataByteLen();
            ArrayList<DatetimeCellData> lastCellDataList = null ;
            for(int iband = 0 ; iband < bandindices.length; ++ iband )
            {
                //bandindex mean bandindex in the product.
                int bandindex = bandindices[iband] ;
                if( bandindex>=0 && bandindex < pdt.bandList.size() )
                {
                    int curr_hPid = pdt.bandList.get(bandindex).hPid ;//hPid of this band data.
                    int curr_bsqIndex = pdt.bandList.get(bandindex).bsqIndex;//bsqIndex of this band data.
                    //if this band in side last tile, then read from last tile data buffer.
                    if( curr_hPid == last_hPid && lastCellDataList!=null && resultTileData!=null ){
                        for(int ids=0;ids<resultTileData.numds;++ids )
                        {
                            copyBytes2Bytes(lastCellDataList.get(ids).data,
                                    curr_bsqIndex , bandbytesize , resultTileData.tiledataArray[ids], iband );
                        }
                    }else{
                        if( pdt.source.equals("hbase") )
                        {
                            lastCellDataList = this.getFilteredDatetimeCellData(
                                    pdt.hbaseTable.hTableName,
                                    pdt.hbaseTable.hFamily,
                                    pdt.hbaseTable.hPidByteNum,
                                    curr_hPid,
                                    fromdtInclude,
                                    todtInclude,
                                    filterMon,
                                    filterDay,
                                    filterHour,
                                    filterMinu,
                                    filterSec,
                                    pdt.hbaseTable.hYXByteNum,
                                    z,y,x) ;

                        }else if( pdt.source.equals("file") )
                        {
                            lastCellDataList = this.getFilteredDatetimeCellDataFromFileSystem(
                                    pdt.hbaseTable.hTableName,
                                    curr_hPid,
                                    fromdtInclude,
                                    todtInclude,
                                    filterMon,
                                    filterDay,
                                    filterHour,
                                    filterMinu,
                                    filterSec,z,y,x) ;

                        }else{
                            errorMessage = "Error : pdt.source is not supported for '" + pdt.source+"'. " ;
                            return null ;
                        }


                        if( lastCellDataList==null ){
                            errorMessage = "get emtpty range of cell data.";
                            return null ;
                        }
                        if( resultTileData==null ){
                            resultTileData = new TileData(lastCellDataList.size()) ;
                            resultTileData.height = pdt.tileHei;
                            resultTileData.width = pdt.tileWid;
                            resultTileData.nband = bandindices.length;
                            resultTileData.dataType = pdt.dataType;
                            resultTileData.x = x;
                            resultTileData.y = y;
                            resultTileData.z = z ;
                            int pixelLen = pdt.getDataByteLen() ;
                            for(int ids=0;ids<resultTileData.numds;++ids )
                            {
                                resultTileData.datetimeArray[ids] = lastCellDataList.get(ids).dt;
                                resultTileData.tiledataArray[ids] = new byte[
                                                pdt.tileWid
                                                *pdt.tileHei
                                                *bandindices.length
                                                *pixelLen] ;
                            }
                        }
                        last_hPid = curr_hPid ;
                        for(int ids=0;ids<resultTileData.numds;++ids )
                        {
                            if(resultTileData.datetimeArray[ids] != lastCellDataList.get(ids).dt)
                            {
                                System.out.println("Warning : resultTileData.datetimeArray[ids] not equal lastCellDataList.get(ids).dt,");
                                System.out.println("    ids="+ids);
                                System.out.println("    resultTileData.datetimeArray[ids]="+resultTileData.datetimeArray[ids]);
                                System.out.println("    lastCellDataList.get(ids).dt="+lastCellDataList.get(ids).dt);
                            }
                            copyBytes2Bytes(lastCellDataList.get(ids).data,
                                    curr_bsqIndex , bandbytesize , resultTileData.tiledataArray[ids], iband );
                        }
                    }
                }else{
                    //no this bandindex
                    errorMessage = "no bandindex of " + bandindex ;
                    return null ;
                }
            }
            return resultTileData;
        } catch (Exception e) {
            // TODO Auto-generated catch block
            errorMessage = "Error : getTileData exception of " + e.getMessage() ;
            return null ;
        }

    }

    //old api , keep for old api use.
    public ColorRamp getColorRamp( String crid ){
        System.out.println("in java getColorRamp colorid: "+crid) ;
        ColorRamp cr = new ColorRamp() ;
        cr.Nodata = -1 ;
        cr.NodataColor = new byte[4] ;
        cr.NodataColor[0] = cr.NodataColor[1]=cr.NodataColor[2]=cr.NodataColor[3]=0 ;
        cr.ivalues = new int[10] ;
        cr.r = new byte[10] ;
        cr.g = new byte[10] ;
        cr.b = new byte[10] ;
        cr.a = new byte[10] ;
        cr.labels = new String[10] ;
        for(int ic = 0 ; ic<10 ; ++ ic )
        {
            cr.ivalues[ic] = ic*25 ;
            cr.r[ic] = 0 ;
            cr.g[ic] = (byte)(ic*25) ;
            cr.b[ic] = 0 ;
            cr.a[ic] = (byte)255 ;
            cr.labels[ic] = "ll" ;
        }
        return cr ;
    }

    //JPeStyle used to substitute ColorRamp
    public JPeStyle getStyle(String styleid ){
        System.out.println("in java getStyle styleid: "+styleid) ;

        JPeStyle style = new JPeStyle() ;
        style.bands = new int[1] ;
        style.bands[0] = 0 ;

        style.nodatacolor = new JPeColorElement() ;

        style.type = "linear" ;

        style.colors = new JPeColorElement[2] ;

        style.colors[0] = new JPeColorElement() ;
        style.colors[0].r = 0 ;
        style.colors[0].g = 0;
        style.colors[0].b = 0;
        style.colors[0].a = (byte)255 ;
        style.colors[0].val = 1 ;

        style.colors[1] = new JPeColorElement() ;
        style.colors[1].r = (byte)255 ;
        style.colors[1].g = (byte)255 ;
        style.colors[1].b = (byte)255 ;
        style.colors[1].a = (byte)255 ;
        style.colors[1].val = 255 ;

        style.vranges = new JPeVRangeElement[2] ;
        style.vranges[0] = new JPeVRangeElement();
        style.vranges[0].minval = 1 ;
        style.vranges[0].maxval = 128 ;

        style.vranges[1] = new JPeVRangeElement();
        style.vranges[1].minval = 127 ;
        style.vranges[1].maxval = 255 ;
        System.out.println("in java , style ok") ;
        return style ;
    }


    //2022-3-5 二进制数据写入HBase
    public boolean writeBinaryDataIntoHBase(byte[] bytesData ,
                                            String hbaseTableName,
                                            String fami,
                                            byte[] qualifier,
                                            byte[] rowkey )
    {
        try {
            Connection conn = getHBaseConnection();
            Table table = conn.getTable(TableName.valueOf(hbaseTableName));
            Put put = new Put(  rowkey ) ;
            put.addColumn( fami.getBytes() ,  qualifier , bytesData ) ;
            table.put(put);
            table.close();
            return true ;
        }catch (Exception ex)
        {
            errorMessage = "Error : writeBinaryDataIntoHBase write cell data exception :" + ex.getMessage() ;
            return false;
        }
    }

    //2022-3-5 二进制数据读取HBase
    public byte[] readBinaryDataIntoHBase(  String hbaseTableName,
                                            String fami,
                                            byte[] qualifier,
                                            byte[] rowkey )
    {
        try {
            Connection conn = getHBaseConnection();
            Table table = conn.getTable(TableName.valueOf(hbaseTableName));
            Get get1 = new Get(rowkey);
            get1.addColumn( fami.getBytes() ,  qualifier ) ;
            Result getResult = table.get(get1) ;
            table.close();
            return getResult.getValue( fami.getBytes() , qualifier) ;
        }catch (Exception ex)
        {
            errorMessage = "Error : readBinaryDataIntoHBase get cell data exception :" + ex.getMessage() ;
            return null;
        }
    }



    //获取ROI的HSEG.TLV二进制数据 成功返回完整二进制数组，反之返回空指针
    public byte[] getRoiHsegTlv( int isUserRoi, //0-sys_roi, 1-user_roi
                                 int rid        //primarykey in roi mysql table
    )
    {
        System.out.println("in java getRoiHsegTlv "+isUserRoi+","+rid ) ;
        String hbaseTableName = "sys_roi" ;
        if( isUserRoi==1 ){
            //user_roi
            hbaseTableName = "user_roi" ;
        }
        int qualifier = 1 ;//2022-3-19
        HBasePixelEngineHelper hbaseHelper = new HBasePixelEngineHelper() ;
        byte[] tlvdata = hbaseHelper.readBinaryDataIntoHBase(hbaseTableName,"hseg.tlv",Bytes.toBytes(qualifier),Bytes.toBytes(rid)) ;
        if(tlvdata==null){
            System.out.println("Failed, hbaseHelper.readBinaryDataIntoHBase return a null byte array.");
        }
        return tlvdata ;
    }

    //2022-4-3
    public TileData getTileDataCollection( String dsName, long[] dtarr, int z,int y,int x)
    {
        System.out.println("in java getTileDataCollection dtarr.len " + dtarr.length + " z,y,x " + z + "," + y+","+x ) ;

        try {
            JRDBHelperForWebservice rdb = new JRDBHelperForWebservice() ;
            JProduct pdt = rdb.rdbGetProductInfoByName(dsName) ;
            if( pdt==null ){
                errorMessage="Error : not find info of "+dsName ;
                System.out.println("debug " + errorMessage);//2022-6-6
                return null ;
            }
            if( pdt.bandList.size() == 0 ){
                errorMessage = "Error : empty bandlist." ;
                System.out.println("debug " + errorMessage);//2022-6-6
                return null ;
            }
            int[] bandArr = new int[pdt.bandList.size()];
            for(int ii=0;ii<bandArr.length;++ii) bandArr[ii] = ii ;
            List<Long> dtlist = new ArrayList<>() ;
            List<TileData> tempTileDataList = new ArrayList<>() ;
            for(int idt = 0 ; idt < dtarr.length; ++ idt )
            {
                long dt1 = dtarr[idt] ;
                TileData tempTileData1 = this.getTileData(dt1,dsName,bandArr,z,y,x) ;
                if( tempTileData1!=null )
                {
                    System.out.println("debug has dt:"+dt1);//2022-6-6
                    dtlist.add(dt1) ;
                    tempTileDataList.add(tempTileData1) ;
                }else{
                    System.out.println("debug no dt:"+dt1);//2022-6-6
                }
            }

            if( dtlist.size()==0 ){
                errorMessage = "Error : no valid datetime data." ;
                System.out.println("debug " + errorMessage);//2022-6-6
                return null ;
            }

            TileData resultTileData = new TileData( dtlist.size() ) ;
            int index = 0;
            for( TileData td1 : tempTileDataList)
            {
                if( index==0 ){
                    resultTileData.x = x;
                    resultTileData.y = y;
                    resultTileData.z = z;
                    resultTileData.dataType = td1.dataType ;
                    resultTileData.nband =  td1.nband ;
                    resultTileData.width = td1.width;
                    resultTileData.height = td1.height ;
                }
                resultTileData.datetimeArray[index] = dtlist.get(index) ;
                resultTileData.tiledataArray[index] = td1.tiledataArray[0] ;
                ++index ;
            }
            return resultTileData ;
        } catch (Exception e) {
            // TODO Auto-generated catch block
            errorMessage = "Error : getTileDataCollection exception of " + e.getMessage() ;
            System.out.println("debug " + errorMessage);//2022-6-6
            return null ;
        }
    }

    //2022-4-3
    public JDtCollection[] buildDatetimeCollections(
            String dsName,
            long whole_start ,
            int whole_start_inc , //0 or 1
            long whole_stop ,
            int whole_stop_inc ,
            String repeat_type , // '' 'm' 'y'
            long repeat_start,
            int repeat_start_inc,
            long repeat_stop,
            int repeat_stop_inc,
            int repeat_stop_nextyear //0 or 1
    ){
        System.out.println("in java buildDatetimeCollections " ) ;

        try {
            JRDBHelperForWebservice rdb = new JRDBHelperForWebservice();
            JDtCollectionBuilder dtcBuilder = new JDtCollectionBuilder() ;
            dtcBuilder.wholePeriod.startDt = whole_start ;
            dtcBuilder.wholePeriod.startInclusive = whole_start_inc==1 ;
            dtcBuilder.wholePeriod.stopDt = whole_stop ;
            dtcBuilder.wholePeriod.stopInclusive = whole_stop_inc==1 ;
            dtcBuilder.repeatType = repeat_type ;
            dtcBuilder.repeatPeriod.startDt = repeat_start ;
            dtcBuilder.repeatPeriod.startInclusive = repeat_start_inc==1 ;
            dtcBuilder.repeatPeriod.stopDt = repeat_stop ;
            dtcBuilder.repeatPeriod.stopInclusive = repeat_stop_inc==1 ;
            dtcBuilder.repeatPeriod.stopInNextYear = repeat_stop_nextyear ;

            JDtCollection[] dtcArray = rdb.buildDtCollection(dsName,dtcBuilder) ;
            return dtcArray ;
        }catch (Exception ex)
        {
            errorMessage = "Error : buildDatetimeCollections exception " + ex.getMessage();
            return null ;
        }

    }
}

