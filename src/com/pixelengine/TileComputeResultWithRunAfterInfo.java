package com.pixelengine;
////////////////////////////////////////////////////////
//
//
//这个类的位置不能变，要在com/pixelengine/ 下，否则c++会找不到这个类
//update 2022-3-22 1725
//
/////////////////////////////////////////////////////////
import com.pixelengine.DataModel.JDsnameDts;

import java.nio.ByteBuffer;
import java.util.ArrayList;

public class TileComputeResultWithRunAfterInfo {
    public int status ; // 0 is ok
    public String log ; // error or log messages
    public int outType; //0-dataset , 1-png
    public int dataType; // 1-byte, 2-u16 , 3-i16 , 4-u32 , 5-i32 , 6-f32, 7-f64
    public int width,height,nbands;
    public byte[] binaryData ;
    public int z,y,x;
    public String dsnameDtArrStr ;
    public String roi2ArrStr ;

    public int getDataByteLen() {
        switch (dataType){
            case 1:return 1;
            case 2:return 2;
            case 3:return 2;
            case 4:return 4;
            case 5:return 4;
            case 6:return 4;
            case 7:return 8;
            default:return 0;
        }
    }
    public double getValue(int ix,int iy,int ib)
    {
        if( ix>=0 && ix < width && iy>=0 && iy < height && ib>=0 && ib <nbands)
        {
            int dlen = this.getDataByteLen();
            if( dlen==0 ) {
                return 0 ;
            }else{
                int pos0 = (ib * width*height + iy *width + ix)*dlen;
                if( dataType==1 ){
                    ByteBuffer bb = ByteBuffer.allocate(2);
                    bb.put(0,(byte)0) ;
                    bb.put(1,binaryData[pos0]) ;
                    return (double)bb.getShort(0) ;
                }else if( dataType==2 ){
                    ByteBuffer bb = ByteBuffer.allocate(4);
                    bb.put(0,(byte)0);
                    bb.put(1,(byte)0);
                    bb.put(2,binaryData[pos0+1]) ;
                    bb.put(3,binaryData[pos0+0]) ;
                    return (double)bb.getInt(0);
                }else if( dataType==3 ){
                    ByteBuffer bb = ByteBuffer.allocate(2);
                    bb.put(0,binaryData[pos0+1]) ;
                    bb.put(1,binaryData[pos0+0]) ;
                    return (double)bb.getShort(0);
                }else if( dataType==4 )
                {
                    ByteBuffer bb = ByteBuffer.allocate(8);
                    bb.put(0,(byte)0);
                    bb.put(1,(byte)0);
                    bb.put(2,(byte)0);
                    bb.put(3,(byte)0);
                    bb.put(4,binaryData[pos0+3]) ;
                    bb.put(5,binaryData[pos0+2]) ;
                    bb.put(6,binaryData[pos0+1]) ;
                    bb.put(7,binaryData[pos0+0]) ;
                    return (double)bb.getLong(0);
                }else if( dataType==5 )
                {
                    ByteBuffer bb = ByteBuffer.allocate(4);
                    bb.put(0,binaryData[pos0+3]) ;
                    bb.put(1,binaryData[pos0+2]) ;
                    bb.put(2,binaryData[pos0+1]) ;
                    bb.put(3,binaryData[pos0+0]) ;
                    return (double)bb.getInt(0);
                }else if( dataType==6 )
                {
                    ByteBuffer bb = ByteBuffer.allocate(4);
                    bb.put(0,binaryData[pos0+3]) ;//bugfixed
                    bb.put(1,binaryData[pos0+2]) ;
                    bb.put(2,binaryData[pos0+1]) ;
                    bb.put(3,binaryData[pos0+0]) ;
                    return (double)bb.getFloat(0);
                }else if( dataType==7 )
                {
                    ByteBuffer bb = ByteBuffer.allocate(8);
                    bb.put(0,binaryData[pos0+7]) ;
                    bb.put(1,binaryData[pos0+6]) ;
                    bb.put(2,binaryData[pos0+5]) ;
                    bb.put(3,binaryData[pos0+4]) ;
                    bb.put(4,binaryData[pos0+3]) ;
                    bb.put(5,binaryData[pos0+2]) ;
                    bb.put(6,binaryData[pos0+1]) ;
                    bb.put(7,binaryData[pos0+0]) ;
                    return bb.getDouble(0);
                }
                return 0;
            }
        }else{
            return 0;
        }
    }

    public ArrayList<JDsnameDts> getDsnameDtsArray() {
        ArrayList<JDsnameDts> arr = new ArrayList<>();
        String[] dsdtstrarr = dsnameDtArrStr.split(";") ;
        for(int i = 0 ; i<dsdtstrarr.length;++i){
            System.out.println(dsdtstrarr[i]);
            if( dsdtstrarr[i].length()==0 ) continue;
            String[] dsdt = dsdtstrarr[i].split(",") ;
            if( dsdt.length!=2 ){
                System.out.println("bad dsname,dt pair.");
                return null ;
            }
            int tindex = getIndexOfDsname(arr , dsdt[0]) ;
            if( tindex >=0 ){
                arr.get(tindex).dtarr.add( Long.valueOf(dsdt[1])) ;
            }else{
                JDsnameDts jdsdt = new JDsnameDts() ;
                jdsdt.dsname = dsdt[0] ;
                jdsdt.dtarr.add( Long.valueOf(dsdt[1])) ;
                arr.add(jdsdt) ;
            }
        }
        return arr;
    }

    private int getIndexOfDsname(ArrayList<JDsnameDts> arr,String dsname){
        for(int i = 0 ; i<arr.size();++i){
            if( arr.get(i).dsname.compareTo(dsname) == 0 ){
                return i ;
            }
        }
        return -1 ;
    }



    public ArrayList<String> getRoi2Array() {
        ArrayList<String> res = new ArrayList<>() ;
        String[] strarr = roi2ArrStr.split(";") ;
        for(int i = 0 ; i<strarr.length;++i)
        {
            if( strarr[i].length()==0 )continue ;
            res.add(strarr[i]) ;
        }
        return res;
    }
}

