package com.pixelengine.DataModel;
//2022-01-01


import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;

public class WHBaseUtil {

    public static class TileId implements Serializable {
        public TileId(int pidbytelen , int xybytelen ){
            pidlen=pidbytelen;
            xylen=xybytelen;
        }
        public Integer bucket , pid , z , y , x , pidlen, xylen;
    }

    public static byte ComputeBucketId( int z , int y , int x )
    {
        int numberRegion = 10 ;
        return (byte)((x+y)%numberRegion) ;
    }

    /// pdtByteLen = 1,4
    /// pdtid = byte/int
    /// xyByteLen = 2,4
    /// x,y = short/int
    public static byte[] GenerateRowkey( int pdtByteLen,
                                         int pdtid ,
                                         int xyByteLen ,
                                         int z ,
                                         int y ,
                                         int x
    )   {
        try{
            byte bucketid = ComputeBucketId( z , y , x ) ;
            ByteArrayOutputStream bos = new ByteArrayOutputStream() ;
            DataOutputStream dos = new DataOutputStream(bos);
            dos.writeByte(bucketid);
            if( pdtByteLen == 1){
                dos.writeByte( (byte)pdtid) ;
            }else{
                //int32
                dos.writeInt(pdtid); ;
            }
            dos.writeByte((byte)z);
            if( xyByteLen == 2){
                dos.writeShort( (short)y);
                dos.writeShort( (short)x);
            }else{
                dos.writeInt(  y);
                dos.writeInt(  x);
            }
            dos.flush();
            return bos.toByteArray();
        }catch(Exception e)
        {
            return null ;
        }

    }

    /// 不适用自动计算的bucket编号，采用制定bucket编号
    /// pdtByteLen = 1,4
    /// pdtid = byte/int
    /// xyByteLen = 2,4
    /// x,y = short/int
    public static byte[] GenerateRowkey2(
        int ibuck,
        int pdtByteLen,
        int pdtid ,
        int xyByteLen ,
        int z ,
        int y ,
        int x
    )   {
        try{
            byte bucketid = (byte)ibuck ;
            ByteArrayOutputStream bos = new ByteArrayOutputStream() ;
            DataOutputStream dos = new DataOutputStream(bos);
            dos.writeByte(bucketid);
            if( pdtByteLen == 1){
                dos.writeByte( (byte)pdtid) ;
            }else{
                //int32
                dos.writeInt(pdtid); ;
            }
            dos.writeByte((byte)z);
            if( xyByteLen == 2){
                dos.writeShort( (short)y);
                dos.writeShort( (short)x);
            }else{
                dos.writeInt(  y);
                dos.writeInt(  x);
            }
            dos.flush();
            return bos.toByteArray();
        }catch(Exception e)
        {
            return null ;
        }

    }

    public static TileId ConvertTileIdByRowkey(byte[] rowkey, int pdtByteLen, int xyByteLen )
            throws IOException
    {
        //bucket pid z y x
        int bytelen = 2 ;
        if( pdtByteLen==1 ) {
            bytelen+=1;
        }
        else {
            bytelen +=4 ;
        }
        if( xyByteLen==2 ){
            bytelen += 4 ;
        }else{
            bytelen += 8 ;
        }
        if( rowkey.length != bytelen ) {
            System.out.println("Error : ConvertTileIdByRowkey rowkey.len("+rowkey.length+") is not equal:"+bytelen) ;
            return null ;
        }
        TileId tid  = new TileId(pdtByteLen,xyByteLen) ;
        tid.bucket = (int)rowkey[0] ;
        if( pdtByteLen==1 ){
            tid.pid = (int)rowkey[1];
            tid.z = (int)rowkey[2] ;
            if( xyByteLen==2 ){
                tid.y = (int)byteArr2short(rowkey , 3) ;
                tid.x = (int)byteArr2short( rowkey , 5 ) ;
            }else{
                tid.y = (int)byteArr2int(rowkey , 3) ;
                tid.x = (int)byteArr2int( rowkey , 7 ) ;
            }
        }else{
            tid.pid = byteArr2int( rowkey,1);
            tid.z = (int)rowkey[5] ;
            if( xyByteLen==2 ){
                tid.y = (int)byteArr2short(rowkey , 6) ;
                tid.x = (int)byteArr2short( rowkey , 8 ) ;
            }else{
                tid.y = (int)byteArr2int(rowkey , 6) ;
                tid.x = (int)byteArr2int( rowkey , 10 ) ;
            }
        }
        return tid;
    }
    public static short byteArr2short( byte[] barr , int offset )
    {
        ByteBuffer bb = ByteBuffer.allocate(2) ;
        bb.put(0 , barr[offset+0]) ;
        bb.put(1 , barr[offset+1]) ;
        return bb.getShort(0) ;
    }
    public static int byteArr2int( byte[] barr , int offset )
    {
        ByteBuffer bb = ByteBuffer.allocate(4) ;
        bb.put(0 , barr[offset+0]) ;
        bb.put(1 , barr[offset+1]) ;
        bb.put(2 , barr[offset+2]) ;
        bb.put(3 , barr[offset+3]) ;
        return bb.getInt(0) ;
    }


}
