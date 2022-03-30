package com.pixelengine;
////////////////////////////////////////////////////////
//
//
//这个类的位置不能变，要在com/pixelengine/ 下，否则c++会找不到这个类
//update 2022-2-13 1020
//
/////////////////////////////////////////////////////////
public class TileData {
    public TileData(int num){
        datetimeArray = new long[num] ;
        tiledataArray = new byte[num][] ;
        this.numds = num ;
    }
    public long[] datetimeArray = null ;
    public byte[][] tiledataArray = null ;//返回的数据应该是解压缩过的
    public int width = 0 ;
    public int height= 0 ;
    public int nband = 0 ;
    public int numds = 0 ;
    public int dataType = 0 ;//1-byte 2-u16 3-i16 4-u32 5-i32 6-f32 7-f64
    public int x=0;
    public int y=0;
    public int z=0;
    //public String computeOnceData = "" ;
}

