package com.pixelengine.DataModel;
// 2022-3-23


import java.io.Serializable;

public class TileXYZ implements Serializable {
    public Integer z,y,x ;
    public TileXYZ(int z1,int y1,int x1){
        z = z1 ;y = y1 ;x = x1 ;
    }

}
