package com.pixelengine.DataModel;
//2022-9-4 separate from WTilecomputingSerialProcessor

import scala.Serializable;

public class SparkTaskParams implements Serializable {
    public int z,y,x ;
    public long[] dtCollection;
}
