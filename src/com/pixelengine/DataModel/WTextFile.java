package com.pixelengine.DataModel;
//2022-01-01

import java.nio.file.Files;
import java.nio.file.Paths;

public class WTextFile {

    public static String readFileAsString(String filepath)
    {
        try{
            String data  = new String(Files.readAllBytes(Paths.get(filepath)));
            return data ;
        }catch (Exception ex ){
            System.out.println("WTextFile.readFileAsString exception:"+ex.getMessage());
            return null ;
        }
    }
}
