package com.pixelengine.tools;
//2022-3-22

public class JScriptTools {

    /// sduiJsonText 应该是 null '{}' '' 'null' 'undefined' '{"s1":1,"s2":10.0}'
    public static  String assembleScriptWithSDUI(String scriptOrig,String sduiJsonText)
    {
        String newScript = scriptOrig;
        if( newScript.contains("sdui={") == true ){
            if( sduiJsonText!=null
                    && sduiJsonText.compareTo("{}") != 0
                    && sduiJsonText.compareTo("") !=0
                    && sduiJsonText.compareTo("null") !=0
                    && sduiJsonText.compareTo("undefined") !=0
            ){
                String sduiJsonStr2 = "\nsdui=" + sduiJsonText + ";\n" ;
                newScript = newScript.replace("function main(" , sduiJsonStr2 + "function main(") ;
            }
        }
        return newScript ;
    }
}
