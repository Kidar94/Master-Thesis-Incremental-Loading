package Configuration;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class SqlGraphConfiguration {

    private static final String PATH_TO_CONFIG_FILE = "src/main/resources/config.properties";
    private static String DATASETDIRECTORY;
    private static String DATASETFILES;
    private static String VERTEXESOUTPUT;
    private static String OUTGOINGLABELSMAPPING;
    private static String INCOMINGLABELSMAPPING;
    private static String TMPDIRECTORY;
    private static String ARRAYLENGTH;
    public SqlGraphConfiguration()  {
        Properties configProperties= new Properties();
        FileInputStream in=null;
        try {
            in=new FileInputStream(PATH_TO_CONFIG_FILE);
            configProperties.load(in);
            DATASETDIRECTORY=String.valueOf(configProperties.getProperty("Dataset"));
            DATASETFILES=String.valueOf(configProperties.getProperty("DatasetFiles"));
            VERTEXESOUTPUT=String.valueOf(configProperties.getProperty("VertexesOutput"));
            OUTGOINGLABELSMAPPING=String.valueOf(configProperties.getProperty("OutgoingLabelsMapping"));
            INCOMINGLABELSMAPPING=String.valueOf(configProperties.getProperty("IncomingLabelsMapping"));
            TMPDIRECTORY=String.valueOf(configProperties.get("tmpFiles"));
            ARRAYLENGTH=String.valueOf(configProperties.get("arrayLength"));

        }   catch (IOException e)   {
            e.printStackTrace();
        }
    }

    public static String getDatasetDirectory()  {
        return DATASETDIRECTORY;
    }

    public static String getDatasetFiles()  { return DATASETFILES;}

    public static String getVertexesOutput()    { return VERTEXESOUTPUT;}

    public static String getOutgoingLabelsMapping() {return OUTGOINGLABELSMAPPING;}

    public static String getIncomingLabelsMapping() {return INCOMINGLABELSMAPPING;}

    public static String getTemporaryFilesDirectory()   {return TMPDIRECTORY;}
    public static String getArrayLength()   {return ARRAYLENGTH;}
}
