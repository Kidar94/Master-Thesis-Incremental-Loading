package SqlGraphLoading;

import Configuration.SqlGraphConfiguration;
import Input.Input;
import Parsing.Parsing;
import Preprocessing.Preprocessing;
import Relabeling.Relabeling;
import com.opencsv.CSVReader;

import java.io.*;
import java.lang.reflect.Array;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;

public class SqlGraphLoading {
    private SqlGraphConfiguration sqlGraphConfiguration= new SqlGraphConfiguration();

    public SqlGraphLoading()   { }

    public void importVertices() throws IOException {
        String in=sqlGraphConfiguration.getDatasetDirectory();
        CSVFilesToList csvFilesToList= new CSVFilesToList();
        HashMap<String, ArrayList> csvFileNames= csvFilesToList.importDatasetFileNames();
        ArrayList<String> vertexArray=csvFileNames.get("VertexFile");
        ArrayList<String> attributeArray=csvFileNames.get("AttributeFile");
        ArrayList<String> relationshipArray=csvFileNames.get("RelationshipFile");
        ArrayList<String> filesToMerge=new ArrayList<>();
        String vertexesOutput=sqlGraphConfiguration.getVertexesOutput();

        for(String vertex:vertexArray)  {
            int indx=vertex.indexOf("_");
            String vertexType=vertex.substring(0,indx);
            String filePath=in+"/"+vertex+".csv";
            Preprocessing preprocessing= new Preprocessing(filePath);
            String outputPreprocessingVertex=preprocessing.externalSorting();
            Parsing parsing= new Parsing();
            String outputParsingVertex=parsing.VAParsing(outputPreprocessingVertex);
            String attributesadded="";
            for(String attribute:attributeArray)    {
                if (attribute.contains(vertexType)) {
                    String filePath2=in+"/"+attribute+".csv";
                    Preprocessing preprocessingAttribute= new Preprocessing(filePath2);
                    String outputPreprocessingAttribute=preprocessingAttribute.externalSorting();
                    attributesadded=parsing.addingAttributes(outputParsingVertex, outputPreprocessingAttribute);
                    outputParsingVertex=attributesadded;
                }
            }
            filesToMerge.add(outputParsingVertex);
        }
        Relabeling relabeling= new Relabeling();
        String mergeVertexes=relabeling.attributingNewIds(filesToMerge, vertexesOutput);
        int debug;
        debug=1;
    }
}
