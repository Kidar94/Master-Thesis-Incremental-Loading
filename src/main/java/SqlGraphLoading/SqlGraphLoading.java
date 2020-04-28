package SqlGraphLoading;

import Configuration.SqlGraphConfiguration;
import Input.Input;
import Parsing.Parsing;
import Preprocessing.*;
import Relabeling.*;
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
        ArrayList<String> relationshipDivided= new ArrayList<>();
        ArrayList<String> relationshipFiles= new ArrayList<>();
        HashMap<String,ArrayList> relationshipPerVertexType= new HashMap<>();
        ArrayList<String> relationshipNamesList= new ArrayList<>();

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

        for(String relationship:relationshipArray)  {
            int indx=relationship.indexOf("0");
            relationshipNamesList.add(relationship.substring(0,indx));
            String filePath=in+"/"+relationship+".csv";
            Preprocessing relationshipSorting= new Preprocessing(filePath);
            String outputPreprocessingRelationship=relationshipSorting.externalSorting();
            RelationshipPreprocessing relationshipPreprocessing= new RelationshipPreprocessing();
            relationshipDivided=relationshipPreprocessing.relationshipDivide(outputPreprocessingRelationship);
            relationshipFiles.add(relationshipDivided.get(0));
            relationshipFiles.add(relationshipDivided.get(1));
        }

        for(String vertex:vertexArray)  {
            String in1=in+"/";
            int indx=vertex.indexOf("_0_0");
            vertex=vertex.substring(0,indx);
            ArrayList<String> relationshipPerType= new ArrayList<>();
            for(String relationship:relationshipFiles)  {
                String relationship1=relationship;
                File f= new File(relationship);
                relationship=f.getName();
                if(relationship.contains("source"))    {
                    int indx1=relationship.indexOf("_");
                    relationship=relationship.substring(0,indx1);
                }   else    {
                    int indx1=relationship.indexOf("_");
                    relationship=relationship.substring(indx1+1);
                    indx1=relationship.indexOf("_");
                    relationship=relationship.substring(indx1+1);
                    indx1=relationship.indexOf("_");
                    relationship=relationship.substring(0,indx1);
                }
                if(vertex.equals(relationship)) {
                    relationshipPerType.add(relationship1);
                }
            }
            relationshipPerVertexType.put(vertex,relationshipPerType);
        }

        Relabeling relabeling= new Relabeling();
        ArrayList<ArrayList> relabelingOutput=relabeling.attributingNewIds(filesToMerge, vertexesOutput, relationshipPerVertexType, relationshipNamesList);
        ArrayList<String>   vertexesTypeIds=relabelingOutput.get(0);
        ArrayList<String>   relationshipList=relabelingOutput.get(1);

        String outgoingMappingCsvFile=sqlGraphConfiguration.getOutgoingLabelsMapping();
        HashMap<String, Integer>    mappingLabelsColumns=new HashMap<>();
        BufferedReader  outgoingMappingReader=null;

        try {
            outgoingMappingReader= new BufferedReader(new FileReader(outgoingMappingCsvFile));
            String line="";
            String key="";
            String valueString="";
            int indx;
            while((line=outgoingMappingReader.readLine())!=null)   {
                indx=line.indexOf("|");
                key=line.substring(0,indx);
                valueString=line.substring(indx+1);
                mappingLabelsColumns.put(key, Integer.valueOf(valueString));
            }
        }   catch(IOException e)    {
            e.printStackTrace();
        }   finally {
            if(outgoingMappingReader!=null) {
                outgoingMappingReader.close();
            }
        }
        OALoading oaLoading=new OALoading();
        oaLoading.OALoadingTable(vertexesOutput,relationshipList, mappingLabelsColumns, vertexesTypeIds);

        int debug;
        debug=1;
    }
}
