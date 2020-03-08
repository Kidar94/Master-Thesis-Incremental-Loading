package SqlGraphLoading;

import Configuration.SqlGraphConfiguration;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class CSVFilesToList {
    private SqlGraphConfiguration sqlGraphConfiguration= new SqlGraphConfiguration();
    private HashMap<String, ArrayList> csvFileNames= new HashMap<>();

    public HashMap<String, ArrayList> importDatasetFileNames() throws IOException {
        BufferedReader br=null;
        try {
            br= new BufferedReader(new FileReader(sqlGraphConfiguration.getDatasetFiles()));
            String line=br.readLine();
            int indx;
            String key;
            String value;
            ArrayList<String> vertexArray= new ArrayList<>();
            ArrayList<String> attributeArray= new ArrayList<>();
            ArrayList<String> relationshipArray= new ArrayList<>();
            while((line=br.readLine())!= null)  {
                indx=line.indexOf("|");
                value=line.substring(indx+1);
                key=line.substring(0, indx);
                if(value.equals("VertexFile"))  {
                    vertexArray.add(key);
                    csvFileNames.put("VertexFile", vertexArray);
                }
                else if(value.equals("AttributeFile"))  {
                    attributeArray.add(key);
                    csvFileNames.put("AttributeFile", attributeArray);
                }
                else if(value.equals("RelationshipFile"))   {
                    relationshipArray.add(key);
                    csvFileNames.put("RelationshipFile", relationshipArray);
                }
            }
        }   catch(IOException e)    {
            e.printStackTrace();
        }   finally {
            if(br!=null)    {
                br.close();
            }
        }
        return csvFileNames;
    }
}
