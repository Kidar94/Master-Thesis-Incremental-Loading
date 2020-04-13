package Relabeling;

import Configuration.SqlGraphConfiguration;

import java.io.*;
import java.util.ArrayList;

public class RelationshipRelabeling {

    private String newName="";
    private SqlGraphConfiguration sqlGraphConfiguration= new SqlGraphConfiguration();
    private String tmpDirectory=sqlGraphConfiguration.getTemporaryFilesDirectory();


    public String relabeling(String relationshipPath, ArrayList<String> newLabels, Long firstIdInt)    throws IOException   {
        BufferedReader brRelationships=null;
        BufferedWriter bwRelationships=null;

        try {
            brRelationships= new BufferedReader(new FileReader(relationshipPath));

            File fileInput= new File(relationshipPath);
            String fileInputName=fileInput.getName();
            File tempFileOutput = File.createTempFile(fileInputName, "relabeledRelationship.csv", new File(tmpDirectory));
            tempFileOutput.deleteOnExit();
            newName=tempFileOutput.getPath();
            bwRelationships= new BufferedWriter(new FileWriter(newName));
            String line1=brRelationships.readLine();
            int indxAttributes=line1.indexOf("|");
            String line2="";
            Boolean attributes=false;
            if(indxAttributes>0)    {
                attributes=true;
            }
            bwRelationships.write(line1);
            bwRelationships.newLine();

            String id1="";
            String id2="";
            Integer tmp=0;
            Long newId=0L;

            while((line1=brRelationships.readLine())!=null)  {
                indxAttributes=line1.indexOf("|");
                if(indxAttributes>0)  {
                    line2=line1.substring(indxAttributes);
                    line1=line1.substring(0,indxAttributes);
                }
                id1 = line1;

                int min=0;
                int max=newLabels.size();
                Long id1Int=Long.valueOf(id1);
                boolean bool=false;
                boolean bool2=true;
                if(id1.equals(id2)&&id1!="") {
                    bool=true;
                }
                while (!bool& bool2) {
                    tmp=min+(max-min)/2;
                    id2=newLabels.get(tmp);
                    Long id2Int=Long.valueOf(id2);
                    if(id1Int>id2Int)   {
                        min=tmp+1;

                    }   else if(id1Int<id2Int)  {
                        max=tmp-1;
                    }
                    bool = id1.equals(id2);
                    bool2=tmp<=newLabels.size()-1;
                }
                if(bool)    {
                    newId=firstIdInt+tmp;
                    bwRelationships.write(newId.toString()+line2);
                    bwRelationships.newLine();
                }
            }
        }   catch(IOException e)    {
            e.printStackTrace();
        }   finally {
            if(brRelationships!=null)   {
                brRelationships.close();
            }

            if(bwRelationships!=null)   {
                bwRelationships.close();
            }

        }
        return newName;
    }
}