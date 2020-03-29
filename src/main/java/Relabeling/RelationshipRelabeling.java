package Relabeling;

import java.io.*;
import java.util.ArrayList;

public class RelationshipRelabeling {

    public void relabeling(String relationshipPath, ArrayList<String> newLabels)    throws IOException   {
        BufferedReader brRelationships=null;
        BufferedWriter bwRelationships=null;

        try {
            brRelationships= new BufferedReader(new FileReader(relationshipPath));
            bwRelationships= new BufferedWriter(new FileWriter(relationshipPath+"relabeled2.csv"));
            String line1="";

            String id1="";
            String id2="";
            Integer tmp=0;
            int i=0;

            while((line1=brRelationships.readLine())!=null)  {
                i++;
                id1 = line1;
                String line2 = "";

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
                    bool2=tmp<newLabels.size()-1;
                }
                if(bool)    {
                    bwRelationships.write(tmp.toString());
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



    }

}