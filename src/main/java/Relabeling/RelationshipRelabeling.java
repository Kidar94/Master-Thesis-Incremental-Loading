package Relabeling;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;

public class RelationshipRelabeling {

    public RelationshipRelabeling() {}

    public void verticesRelabeling(HashMap<String, ArrayList> relationshipPerVertexType, ArrayList<String> relabeledVertexFiles) throws IOException   {
        BufferedReader brRelationship= null;
        BufferedReader brVertex= null;
        BufferedWriter bw= null;
        String id1="";
        String id2="";
        String newId="";
        for(String type:relationshipPerVertexType.keySet()) {
            for(String vertexFile:relabeledVertexFiles) {
                File file= new File(vertexFile);
                String type2=file.getName();
                int indx=type2.indexOf("_");
                type2=type2.substring(0,indx);
                if(type.equals(type2))  {
                    ArrayList<String> relationshipFiles=relationshipPerVertexType.get(type);
                    for(String relationshipFile:relationshipFiles) {
                        try {
                            bw= new BufferedWriter(new FileWriter(relationshipFile+"relabeled2.csv"));
                            brRelationship= new BufferedReader(new FileReader(relationshipFile));
                            String line1="";
                            while((line1=brRelationship.readLine())!=null)  {
                                id1=line1;
                                String line2="";
                                brVertex=new BufferedReader(new FileReader(vertexFile));
                                boolean bool=id1.equals(id2);

                                while(!bool)   {
                                    line2=brVertex.readLine();
                                    if(line2!=null) {
                                        int indx2=line2.indexOf("|");
                                        newId=line2.substring(0,indx2);
                                        indx2= line2.indexOf("\"id\":\"");
                                        line2=line2.substring(indx2+6);
                                        indx2=line2.indexOf("\"");
                                        id2=line2.substring(0,indx2);
                                        bool=id1.equals(id2);
                                    }
                                }
                                if(bool)    {
                                    bw.write(newId);
                                    bw.newLine();
                                }
                            }
                        }   catch (IOException e)   {
                            e.printStackTrace();
                        }   finally {
                            if(brRelationship!=null)   {
                                brRelationship.close();
                            }
                            if(brVertex!=null)   {
                                brVertex.close();
                            }
                        }


                    }


                }
            }
        }



    }

}
