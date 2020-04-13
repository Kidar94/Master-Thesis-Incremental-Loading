package SqlGraphLoading;

import Configuration.SqlGraphConfiguration;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;

public class OALoading {

    private SqlGraphConfiguration sqlGraphConfiguration= new SqlGraphConfiguration();
    private String tmpDirectory=sqlGraphConfiguration.getTemporaryFilesDirectory();

    public void OALoadingTable(ArrayList<String> relationshipList, HashMap<String, Integer> labelMapping, ArrayList<String> vertexIdsMapping)   throws IOException    {
        String vertexesIds;
        int indx;
        Long firstId;
        Long lastId;
        ArrayList<String>   relationplistByType=null;
        File file1=null;
        String pathName="";
        String vertexType2;
        int indx2;
        String label="";
        ArrayList<String> OASubFiles= null;
        HashMap<BufferedReader, String> brArray=null;
        HashMap<String, BufferedReader> brArray2 =null;
        int numberOfColumns=labelMapping.get("numberOfColumns");

        for(String vertexType:vertexIdsMapping) {
            indx=vertexType.indexOf("|");
            vertexesIds=vertexType.substring(indx+1);
            vertexType=vertexType.substring(0,indx);
            indx=vertexesIds.indexOf("-");
            firstId=Long.valueOf(vertexesIds.substring(0,indx));
            lastId=Long.valueOf(vertexesIds.substring(indx+1));
            relationplistByType= new ArrayList<>();

            for(String relationshipFile:relationshipList)   {
                file1=new File(relationshipFile);
                pathName=file1.getName();
                indx2=pathName.indexOf("_");
                vertexType2=pathName.substring(0,indx2);

                if(vertexType.equals(vertexType2)) {
                    relationplistByType.add(relationshipFile);
                }
            }

            OASubFiles= new ArrayList<>();

            for(String relationshipPath:relationplistByType)    {
                file1=new File(relationshipPath);
                File tempFile= File.createTempFile(relationshipPath, "OAsubFile.csv",new File(tmpDirectory));
                OASubFiles.add(tempFile.getPath());
                tempFile.deleteOnExit();
                label=file1.getName();
                indx2=label.indexOf("_");
                label=label.substring(indx2+1);
                indx2=label.indexOf("_");
                label=label.substring(0,indx2);
                String line="";
                BufferedReader br=null;
                BufferedWriter bwSubType=null;
                String  vid1="";
                String  vid2="";
                String  eid1="";
                String  eid2="";
                String  target1="";
                String target2="";
                int indx3;
                String newLine="";
                String eidArray="";
                String targetArray="";
                try {
                    br= new BufferedReader(new FileReader(relationshipPath));
                    bwSubType= new BufferedWriter(new FileWriter(tempFile));
                    line=br.readLine();
                    indx3=line.indexOf("|");
                    eid1=line.substring(0,indx3);
                    line=line.substring(indx3+1);
                    indx3=line.indexOf("|");
                    vid1=line.substring(0,indx3);
                    line=line.substring(indx3+1);
                    indx3=line.indexOf("|");
                    target1=line.substring(0,indx3);

                    while((line=br.readLine())!=null)   {
                        indx3=line.indexOf("|");
                        eid2=line.substring(0,indx3);
                        line=line.substring(indx3+1);
                        indx3=line.indexOf("|");
                        vid2=line.substring(0,indx3);
                        line=line.substring(indx3+1);
                        indx3=line.indexOf("|");
                        target2=line.substring(0,indx3);
                        if(vid1.equals(vid2))   {
                            eidArray=eidArray+","+eid2;
                            targetArray=targetArray+","+target2;
                            eid1=eid1+","+eid2;
                            target1=target1+","+target2;
                        }   else    {
                            eidArray="["+eid1+"]";
                            targetArray="["+target1+"]";
                            newLine=vid1+"|"+eidArray+"|"+label+"|"+targetArray;
                            vid1=vid2;
                            eid1=eid2;
                            target1=target2;
                            bwSubType.write(newLine);
                            bwSubType.newLine();
                        }
                    }

                }   catch (IOException e)   {
                    e.printStackTrace();
                }   finally {
                    if(br!=null)    {
                        br.close();
                    }
                    if(bwSubType!=null) {
                        bwSubType.close();
                    }
                }
            }

            for(Long i=firstId; i<lastId; i++)    {
                HashMap<String, ArrayList<String>> lineArray= new HashMap<>();
                ArrayList<Long> idsArray= new ArrayList<>();
                ArrayList<String> linesToWrite= new ArrayList<>();
                linesToWrite.add(i.toString());
                int indx1;
                int labelColumn;
                String labelName;
                String filePath;
                Long id;
                try {
                    brArray= new HashMap<>();
                    for(String OASubFile:OASubFiles)    {
                        indx1=OASubFile.indexOf("_");
                        labelName=OASubFile.substring(indx1+1);
                        indx1=labelName.indexOf("_");
                        labelName=labelName.substring(0,indx1);
                        lineArray.put(labelName,new ArrayList());
                        brArray.put(new BufferedReader(new FileReader(OASubFile)), OASubFile);
                    }

                    for(int j=0; j<numberOfColumns; j++)    {
                        linesToWrite.add("|null|null");
                    }

                    for(BufferedReader brElement:brArray.keySet())   {
                        filePath=brArray.get(brElement);
                        indx1=filePath.indexOf("_");
                        labelName=filePath.substring(indx1+1);
                        indx1=labelName.indexOf("_");
                        labelName=labelName.substring(0,indx1);
                        lineArray.get(labelName).add(brElement.readLine());
                        //lineArray.replace(filePath, brElement.readLine());
                    }
                    String line2;
                    String labelName2;
                    ArrayList<String> lines;
                    for(String filePath2:lineArray.keySet()) {
                        lines=lineArray.get(filePath2);
                        if(lines.size()>1)  {
                            for(String items:lines) {

                            }
                        }
                        line2=lines.get(0);
                        indx1=line2.indexOf("|");
                        id=Long.valueOf(line2.substring(0,indx1));
                        //idsArray.add(Long.valueOf(line2));
                        if(id==i)  {

                            indx1=filePath2.indexOf("_");
                            labelName2=filePath2.substring(indx1+1);
                            indx1=labelName2.indexOf("_");
                            labelName2=labelName2.substring(0,indx1);
                            labelColumn=labelMapping.get(labelName2);
                            linesToWrite.set(labelColumn,line2);
                        }
                    }
                }   catch (IOException e)   {
                    e.printStackTrace();
                }   finally {
                    for(BufferedReader brElement:brArray.keySet())    {
                        if(brElement!=null) {
                            brElement.close();
                        }
                    }
                }
            }
        }
    }
}
