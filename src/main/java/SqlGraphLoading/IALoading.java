package SqlGraphLoading;

import Configuration.SqlGraphConfiguration;
import com.google.code.externalsorting.ExternalSort;

import java.io.*;
import java.lang.reflect.Array;
import java.nio.Buffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import Preprocessing.*;

public class IALoading {

    private SqlGraphConfiguration sqlGraphConfiguration= new SqlGraphConfiguration();
    private String tmpDirectory=sqlGraphConfiguration.getTemporaryFilesDirectory();

    public void IALoadingTable(String outputDirectory, ArrayList<String> relationshipList, HashMap<String, Integer> labelMapping, ArrayList<String> vertexIdsMapping)   throws IOException    {
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
        ArrayList<String> IASubFiles= null;
        int numberOfColumns=labelMapping.get("numberOfColumns");
        HashMap<String, ArrayList<String>> labelsOccurence= null;
        ArrayList<String> pathList=null;
        File temporaryFile=null;
        String vertexType1;
        BufferedWriter bwOATable=null;
        ArrayList<String> relationshipList2= new ArrayList<>();
        Long ia_id=0L;

        for(String relationshipFilePath: relationshipList)  {
            File relationshipFile=new File(relationshipFilePath);
            String relationshipFileName=relationshipFile.getName();

            File relationshipIngoingSorted = File.createTempFile(relationshipFileName, "IngoingSorted.csv", new File(tmpDirectory));
            relationshipIngoingSorted.deleteOnExit();
            vidIngoingComparator comparator = new vidIngoingComparator();
            List<File> sortInBatch= ExternalSort.sortInBatch(relationshipFile, comparator);
            ExternalSort.mergeSortedFiles(sortInBatch, relationshipIngoingSorted, comparator);
            relationshipList2.add(relationshipIngoingSorted.getPath());
        }
        relationshipList= new ArrayList<>(relationshipList2);




        try {
            String outputFileName=outputDirectory+"/IATable.csv";
            bwOATable=new BufferedWriter(new FileWriter(outputFileName));
            for(String vertexType:vertexIdsMapping) {
                indx=vertexType.indexOf("|");
                vertexesIds=vertexType.substring(indx+1);
                vertexType=vertexType.substring(0,indx);
                vertexType1=vertexType;
                indx=vertexesIds.indexOf("-");
                firstId=Long.valueOf(vertexesIds.substring(0,indx));
                lastId=Long.valueOf(vertexesIds.substring(indx+1));
                relationplistByType= new ArrayList<>();

                for(String relationshipFile:relationshipList)   {
                    file1=new File(relationshipFile);
                    pathName=file1.getName();
                    indx2=pathName.indexOf("_");
                    pathName=pathName.substring(indx2+1);
                    indx2=pathName.indexOf("_");
                    pathName=pathName.substring(indx2+1);
                    indx2=pathName.indexOf("_");
                    vertexType2=pathName.substring(0,indx2);

                    if(vertexType.equals(vertexType2)) {
                        relationplistByType.add(relationshipFile);
                    }
                }
                labelsOccurence= new HashMap<>();
                for(String relationshipPath:relationplistByType)    {
                    indx2=relationshipPath.indexOf("_");
                    relationshipPath=relationshipPath.substring(indx2+1);
                    indx2=relationshipPath.indexOf("_");
                    relationshipPath=relationshipPath.substring(0,indx2);
                    labelsOccurence.put(relationshipPath,new ArrayList<>());
                }

                for(String relationshipPath:relationplistByType)    {
                    String relationshipPath2=relationshipPath;
                    indx2=relationshipPath.indexOf("_");
                    relationshipPath=relationshipPath.substring(indx2+1);
                    indx2=relationshipPath.indexOf("_");
                    relationshipPath=relationshipPath.substring(0,indx2);
                    labelsOccurence.get(relationshipPath).add(relationshipPath2);
                }
                BufferedReader brLabel=null;
                BufferedWriter bwLabel=null;
                String fileoutput="";
                for(String path:labelsOccurence.keySet())   {       //Write the files that have the same label in the same file
                    if(labelsOccurence.get(path).size()>1)  {
                        pathList=labelsOccurence.get(path);
                        try {
                            temporaryFile= File.createTempFile("_"+path+"_", ".csv", new File(tmpDirectory));
                            bwLabel= new BufferedWriter(new FileWriter(temporaryFile));

                            for(String path1:pathList)   {
                                relationplistByType.remove(path1);
                                try {
                                    brLabel= new BufferedReader(new FileReader(path1));
                                    String line;
                                    while ((line=brLabel.readLine())!=null)  {
                                        bwLabel.write(line);
                                        bwLabel.newLine();
                                    }
                                }   catch (IOException e)   {
                                    e.printStackTrace();
                                }   finally {
                                    if(brLabel!=null)   {
                                        brLabel.close();
                                    }
                                }
                            }
                        }   catch (IOException e)   {
                            e.printStackTrace();
                        }   finally {
                            if(bwLabel!=null) {
                                bwLabel.close();
                            }
                        }
                        try {
                            String fileInputName=temporaryFile.getName();
                            File tempFileOutput = File.createTempFile(fileInputName, "IngoingSorted.csv", new File(tmpDirectory));
                            tempFileOutput.deleteOnExit();
                            fileoutput=tempFileOutput.getPath();
                            vidIngoingComparator comparator = new vidIngoingComparator();
                            List<File> sortInBatch= ExternalSort.sortInBatch(temporaryFile, comparator);
                            ExternalSort.mergeSortedFiles(sortInBatch, tempFileOutput, comparator);
                            relationplistByType.add(fileoutput);
                        }   catch (IOException e)   {
                            e.printStackTrace();
                        }
                    }
                }

                IASubFiles= new ArrayList<>();
                for(String relationshipPath:relationplistByType)    {
                    file1=new File(relationshipPath);
                    File tempFile= File.createTempFile(relationshipPath, "IAsubFile.csv",new File(tmpDirectory));
                    IASubFiles.add(tempFile.getPath());
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

                        if(line!=null) {
                            indx3=line.indexOf("|");
                            eid1=line.substring(0,indx3);
                            line=line.substring(indx3+1);
                            indx3=line.indexOf("|");
                            target1=line.substring(0,indx3);
                            line=line.substring(indx3+1);
                            indx3=line.indexOf("|");
                            vid1=line.substring(0,indx3);
                            eidArray="["+eid1;
                            targetArray="["+target1;

                        }

                        while((line=br.readLine())!=null)   {
                            indx3=line.indexOf("|");
                            eid2=line.substring(0,indx3);
                            line=line.substring(indx3+1);
                            indx3=line.indexOf("|");
                            target2=line.substring(0,indx3);
                            line=line.substring(indx3+1);
                            indx3=line.indexOf("|");
                            vid2=line.substring(0,indx3);
                            if(vid1.equals(vid2))   {
                                eidArray=eidArray+","+eid2;
                                targetArray=targetArray+","+target2;
                            }   else    {
                                eidArray=eidArray+"]";
                                targetArray=targetArray+"]";
                                newLine=vid1+"|"+eidArray+"|"+label+"|"+targetArray;
                                vid1=vid2;
                                eid1=eid2;
                                target1=target2;
                                eidArray="["+eid1;
                                targetArray="["+target1;

                                bwSubType.write(newLine);
                                bwSubType.newLine();
                            }

                        }
                        if(vid1!="") {
                            eidArray=eidArray+"]";
                            targetArray=targetArray+"]";
                            newLine=vid1+"|"+eidArray+"|"+label+"|"+targetArray;
                            bwSubType.write(newLine);
                            bwSubType.newLine();
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
                BufferedWriter bwAllRelationship=null;
                File temprelationshipFiles=null;
                try {
                    temprelationshipFiles=File.createTempFile(vertexType1+"IngoingRelationships",".csv", new File(tmpDirectory));
                    bwAllRelationship= new BufferedWriter(new FileWriter(temprelationshipFiles));

                    for(String path:IASubFiles)  {
                        BufferedReader brSubFile=null;
                        try {
                            brSubFile=new BufferedReader(new FileReader(path));
                            String line;
                            while((line=brSubFile.readLine())!=null)    {
                                bwAllRelationship.write(line);
                                bwAllRelationship.newLine();
                            }


                        }   catch(IOException e)    {
                            e.printStackTrace();
                        }   finally {
                            if(brSubFile!=null)   {
                                brSubFile.close();
                            }
                        }

                    }


                }   catch (IOException e)   {
                    e.printStackTrace();
                }   finally {
                    if(bwAllRelationship!=null)   {
                        bwAllRelationship.close();
                    }
                }

                String fileInputName=temprelationshipFiles.getName();
                File tempFileOutput = File.createTempFile(fileInputName, "IngoingSorted.csv", new File(tmpDirectory));
                tempFileOutput.deleteOnExit();
                fileoutput=tempFileOutput.getPath();
                vidComparator2 comparator = new vidComparator2();
                List<File> sortInBatch= ExternalSort.sortInBatch(temprelationshipFiles, comparator);
                ExternalSort.mergeSortedFiles(sortInBatch, tempFileOutput, comparator);

                BufferedReader br=null;
                BufferedWriter bw=null;
                String line;
                String line2;
                String idString;
                String idLine="";
                String idLine2="";
                String labelLine;
                int labelColumnNumber;
                int indxLine;
                Long id=firstId;
                Boolean boolReadLine=false;
                Boolean boolId=true;
                try {
                    br= new BufferedReader(new FileReader(fileoutput));

                    File tempFile = File.createTempFile(fileInputName, "IASubTable.csv", new File(tmpDirectory));
                    tempFileOutput.deleteOnExit();
                    bw= new BufferedWriter(new FileWriter(tempFile));
                    String linesToWriteStringEmpty="";

                    for(int j=0; j<numberOfColumns; j++)    {
                        linesToWriteStringEmpty=linesToWriteStringEmpty+"|null|null|null";
                    }

                    line=br.readLine();
                    if(line!=null)  {
                        boolReadLine=true;
                    }


                    while(boolReadLine&& boolId) {

                        if(line!=null)  {
                            idLine=line.substring(0,line.indexOf("|"));
                        }
                        idLine2=idLine;

                        ArrayList<String> linesToWrite= new ArrayList<>();

                        String linesToWriteString="";
                        for(int j=0; j<numberOfColumns; j++)    {
                            linesToWrite.add("|null|null|null");
                        }

                        while(idLine.equals(idLine2)&&idLine!=""&&boolReadLine)   {
                            line2=line;
                            indxLine=line2.indexOf("|");
                            line2=line2.substring(indxLine+1);
                            line=line.substring(indxLine);
                            indxLine=line2.indexOf("|");
                            line2=line2.substring(indxLine+1);
                            indxLine=line2.indexOf("|");
                            labelLine=line2.substring(0,indxLine);
                            labelColumnNumber=labelMapping.get(labelLine);
                            linesToWrite.set(labelColumnNumber,line);
                            line=br.readLine();
                            if(line!=null)  {
                                idLine2=line.substring(0,line.indexOf("|"));
                            }   else {
                                boolReadLine=false;
                            }
                        }


                        for (int k=0; k<linesToWrite.size();k++)    {
                            linesToWriteString=linesToWriteString+linesToWrite.get(k);
                        }
                        while(!(idLine.equals(id.toString()))&&(id<=lastId))  {
                            bwOATable.write(ia_id.toString()+"|"+id.toString()+linesToWriteStringEmpty);
                            bwOATable.newLine();
                            id++;
                            ia_id++;
                        }
                        if(id>lastId)   {
                            boolId=false;
                        }

                        //bw.write(idLine+linesToWriteString);
                        //bw.newLine();
                        bwOATable.write(ia_id.toString()+"|"+idLine+linesToWriteString);
                        bwOATable.newLine();
                        id++;
                        ia_id++;
                    }
                    while (id<=lastId)  {
                        bwOATable.write(ia_id.toString()+"|"+id.toString()+linesToWriteStringEmpty);
                        bwOATable.newLine();
                        id++;
                        ia_id++;
                    }

                }   catch (IOException e)   {
                    e.printStackTrace();
                }   finally {
                    if(br!=null)    {
                        br.close();
                    }
                    if(bw!=null)    {
                        bw.close();
                    }
                }
            }
        }   catch (IOException e)   {
            e.printStackTrace();
        }   finally {
            if(bwOATable!=null)  {
                bwOATable.close();
            }
        }
    }
}
