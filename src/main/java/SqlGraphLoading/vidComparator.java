package SqlGraphLoading;
import java.util.Comparator;

import java.util.Comparator;

public class vidComparator  implements Comparator<String> {
    @Override
    public int compare(String input1, String input2) {
        int indx1=input1.indexOf("|");
        int indx2=input2.indexOf("|");
        String idstr1=input1.substring(indx1+1);
        String idstr2=input2.substring(indx2+1);
        indx1=idstr1.indexOf("|");
        indx2=idstr2.indexOf("|");
        idstr1=idstr1.substring(0,indx1);
        idstr2=idstr2.substring(0,indx2);
        Long id1=0L;
        Long id2=0L;
        try {
            id1= Long.valueOf(idstr1);
        }   catch (NumberFormatException e) {
            id1=-1L;
        }
        try {
            id2=Long.valueOf(idstr2);
        }   catch(NumberFormatException e)  {
            id2=id1-1;
        }
        return Long.compare(id1,id2);
    }
}
