import java.io.*;
import java.util.*;

public class Data {
    public static void main(String args[]) throws IOException{
        File customers=new File("Customers.csv");
        customers.createNewFile();
        File transactions=new File("Transactions.csv");
        transactions.createNewFile();
        FileWriter fw=new FileWriter("Customers.csv");
        /*fw.append("ID");
        fw.append(",");
        fw.append("Name");
        fw.append(",");
        fw.append("Age");
        fw.append(",");
        fw.append("Gender");
        fw.append(",");
        fw.append("CountryCode");
        fw.append(",");
        fw.append("Salary");*/
        int numCustomers=50000;
        for(int i=0;i<numCustomers;i++){
            int ID=i+1;

            Random r=new Random();
            int nameLenLow=10;
            int nameLenHigh=21;
            int nameLen=r.nextInt(nameLenHigh-nameLenLow)+nameLenLow;
            String nameString="abcdefghijklmnopqrstuvwxyz"; 
            StringBuilder sb=new StringBuilder(nameLen);
            for (int j=0;j<nameLen;j++){
                int index=(int)(nameString.length()*Math.random());
                sb.append(nameString.charAt(index));
            }
            String Name=sb.toString();
            //System.out.println(sb.toString());

            int ageLow=10;
            int ageHigh=71;
            int age=r.nextInt(ageHigh-ageLow)+ageLow;

            int ccLow=1;
            int ccHigh=11;
            int cc=r.nextInt(ccHigh-ccLow)+ccLow;

            int salLow=100;
            int salHigh=10000;
            double sal=r.nextInt(salHigh-salLow)+salLow+r.nextDouble();

            int gLow=1;
            int gHigh=10;
            int g=r.nextInt(gHigh-gLow)+gLow;
            float t=g%2;
            String gender="a";
            if (t==0){
                gender="male";
                //System.out.println(gender);
            }
            else{
                gender="female";
                //System.out.println(gender);
            }
            //System.out.println(gender);
            fw.append(ID+"");
            fw.append(",");
            fw.append(Name);
            fw.append(",");
            fw.append(age+"");
            fw.append(",");
            fw.write(gender);
            fw.append(",");
            fw.append(cc+"");
            fw.append(",");
            fw.append(sal+"");
            fw.append("\n");
        }
        fw.flush();
        fw.close();


        FileWriter fw2=new FileWriter("Transactions.csv");
        /*fw2.append("TransID");
        fw2.append(",");
        fw2.append("CustID");
        fw2.append(",");
        fw2.append("TransTotal");
        fw2.append(",");
        fw2.append("TransNumItems");
        fw2.append(",");
        fw2.append("TransDesc");*/

        int numTransactions=5000000;

        for (int k=0;k<numTransactions;k++){
            int transId=k+1;

            Random r=new Random();
            int cIdLow=1;
            int cIdHigh=50001;
            int cId=r.nextInt(cIdHigh-cIdLow)+cIdLow;

            int transTotalLow=10;
            int transTotalHigh=1000;
            double transTotal=r.nextInt(transTotalHigh-transTotalLow)+transTotalLow+r.nextDouble();

            int transItemsLow=1;
            int transItemsHigh=11;
            int transItems=r.nextInt(transItemsHigh-transItemsLow)+transItemsLow;

            String transDescString="abcdefghijklmnopqrstuvwxyz";
            int descLenLow=20;
            int descLenHigh=51;
            int descLen=r.nextInt(descLenHigh-descLenLow)+descLenLow;
            StringBuilder sb=new StringBuilder(descLen);
            for (int j=0;j<descLen;j++){
                int index=(int)(transDescString.length()*Math.random());
                sb.append(transDescString.charAt(index));
            }
            String transDesc=sb.toString();

            fw2.append(transId+"");
            fw2.append(",");
            fw2.append(cId+"");
            fw2.append(",");
            fw2.append(transTotal+"");
            fw2.append(",");
            fw2.write(transItems+"");
            fw2.append(",");
            fw2.append(transDesc);
            fw2.append("\n");
        }
        fw2.flush();
        fw2.close();
    }
}
