import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;


public class Main {
	
	
	
	public static double I(int a, int b){
		if(a == 0 || b == 0){
			return 0.0;
		}
		double firstFraction = (double) a/(a+b);
		//System.out.println(firstFraction);
		double secondFraction = (double) b/(b+a);
		//System.out.println(secondFraction);
		double firstLog = (Math.log(firstFraction)/Math.log(2));
		//System.out.println(firstLog);
		double secondLog = (Math.log(secondFraction)/Math.log(2));
		//System.out.println(secondLog);
		
		return -( (firstFraction * firstLog) + (secondFraction * secondLog) );
		
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Connection con;
        Statement stmt;
        ResultSet rs;
        ResultSetMetaData rsmd;
        
        /* Database credentials */
        String user = "cse4701";
        String password = "";
        String host = "query.engr.uconn.edu";
        String port = "1521";
        String sid = "BIBCI";
        String url = "jdbc:oracle:thin:@" + host + ":" + port + ":" + sid;
        
        
        
        
        // represents the total deaths and survivors
        int totalNo = 0;
        int totalYes = 0;
        /*
         * This is where I create an array of the conditional distribution tables.
         * [0][0] = 1/Y
         * [0][1] = 1/N
         * [1][0] = 0/Y
         * [1][1] = 0/N
         * 
         * value [0][0] for each table will represent O_CNT for each table
         * since it is the situation where each attribute is equal
         * (assuming Y = 1)
         * 
         */
        int[][] table_APC = new int[2][2];
        int[][] table_TP53 = new int[2][2];
        int[][] table_KRAS = new int[2][2];
        int[][] table_PIK3CA = new int[2][2];
        int[][] table_PTEN = new int[2][2];
        int[][] table_ATM = new int[2][2];
        int[][] table_MUC4 = new int[2][2];
        int[][] table_SMAD4 = new int[2][2];
        int[][] table_SYNE1 = new int[2][2];
        int[][] table_FBXW7 = new int[2][2];
        ArrayList<int[][]> table_list = new ArrayList<int[][]>();
        table_list.add(table_APC);
        table_list.add(table_TP53);
        table_list.add(table_KRAS);
        table_list.add(table_PIK3CA);
        table_list.add(table_PTEN);
        table_list.add(table_ATM);
        table_list.add(table_MUC4);
        table_list.add(table_SMAD4);
        table_list.add(table_SYNE1);
        table_list.add(table_FBXW7);
        
        // list of strings representing each gene
        String mutations[] = new String[10];
        mutations[0] = "APC";
        mutations[1] = "TP53";
        mutations[2] = "KRAS";
        mutations[3] = "PIK3CA";
        mutations[4] = "PTEN";
        mutations[5] = "ATM";
        mutations[6] = "MUC4";
        mutations[7] = "SMAD4";
        mutations[8] = "SYNE1";
        mutations[9] = "FBXW7";
        
        /*
         * This list will contain the IG value for each gene
         * calculation for this will be done below
         * 
         */
        double I_values[] = new double[10];
        // contains the info(D) described in the Q&A
        double I_data = 0.0;
        
        
        
        try{
        	DriverManager.registerDriver(new oracle.jdbc.OracleDriver());
            con = DriverManager.getConnection(url, user, password);
            stmt = con.createStatement();
            
            /*
             * The following parts are SQL statements sent to the database to extract
             * needed data.
             * 
             * This one in particular will get us the total number of deceased.
             */
            String sql = "select count(STATUS) from IG_READY where STATUS = 'Y' ";
            rs = stmt.executeQuery(sql);
            rsmd = rs.getMetaData();
            
            while (rs.next()) {
                ArrayList<Object> obArray = new ArrayList<Object>();
                for (int i = 0; i < rsmd.getColumnCount(); i++) {
                	//not the most efficient way to set the value of total deceased
                	//but it lets me recycle old code so woo!
                    obArray.add(rs.getObject(i + 1));
                    //System.out.print(obArray.toArray()[i] + " ");
                    totalYes = ((BigDecimal) obArray.toArray()[i]).intValue();
                    System.out.print(totalYes);
                }
                
                System.out.println("");
                
            }
            
            //get the total number of survivors
            sql = "select count(STATUS) from IG_READY where STATUS = 'N' ";
            rs = stmt.executeQuery(sql);
            rsmd = rs.getMetaData();
            
            while (rs.next()) {
                ArrayList<Object> obArray = new ArrayList<Object>();
                for (int i = 0; i < rsmd.getColumnCount(); i++) {
                	//not the most efficient way to set the value of total deceased
                	//but it lets me recycle old code so woo!
                    obArray.add(rs.getObject(i + 1));
                    //System.out.print(obArray.toArray()[i] + " ");
                    totalNo = ((BigDecimal) obArray.toArray()[i]).intValue();
                    System.out.print(totalNo);
                }
                
                System.out.println("");
                
            }
            
            /*
             * This loop will repeat a series of queries for each gene type.
             * This will utilize the mutations array to create a query for each gene.
             * In the end, the CDTs will be fully populated, leaving us to only calculate IG.
             * 
             */
            for(int j = 0; j < mutations.length; j++){
            	//This query finds the intersection of STATUS = Y and the given gene = 1
            	//the results is placed in CDT position 00
            	sql = "select count(STATUS) from IG_READY where STATUS = 'Y' and " + mutations[j] + " = 1";
                rs = stmt.executeQuery(sql);
                rsmd = rs.getMetaData();
                
                while (rs.next()) {
                    ArrayList<Object> obArray = new ArrayList<Object>();
                    for (int i = 0; i < rsmd.getColumnCount(); i++) {
                    	//not the most efficient way to set the value of total deceased
                    	//but it lets me recycle old code so woo!
                        obArray.add(rs.getObject(i + 1));
                        //System.out.print(obArray.toArray()[i] + " ");
                        table_list.get(j)[0][0] = ((BigDecimal) obArray.toArray()[i]).intValue();
                        //System.out.print(table_list.get(i)[0][0]);
                    }
                    
                    //System.out.println("");
                    
                }
                
                //This query finds the intersection of STATUS = Y and the given gene = 0
            	//the results is placed in CDT position 10
                sql = "select count(STATUS) from IG_READY where STATUS = 'Y' and " + mutations[j] + " = 0";
                rs = stmt.executeQuery(sql);
                rsmd = rs.getMetaData();
                
                while (rs.next()) {
                    ArrayList<Object> obArray = new ArrayList<Object>();
                    for (int i = 0; i < rsmd.getColumnCount(); i++) {
                    	//not the most efficient way to set the value of total deceased
                    	//but it lets me recycle old code so woo!
                        obArray.add(rs.getObject(i + 1));
                        //System.out.print(obArray.toArray()[i] + " ");
                        table_list.get(j)[1][0] = ((BigDecimal) obArray.toArray()[i]).intValue();
                        //System.out.print(table_list.get(i)[0][1]);
                    }
                    
                    //System.out.println("");
                    
                }
                
                //This query finds the intersection of STATUS = N and the given gene = 1
            	//the results is placed in CDT position 01
                sql = "select count(STATUS) from IG_READY where STATUS = 'N' and " + mutations[j] + " = 1";
                rs = stmt.executeQuery(sql);
                rsmd = rs.getMetaData();
                
                while (rs.next()) {
                    ArrayList<Object> obArray = new ArrayList<Object>();
                    for (int i = 0; i < rsmd.getColumnCount(); i++) {
                    	//not the most efficient way to set the value of total deceased
                    	//but it lets me recycle old code so woo!
                        obArray.add(rs.getObject(i + 1));
                        //System.out.print(obArray.toArray()[i] + " ");
                        table_list.get(j)[0][1] = ((BigDecimal) obArray.toArray()[i]).intValue();
                        //System.out.print(table_list.get(i)[1][0]);
                    }
                    
                    //System.out.println("");
                    
                }
                
                //This query finds the intersection of STATUS = N and the given gene = 0
            	//the results is placed in CDT position 11
                sql = "select count(STATUS) from IG_READY where STATUS = 'N' and " + mutations[j] + " = 0";
                rs = stmt.executeQuery(sql);
                rsmd = rs.getMetaData();
                
                while (rs.next()) {
                    ArrayList<Object> obArray = new ArrayList<Object>();
                    for (int i = 0; i < rsmd.getColumnCount(); i++) {
                    	//not the most efficient way to set the value of total deceased
                    	//but it lets me recycle old code so woo!
                        obArray.add(rs.getObject(i + 1));
                        //System.out.print(obArray.toArray()[i] + " ");
                        table_list.get(j)[1][1] = ((BigDecimal) obArray.toArray()[i]).intValue();
                        //System.out.print(table_list.get(i)[1][1]);
                    }
                    
                    //System.out.println("");
                    
                }
                System.out.println("Matrix for " + mutations[j]);
                System.out.println(table_list.get(j)[0][0] + " | " + table_list.get(j)[0][1]);
                System.out.println(table_list.get(j)[1][0] + " | " + table_list.get(j)[1][1]);
            }
            /*
             * This segment does the calculations to get each IG.
             * first, Info(D) is determined with the total living and total deceased
             * then, we traverse through each CDT.
             * the equation used for this step can be found on lecture slide 15 slide #10
             */
            I_data = I(totalYes, totalNo);
            System.out.println("I_data = " + I_data);
            for(int i = 0; i < I_values.length; i++){
            	//System.out.println("" + table_list.get(i)[0][0] + " " + table_list.get(i)[0][1] + " " + table_list.get(i)[1][0] + " " + table_list.get(i)[1][1]);
            	double fractionA = (double) (table_list.get(i)[0][0] + (table_list.get(i)[0][1])) / (totalYes + totalNo);
            	//System.out.println("fractionA for " + mutations[i] + " = " + fractionA);
            	double fractionB = (double) (table_list.get(i)[1][0] + (table_list.get(i)[1][1])) / (totalYes + totalNo);
            	//System.out.println("fractionB for " + mutations[i] + " = " + fractionB);
            	double I_A = I(table_list.get(i)[0][0], table_list.get(i)[0][1]);
            	//System.out.println("I_A for " + mutations[i] + " = " + I_A);
            	double I_B = I(table_list.get(i)[1][0], table_list.get(i)[1][1]);
            	//System.out.println("I_B for " + mutations[i] + " = " + I_B);
            	I_values[i] = I_data - (fractionA * I_A) - (fractionB * I_B);
            	System.out.println("Gain for " + mutations[i] + " = " + I_values[i]);
            }
            System.out.println("Hi there");
            
        } catch (SQLException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        /*
         * This segment of code just sends the SQL statements to my local database
         * so that I can neatly model the data similar to how it is shown in the problem description.
         * 
         */
        try{

        	try {

				Class.forName("com.mysql.jdbc.Driver");
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        	DriverManager.registerDriver(new oracle.jdbc.OracleDriver());
            con = DriverManager.getConnection("jdbc:mysql://localhost:3306/PROJ2PT2", "root", "");
            stmt = con.createStatement();

            String sql = "";
            //stmt.executeUpdate(sql);
            
            for(int x=0; x<mutations.length; x++){
            	String stuff = String.format("%.8f", I_values[x]);
            	sql = "INSERT INTO PRODUCT VALUES (\"";
            	sql = sql + mutations[x] + "\", " + stuff + ", " + table_list.get(x)[0][0] + ")";
            	
            	System.out.println(sql);
            	stmt.executeUpdate(sql);
            	
            	//System.out.println("");
            }
            
            
            
            
        } catch (SQLException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
	}

}
