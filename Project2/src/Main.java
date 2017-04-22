/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
//package edu.uconn.engr.bibci.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * 
 * This class is a simple example to access oracle database and list all the objects.
 * We can use a oracle client to browse the database with the credentials mentioned below in the code.
 * The database has to be accessible first. If its not publicly accessible we should use UCONN's VPN to acquire UCONN IP.
 * 
 * To run this file, we have to import ojdbc6.jar file into class library. 
 * If you are using NetBeans IDE, you can right-click on library folder and "Add JAR/Folder" to import the jar file.
 * After you import it, it is going to show in library section of your project.
 * 
 * 
 * SQL Developer is one of the best available oracle GUI clients available.
 * This can be downloaded for free from oracle's website.
 * http://www.oracle.com/technetwork/developer-tools/sql-developer/overview/index.html
 * 
 */
public class Main {
	
	/*
	 * A summary of what will happen in this main method:
	 * 1) a connection will be established with the Cancer database
	 * 2) A 2D array representing a SQL table will be created
	 * 3) said array will be populated through modifying query results
	 * 4) a connection will be established with my local sql table
	 * 5) said array will be translated into INSERT statements
	 * 6) a populated SQL table will be born!
	 * 
	 * 
	 */

    public static void main(String args[]) {
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
        
        ArrayList<ArrayList<Object>> output = new ArrayList<ArrayList<Object>>();
        ArrayList<Object> namesList = new ArrayList<Object>();
        ArrayList<Object> APC = new ArrayList<Object>();
        ArrayList<Object> TP53 = new ArrayList<Object>();
        ArrayList<Object> KRAS = new ArrayList<Object>();
        ArrayList<Object> PIK3CA = new ArrayList<Object>();
        ArrayList<Object> PTEN = new ArrayList<Object>();
        ArrayList<Object> ATM = new ArrayList<Object>();
        ArrayList<Object> MUC4 = new ArrayList<Object>();
        ArrayList<Object> SMAD4 = new ArrayList<Object>();
        ArrayList<Object> SYNE1 = new ArrayList<Object>();
        ArrayList<Object> FBXW7 = new ArrayList<Object>();
        ArrayList<Object> STATUS = new ArrayList<Object>();
        ArrayList<Object> TEMP = new ArrayList<Object>();

        try {
            DriverManager.registerDriver(new oracle.jdbc.OracleDriver());
            con = DriverManager.getConnection(url, user, password);
            stmt = con.createStatement();

            String sql = "select * from CLINICAL";
            rs = stmt.executeQuery(sql);
            rsmd = rs.getMetaData();

            int count = 0;
            while (rs.next()) {
                ArrayList<Object> obArray = new ArrayList<Object>();
                for (int i = 0; i < rsmd.getColumnCount(); i++) {
                	if(i==0){
                		namesList.add(rs.getObject(i+1));
                	}
                	if(i==5){
                		if(rs.getObject(i+1).equals("DECEASED")){
                			STATUS.add(0);
                		}else{
                			STATUS.add(1);
                		}
                	}
                    obArray.add(rs.getObject(i + 1));
                    System.out.print(obArray.toArray()[i] + " ");
                    count++;
                }
                System.out.println("");
                
            }
            for(int x=0; x<namesList.size(); x++){
            	System.out.print(namesList.get(x) + " ");
            	System.out.println(STATUS.get(x));
            	
            }
            output.add(namesList);
            
           /*
            * this segment of code will be repeated for all genes needed
            * the code will send a query that returns a column of patients
            * who have a mutation in the gene and the gene is not silent
            * all patient ids will be added to a new array TEMP
            * the array namesList, which contains all of the patient ids
            * will be compared with TEMP
            * if there is a matching ID in namesList and TEMP, the value for
            * this given gene is set to 1
            * if there is no match, set to 0
            */
            
            sql = "select PATIENT_ID FROM MUTATION WHERE (GENE_SYMBOL = 'APC') AND (VARIANT_CLASSIFICATION != 'Silent')";
            rs = stmt.executeQuery(sql);
            rsmd = rs.getMetaData();
            while(rs.next()){
            	ArrayList<Object> obArray = new ArrayList<Object>();
            	for (int i = 0; i < rsmd.getColumnCount(); i++) {
                    obArray.add(rs.getObject(i + 1));
                    TEMP.add(rs.getObject(i+1));
                    //System.out.print(obArray.toArray()[i] + " ");
                    count++;
                }
                //System.out.println("");
            }
            for(int x = 0; x<namesList.size(); x++){
            	boolean tracker = false;
            	for(int y = 0; y<TEMP.size(); y++){
            		if((namesList.get(x) + "-01").equals((String)TEMP.get(y))){
            			APC.add(1);
            			tracker = true;
            			break;
            		}
            	}
            	if(!tracker){
            		APC.add(0);
            	}
            }
            for(int x=0; x<namesList.size(); x++){
            	//System.out.print(namesList.get(x) + " ");
            	//System.out.println(APC.get(x));
            	
            }
            output.add(APC);
            TEMP = new ArrayList<Object>();
            
            //for TP53
            sql = "select PATIENT_ID FROM MUTATION WHERE (GENE_SYMBOL = 'TP53') AND (VARIANT_CLASSIFICATION != 'Silent')";
            rs = stmt.executeQuery(sql);
            rsmd = rs.getMetaData();
            while(rs.next()){
            	ArrayList<Object> obArray = new ArrayList<Object>();
            	for (int i = 0; i < rsmd.getColumnCount(); i++) {
                    obArray.add(rs.getObject(i + 1));
                    TEMP.add(rs.getObject(i+1));
                    //System.out.print(obArray.toArray()[i] + " ");
                    count++;
                }
                //System.out.println("");
            }
            for(int x = 0; x<namesList.size(); x++){
            	boolean tracker = false;
            	for(int y = 0; y<TEMP.size(); y++){
            		if((namesList.get(x) + "-01").equals((String)TEMP.get(y))){
            			TP53.add(1);
            			tracker = true;
            			break;
            		}
            	}
            	if(!tracker){
            		TP53.add(0);
            	}
            }
            for(int x=0; x<namesList.size(); x++){
            	//System.out.print(namesList.get(x) + " ");
            	//System.out.println(TP53.get(x));
            	
            }
            output.add(TP53);
            TEMP = new ArrayList<Object>();
            
            sql = "select PATIENT_ID FROM MUTATION WHERE (GENE_SYMBOL = 'KRAS') AND (VARIANT_CLASSIFICATION != 'Silent')";
            rs = stmt.executeQuery(sql);
            rsmd = rs.getMetaData();
            while(rs.next()){
            	ArrayList<Object> obArray = new ArrayList<Object>();
            	for (int i = 0; i < rsmd.getColumnCount(); i++) {
                    obArray.add(rs.getObject(i + 1));
                    TEMP.add(rs.getObject(i+1));
                    //System.out.print(obArray.toArray()[i] + " ");
                    count++;
                }
                //System.out.println("");
            }
            for(int x = 0; x<namesList.size(); x++){
            	boolean tracker = false;
            	for(int y = 0; y<TEMP.size(); y++){
            		if((namesList.get(x) + "-01").equals((String)TEMP.get(y))){
            			KRAS.add(1);
            			tracker = true;
            			break;
            		}
            	}
            	if(!tracker){
            		KRAS.add(0);
            	}
            }
            for(int x=0; x<namesList.size(); x++){
            	//System.out.print(namesList.get(x) + " ");
            	//System.out.println(TP53.get(x));
            	
            }
            output.add(KRAS);
            TEMP = new ArrayList<Object>();
            
            
            sql = "select PATIENT_ID FROM MUTATION WHERE (GENE_SYMBOL = 'PIK3CA') AND (VARIANT_CLASSIFICATION != 'Silent')";
            rs = stmt.executeQuery(sql);
            rsmd = rs.getMetaData();
            while(rs.next()){
            	ArrayList<Object> obArray = new ArrayList<Object>();
            	for (int i = 0; i < rsmd.getColumnCount(); i++) {
                    obArray.add(rs.getObject(i + 1));
                    TEMP.add(rs.getObject(i+1));
                    //System.out.print(obArray.toArray()[i] + " ");
                    count++;
                }
                //System.out.println("");
            }
            for(int x = 0; x<namesList.size(); x++){
            	boolean tracker = false;
            	for(int y = 0; y<TEMP.size(); y++){
            		if((namesList.get(x) + "-01").equals((String)TEMP.get(y))){
            			PIK3CA.add(1);
            			tracker = true;
            			break;
            		}
            	}
            	if(!tracker){
            		PIK3CA.add(0);
            	}
            }
            for(int x=0; x<namesList.size(); x++){
            	//System.out.print(namesList.get(x) + " ");
            	//System.out.println(TP53.get(x));
            	
            }
            output.add(PIK3CA);
            TEMP = new ArrayList<Object>();
            
            sql = "select PATIENT_ID FROM MUTATION WHERE (GENE_SYMBOL = 'PTEN') AND (VARIANT_CLASSIFICATION != 'Silent')";
            rs = stmt.executeQuery(sql);
            rsmd = rs.getMetaData();
            while(rs.next()){
            	ArrayList<Object> obArray = new ArrayList<Object>();
            	for (int i = 0; i < rsmd.getColumnCount(); i++) {
                    obArray.add(rs.getObject(i + 1));
                    TEMP.add(rs.getObject(i+1));
                    //System.out.print(obArray.toArray()[i] + " ");
                    count++;
                }
                //System.out.println("");
            }
            for(int x = 0; x<namesList.size(); x++){
            	boolean tracker = false;
            	for(int y = 0; y<TEMP.size(); y++){
            		if((namesList.get(x) + "-01").equals((String)TEMP.get(y))){
            			PTEN.add(1);
            			tracker = true;
            			break;
            		}
            	}
            	if(!tracker){
            		PTEN.add(0);
            	}
            }
            for(int x=0; x<namesList.size(); x++){
            	//System.out.print(namesList.get(x) + " ");
            	//System.out.println(TP53.get(x));
            	
            }
            output.add(PTEN);
            TEMP = new ArrayList<Object>();
            
            sql = "select PATIENT_ID FROM MUTATION WHERE (GENE_SYMBOL = 'ATM') AND (VARIANT_CLASSIFICATION != 'Silent')";
            rs = stmt.executeQuery(sql);
            rsmd = rs.getMetaData();
            while(rs.next()){
            	ArrayList<Object> obArray = new ArrayList<Object>();
            	for (int i = 0; i < rsmd.getColumnCount(); i++) {
                    obArray.add(rs.getObject(i + 1));
                    TEMP.add(rs.getObject(i+1));
                    //System.out.print(obArray.toArray()[i] + " ");
                    count++;
                }
                //System.out.println("");
            }
            for(int x = 0; x<namesList.size(); x++){
            	boolean tracker = false;
            	for(int y = 0; y<TEMP.size(); y++){
            		if((namesList.get(x) + "-01").equals((String)TEMP.get(y))){
            			ATM.add(1);
            			tracker = true;
            			break;
            		}
            	}
            	if(!tracker){
            		ATM.add(0);
            	}
            }
            for(int x=0; x<namesList.size(); x++){
            	//System.out.print(namesList.get(x) + " ");
            	//System.out.println(TP53.get(x));
            	
            }
            output.add(ATM);
            TEMP = new ArrayList<Object>();
            
            sql = "select PATIENT_ID FROM MUTATION WHERE (GENE_SYMBOL = 'MUC4') AND (VARIANT_CLASSIFICATION != 'Silent')";
            rs = stmt.executeQuery(sql);
            rsmd = rs.getMetaData();
            while(rs.next()){
            	ArrayList<Object> obArray = new ArrayList<Object>();
            	for (int i = 0; i < rsmd.getColumnCount(); i++) {
                    obArray.add(rs.getObject(i + 1));
                    TEMP.add(rs.getObject(i+1));
                    //System.out.print(obArray.toArray()[i] + " ");
                    count++;
                }
                //System.out.println("");
            }
            for(int x = 0; x<namesList.size(); x++){
            	boolean tracker = false;
            	for(int y = 0; y<TEMP.size(); y++){
            		if((namesList.get(x) + "-01").equals((String)TEMP.get(y))){
            			MUC4.add(1);
            			tracker = true;
            			break;
            		}
            	}
            	if(!tracker){
            		MUC4.add(0);
            	}
            }
            for(int x=0; x<namesList.size(); x++){
            	//System.out.print(namesList.get(x) + " ");
            	//System.out.println(TP53.get(x));
            	
            }
            output.add(MUC4);
            TEMP = new ArrayList<Object>();
            
            sql = "select PATIENT_ID FROM MUTATION WHERE (GENE_SYMBOL = 'SMAD4') AND (VARIANT_CLASSIFICATION != 'Silent')";
            rs = stmt.executeQuery(sql);
            rsmd = rs.getMetaData();
            while(rs.next()){
            	ArrayList<Object> obArray = new ArrayList<Object>();
            	for (int i = 0; i < rsmd.getColumnCount(); i++) {
                    obArray.add(rs.getObject(i + 1));
                    TEMP.add(rs.getObject(i+1));
                    //System.out.print(obArray.toArray()[i] + " ");
                    count++;
                }
                //System.out.println("");
            }
            for(int x = 0; x<namesList.size(); x++){
            	boolean tracker = false;
            	for(int y = 0; y<TEMP.size(); y++){
            		if((namesList.get(x) + "-01").equals((String)TEMP.get(y))){
            			SMAD4.add(1);
            			tracker = true;
            			break;
            		}
            	}
            	if(!tracker){
            		SMAD4.add(0);
            	}
            }
            for(int x=0; x<namesList.size(); x++){
            	//System.out.print(namesList.get(x) + " ");
            	//System.out.println(TP53.get(x));
            	
            }
            output.add(SMAD4);
            TEMP = new ArrayList<Object>();
            
            sql = "select PATIENT_ID FROM MUTATION WHERE (GENE_SYMBOL = 'SYNE1') AND (VARIANT_CLASSIFICATION != 'Silent')";
            rs = stmt.executeQuery(sql);
            rsmd = rs.getMetaData();
            while(rs.next()){
            	ArrayList<Object> obArray = new ArrayList<Object>();
            	for (int i = 0; i < rsmd.getColumnCount(); i++) {
                    obArray.add(rs.getObject(i + 1));
                    TEMP.add(rs.getObject(i+1));
                    //System.out.print(obArray.toArray()[i] + " ");
                    count++;
                }
                //System.out.println("");
            }
            for(int x = 0; x<namesList.size(); x++){
            	boolean tracker = false;
            	for(int y = 0; y<TEMP.size(); y++){
            		if((namesList.get(x) + "-01").equals((String)TEMP.get(y))){
            			SYNE1.add(1);
            			tracker = true;
            			break;
            		}
            	}
            	if(!tracker){
            		SYNE1.add(0);
            	}
            }
            for(int x=0; x<namesList.size(); x++){
            	//System.out.print(namesList.get(x) + " ");
            	//System.out.println(TP53.get(x));
            	
            }
            output.add(SYNE1);
            TEMP = new ArrayList<Object>();
            
            sql = "select PATIENT_ID FROM MUTATION WHERE (GENE_SYMBOL = 'FBXW7') AND (VARIANT_CLASSIFICATION != 'Silent')";
            rs = stmt.executeQuery(sql);
            rsmd = rs.getMetaData();
            while(rs.next()){
            	ArrayList<Object> obArray = new ArrayList<Object>();
            	for (int i = 0; i < rsmd.getColumnCount(); i++) {
                    obArray.add(rs.getObject(i + 1));
                    TEMP.add(rs.getObject(i+1));
                    //System.out.print(obArray.toArray()[i] + " ");
                    count++;
                }
                //System.out.println("");
            }
            for(int x = 0; x<namesList.size(); x++){
            	boolean tracker = false;
            	for(int y = 0; y<TEMP.size(); y++){
            		if((namesList.get(x) + "-01").equals((String)TEMP.get(y))){
            			FBXW7.add(1);
            			tracker = true;
            			break;
            		}
            	}
            	if(!tracker){
            		FBXW7.add(0);
            	}
            }
            for(int x=0; x<namesList.size(); x++){
            	//System.out.print(namesList.get(x) + " ");
            	//System.out.println(TP53.get(x));
            	
            }
            output.add(FBXW7);
            
            /*
             * This last bit of code adds the final piece to the output 2D array,
             * which is the STATUS column that was created in the initial query.
             * after that, Print statements are used to make sure that the columns
             * are added properly to the array of arrays.
             * the print statement is later used as a base to create insert statements.
             */
            output.add(STATUS);
            
            for(int x=0; x<output.get(0).size(); x++){
            	for(int y=0; y<output.size(); y++){
            		System.out.print(output.get(y).get(x) + " ");
            	}
            	System.out.println("");
            }
            
            

            
        } catch (SQLException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }

        try{
            /*
             * So I figured out that you need to add mysql-connector to the build path
             * in order to use Class.forName("com.mysql.jdbc.Driver")
             * which is needed to access a local host via mySQL.
             * Anyways, what this block of code does is takes the 2D array 'output'
             * that was created from the earlier code and converting it into INSERT
             * SQL statements.
             * it will take each row from the output 2D array and add each element
             * one by one to the query statement variable "sql"
             * this uses stmt.excecuteUpdate(sql) in place of stmt.executeQuery(sql)
             * so that we can properly modify out local table with our java code.
             * Print statements are added to see what the query looks like before sending
             * Used for testing purposes.
             */
        	try {
				Class.forName("com.mysql.jdbc.Driver");
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        	DriverManager.registerDriver(new oracle.jdbc.OracleDriver());
            con = DriverManager.getConnection("jdbc:mysql://localhost:3306/IG_READY", "root", "");
            stmt = con.createStatement();

            String sql = "";
            //stmt.executeUpdate(sql);
            
            for(int x=0; x<output.get(0).size(); x++){
            	sql = "INSERT INTO IG_READY VALUES (";
            	for(int y=0; y<output.size(); y++){
            		if(y==0){
            			sql = sql + "\"";
            		}
            		sql = sql + output.get(y).get(x);
            		if(y==0){
            			sql = sql + "\"";
            		}
            		sql = sql + ", ";
            		//System.out.print(output.get(y).get(x) + " ");
            	}
            	sql = sql.substring(0, sql.length() - 2);
            	sql = sql + ")";
            	System.out.println(sql);
            	stmt.executeUpdate(sql);
            	
            	//System.out.println("");
            }
            
            /*
            while (rs.next()) {
                ArrayList<Object> obArray = new ArrayList<Object>();
                for (int i = 0; i < rsmd.getColumnCount(); i++) {
                    obArray.add(rs.getObject(i + 1));
                    System.out.print(obArray.toArray()[i] + " ");
                }
                System.out.println("");
                
            }*/
            
            
            
        } catch (SQLException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        } 
    }
}