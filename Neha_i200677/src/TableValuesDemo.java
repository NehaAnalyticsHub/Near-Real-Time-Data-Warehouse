
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
//import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.sql.Statement;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
//import java.util.Iterator;

public class TableValuesDemo {
 static //  @SuppressWarnings("null")
	 String databasename;
	static String user;
	static String password1;
	
	static void myMethod(Map<String, ArrayList<Object>>hm, ArrayBlockingQueue<String[]> queue, List<Entry<String, ArrayList<Object>>> first, List<Entry<String, ArrayList<Object>>> second) throws ClassNotFoundException, SQLException {
		Object transid;
		String prodid = null;
		String prodname;
		String supid = null;
        String supname;
        Object price = null;
        String storeid;
		String storename1;
		String timeid1;
		String custid1 = null;
		ArrayList<Object> custname;
		ArrayList<Object> prodcategories;
		Object date1;
		Object quantity;
		String id;
		double sales1;
		String[] variable;
	//	String databasename;
	//	String password1;
	//	String user;
		
	//	databasename="i200677";
    //		password1="1111";
	//	user="root";
			
		
		 Connection con = null;
		 
		 Properties connProperties;
		 connProperties = new Properties();
		 connProperties.setProperty("user", user);
		 connProperties.setProperty("password",password1);

		 Class.forName("com.mysql.cj.jdbc.Driver");
		 con = DriverManager.getConnection("jdbc:mysql://localhost:3306/" + databasename, connProperties);
	     
	     
	    // con = DriverManager.getConnection("jdbc:mysql://localhost:3306/databasename,user,password");
	    
	
		int size=first.size();
		variable=queue.poll();
	//	System.out.println("variable "+variable[1]); 	
	/*	for(int f=0;f<variable.length;f++)
		{	
		if(variable[f]!=null)
		{	
	*/	
		for (int c = 0; c < size; c++)
		{
		//	System.out.println("variable "+variable[f]);
		//	System.out.println("master table "+first.get(c).getKey());
			if(hm.get(first.get(c).getKey())==null)
			//if(variable[f]!=first.get(c).getKey())	
			{
			//	System.out.println("not match");
			}
			else
			{
				System.out.println("match");
				ArrayList<Object> keyvalues=hm.get(first.get(c).getKey());
				int d=0;
				while(d<keyvalues.size())
				{
					id=(String)keyvalues.get(d);
					char first1 = id.charAt(0);
					if(first1=='P') {
						prodid=id;
						custid1=first.get(c).getKey();
						custname=first.get(c).getValue();
						Object name=custname.get(0);
						Statement s=con.createStatement();
						String sql = "select *from customer where CUSTOMER_ID='"+custid1+"'";
		                ResultSet resultSet1 = s.executeQuery(sql);
		                if(resultSet1.next() == false)
		                {	
					    s.executeUpdate("INSERT INTO `customer` VALUE ('"+custid1+"','"+name+"')");
		                }
					}
					if(first1=='C')
					{
						custid1=id;
						prodid=first.get(c).getKey();
						prodcategories=first.get(c).getValue();
						prodname=(String) prodcategories.get(0);
						supid=(String) prodcategories.get(1);
						supname=(String) prodcategories.get(2);
						
						price=prodcategories.get(3);
						
						Statement s=con.createStatement();
						String sql = "select *from supplier where SUPPLIER_ID='"+supid+"'";
		                ResultSet resultSet1 = s.executeQuery(sql);
		                if(resultSet1.next() == false)
		                {	
					    s.executeUpdate("INSERT INTO supplier VALUE('"+supid+"','"+supname+"')");
		                }
						String sql1 = "select *from products where PRODUCT_ID='"+prodid+"'";
		                ResultSet resultSet11 = s.executeQuery(sql1);
		                if(resultSet11.next() == false)
		                {	
					    s.executeUpdate("INSERT INTO products VALUE('"+prodid+"','"+prodname+"')");
		                }
		                }
					d=d+1;
					transid = keyvalues.get(d);
					d=d+1;
					storename1=(String) keyvalues.get(d);
					
					d=d+1;
					timeid1=(String) keyvalues.get(d);
					
					d=d+1;
					storeid=(String) keyvalues.get(d);
					
					d=d+1;
					date1 = keyvalues.get(d);
					
					d=d+1;
					quantity = keyvalues.get(d);

					d=d+1;
					
					if(price!=null)
					{	
					sales1=(double)price * (double)quantity;
					}
					else {
					sales1=0;
					}
					
					Statement s=con.createStatement();
					String sql = "select *from location where STORE_ID='"+storeid+"'";
	                ResultSet resultSet1 = s.executeQuery(sql);
	                if(resultSet1.next() == false)
	                {	
				    s.executeUpdate("INSERT INTO location VALUE('"+storeid+"','"+storename1+"')");
	                }
				    
	                String sql1 = "select *from time1 where TIME_ID='"+timeid1+"'";
	                ResultSet resultSet11 = s.executeQuery(sql1);
	                if(resultSet11.next() == false)
	                {	
				    Statement s1=con.createStatement();
				    s1.executeUpdate("INSERT INTO time1 VALUE('"+timeid1+"','"+date1+"')");
	                }
	                

				    Statement s11=con.createStatement();
				    s11.executeUpdate("INSERT INTO tranformed VALUE('"+prodid+"','"+custid1+"','"+storeid+"','"+timeid1+"','"+quantity+"','"+supid+"','"+sales1+"')");
				}			 
				}
			}
	//	}
//		}
		
		int size1=second.size();
	/*	for(int f=0;f<variable.length;f++)
		{
		if(variable[f]!=null)
		{	
	*/	
		for (int c = 0; c < size1; c++)
		{
			if(hm.get(second.get(c).getKey())==null)
			//if(second.get(c).getKey()!=variable[f])	
			{
				//System.out.println("not match");
			}
			else
			{
				ArrayList<Object> keyvalues=hm.get(second.get(c).getKey());
				int d=0;
				while(d<keyvalues.size())
				{
					id=(String)keyvalues.get(d);
					char second1 = id.charAt(0);
					if(second1=='P') {
						prodid=id;
						custid1=second.get(c).getKey();
						custname=second.get(c).getValue();
						Object name=custname.get(0);
						Statement s=con.createStatement();
						String sql = "select *from customer where CUSTOMER_ID='"+custid1+"'";
		                ResultSet resultSet1 = s.executeQuery(sql);
		                if(resultSet1.next() == false)
		                {	
					    s.executeUpdate("INSERT INTO `customer` VALUE ('"+custid1+"','"+name+"')");
		                }
					}
					if(second1=='C')
					{
						custid1=id;
						prodid=second.get(c).getKey();
						prodcategories=second.get(c).getValue();
						prodname=(String) prodcategories.get(0);
						supid=(String) prodcategories.get(1);
						supname=(String) prodcategories.get(2);
						
						price=prodcategories.get(3);
						
						Statement s=con.createStatement();
						String sql = "select *from supplier where SUPPLIER_ID='"+supid+"'";
		                ResultSet resultSet1 = s.executeQuery(sql);
		                if(resultSet1.next() == false)
		                {	
					    s.executeUpdate("INSERT INTO supplier VALUE('"+supid+"','"+supname+"')");
		                }
						String sql1 = "select *from products where PRODUCT_ID='"+prodid+"'";
		                ResultSet resultSet11 = s.executeQuery(sql1);
		                if(resultSet11.next() == false)
		                {	
					    s.executeUpdate("INSERT INTO products VALUE('"+prodid+"','"+prodname+"')");
		                }
		                }
		 
					d=d+1;
					transid = keyvalues.get(d);
					
					d=d+1;
					storename1=(String) keyvalues.get(d);
					
					d=d+1;
					timeid1=(String) keyvalues.get(d);
					
					d=d+1;
					storeid=(String) keyvalues.get(d);
					
					d=d+1;
					date1 = keyvalues.get(d);
					
					d=d+1;
					quantity = keyvalues.get(d);

					d=d+1;
					
					if(price!=null)
					{	
					sales1=(double)price * (double)quantity;
					}
					else {
						sales1=0;
						}
					if(supid==null)
					{
						supid="SP-0";
					}
					
					Statement s=con.createStatement();
					String sql = "select *from location where STORE_ID='"+storeid+"'";
	                ResultSet resultSet1 = s.executeQuery(sql);
	                if(resultSet1.next() == false)
	                {	
				    s.executeUpdate("INSERT INTO location VALUE('"+storeid+"','"+storename1+"')");
	                }
				    
	                String sql1 = "select *from time1 where TIME_ID='"+timeid1+"'";
	                ResultSet resultSet11 = s.executeQuery(sql1);
	                if(resultSet11.next() == false)
	                {	
				    Statement s1=con.createStatement();
				    s1.executeUpdate("INSERT INTO time1 VALUE('"+timeid1+"','"+date1+"')");
	                }
	                

				    Statement s11=con.createStatement();
				    s11.executeUpdate("INSERT INTO tranformed VALUE('"+prodid+"','"+custid1+"','"+storeid+"','"+timeid1+"','"+quantity+"','"+supid+"','"+sales1+"')");
				}			 
				}
			}
	//	}
	//}
	con.close();
	}	 
	


public static void main(String[] args) {
      Connection con = null;
      Statement statement = null;
      int n=0;
      int i=0;
      String databasename1="db";
      
      try {
         Map<String, ArrayList<Object>> hm = new HashMap<String, ArrayList<Object>>();
         Map<String, ArrayList<Object>> hm1 = new HashMap<String, ArrayList<Object>>();
         ArrayList<String[]>array1= new ArrayList<String[]>(4);
         ArrayBlockingQueue<String[]> queue = new ArrayBlockingQueue<String[]>(2);
          
         Scanner myObj = new Scanner(System.in);  // Create a Scanner object
 	     System.out.println("Enter databasename");
 	     databasename = myObj.nextLine();
 	    
 	     System.out.println("Enter root(user)");
 	     user = myObj.nextLine();
 	    
 	     System.out.println("Enter password");
 	     password1 = myObj.nextLine();
 	     
 	     
		 
		 Properties connProperties;
		 connProperties = new Properties();
		 connProperties.setProperty("user", user);
		 connProperties.setProperty("password",password1);

		 Class.forName("com.mysql.cj.jdbc.Driver");
		 con = DriverManager.getConnection("jdbc:mysql://localhost:3306/" + databasename1, connProperties);
         
    //     Class.forName("com.mysql.cj.jdbc.Driver");
    //     con = DriverManager.getConnection("jdbc:mysql://localhost:3306/db", "root", "1111");
         statement = (Statement) con.createStatement();

         
         String name;
         String storeid;
         String id;
         String id1;
         String sql;
         String time1;
         String supid;
         String supname;
         String price;
         String transaction[]=new String[100 + 1];
         
         
         
         

                 sql = "select *from Customers";
                 ResultSet resultSet1 = statement.executeQuery(sql);
                 while (resultSet1.next()) {
                 id=resultSet1.getString("CUSTOMER_ID");
                 name=resultSet1.getString("CUSTOMER_NAME");
                 if(hm1.get(id)==null)
                 {
                	 hm1.put(id,new ArrayList<Object>());
                	 hm1.get(id).add(name);      	 
                 }
                 else {
                	  
                	 hm1.get(id).add(name); 
                 }
                 }
         
                 
                 sql = "select *from PRODUCTS";
                 ResultSet resultSet2 = statement.executeQuery(sql);
                 while (resultSet2.next()) {
                 id=resultSet2.getString("PRODUCT_ID");
                 name=resultSet2.getString("PRODUCT_NAME");
                 supid=resultSet2.getString("SUPPLIER_ID");
                 supname=resultSet2.getString("SUPPLIER_NAME");
                 double price1 = resultSet2.getDouble("PRICE");
                 
                 if(hm1.get(id)==null)
                 {
                	 hm1.put(id,new ArrayList<Object>());
                	 hm1.get(id).add(name);
                	 hm1.get(id).add(supid);
                	 hm1.get(id).add(supname);
                	 hm1.get(id).add(price1);
                 }
                 else {
                	 hm1.get(id).add(name); 
                	 hm1.get(id).add(supid);
                	 hm1.get(id).add(supname);
                	 hm1.get(id).add(price1);
                 }
                 }
                          
                 
                Set<Entry<String, ArrayList<Object>>> s = hm1.entrySet();
                List<Map.Entry<String, ArrayList<Object>>> array = new ArrayList<>(s); 
                int size = array.size();       
                // Creating two empty lists
                List<Map.Entry<String, ArrayList<Object>>> first = new ArrayList<>();
                List<Map.Entry<String, ArrayList<Object>>> second = new ArrayList<>();
         
                // Step 1
                // (First size)/2 element copy into list
                // first and rest second list
                for (int c = 0; c < size / 2; c++)
                    first.add(array.get(c));
         
                // Step 2
                // (Second size)/2 element copy into list first and
                // rest second list
                for (int c = size / 2; c < size; c++)
                    second.add(array.get(c));
                
                
               
                                 
         
         sql = "select *from Transactions";
         ResultSet resultSet = statement.executeQuery(sql);
         
         while (resultSet.next()) { 
         id=resultSet.getString("CUSTOMER_ID");
         id1=resultSet.getString("PRODUCT_ID");
         double transactionid1 = resultSet.getDouble("TRANSACTION_ID");
         storeid=resultSet.getString("STORE_ID");
         Date date11=resultSet.getDate("T_DATE");
         double quantity1 = resultSet.getDouble("QUANTITY");
         name=resultSet.getString("STORE_NAME");
         time1=resultSet.getString("TIME_ID");
         if(hm.get(id)==null)
         {
        	 hm.put(id,new ArrayList<Object>());
        	 hm.get(id).add(id1); 
        	 hm.get(id).add(transactionid1); 
        	 hm.get(id).add(name); 
        	 hm.get(id).add(time1);
        	 hm.get(id).add(storeid);
        	 hm.get(id).add(date11);
        	 hm.get(id).add(quantity1);
        	 transaction[i]=id;        	 
         }
         else {
        	 hm.get(id).add(id1);
        	 hm.get(id).add(transactionid1); 
        	 hm.get(id).add(name); 
        	 hm.get(id).add(time1); 
        	 hm.get(id).add(storeid);
        	 hm.get(id).add(date11);
        	 hm.get(id).add(quantity1);
         }
         if(hm.get(id1)==null)
         {
        	 hm.put(id1,new ArrayList<Object>());
        	 hm.get(id1).add(id);
        	 hm.get(id1).add(transactionid1); 
        	 hm.get(id1).add(name); 
        	 hm.get(id1).add(time1);
        	 hm.get(id1).add(storeid);
        	 hm.get(id1).add(date11);
         	 hm.get(id1).add(quantity1);
        	 transaction[i]=id1;        	 
         }
         else {
        	 hm.get(id1).add(id);
        	 hm.get(id1).add(transactionid1); 
        	 hm.get(id1).add(name); 
        	 hm.get(id1).add(time1); 
        	 hm.get(id1).add(storeid);
        	 hm.get(id1).add(date11);
        	 hm.get(id1).add(quantity1);
         }
   //     System.out.println("transaction "+transaction); 
        if(n==25)
        {
        queue.add(transaction);
        }
        if(n==50)
         {
         System.out.println("here in queue "+n);  	 
          queue.add(transaction);
         // System.out.println("queue "+queue.poll());
          myMethod(hm,queue,first,second);
          hm.clear();
          queue.poll();
          n=0;
          i=0;
         }
         n=n+1;	 
         i=i+1;
      }
 /*        String neha3[]=q.poll();
 //        System.out.println(neha3.length);
         for (int j = 0; j < neha3.length; j++) {
      	//   System.out.println(neha3[j]);
      	 }  */
         
         System.out.println("hashmap"+ hm);
      }
      catch (Exception e) {
         e.printStackTrace();
      }
   }
}