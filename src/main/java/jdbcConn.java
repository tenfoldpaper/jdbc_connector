import tech.tablesaw.api.ColumnType;
import tech.tablesaw.api.Table;
import tech.tablesaw.io.csv.CsvReadOptions;
import tech.tablesaw.io.csv.CsvReader;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;


public class jdbcConn {

        public static void main(String[] args){

            String connectionUrl = "jdbc:sqlserver://DESKTOP-0RSSPCU\\SQLEXPRESS;" +
                    "databaseName=DuplID_db1;" +
                    "integratedSecurity=true;";

            Connection con;
            PreparedStatement pst = null;
            try{
                con = DriverManager.getConnection(connectionUrl);
                con.setAutoCommit(false);
//                Statement stmt = con.createStatement();
//                String SQL = "SELECT TOP 10 * FROM BSEG";
//                ResultSet rs  = stmt.executeQuery(SQL);

//                while(rs.next()){
//                    System.out.println(rs.getString("BELNR"));
//                }
            }
            catch(SQLException e){
                e.printStackTrace();
                return;
            }

            String[] tableNames = {"BSEG", "BKPF", "LFA1", "TCURF", "TCURR", "TCURX"};

            for (String tableName : tableNames){

                try{
                    File dir = new File("C:\\Users\\Jin-MainPC\\IdeaProjects\\jdbc11\\csv_files\\" + tableName);
                    String[] fileNames= showFiles(dir.listFiles());
                    for (String fileName : fileNames) {

                        Table initTable = Table.read().file(fileName);
                        int colNumbers = initTable.columnCount();
                        String[] colNames = initTable.columnNames().toArray(new String[0]);
                        ColumnType[] cTypes = initTable.columnTypes();
                        for(int c = 0; c < cTypes.length; c++){
                            cTypes[c] = ColumnType.STRING;
                        }

                        CsvReadOptions builder =
                                CsvReadOptions.builder(fileName)
                                        .columnTypes(cTypes)
                                        .build();
                        Table csvTable = Table.read().csv(builder);


                        csvTable.stream().forEach(
                                row -> {
                                    String sqlStatement = "INSERT INTO " + tableName + " VALUES (";

                                    List<Integer> nullPositions = new ArrayList<Integer>();


                                    for(int i = 0; i < colNumbers; i++){
                                        String currString = row.getString(i);
                                        if(currString == "" || currString == null){
                                            nullPositions.add(i + 1); // save the position of those columns with null values.
                                            currString = "?";
                                        }
                                        // do comparison for whether the column type is int, date, float, or just str.
                                        sqlStatement += "'" + currString + "'" + ",";

                                    }
                                    // extract the values, line by line, from the csv file
                                    sqlStatement = sqlStatement.substring(0, sqlStatement.length() - 1); // get rid of last , symbol
                                    sqlStatement += ")";
                                    try {
                                        con.prepareStatement(sqlStatement);
                                        Integer[] nullPos = new Integer[ nullPositions.size() ];
                                        nullPositions.toArray(nullPos);

                                        if(nullPos.length > 0){
                                            // loop through the null positions and set them properly
                                            for(int n = 0; n < nullPos.length; n++){
                                                pst.setString(nullPos[n], null);
                                            }
                                        }

                                    }
                                    catch(SQLException e){
                                        e.printStackTrace();
                                        return;
                                    }
                                    if(true){
                                        return;
                                    }
                                }
                        );

                    }
                }
                catch(Exception e){
                    e.printStackTrace();
                }

            }

        }

        public static String[] showFiles(File[] files) {

            List<String> fileNames = new ArrayList<String>();
            for (File file : files) {
                if (file.isDirectory()) {
                    System.out.println("Directory: " + file.getAbsolutePath());
                }
                else{
                    fileNames.add(file.getAbsolutePath());
                }
            }
            String[] result = new String[ fileNames.size() ];
            fileNames.toArray(result);
            return result;
        }

    }

