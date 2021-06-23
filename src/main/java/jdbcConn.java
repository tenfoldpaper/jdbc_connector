import com.google.common.base.Predicates;
import org.apache.commons.lang3.time.DateUtils;
import tech.tablesaw.api.ColumnType;
import tech.tablesaw.api.Table;
import tech.tablesaw.io.csv.CsvReadOptions;

import java.io.File;
import java.sql.*;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class jdbcConn {

        private static final List<String> dateColumns = Arrays.asList("AUGDT", "ZFBDT", "MADAT",
                "BLDAT", "BUDAT", "CPUDT",
                "CPUTM","AEDAT", "REINDAT", "ERDAT");
        private static final List<String> floatColumns = Arrays.asList("WRBTR", "DMBTR");
        private static final List<String> intColumns = Arrays.asList("GJAHR", "AUGGJ", "MONAT");

        public static void main(String[] args){

            String connectionUrl = "jdbc:sqlserver://DESKTOP-26DMNN8\\TEW_SQLEXPRESS;" +
                    "databaseName=Duplid_SAP;" +
                    "integratedSecurity=true;";

            Connection con;
            //PreparedStatement pst = null;
            try{
                con = DriverManager.getConnection(connectionUrl);
                con.setAutoCommit(false);
                Statement stmt = con.createStatement();
                String SQL = "SELECT TOP 10 * FROM BSEG";
                ResultSet rs  = stmt.executeQuery(SQL);

                while(rs.next()){
                    System.out.println(rs.getString("BELNR"));
                }
            }
            catch(SQLException e){
                e.printStackTrace();
                return;
            }

            String[] tableNames = {"LFA1", "TCURF", "TCURR", "TCURX"};

            for (String tableName : tableNames){

                try{
                    String pathName = "C:\\Users\\pc\\Documents\\GitHub\\jdbc_connector\\csv_files\\";
                    File dir = new File(pathName + tableName);
                    String baseFile = pathName + tableName + "\\" + tableName + "_base.csv";
                    String[] fileNames= showFiles(dir.listFiles());
                    if(fileNames == null){
                        continue;
                    }
                    for (String fileName : fileNames) {
                        if(fileName.contains("_base.csv")){
                            continue;
                        }
                        Table initTable = Table.read().file(baseFile);
                        int colNumbers = initTable.columnCount();
                        String[] colNames = initTable.columnNames().toArray(new String[0]);

                        if(tableExistsSQL(con, tableName)){ // if table exists, compare its columns with the new dataset
                            Statement stmt = con.createStatement();
                            ResultSet rs = stmt.executeQuery("select * from " + tableName);
                            ResultSetMetaData rsMetaData = rs.getMetaData();
                            List<String> dbTableColsList = new ArrayList<String>();
                            for(int i = 1; i <= rsMetaData.getColumnCount(); i++){
                                dbTableColsList.add(rsMetaData.getColumnName(i));
                            }

                            // we must do 2 checks to verify identity:
                            // 1. Are all columns equal?
                            // 2. Is the order the same?
                            // And we want to modify the table in the DB to match the incoming file's format.
                            String[] dbTableCols = dbTableColsList.toArray(new String[0]);

                            boolean colLengthEquality = dbTableCols.length == colNumbers; // check if they have same # of columns
                            boolean colContentEquality = true;

                            if(colLengthEquality){
                                for(int i = 0; i < colNumbers; i++){
                                    if(dbTableCols[i] != colNames[i]){
                                        colContentEquality = false;
                                    }
                                }
                            }

                            if(!colContentEquality || !colLengthEquality){ //if the two tables are not exactly the same

                                // Create a temporary table and copy the old content over
                                boolean csvIsSubset = dbTableColsList.containsAll(initTable.columnNames());
                                Statement genericStmt = con.createStatement();
                                if(csvIsSubset){ // the db table already contains all the columns of the new csv file
                                                 // so SELECT INTO is possible
                                    String selectInto = "SELECT ";
                                    for(int i = 0; i < colNumbers; i++){
                                        selectInto += colNames[i] + ", ";
                                    }
                                    // get rid of the final ", "
                                    selectInto = selectInto.substring(0, selectInto.length()-2);
                                    selectInto += " INTO " + tableName + "_temp" + " FROM " + tableName + ";";
                                    genericStmt.executeUpdate(selectInto);

                                }

                                else{ // the db table doesn't contain all the columns, meaning we need to recreate the table from scratch.
                                    String createTable = "CREATE TABLE " + tableName + "_temp(";
                                    for(int i = 0; i < colNumbers; i++){
                                        String dataTypeString;
                                        if(intColumns.contains(colNames[i])){
                                            dataTypeString = "int null, ";
                                        }
                                        else if( floatColumns.contains(colNames[i]) ){
                                            dataTypeString = "float null, ";
                                        }
                                        else if (dateColumns.contains(colNames[i])) {
                                            dataTypeString = "datetime2 null, ";
                                        }
                                        else{
                                            dataTypeString = "nvarchar(50) null, ";
                                        }
                                        createTable += colNames[i] + " " + dataTypeString;

                                    }
                                    createTable += ");";
                                    genericStmt.executeUpdate(createTable);

                                    // check what columns we can still use from the old table
                                    String existingColsString = "";
                                    for(int i = 0; i < dbTableCols.length; i++) {
                                        if (initTable.columnNames().contains(dbTableCols[i])){
                                            existingColsString += dbTableCols[i] + ", ";
                                        }
                                    }
                                    // get rid of the final ", "
                                    existingColsString = existingColsString.substring(0, existingColsString.length()-2);

                                    String insertString = "INSERT INTO " + tableName + "_temp (" + existingColsString + ") ";
                                    insertString += "SELECT " + existingColsString + " ";
                                    insertString += "FROM " + tableName + ";";
                                    genericStmt.executeUpdate(insertString);

                                }

                                // delete the old table
                                genericStmt.executeUpdate("DROP TABLE " + tableName + ";");
                                // rename the new table

                                // MySQL's way of renaming
                                //genericStmt.executeUpdate("RENAME TABLE " + tableName + "_temp TO " + tableName + ";");

                                // SQL Server's way of renaming
                                genericStmt.executeUpdate("EXEC sp_rename 'dbo." + tableName + "_temp', '" + tableName+"';");
                                //con.commit();
                                System.out.println("That didn't crash");
                                // we can now insert the csv file's content into the table
                            }

                        }


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

                                    for(int i = 0; i < colNumbers; i++){
                                        String currString = "?";
                                        sqlStatement += currString + ",";
                                    }
                                    sqlStatement = sqlStatement.substring(0, sqlStatement.length() - 1); // get rid of last , symbol
                                    sqlStatement += ")";
                                    try {
                                        PreparedStatement pst = con.prepareStatement(sqlStatement);
                                        for(int i = 1; i <= colNumbers; i++){
                                            String currString = row.getString(i-1).trim();
                                            String currColumn = colNames[i-1];
                                            if(currString == "" || currString == null || currString.contains("00000000")){
                                                pst.setObject(i, null);
                                            }
                                            else if(intColumns.contains(currColumn)){
                                                pst.setInt(i, Integer.parseInt(currString));
                                            }
                                            else if( floatColumns.contains(currColumn) ){
                                                pst.setFloat(i, Float.parseFloat(currString));
                                            }
                                            else if (dateColumns.contains(currColumn)){
                                                if(currString.length() == 8){ // SAP YYYYMMDD Format
                                                    DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyyMMdd");
                                                    LocalDate currDate = LocalDate.parse(currString, format);
                                                    pst.setObject(i, currDate);
                                                }
                                                else if(currString.length() == 14) { // SAP YYYYMMDDhhmmss format
                                                    DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
                                                    LocalDateTime currDate = LocalDateTime.parse(currString, format);
                                                    pst.setObject(i, currDate);
                                                } // SAP YYYYMMDDhhmmss,mmmuuun format needs to be added as 4th option
                                                else{
                                                    DateTimeFormatter format = DateTimeFormatter.ofPattern("HHmmss");
                                                    LocalTime currDate = LocalTime.parse(currString, format);
                                                    pst.setObject(i, currDate);
                                                }
                                            }
                                            else{
                                                pst.setString(i, currString);
                                            }

                                        }
                                        pst.executeUpdate();
                                        pst.close();

                                    }
                                    catch(SQLException e){
                                        e.printStackTrace();
                                    }
                                    catch(DateTimeException e){
                                        e.printStackTrace();
                                    }
                                    catch(Exception e){
                                        e.printStackTrace();
                                    }

                                }
                        );

                    }
                }
                catch(Exception e){
                    e.printStackTrace();
                }

            }
            try {
                con.commit();
                con.close();
            }
            catch(SQLException e){
                e.printStackTrace();
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

    static boolean tableExistsSQL(Connection connection, String tableName) throws SQLException {
        PreparedStatement preparedStatement = connection.prepareStatement("SELECT count(*) "
                + "FROM information_schema.tables "
                + "WHERE table_name = ?;" );
        preparedStatement.setString(1, tableName);

        ResultSet resultSet = preparedStatement.executeQuery();
        resultSet.next();
        System.out.println(resultSet.getInt(1));
        return resultSet.getInt(1) != 0;
    }

}

