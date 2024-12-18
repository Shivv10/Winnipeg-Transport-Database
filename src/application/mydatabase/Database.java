package application.mydatabase;


import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

public class Database {
    private Connection connection;
    private static final String regex = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";
    private final String cfgFilePath = "src/application/auth.cfg";

    public Database() {

    }

    /**
     * This method will establish the connection to the uranium database.
     * If you are not on campus wifi, you must be connect to UofM VPN
     * Driver file according to the java version is requied, auth.cfg file to the
     * root director this project is also required
     *
     * @return null on success, or an erro message if something went wrong
     */
    public String startup() {
        String response = null;

        Properties prop = new Properties();

        try {
            FileInputStream configFile = new FileInputStream(cfgFilePath);
            prop.load(configFile);
            configFile.close();
            final String username = (prop.getProperty("username"));
            final String password = (prop.getProperty("password"));

            String url = "jdbc:sqlserver://uranium.cs.umanitoba.ca:1433;"
                    + "database=cs3380;"
                    + "user=" + username + ";"
                    + "password= " + password + ";"
                    + "encrypt=false;trustServerCertificate=false;loginTimeout=30;";

            this.connection = DriverManager.getConnection(url);

        } catch (FileNotFoundException fnf) {
            response = "\nAn error occurred: config file not found.";
        } catch (IOException io) {
            response = "\nAn error occurred: could not read config file.";
        } catch (SQLException se) {
            se.printStackTrace();
            response = "\nAn error occured: Failed to establish connection to database";
        }

        return response;
    }

    public String initializeDatabase() {
        String response;

        response = dropAllTables();

        if (response == null) {
            response = createAllTables();
        }

        return response;
    }

    private String createAllTables() {
        String response = null;
    
        try {
            System.out.println("Starting table creation...");
    
            System.out.println("Creating Streets table...");
            insertIntoStreet();
            System.out.println("Streets table created successfully.");

            System.out.println("Creating Stops table...");
            insertIntoStop();
            System.out.println("Stops table created successfully.");
    
            System.out.println("Creating Routes table...");
            insertIntoRoutes();
            System.out.println("Routes table created successfully.");
    
            System.out.println("Creating SpeedLimits table...");
            insertIntoSpeedLimits();
            System.out.println("SpeedLimits table created successfully.");
    
            System.out.println("Creating CyclingNetwork table...");
            insertIntoCyclingNetwork();
            System.out.println("CyclingNetwork table created successfully.");
    
            System.out.println("Creating TrafficCounts table...");
            insertIntoTrafficCount();
            System.out.println("TrafficCounts table created successfully.");
    
            System.out.println("Creating PassUps table...");
            insertIntoPassUps();
            System.out.println("PassUps table created successfully.");
    
            System.out.println("Creating PassengerActivity table...");
            insertIntoPassengerActivity();
            System.out.println("PassengerActivity table created successfully.");

            System.out.println("Creating TransitPerformance table...");
            insertIntoTransitPerformance();
            System.out.println("TransitPerformance table created successfully.");

        } catch (SQLException se) {
            se.printStackTrace();
            response = "SQL Exception: " + se.getMessage();
            response += "\nErasing the whole database";
            dropAllTables();
        } catch (IOException io) {
            io.printStackTrace();
            response = "IO Exception: " + io.getMessage();
            response += "\nErasing the whole database";
            dropAllTables();
        }
    
        return response;
    }
    

    private void insertIntoCyclingNetwork() throws SQLException, IOException {
        try {
            // Create the CyclingNetwork table
            this.connection.createStatement().executeUpdate("CREATE TABLE CyclingNetwork ("
                    + "cyclePathID INT IDENTITY(1,1) PRIMARY KEY, " // Use IDENTITY for auto-increment
                    + "startStreetID INT, "
                    + "endStreetID INT, "
                    + "infrastructureType VARCHAR(MAX), " // Use VARCHAR(MAX) instead of TEXT
                    + "infrastructureName VARCHAR(MAX), "
                    + "roadLocation VARCHAR(MAX), "
                    + "twoWayTravel INT, "
                    + "length INT, "
                    + "FOREIGN KEY(startStreetID) REFERENCES Streets(streetID), "
                    + "FOREIGN KEY(endStreetID) REFERENCES Streets(streetID))");
    
            // Reading data from the CSV file
            BufferedReader br = new BufferedReader(new FileReader("final-data-files/Cycling-Network.csv"));
            PreparedStatement pstmt;
            String inputLine;
            String sql;
            String[] inputArr;
    
            br.readLine(); // Skip the headers
    
            while ((inputLine = br.readLine()) != null) {
                inputArr = inputLine.split(regex);

                if(inputArr[5].equals("TRUE")){
                    inputArr[5] = "1";
                }
                else{
                    inputArr[5] = "0";
                }
    
                // Prepare SQL statement to insert data
                sql = "INSERT INTO CyclingNetwork (startStreetID, endStreetID, infrastructureType, infrastructureName, roadLocation, twoWayTravel, length) "
                        + "VALUES (?, ?, ?, ?, ?, ?, ?)";
                pstmt = connection.prepareStatement(sql);
    
                pstmt.setInt(1, Integer.parseInt(inputArr[0])); // startStreetID
                pstmt.setInt(2, Integer.parseInt(inputArr[1])); // endStreetID
                pstmt.setString(3, inputArr[2]); // infrastructureType
                pstmt.setString(4, inputArr[3]); // infrastructureName
                pstmt.setString(5, inputArr[4]); // roadLocation
                pstmt.setInt(6, Integer.parseInt(inputArr[5])); // twoWayTravel
                pstmt.setInt(7, Integer.parseInt(inputArr[6])); // length
    
                pstmt.executeUpdate(); // Execute the insertion
            }
    
            br.close();
        } catch (IOException io) {
            throw new IOException("An error occurred: Cannot read cycling-path.csv or file does not exist");
        } catch (SQLException se) {
            se.printStackTrace();
            throw new SQLException("An error occurred: Cannot create or insert into CyclingNetwork table");
        }
    }
    

    private void insertIntoTransitPerformance() throws SQLException, IOException {
        try {
            // Create the TransitPerformance table
            this.connection.createStatement().executeUpdate("CREATE TABLE TransitPerformance ("
                    + "performanceID INT IDENTITY(1,1) PRIMARY KEY, "
                    + "routeID INT, "
                    + "stopID INT, "
                    + "routeDestination VARCHAR(MAX), "
                    + "dayType VARCHAR(MAX), "
                    + "scheduledDate DATE, "
                    + "scheduledTime TIME, "
                    + "deviation INT, "
                    + "FOREIGN KEY(routeID) REFERENCES Routes(routeID), "
                    + "FOREIGN KEY(stopID) REFERENCES Stops(stopID))");
    
            // Reading data from the CSV file
            BufferedReader br = new BufferedReader(new FileReader("final-data-files/Transit-Performance.csv"));
            PreparedStatement pstmt;
            String inputLine;
            String sql;
            String[] inputArr;
    
            br.readLine(); // Skip the headers
    
            // Define date and time format for parsing
            SimpleDateFormat dateTimeFormat = new SimpleDateFormat("MM/dd/yyyy, hh:mm:ss");
    
            while ((inputLine = br.readLine()) != null) {
                inputArr = inputLine.split(","); // Assuming CSV is comma-separated
    
                // Trim inputs
                String datePart = inputArr[4].trim();
                String timePart = inputArr[5].trim();
    
                // Parse the date and time
                Date parsedDateTime = dateTimeFormat.parse(datePart + ", " + timePart);
                java.sql.Date sqlDate = new java.sql.Date(parsedDateTime.getTime());
                java.sql.Time sqlTime = new java.sql.Time(parsedDateTime.getTime());
    
                // Parse numeric values
                int deviation = Integer.parseInt(inputArr[6].trim());

                // Prepare SQL statement to insert data
                sql = "INSERT INTO TransitPerformance (routeID, stopID, routeDestination, dayType, scheduledDate, scheduledTime, deviation) "
                        + "VALUES (?, ?, ?, ?, ?, ?, ?)";
                pstmt = connection.prepareStatement(sql);
    
                pstmt.setInt(1, Integer.parseInt(inputArr[0].trim())); // routeID
                pstmt.setInt(2, Integer.parseInt(inputArr[1].trim())); // stopID
                pstmt.setString(3, inputArr[2].trim()); // routeDestination
                pstmt.setString(4, inputArr[3].trim()); // dayType
                pstmt.setDate(5, sqlDate); // scheduledDate
                pstmt.setTime(6, sqlTime); // scheduledTime
                pstmt.setInt(7, deviation); // deviation
    
                pstmt.executeUpdate(); // Execute the insertion
            }
    
            br.close();
        } catch (IOException io) {
            throw new IOException("An error occurred: Cannot read Transit-Performance.csv or file does not exist");
        } catch (ParseException pe) {
            throw new IOException("An error occurred: Invalid date/time format in CSV file");
        } catch (SQLException se) {
            System.out.println("Error creating or populating TransitPerformance table: " + se.getMessage());
            throw se;
        }
    }
     

    private void insertIntoPassUps() throws SQLException, IOException {
        try {
            // Create the PassUps table
            this.connection.createStatement().executeUpdate("CREATE TABLE PassUps ("
                    + "passUpID INT IDENTITY(1,1) PRIMARY KEY, " // Use IDENTITY for auto-increment
                    + "routeID INT, "
                    + "streetID INT, "
                    + "passUpType VARCHAR(MAX), " // Use VARCHAR(MAX) instead of TEXT
                    + "date DATE, "
                    + "dayType VARCHAR(MAX), "
                    + "FOREIGN KEY(routeID) REFERENCES Routes(routeID), "
                    + "FOREIGN KEY(streetID) REFERENCES Streets(streetID))");
    
            // Reading data from the CSV file
            BufferedReader br = new BufferedReader(new FileReader("final-data-files/Pass-Ups.csv"));
            PreparedStatement pstmt;
            String inputLine;
            String sql;
            String[] inputArr;
    
            br.readLine(); // Skip the headers

            // Define the date format for parsing
            SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
    
            while ((inputLine = br.readLine()) != null) {
                inputArr = inputLine.split(","); // Assuming CSV is comma-separated
                
                Date date = dateFormat.parse(inputArr[3]);
    
                // Convert dates to java.sql.Date
                java.sql.Date sqlDate = new java.sql.Date(date.getTime());
    
                // Prepare SQL statement to insert data
                sql = "INSERT INTO PassUps (routeID, streetID, passUpType, date, dayType) "
                        + "VALUES (?, ?, ?, ?, ?)";
    
                pstmt = connection.prepareStatement(sql);
                pstmt.setInt(1, Integer.parseInt(inputArr[0])); // routeID
                pstmt.setInt(2, Integer.parseInt(inputArr[1])); // streetID
                pstmt.setString(3, inputArr[2]); // passUpType
                pstmt.setDate(4, sqlDate);
                pstmt.setString(5, inputArr[4]); // dayType
                pstmt.executeUpdate(); // Execute the insertion
            }
    
            br.close();
        } catch (IOException io) {
            throw new IOException("An error occurred: Cannot read Pass-Ups.csv or file does not exist");
        } catch (ParseException pe) {
            throw new IOException("An error occurred: Invalid date format in Pass-Ups.csv");
        } catch (SQLException se) {
            se.printStackTrace();
            throw new SQLException("An error occurred: Cannot create or insert into PassUps table");
        }
    }
    

    private void insertIntoPassengerActivity() throws SQLException, IOException {
        try {
            // Create the PassengerActivity table
            this.connection.createStatement().executeUpdate("CREATE TABLE PassengerActivity ("
                    + "activityID INT IDENTITY(1,1) PRIMARY KEY, " // Use IDENTITY for auto-increment
                    + "routeID INT, "
                    + "stopID INT, "
                    + "schedulePeriodName VARCHAR(MAX), " // Use VARCHAR(MAX) instead of TEXT
                    + "schedulePeriodStartDate DATE, " // Use DATE for start date
                    + "schedulePeriodEndDate DATE, " // Use DATE for end date
                    + "dayType VARCHAR(MAX), "
                    + "timePeriod VARCHAR(MAX), "
                    + "averageBoardings FLOAT, " // Use FLOAT for numeric values
                    + "averageAlightings FLOAT, "
                    + "FOREIGN KEY(routeID) REFERENCES Routes(routeID), "
                    + "FOREIGN KEY(stopID) REFERENCES Stops(stopID))");
    
            // Reading data from the CSV file
            BufferedReader br = new BufferedReader(new FileReader("final-data-files/Passenger-Activity.csv"));
            PreparedStatement pstmt;
            String inputLine;
            String sql;
            String[] inputArr;
    
            br.readLine(); // Skip headers
    
            // Define the date format for parsing
            SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
    
            while ((inputLine = br.readLine()) != null) {
                inputArr = inputLine.split(","); // Assuming CSV is comma-separated
    
                // Parse the start and end dates
                Date startDate = dateFormat.parse(inputArr[3]);
                Date endDate = dateFormat.parse(inputArr[4]);
    
                // Convert dates to java.sql.Date
                java.sql.Date sqlStartDate = new java.sql.Date(startDate.getTime());
                java.sql.Date sqlEndDate = new java.sql.Date(endDate.getTime());
    
                // Prepare SQL statement to insert data
                sql = "INSERT INTO PassengerActivity (routeID, stopID, schedulePeriodName, schedulePeriodStartDate, "
                        + "schedulePeriodEndDate, dayType, timePeriod, averageBoardings, averageAlightings) "
                        + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
    
                pstmt = connection.prepareStatement(sql);
                pstmt.setInt(1, Integer.parseInt(inputArr[0])); // routeID
                pstmt.setInt(2, Integer.parseInt(inputArr[1])); // stopID
                pstmt.setString(3, inputArr[2]); // schedulePeriodName
                pstmt.setDate(4, sqlStartDate); // schedulePeriodStartDate
                pstmt.setDate(5, sqlEndDate); // schedulePeriodEndDate
                pstmt.setString(6, inputArr[5]); // dayType
                pstmt.setString(7, inputArr[6]); // timePeriod
                pstmt.setDouble(8, Double.parseDouble(inputArr[7])); // averageBoardings
                pstmt.setDouble(9, Double.parseDouble(inputArr[8])); // averageAlightings
                pstmt.executeUpdate(); // Execute insertion
            }
    
            br.close();
        } catch (IOException io) {
            throw new IOException("An error occurred: Cannot read Passenger-Activity.csv or file does not exist");
        } catch (ParseException pe) {
            throw new IOException("An error occurred: Invalid date format in Passenger-Activity.csv");
        } catch (SQLException se) {
            se.printStackTrace();
            throw new SQLException("An error occurred: Cannot create or insert into PassengerActivity table");
        }
    }


    private void insertIntoRoutes() throws SQLException, IOException {
        try {
            // Create the Routes table
            this.connection.createStatement().executeUpdate("CREATE TABLE Routes ("
                    + "routeID INT IDENTITY(1,1) PRIMARY KEY, " // Use IDENTITY for auto-increment
                    + "routeNumber VARCHAR(MAX), "
                    + "routeName VARCHAR(MAX))"); // Use VARCHAR(MAX) instead of TEXT

            // Reading data from the CSV file
            BufferedReader br = new BufferedReader(new FileReader("final-data-files/Routes.csv"));
            PreparedStatement pstmt;
            String inputLine;
            String sql;
            String[] inputArr;

            br.readLine(); // Skip headers

            while ((inputLine = br.readLine()) != null) {
                inputArr = inputLine.split(","); // Assuming CSV is comma-separated

                // Prepare SQL statement to insert data
                sql = "INSERT INTO Routes (routeNumber, routeName) VALUES (?, ?)";

                pstmt = connection.prepareStatement(sql);
                pstmt.setString(1, inputArr[1]); // routeNumber
                pstmt.setString(2, inputArr[2]); // routeName
                pstmt.executeUpdate(); // Execute insertion
            }

            br.close();
        } catch (IOException io) {
            throw new IOException("An error occurred: Cannot read Routes.csv or file does not exist");
        } catch (SQLException se) {
            se.printStackTrace();
            throw new SQLException("An error occurred: Cannot create or insert into Routes table");
        }
    }



    private void insertIntoStop() throws SQLException, IOException {
        try {
            // Create the Stops table
            this.connection.createStatement().executeUpdate("CREATE TABLE Stops ("
                    + "stopID INT IDENTITY(1,1) PRIMARY KEY, " // Use IDENTITY for auto-increment
                    + "streetID INT, "
                    + "stopNumber INT, "
                    + "FOREIGN KEY(streetID) REFERENCES Streets(streetID))");
    
            BufferedReader br = new BufferedReader(new FileReader("final-data-files/Stops.csv"));
            PreparedStatement pstmt;
            String inputLine;
            String sql;
            String[] inputArr;
    
            br.readLine(); // Skip headers
    
            while ((inputLine = br.readLine()) != null) {
                inputArr = inputLine.split(",");
    
                sql = "INSERT INTO Stops (streetID, stopNumber) VALUES (?, ?)";

                pstmt = connection.prepareStatement(sql);
                pstmt.setInt(1, Integer.parseInt(inputArr[1])); // streetID
                pstmt.setInt(2, Integer.parseInt(inputArr[2])); // stopNumber
                pstmt.executeUpdate(); // Execute insertion
            }
    
            br.close();
        } catch (IOException io) {
            throw new IOException("An error occurred: Cannot read Stops.csv or file does not exist");
        } catch (SQLException se) {
            se.printStackTrace();
            throw new SQLException("An error occurred: Cannot create or insert into Stops table");
        }
    }
    


    private void insertIntoStreet() throws SQLException, IOException {
        try {
            // Create the Streets table
            this.connection.createStatement().executeUpdate("CREATE TABLE Streets ("
                    + "streetName VARCHAR(MAX), " // Use VARCHAR(MAX) instead of TEXT
                    + "streetID INT IDENTITY(1,1) PRIMARY KEY)"); // Use IDENTITY for auto-increment
    
            BufferedReader br = new BufferedReader(new FileReader("final-data-files/Street.csv"));
            PreparedStatement pstmt;
            String inputLine;
            String sql;
            String[] inputArr;
    
            br.readLine(); // Skip headers
    
            while ((inputLine = br.readLine()) != null) {
                inputArr = inputLine.split(",");
    
                sql = "INSERT INTO Streets (streetName) VALUES (?)";
    
                pstmt = connection.prepareStatement(sql);
                pstmt.setString(1, inputArr[1]); // streetName
                pstmt.executeUpdate();
            }
    
            br.close();
        } catch (IOException io) {
            throw new IOException("An error occurred: Cannot read Street.csv or file does not exist");
        } catch (SQLException se) {
            se.printStackTrace();
            throw new SQLException("An error occurred: Cannot insert into Streets table");
        }
    }
    


    private void insertIntoTrafficCount() throws SQLException, IOException {
        try {
            // Create the TrafficCounts table
            this.connection.createStatement().executeUpdate("CREATE TABLE TrafficCounts ("
                    + "countID INT IDENTITY(1,1) PRIMARY KEY, " // Use IDENTITY for auto-increment
                    + "streetID INT, "
                    + "countDate DATE, " // Use DATE for countDate
                    + "dayType VARCHAR(MAX), " // Use VARCHAR(MAX) instead of TEXT
                    + "locationDescription VARCHAR(MAX), "
                    + "countDirection VARCHAR(MAX), "
                    + "count15Minutes INT, "
                    + "Configuration VARCHAR(MAX), "
                    + "streetFrom VARCHAR(MAX), "
                    + "streetTo VARCHAR(MAX), "
                    + "FOREIGN KEY(streetID) REFERENCES Streets(streetID))");
    
            BufferedReader br = new BufferedReader(new FileReader("final-data-files/Traffic-Counts.csv"));
            PreparedStatement pstmt;
            String inputLine;
            String sql;
            String[] inputArr;
    
            br.readLine(); // Skip headers

            SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
    
            while ((inputLine = br.readLine()) != null) {
                inputArr = inputLine.split(",");

                Date countDate = dateFormat.parse(inputArr[2]);

                java.sql.Date sqlCountDate = new java.sql.Date(countDate.getTime());

                sql = "INSERT INTO TrafficCounts (streetID, countDate, dayType, locationDescription, countDirection, "
                        + "count15Minutes, Configuration, streetFrom, streetTo) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
    
                pstmt = connection.prepareStatement(sql);
                pstmt.setInt(1, Integer.parseInt(inputArr[1])); // streetID
                pstmt.setDate(2, sqlCountDate); // countDate
                pstmt.setString(3, inputArr[3]); // dayType
                pstmt.setString(4, inputArr[4]); // locationDescription
                pstmt.setString(5, inputArr[5]); // countDirection
                pstmt.setInt(6, Integer.parseInt(inputArr[6])); // count15Minutes
                pstmt.setString(7, inputArr[7]); // Configuration
                pstmt.setString(8, inputArr[8]); // streetFrom
                pstmt.setString(9, inputArr[9]); // streetTo
                pstmt.executeUpdate();
            }
    
            br.close();
        } catch (IOException io) {
            throw new IOException("An error occurred: Cannot read Traffic_Counts.csv or file does not exist");
        } catch (ParseException pe) {
            throw new IOException("An error occurred: Invalid date format in Passenger-Activity.csv");
        } catch (SQLException se) {
            se.printStackTrace();
            throw new SQLException("An error occurred: Cannot insert into TrafficCounts table");
        }
    }
    

    private void insertIntoSpeedLimits() throws SQLException, IOException {
        try {
            // Create the SpeedLimits table
            this.connection.createStatement().executeUpdate("CREATE TABLE SpeedLimits ("
                    + "streetID INT, "
                    + "speedLimit INT, "
                    + "speedLimitDesc VARCHAR(MAX), " // Use VARCHAR(MAX) instead of TEXT
                    + "beginMeasure FLOAT, " // Use FLOAT for numeric values
                    + "endMeasure FLOAT, " // Use FLOAT for numeric values
                    + "jurisdiction VARCHAR(MAX), " // Use VARCHAR(MAX) instead of TEXT
                    + "FOREIGN KEY(streetID) REFERENCES Streets(streetID))");
    
            BufferedReader br = new BufferedReader(new FileReader("final-data-files/Speed-Limits.csv"));
            PreparedStatement pstmt;
            String inputLine;
            String sql;
            String[] inputArr;
    
            br.readLine(); // Skip headers
    
            while ((inputLine = br.readLine()) != null) {
                inputArr = inputLine.split(",");
    
                sql = "INSERT INTO SpeedLimits (streetID, speedLimit, speedLimitDesc, beginMeasure, endMeasure, jurisdiction) "
                        + "VALUES (?, ?, ?, ?, ?, ?)";

                if(inputArr[4].equals(""))
                    inputArr[4] = "50";
    
                pstmt = connection.prepareStatement(sql);
                pstmt.setInt(1, Integer.parseInt(inputArr[0])); // streetID
                pstmt.setInt(2, Integer.parseInt(inputArr[4])); // speedLimit
                pstmt.setString(3, inputArr[5]); // speedLimitDesc
                pstmt.setDouble(4, Double.parseDouble(inputArr[2])); // beginMeasure
                pstmt.setDouble(5, Double.parseDouble(inputArr[3])); // endMeasure
                pstmt.setString(6, inputArr[6]); // jurisdiction
                pstmt.executeUpdate();
            }
    
            br.close();
        } catch (IOException io) {
            throw new IOException("An error occurred: Cannot read Speed-Limits.csv or file does not exist");
        } catch (SQLException se) {
            se.printStackTrace();
            throw new SQLException("An error occurred: Cannot insert into SpeedLimits table");
        }
    }
    


    public String dropAllTables() {
        String response = null;
        try {
            // Dropping each table based on new schema
            PreparedStatement pstmt;

            pstmt = connection.prepareStatement("DROP TABLE IF EXISTS TransitPerformance;");
            pstmt.executeUpdate();

            pstmt = connection.prepareStatement("DROP TABLE IF EXISTS PassengerActivity;");
            pstmt.executeUpdate();

            pstmt = connection.prepareStatement("DROP TABLE IF EXISTS PassUps;");
            pstmt.executeUpdate();

            pstmt = connection.prepareStatement("DROP TABLE IF EXISTS TrafficCounts;");
            pstmt.executeUpdate();

            pstmt = connection.prepareStatement("DROP TABLE IF EXISTS Stops;");
            pstmt.executeUpdate();

            pstmt = connection.prepareStatement("DROP TABLE IF EXISTS SpeedLimits;");
            pstmt.executeUpdate();

            pstmt = connection.prepareStatement("DROP TABLE IF EXISTS Routes;");
            pstmt.executeUpdate();

            pstmt = connection.prepareStatement("DROP TABLE IF EXISTS CyclingNetwork;");
            pstmt.executeUpdate();

            pstmt = connection.prepareStatement("DROP TABLE IF EXISTS Streets;");
            pstmt.executeUpdate();


            System.out.println("All tables dropped successfully.");
        } catch (SQLException se) {
            System.out.println("Error while deleting the tables");
            se.printStackTrace();
            response = "An Error occurred: Something went wrong while deleting tables";
        }

        return response;
    }

    // q14
	public String AVGRoutePA() {

		String result = "";

		try {

			String[] headers = {"streetName", "Count(passUpID)"};
			
			int[] width = {36,"Count(passUpID)".length()};

			result += printLine(width);
			result += printRecords(headers,width);
            result += printLine(width);

			String sql =  "SELECT streetName, Count(passUpID) as Count " 
						+ "FROM PassUps " 
						+ "JOIN Streets ON Streets.streetID = PassUps.streetID "
						+ "GROUP BY streetName;";

						PreparedStatement statement = connection.prepareStatement(sql);

						ResultSet resultSet = statement.executeQuery();
						while (resultSet.next()) {
			
							String[] records= new String[2];
			
							records[0] = resultSet.getString("streetName");
							records[1] = resultSet.getString("Count");
							
							result += printRecords(records, width);
						}
			result += printLine(width);
			resultSet.close();
			statement.close();
			
			}catch (SQLException e) {
				e.printStackTrace(System.out);
				return "";
			}
			return result;
	}

	// q3
	public String limitNearCyclingPath() {

		String result = "";
		try {

			String[] headers = {"cyclePathID", "streetName", "speedLimit"};
			
			int[] width = {headers[0].length(),36,headers[2].length()};

			result += printLine(width);
			result += printRecords(headers,width);
            result += printLine(width);

			String sql =  "SELECT cyclePathID, streetName, speedLimit "
						+ "FROM Streets "
						+ "JOIN CyclingNetwork ON Streets.streetID = CyclingNetwork.startStreetID OR Streets.streetID = CyclingNetwork.endStreetID "
						+ "JOIN SpeedLimits ON Streets.streetID = speedLimits.streetID "
						+ "ORDER BY streetName;";

			PreparedStatement statement = connection.prepareStatement(sql);

			ResultSet resultSet = statement.executeQuery();
			while (resultSet.next()) {

				String[] records= new String[3];

				records[0] = resultSet.getString("cyclePathID");
				records[1] = resultSet.getString("streetName");
				records[2] = resultSet.getString("speedLimit");
				
				result += printRecords(records, width);
			}

			result += printLine(width);

			resultSet.close();
			statement.close();
			
			}catch (SQLException e) {
				e.printStackTrace(System.out);
				return "";
			}
		return result;
	}

	// q2
	public String routeByGivenStreet(String input) {

		String result = "";
		try {
			// Headers
			String[] headers = {"stopID", "routeNumber", "routeName"};
	
			// Array for printing
			int[] width = {14, 14, 50};
	
			// Print headers
			result += printLine(width);
			result += printRecords(headers, width);
			result += printLine(width);
	
			// Check for malicious input
			if (checkInputS(input) == 0) {
				throw new Exception("Malicious Input");
			}
	
			// Query
			String sql = "SELECT MIN(Stops.stopID) AS stopID, routeNumber, MIN(routeName) AS routeName "
					+ "FROM Stops "
					+ "JOIN PassengerActivity ON PassengerActivity.stopID = Stops.stopID "
					+ "JOIN Streets ON Stops.streetID = Streets.streetID "
					+ "JOIN Routes ON Routes.RouteID = PassengerActivity.RouteID "
					+ "WHERE streetName = ? "
					+ "GROUP BY routeNumber "
					+ "UNION "
					+ "SELECT MIN(Stops.stopID) AS stopID, routeNumber, MIN(routeName) AS routeName "
					+ "FROM Stops "
					+ "JOIN TransitPerformance ON TransitPerformance.stopID = Stops.stopID "
					+ "JOIN Routes ON Routes.RouteID = TransitPerformance.RouteID "
					+ "JOIN Streets ON Stops.streetID = Streets.streetID "
					+ "WHERE streetName LIKE ? "
					+ "GROUP BY routeNumber;";
	
			PreparedStatement statement = connection.prepareStatement(sql);
	
			// Set parameters for the query
			statement.setString(1, input + "%");
			statement.setString(2, input + "%");
	
			ResultSet resultSet = statement.executeQuery();
	
			// Process the result set
			while (resultSet.next()) {
				String[] records = new String[3];
	
				records[0] = resultSet.getString("stopID");
				records[1] = resultSet.getString("routeNumber");
				records[2] = resultSet.getString("routeName");
	
				result += printRecords(records, width);
			}

			result += printLine(width);
	
			resultSet.close();
			statement.close();
		} catch (SQLException e) {
			System.out.println(e.getMessage());
			e.printStackTrace(System.out);
			return "";
		} catch (Exception e) {
			System.out.println(e.getMessage());
			return "";
		}
		return result;
	}
	
	

	// q4
	public String trafficCountMoreThan(int threshold) {

		String result = "";
		try {
			// Headers
			String[] headers = {"countID", "countDate","streetName", "AvgCount"};
	
			// Array for printing
			int[] width = {8,14,40,10};
	
			result += printLine(width);
			result += printRecords(headers, width);
			result += printLine(width);
	
			// SQL Query with dynamic threshold
			String sql = "SELECT " + headers[0] + ", " + headers[1] + ", " + headers[2] + ", AVG(count15Minutes) as " + headers[3] + " "
					   + "FROM TrafficCounts "
					   + "JOIN Streets ON TrafficCounts.streetID = Streets.streetID "
					   + "GROUP BY countID, countDate, streetName "
					   + "HAVING AVG(count15Minutes) >= ?;";
	
			PreparedStatement statement = connection.prepareStatement(sql);
	
			// Set the threshold parameter
			statement.setInt(1, threshold);
	
			ResultSet resultSet = statement.executeQuery();
	
			// Print results
			while (resultSet.next()) {
	
				String[] records = new String[headers.length];
	
				for (int i = 0; i < records.length; i++) {
					records[i] = resultSet.getString(headers[i]);
				}
	
				result += printRecords(records, width);
			}
	
			result += printLine(width);
	
			resultSet.close();
			statement.close();
	
		} catch (SQLException e) {
			System.out.println(e.getMessage());
			e.printStackTrace(System.out);
			return "";
		}
		return result;
	}
	

	// q5
	public String countPUType(String input) {	
		String result = "";
		try {
			// Headers
			String[] headers = {"passUpType", "count"};

			// array for printing
			int[] width = {16,8};

			result += printLine(width);
			result += printRecords(headers,width);
            result += printLine(width);

			// check for malicious input
			if(checkInputS(input) == 0){
                throw new Exception("Malicious Input");
            };

			String selectHeader = "";

			// Format headers
			for(int i = 0; i < headers.length; i++){
				if(i == 1) selectHeader += "count(passUpID) as " + headers[i];
				else selectHeader += headers[i];
				if(i != headers.length - 1) selectHeader += ", "; else selectHeader += " ";
			}
			
			String sql =  "SELECT " + selectHeader
						+ "FROM PassUps "
						+ "JOIN Routes ON Routes.routeID = PassUps.routeID "
						+ "WHERE routeNumber = ? "
						+ "GROUP BY " + headers[0] + " ;";
			
			PreparedStatement statement = connection.prepareStatement(sql);

			statement.setString(1, input);

			ResultSet resultSet = statement.executeQuery();

			while (resultSet.next()) {

				String[] records= new String[headers.length];

				for(int i = 0; i < records.length; i++){					
					if(i == 1) records[1] = resultSet.getString("count");
					else records[i] = resultSet.getString(headers[i]);
				}
				
				result += printRecords(records, width);
			}

			result += printLine(width);
			resultSet.close();
			statement.close();

			} catch (SQLException e) {

				System.out.println(e.getMessage());
				e.printStackTrace(System.out);
				return "";
			}catch(Exception e){

				System.out.println(e.getMessage());
				return "";
			} 
		return result;
	}

	// q6
	public String limitForHighCount() {
		String result = "";
		try {
			// Headers
			String[] headers = {"streetName", "AVGSpeedLimit"};

			// array for printing
			int[] width = {36,headers[1].length()};

			result += printLine(width);
			result += printRecords(headers, width);
            result += printLine(width);
			String selectHeader = "";

			// Format headers
			for(int i = 0; i < headers.length; i++){
				if(i == 1) selectHeader += "AVG(speedLimit) as " + headers[i];
				else selectHeader += headers[i];
				if(i != headers.length - 1) selectHeader += ", "; else selectHeader += " " ;
			}
			
			String sql =  "SELECT " + selectHeader
						+ "FROM Streets "
						+ "JOIN TrafficCounts ON Streets.streetID = TrafficCounts.streetID "
						+ "JOIN SpeedLimits ON Streets.streetID = SpeedLimits.streetID "
						+ "WHERE TrafficCounts.count15Minutes >= 150 "
						+ "GROUP BY streetName "
						+ "ORDER BY AVG(speedLimit) DESC;";
			
			
			PreparedStatement statement = connection.prepareStatement(sql);

			ResultSet resultSet = statement.executeQuery();
			
			while (resultSet.next()) {

				String[] records= new String[headers.length];

				for(int i = 0; i < records.length; i++){
					if(i == 1) records[i] = resultSet.getString("AVGSpeedLimit");
					else records[i] = resultSet.getString(headers[i]);					
				}
				
				result += printRecords(records, width);
			}

			result += printLine(width);
			resultSet.close();
			statement.close();

		} catch (SQLException e) {

			System.out.println(e.getMessage());
			e.printStackTrace(System.out);
			return "";
		}
		return result;
	}

	// q7
	public String PUCountByStop() {
		String result = "";
		try{
			// Headers
			String[] headers = {"stopID", "streetName", "TotalPassUps"};

			// array for printing
			int[] width = {6,36,headers[2].length()};

			String selectHeader = "";

			// Format headers
			for(int i = 0; i < headers.length; i++){
				if(i == 2) selectHeader += "COUNT(passUpID) AS " + headers[i];
				else selectHeader += headers[i];
				if(i != headers.length - 1) selectHeader += ", "; else selectHeader += " " ;
			}
			
			String sql =  "SELECT " + selectHeader
						+ "FROM Stops "
						+ "JOIN Streets ON Stops.streetID = Streets.streetID "
						+ "JOIN PassUps ON Streets.streetID = PassUps.streetID "
						+ "GROUP BY " + headers[0] + ", " + headers[1] + " "
						+ "ORDER BY TotalPassUps DESC;";
			
			PreparedStatement statement = connection.prepareStatement(sql);

			ResultSet resultSet = statement.executeQuery();

			// print headers
			result += printRecords(headers, width);
            result += printLine(width);

			while (resultSet.next()) {

				String[] records= new String[headers.length];

				for(int i = 0; i < records.length; i++){
					if(i == 2) records[i] = resultSet.getString("TotalPassUps");
					else records[i] = resultSet.getString(headers[i]);
				}
				
				result += printRecords(records, width);
			}

			resultSet.close();
			statement.close();

		} catch (SQLException e) {

			System.out.println(e.getMessage());
			e.printStackTrace(System.out);
			return "";
		}
		return result;
	}

	// q8
	public String OnTimeByRoute() {
		String result = "";
		try{
			// Headers
			String[] headers = {"routeNumber", "routeName", "AvgTimeDeviation"};

			// array for printing
			int[] width = {10,50,headers[2].length()};

			String selectHeader = "";

			// Format headers
			for(int i = 0; i < headers.length; i++){
				if(i == 2) selectHeader += "AVG(deviation) as " + headers[i];
				else selectHeader += headers[i];
				if(i != headers.length - 1) selectHeader += ", "; else selectHeader += " " ;
			}
			
			String sql =  "SELECT " + selectHeader
						+ "FROM Routes "
						+ "JOIN TransitPerformance ON Routes.routeID = TransitPerformance.routeID "
						+ "GROUP BY " + headers[0] + ", " + headers[1] + " "
						+ "ORDER BY AvgTimeDeviation DESC;";
			
			PreparedStatement statement = connection.prepareStatement(sql);

			ResultSet resultSet = statement.executeQuery();

			result += printLine(width);
			// print headers
			result += printRecords(headers, width);
            result += printLine(width);

			while (resultSet.next()) {

				String[] records= new String[headers.length];

				for(int i = 0; i < records.length; i++){
					if(i == records.length - 1) records[i] = resultSet.getString("AvgTimeDeviation");
					else records[i] = resultSet.getString(headers[i]);
				}
				
				result += printRecords(records, width);
			}

			result += printLine(width);

			resultSet.close();
			statement.close();

		}catch (SQLException e) {

			System.out.println(e.getMessage());
			e.printStackTrace(System.out);
			return "";
		}
		return result;
	}

	// q9
	public String stopCountByStreet() {
		String result = "";
		try{
			// Headers
			String[] headers = {"streetName", "TotalBusStops"};

			// array for printing
			int[] width = {36,headers[1].length()};

			String selectHeader = "";

			// Format headers
			for(int i = 0; i < headers.length; i++){
				if(i == 1) selectHeader += "COUNT(stopID) AS " + headers[i];
				else selectHeader += headers[i];
				if(i != headers.length - 1) selectHeader += ", "; else selectHeader += " " ;
			}
			
			String sql =  "SELECT " + selectHeader
						+ "FROM Streets "
						+ "JOIN Stops ON Streets.streetID = Stops.streetID "
						+ "GROUP BY " + headers[0] +" "
						+ "ORDER BY TotalBusStops DESC;";						
			
			PreparedStatement statement = connection.prepareStatement(sql);

			ResultSet resultSet = statement.executeQuery();

			result += printLine(width);
			// print headers
			result += printRecords(headers, width);
            result += printLine(width);

			while (resultSet.next()) {

				String[] records= new String[headers.length];

				for(int i = 0; i < records.length; i++){
					if(i == records.length - 1) records[i] = resultSet.getString("TotalBusStops");
					else records[i] = resultSet.getString(headers[i]);
				}
				
				result += printRecords(records, width);
			}

			result += printLine(width);

			resultSet.close();
			statement.close();

		}catch (SQLException e) {
			System.out.println(e.getMessage());
			e.printStackTrace(System.out);
			return "";
		}
		return result;
	}

	// q10
	public String AVGStatByCycling() {
		String result = "";
		try{
			
			// Headers
			String[] headers = {"streetName", "AvgTrafficCount", "AvgSpeedLimit"};

			// array for printing
			int[] width = {36,headers[1].length(),headers[2].length()};

			String selectHeader = "";

			// Format headers
			for(int i = 0; i < headers.length; i++){
				if(i == 1) selectHeader += "AVG(count15Minutes) AS " + headers[i];
				else if(i == 2) selectHeader += "AVG(speedLimit) AS " + headers[i];
				else selectHeader += headers[i];
				if(i != headers.length - 1) selectHeader += ", "; else selectHeader += " " ;
			}
			
			String sql =  "SELECT " + selectHeader
						+ "FROM Streets "
						+ "JOIN TrafficCounts ON Streets.streetID = TrafficCounts.streetID "
						+ "JOIN CyclingNetwork ON Streets.streetID = CyclingNetwork.startStreetID OR Streets.StreetID = CyclingNetwork.endStreetID "
						+ "JOIN SpeedLimits ON SpeedLimits.streetID = Streets.StreetID "
						+ "GROUP BY streetName "
						+ "ORDER BY AvgTrafficCount DESC; ";
			
			PreparedStatement statement = connection.prepareStatement(sql);

			ResultSet resultSet = statement.executeQuery();

			result += printLine(width);
			// print headers
			result += printRecords(headers, width);
            result += printLine(width);

			while (resultSet.next()) {

				String[] records= new String[headers.length];

				for(int i = 0; i < records.length; i++){
					if(i == records.length - 1) records[i] = resultSet.getString("AvgSpeedLimit");
					else if(i == 1) records[i] = resultSet.getString("AvgTrafficCount");
					else records[i] = resultSet.getString(headers[i]);
				}
				
				result += printRecords(records, width);
			}

			result += printLine(width);
			resultSet.close();
			statement.close();

		}catch (SQLException e) {

			System.out.println(e.getMessage());
			e.printStackTrace(System.out);
			return "";
		}
		return result;
	}

	// q11
	public String TrafficCountWithPU() {
		String result = "";
		try{
			// Headers
			String[] headers = {"streetName", "AvgTrafficCount", "TotalPassUps"};

			// array for printing
			int[] width = {36,headers[1].length(),headers[2].length()};

			String selectHeader = "";

			// Format headers
			for(int i = 0; i < headers.length; i++){
				if(i == 1) selectHeader += "AVG(count15Minutes) AS " + headers[i];
				else if(i == 2) selectHeader += "COUNT(passUpID) as " + headers[i];
				else selectHeader += headers[i];
				if(i != headers.length - 1) selectHeader += ", "; else selectHeader += " " ;
			}
			
			String sql =  "SELECT " + selectHeader
						+ "FROM Streets "
						+ "JOIN TrafficCounts ON Streets.streetID = TrafficCounts.streetID "
						+ "JOIN PassUps ON PassUps.streetID= Streets.streetID "
						+ "GROUP BY streetName "
						+ "ORDER BY AVG(count15Minutes) DESC; ";
			
			PreparedStatement statement = connection.prepareStatement(sql);

			ResultSet resultSet = statement.executeQuery();

			result += printLine(width);

			// print headers
			result += printRecords(headers, width);
            result += printLine(width);

			while (resultSet.next()) {

				String[] records= new String[headers.length];

				for(int i = 0; i < records.length; i++){
					if(i == records.length - 1) records[i] = resultSet.getString("TotalPassUps");
					else if(i == 1) records[i] = resultSet.getString("AvgTrafficCount");
					else records[i] = resultSet.getString(headers[i]);
				}
				
				result += printRecords(records, width);
			}

			result += printLine(width);

			resultSet.close();
			statement.close();

		}catch (SQLException e) {
			System.out.println(e.getMessage());
			e.printStackTrace(System.out);
			return "";
		}
		return result;
	}	

	// q12
	public String PassUpDueToLate() {
		String result  = "";
		try{

			// Headers
			String[] headers = {"routeNumber", "routeName", "stopNumber"};

			// array for printing
			int[] width = {headers[0].length(),50,headers[2].length()};

			String selectHeader = "";

			// Format headers
			for(int i = 0; i < headers.length; i++){
				selectHeader += headers[i];
				if(i != headers.length - 1) selectHeader += ", "; else selectHeader += " " ;
			}
			
			String sql =  "WITH PassUpsPerStopPerRoute AS ( "
						+ 	"SELECT routeID, Stops.stopID, COUNT(*) AS numPassUp "
						+ 	"FROM PassUps "
						+ 	"JOIN Streets ON PassUps.streetID = Streets.streetID "
						+ 	"JOIN Stops ON Stops.streetID = Streets.streetID "
						+	"GROUP BY routeID, Stops.stopID ), "
						+ "PerformancePerStopPerRoute AS( "
						+ 	"SELECT routeID, Stops.stopID, AVG(deviation) AS avgDeviation "
						+ 	"FROM TransitPerformance "
						+ 	"JOIN Stops ON Stops.stopID = TransitPerformance.stopID "
						+ 	"GROUP BY routeID, Stops.stopID ) "
						+ "SELECT routeNumber, routeName, stopNumber "
						+ "FROM PassUpsPerStopPerRoute "
						+ "JOIN PerformancePerStopPerRoute ON PassUpsPerStopPerRoute.routeID = PerformancePerStopPerRoute.routeID "
						+ "AND PassUpsPerStopPerRoute.stopID = PerformancePerStopPerRoute.stopID "
						+ "JOIN Stops ON Stops.stopID = PassUpsPerStopPerRoute.stopID "
						+ "JOIN Routes ON Routes.routeID = PassUpsPerStopPerRoute.routeID " 
						+ "WHERE avgDeviation > 300 AND numPassUp > 0; ";
			
			PreparedStatement statement = connection.prepareStatement(sql);

			ResultSet resultSet = statement.executeQuery();

			result += printLine(width);

			// print headers
			result += printRecords(headers, width);
            result += printLine(width);

			while (resultSet.next()) {

				String[] records= new String[headers.length];

				for(int i = 0; i < records.length; i++){
					records[i] = resultSet.getString(headers[i]);
				}
				
				result += printRecords(records, width);
			}

			result += printLine(width);

			resultSet.close();
			statement.close();

		}catch (SQLException e) {
			System.out.println(e.getMessage());
			e.printStackTrace(System.out);
			return "";
		}
		return result;
	}

	// q13
	public String StopCountByCycling() {
		String result = "";
		try{

			// Headers
			String[] headers = {"cyclePathID", "streetName", "StopCount"};

			// array for printing
			int[] width = {11,50,headers[2].length()};

			String selectHeader = "SELECT ";

			// Format headers
			for(int i = 0; i < headers.length; i++){
				if(i == headers.length - 1) selectHeader += "COUNT(*) as " + headers[i];
				else selectHeader += headers[i];
				if(i != headers.length - 1) selectHeader += ", "; else selectHeader += " " ;
			}
			
			String sql =  selectHeader
						+ "FROM CyclingNetwork "
						+ "JOIN Streets ON Streets.streetID = CyclingNetwork.startStreetID or Streets.streetID = CyclingNetwork.endStreetID "
						+ "JOIN Stops ON Stops.streetID = Streets.streetID "
						+ "GROUP BY cyclePathID, streetName "
						+ "ORDER BY StopCount DESC;";
			
			PreparedStatement statement = connection.prepareStatement(sql);

			ResultSet resultSet = statement.executeQuery();

			result += printLine(width);

			// print headers
			result += printRecords(headers, width);
            result += printLine(width);

			while (resultSet.next()) {

				String[] records= new String[headers.length];

				for(int i = 0; i < records.length; i++){
					if(i == 2) records[i] = resultSet.getString("StopCount"); 
					else records[i] = resultSet.getString(headers[i]);
				}
				
				result += printRecords(records, width);
			}

			result += printLine(width);

			resultSet.close();
			statement.close();

		}catch (SQLException e) {
			System.out.println(e.getMessage());
			e.printStackTrace(System.out);
			return "";
		}
		return result;
	}	

	// q1
	public String StopByRoute(String input) {
		String result = "";
		try{
			
			// check for malicious input
			if(checkInputS(input) == 0){
                throw new Exception("Malicous Input");
            };

			// Headers
			String[] headers = {"stopID", "stopNumber", "routeName"};

			// array for printing
			int[] width = {8,headers[1].length(),50};

			// Format headers
			String selectHeader = "SELECT ";
			for(int i = 0; i < headers.length; i++){
				if(i == 0) selectHeader += "Stops." + headers[i];
				else selectHeader += headers[i];
				if(i != headers.length - 1) selectHeader += ", "; else selectHeader += " " ;
			}
			
			String sql =  selectHeader
						+ "FROM Stops "
						+ "JOIN PassengerActivity ON PassengerActivity.stopID = Stops.stopID "
						+ "JOIN Routes ON PassengerActivity.routeID = Routes.routeID "
						+ "Where routeNumber = ? "
						+ "UNION "
						+ selectHeader
						+ "FROM Stops "
						+ "JOIN TransitPerformance ON TransitPerformance.stopID = Stops.stopID "
						+ "JOIN Routes ON Routes.routeID = TransitPerformance.routeID "
						+ "Where routeNumber = ? ;";								
			
			PreparedStatement statement = connection.prepareStatement(sql);

			statement.setString(1, input);
			statement.setString(2, input);

			ResultSet resultSet = statement.executeQuery();

			result += printLine(width);

			// print headers
			result += printRecords(headers, width);
            result += printLine(width);

			while (resultSet.next()) {

				String[] records= new String[headers.length];

				for(int i = 0; i < records.length; i++)
					if(i == 0) records[i] = resultSet.getString("stopID");
					else records[i] = resultSet.getString(headers[i]);
				
				result += printRecords(records, width);
			}

			result += printLine(width);

			resultSet.close();
			statement.close();

		}catch (SQLException e) {
			System.out.println(e.getMessage());
			e.printStackTrace(System.out);
			return "";
		}  catch(Exception e){
			System.out.println(e.getMessage());
			return "";
		} 
		return result;
	}

	// q15 
	public String safetyBoundries(String input1, String input2) {
		String result = "";
		try{

			// check input
			if(checkInputS(input1) == 0){
                throw new Exception("Malicous Input");
            };
			if(checkInputS(input2) == 0){
                throw new Exception("Malicous Input");
            };

			// Headers
			String[] headers = {"streetName", "PassengerActivity.dayType","AvgCount", "AvgBoardings"};

			// array for printing
			int[] width = {36,headers[1].length() + 2,headers[2].length(),headers[3].length()};

			// Format headers
			String selectHeader = "SELECT ";
			for(int i = 0; i < headers.length; i++){
				if(i == 2) selectHeader += "AVG(count15Minutes) as " + headers[i];
				else if(i == 3) selectHeader += "AVG(averageBoardings) as " + headers[i];
				else selectHeader += headers[i];
				if(i != headers.length - 1) selectHeader += ", "; else selectHeader += " " ;
			}
			
			String sql =  selectHeader
						+ "FROM TrafficCounts "
						+ "JOIN Streets ON TrafficCounts.streetID = Streets.streetID "
						+ "JOIN Stops ON Stops.streetID = Streets.streetID "
						+ "JOIN PassengerActivity ON Stops.stopID = PassengerActivity.stopID "
						+ "GROUP BY streetName, PassengerActivity.dayType "
						+ "HAVING AVG(count15Minutes) >= ? "
						+ "And AVG(averageBoardings) >= ? "
						+ "ORDER BY AVG(count15Minutes) DESC, AVG(averageBoardings) DESC;";

			
			PreparedStatement statement = connection.prepareStatement(sql);

			statement.setString(1, input1);
			statement.setString(2, input2);

			ResultSet resultSet = statement.executeQuery();

			result += printLine(width);

			// print headers
			result += printRecords(headers, width);
            result += printLine(width);

			while (resultSet.next()) {

				String[] records= new String[headers.length];

				for(int i = 0; i < records.length; i++){
					if(i == 2) records[i] = resultSet.getString("AvgCount");
					else if(i == 3) records[i] = resultSet.getString("AvgBoardings");
					else if(i == 1) records[i] = resultSet.getString("dayType");
					else records[i] = resultSet.getString(headers[i]);
				}
				
				result += printRecords(records, width);
			}

			result += printLine(width);

			resultSet.close();
			statement.close();

		}catch (SQLException e) {
			System.out.println(e.getMessage());
			e.printStackTrace(System.out);
			return "";
		}  catch(Exception e){
			System.out.println(e.getMessage());
			return "";
		} 
		return result;
	}

	// q16
	public String PUCountAndPassengerActivity() {
		String result = "";
		try{

			// Headers
			String[] headers = {"dayType", "routeNumber", "routeName","streetName", "AvgBoardings", "AvgPassUp"};

			// array for printing
			int[] width = {headers[0].length() + 4,headers[1].length(),50,36,headers[4].length(),headers[5].length()};

			String selectHeader = "SELECT ";

			// Format headers
			for(int i = 0; i < headers.length; i++){
				if(i == headers.length - 1) selectHeader += "COUNT(*) as " + headers[i];
				else selectHeader += headers[i];
				if(i != headers.length - 1) selectHeader += ", "; else selectHeader += " " ;
			}
			
			String sql =  "SELECT PassUps.dayType, routeNumber, routeName, streetName, AVG(AverageBoardings) as AvgBoardings, Count(Routes.routeID) as AvgPassUp "
						+ "FROM PassUps "
						+ "JOIN Streets On PassUps.streetID = Streets.streetID "
						+ "JOIN Stops ON PassUps.streetID = Stops.streetID "
						+ "JOIN Routes ON PassUPs.routeID = Routes.routeID "
						+ "JOIN PassengerActivity ON PassengerActivity.routeID = Routes.routeID "
						+ "AND PassengerActivity.stopID = Stops.stopID "
						+ "AND PassengerActivity.dayType = PassUps.dayType "
						+ "GROUP BY PassUps.dayType, routeNumber, routeName, streetName "
						+ "ORDER BY AvgBoardings DESC;";
			
			PreparedStatement statement = connection.prepareStatement(sql);

			ResultSet resultSet = statement.executeQuery();

			result += printLine(width);

			// print headers
			result += printRecords(headers, width);
            result += printLine(width);

			while (resultSet.next()) {

				String[] records= new String[headers.length];

				for(int i = 0; i < records.length; i++){
					if(i == 4) records[i] = resultSet.getString("AvgBoardings");
					else if(i == 5) records[i] = resultSet.getString("AvgPassUp");
					else records[i] = resultSet.getString(headers[i]);
				}
				
				result += printRecords(records, width);
			}

			result += printLine(width);

			resultSet.close();
			statement.close();

		}catch (SQLException e) {
			System.out.println(e.getMessage());
			e.printStackTrace(System.out);
			return "";
		}  catch(Exception e){
			System.out.println(e.getMessage());
			return "";
		} 
		return result;
	}	

	// q18
	public String bestPerformance() {
		String result = "";
		try{

			// Headers 
			String[] headers = {"dayType", "routeNumber", "routeName", "AvgTimeDev", "DayCount", "AvgBoardings", "AvgTrafficCount"};

			// array for printing
			int[] width = {headers[0].length() + 4,headers[1].length(),50,headers[3].length(),headers[4].length(),headers[5].length(),headers[6].length()} ;

			String selectHeader = "SELECT ";

			// Format headers
			for(int i = 0; i < headers.length; i++){
				if(i == 3) selectHeader += "AVG(ABS(deviation)) as " + headers[i];
				else if(i == 0) selectHeader += "PassengerActivity." + headers[i];
				else if(i == 5) selectHeader += "AVG(averageBoardings) as " + headers[i];
				else if(i == 6) selectHeader += "AVG(count15Minutes) as " + headers[i];
				else selectHeader += headers[i];
				if(i != headers.length - 1) selectHeader += ", "; else selectHeader += " " ;
			}
			
			String sql =  "WITH PassUpsByDay AS ("
						+ 	"SELECT dayType, routeID, Stops.streetID, COUNT(PassUps.routeID) AS dayCount "
						+ 	"FROM PassUps "
						+ 	"JOIN Stops ON Stops.streetID = PassUps.streetID "
						+ 	"GROUP BY dayType, routeID, Stops.streetID)"
						+ selectHeader
						+ "FROM PassUpsByDay "
						+ "JOIN Routes ON PassUpsByDay.routeID = Routes.routeID "
						+ "JOIN Stops ON PassUpsByDay.streetID = Stops.streetID "
						+ "JOIN PassengerActivity ON PassengerActivity.stopID = Stops.stopID "
						+ "AND PassengerActivity.dayType = PassUpsByDay.dayType "
						+ "JOIN TransitPerformance ON TransitPerformance.StopID = Stops.StopID "
						+ "AND TransitPerformance.routeID = Routes.routeID "
						+ "AND TransitPerformance.dayType = PassUpsByDay.dayType "
						+ "JOIN TrafficCounts ON TrafficCounts.streetID = PassUpsByDay.streetID "
						+ "GROUP BY PassengerActivity.dayType, routeNumber, routeName, dayCount "
						+ "ORDER BY "+ headers[3] +" DESC, "+ headers[4] +" DESC, "+ headers[5] +" ASC, " + headers[6] +" ASC;";
			
			PreparedStatement statement = connection.prepareStatement(sql);

			ResultSet resultSet = statement.executeQuery();

			result += printLine(width);

			// print headers
			result += printRecords(headers, width);
            result += printLine(width);

			while (resultSet.next()) {

				String[] records= new String[headers.length];

				for(int i = 0; i < records.length; i++){
					if(i == 3) records[i] = resultSet.getString("AvgTimeDev");
					else if(i == 0) records[i] = resultSet.getString("dayType");
					else if(i == 4) records[i] = resultSet.getString("DayCount");
					else if(i == 5) records[i] = resultSet.getString("AvgBoardings");
					else if(i == 6) records[i] = resultSet.getString("AvgTrafficCount");
					else records[i] = resultSet.getString(headers[i]);
				}
				
				result += printRecords(records, width);
			}

			result += printLine(width);

			resultSet.close();
			statement.close();

		}catch (SQLException e) {
			System.out.println(e.getMessage());
			e.printStackTrace(System.out);
			return "";
		}  catch(Exception e){
			System.out.println(e.getMessage());
			return "";
		} 
		return result;
	}	
	
	// q17 
	public String busyRoute() {
		String result = "";
		try{

			// Headers 
			String[] headers = {"stopID", "streetName", "AvgBoarding"};

			// array for printing
			int[] width = {8,36,12};

			String selectHeader = "SELECT ";
			
			String sql =  "SELECT Stops.stopID, streetName, AVG(AverageBoardings) AS AvgBoarding "
						+ "FROM Stops "
						+ "JOIN PassengerActivity ON Stops.stopID = PassengerActivity.stopID "
						+ "JOIN Streets ON Streets.streetID = Stops.streetID "
						+ "GROUP BY Stops.stopID, streetName "
						+ "ORDER BY AVG(AverageBoardings) DESC;";
			
			PreparedStatement statement = connection.prepareStatement(sql);

			ResultSet resultSet = statement.executeQuery();

			result += printLine(width);

			// print headers
			result += printRecords(headers, width);
            result += printLine(width);

			while (resultSet.next()) {

				String[] records= new String[headers.length];

				for(int i = 0; i < records.length; i++){
					
					records[i] = resultSet.getString(headers[i]);
				}
				
				result += printRecords(records, width);
			}

			result += printLine(width);

			resultSet.close();
			statement.close();

		}catch (SQLException e) {
			System.out.println(e.getMessage());
			e.printStackTrace(System.out);
			return "";
		}  catch(Exception e){
			System.out.println(e.getMessage());
			return "";
		} 
		return result;
	}	

	private int checkInputS(String input){
		// check for malicious input
		if(input.contains(";") || input.contains("--") || input.toLowerCase().contains(" or ")){
			return 0;
		}
		return 1;
	}

    private String printLine(int[] width){
        String result = "";
        for(int i = 0; i < width.length; i++){
            for(int j = 0; j < width[i]; j++){
                result += "-";
            }
			result += "-";
        }
        return result + "\n";
    }

	private String printRecords(String[] records, int[] width){
		// print headers
		String result = "";
		for(int i = 0; i < records.length; i++){
			result += centerText(records[i], width[i]) + "|";
		}
		return result + "\n";
	}

	private String centerText(String text, int width) {
        int padding = (width - text.length()) / 2;
		if(padding < width - text.length() - padding) padding ++;
		if(padding < 0) return text.substring(0, width);
		else {
			String paddedText = " ".repeat(padding) + text;
			// If padding is odd, add one more space on the right
			while (paddedText.length() < width) {
				paddedText += " ";
			}
			return paddedText;
		}
    }
}