package application;

import application.mydatabase.Database;
import java.util.Scanner;

public class App {
    public static void main(String[] args) {
        String reponse;
        Database db = new Database();
        reponse = db.startup();

        if (reponse != null) {
            System.out.println(reponse);
        } else {
            simulate(db);
        }

        System.out.println("\nEnd of processing\n");
    }

    private static void simulate(Database db) {

        Scanner consoleIn = new Scanner(System.in);// Scanner that takes input from console
        System.out.println();// Getting on a new line
        System.out.println("Welcome to Winnipeg Road Transport Database");// label

        String cmd = nextNonEmptyLine(consoleIn, "To get started, ENTER 'm' for Menu: ");
        String output;

        boolean cont = true;

        while (cont) {
            output = processCommand(db, cmd);
            System.out.println(output);
            cmd = nextNonEmptyLine(consoleIn, "Choice >> ");
            cont = cmd != null && !cmd.equalsIgnoreCase("e");
        }

        System.out.println("\nExiting Winnipeg Road Transport Database. Have a great day!\n");
        consoleIn.close();

    }

    
    private static String processDatabase(Database db) {
        System.out.println("Initializing the Database, this might take about 4-5 minutes");
        System.out.println(
                "------------------------------------------------------------------------------");
        String response = db.initializeDatabase();
        if (response == null) {
            response = "Database successfully initialized";
        }
        return response;
    }

    private static String processDropDB(Database db) {
        String response;
        System.out.println("Droping the Database, the queries will not work untill database is not initialized again");
        System.out.println(
                "------------------------------------------------------------------------------");

        if ((response = db.dropAllTables()) == null) {
            response = "Database Dropped successfully";
        }

        return response;
    }

    private static void displayQueriesMenu(){
        System.out.println("\nThe command for queries is q + the query number you want to run (i.e. q1, q12)");
        System.out.println("-------------------------------------------------------------------------------\n");
        System.out.println("\tq1 <Route Number> : Get a list of stops along the specified route.\n");
        System.out.println("\tq2 <Street Name> : Get a list of routes that pass through the specified street.\n");
        System.out.println("\tq3 : Shows speed limits of streets close to cycling paths.\n");
        System.out.println("\tq4 <Traffic Count Threshold> : Shows streets where traffic counts were recorded equal to or more than the specified threshold.\n");
        System.out.println("\tq5 <Route Number> : Counts the number of pass-ups for each type on the specified route.\n");
        System.out.println("\tq6 : Calculates the average speed limit for streets with high traffic counts and bus stops.\n");
        System.out.println("\tq7 : Counts the total number of pass-ups at each bus stop location.\n");
        System.out.println("\tq8 : Calculates the average time deviation for each bus route.\n");
        System.out.println("\tq9 : Counts the number of bus stops on each street.\n");
        System.out.println("\tq10 : Calculates the average traffic count and speed limit for streets with cycling paths.\n");
        System.out.println("\tq11 : Examines the relationship between traffic counts and pass-ups on streets with bus stops.\n");
        System.out.println("\tq12 : Tallies the number of pass-ups caused by being behind schedule.\n");
        System.out.println("\tq13 : Shows which cycling paths run along streets with bus stops.\n");
        System.out.println("\tq14 : Shows routes with their average pass-ups count.\n");
        System.out.println("\tq15 <Traffic Count Threshold> <Boarding Count Threshold> : Examines the relationship between street busyness and passenger boarding activity.\n");
        System.out.println("\tq16 : Shows average pass-up count and average passenger boardings on different day types and routes.\n");
        System.out.println("\tq17 : Calculates the average boarding activity at specific stops.\n");
        System.out.println("\tq18 : Displays the best-performing bus routes based on on-time performance, pass-ups, passenger activity, and traffic counts.\n");
        System.out.println("-------------------------------------------------------------------------------\n");
        displayMenu();
    }
    

    private static String processCommand(Database db, String cmd) {
        String[] args = cmd.split("\\s+");
    
        if (args[0].equalsIgnoreCase("m")) {
            displayMenu();
            return "";
        } else if (args[0].equalsIgnoreCase("i")) {
            return processDatabase(db);
        } else if (args[0].equalsIgnoreCase("d")) {
            return processDropDB(db);
        } else if (args[0].equalsIgnoreCase("q?")) { 
            displayQueriesMenu();
            return "";
        } else if (args[0].equalsIgnoreCase("q14")) { 
            return db.AVGRoutePA();
        }
    
        // Query handling
        else if (args[0].equalsIgnoreCase("q3")) {
            return db.limitNearCyclingPath();
        } else if (args[0].equalsIgnoreCase("q2")) {
            if (args.length < 2) return "Need argument for this query (Street Name).";
            else return db.routeByGivenStreet(args[1]);
        } else if (args[0].equalsIgnoreCase("q4")) {
            if (args.length < 2) return "Need argument for this query (Traffic Count Threshold).";
            try {
                int threshold = Integer.parseInt(args[1]); // Parse the threshold value
                return db.trafficCountMoreThan(threshold);
            } catch (NumberFormatException e) {
                return "Invalid argument for q4. Please provide a valid numeric threshold.";
            }
        } else if (args[0].equalsIgnoreCase("q5")) {
            if (args.length < 2) return "Need argument for this query (Route Number).";
            else return db.countPUType(args[1]);
        } else if (args[0].equalsIgnoreCase("q6")) {
            return db.limitForHighCount();
        } else if (args[0].equalsIgnoreCase("q7")) {
            return db.PUCountByStop();
        } else if (args[0].equalsIgnoreCase("q8")) {
            return db.OnTimeByRoute();
        } else if (args[0].equalsIgnoreCase("q9")) {
            return db.stopCountByStreet();
        } else if (args[0].equalsIgnoreCase("q10")) {
            return db.AVGStatByCycling();
        } else if (args[0].equalsIgnoreCase("q11")) {
            return db.TrafficCountWithPU();
        } else if (args[0].equalsIgnoreCase("q12")) {
            return db.PassUpDueToLate();
        } else if (args[0].equalsIgnoreCase("q13")) {
            return db.StopCountByCycling();
        } else if (args[0].equalsIgnoreCase("q1")) {
            if (args.length < 2) return "Missing argument for this query (Route Number).";
            else return db.StopByRoute(args[1]);
        } else if (args[0].equalsIgnoreCase("q15")) {
            if (args.length < 3) return "Missing argument(s) for this query (Traffic Count and Boarding Count Thresholds).";
            else return db.safetyBoundries(args[1], args[2]);
        } else if (args[0].equalsIgnoreCase("q16")) {
            return db.PUCountAndPassengerActivity();
        } else if (args[0].equalsIgnoreCase("q17")) {
            return db.busyRoute();
        } else if (args[0].equalsIgnoreCase("q18")) {
            return db.bestPerformance();
        } else {
            return "Invalid choice. Enter 'm' for Menu";
        }
    }
    

    private static void displayMenu() {
        
        System.out.println("\ti - Initialize the database\n");
        System.out.println("\td - Delete the Database\n");
        System.out.println("\tm - Display the Menu.\n");
        System.out.println("\tq? - Display the queries instruction.\n");
        System.out.println("\te - Exit the system.");

    }


    /**
     * Helper method for Scanner to skip over empty lines.
     * Print the prompt on each line of input.
     */
    private static String nextNonEmptyLine(Scanner in, String prompt) {
        String line = null;

        System.out.print(prompt);
        while (line == null && in.hasNextLine()) {
            line = in.nextLine();
            if (line.trim().length() == 0) {
                line = null;
                System.out.print(prompt);
            }
        }

        return line;
    }
}

