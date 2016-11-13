
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author cloudera
 */
public class HiveConsole {

    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    public static void main(String[] args) {

        try {

            if (args.length < 2) {
                System.out.println("El comando es demasiado corto y debe comenzar con run o get");
                return;
            }
            String cmd = "";
            for (int i = 1; i < args.length; i++) {
                cmd += " " + args[i];
            }

            try {
                Class.forName(driverName);
            } catch (ClassNotFoundException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                System.exit(1);
            }
            //replace "hive" here with the name of the user the queries should run as
            Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000", "", "");

            if (args[0].equals("run")) {

            } else if (args[0].equals("get")) {

            } else {
                System.out.println("debe comenzar con run o get");
            }
        } catch (SQLException ex) {
            Logger.getLogger(HiveConsole.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
