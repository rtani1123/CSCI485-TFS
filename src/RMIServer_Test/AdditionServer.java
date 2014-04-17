package RMIServer_Test;
import java.rmi.*;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.*;


public class AdditionServer {

	public AdditionServer() {
		
	}
	
	public void setupServer() {
		
		
	}
	
	public static void main(String[] argv) {
		try {
			System.setSecurityManager(new RMISecurityManager());
			
			Registry registry = LocateRegistry.createRegistry(1099);
			Addition A = new Addition();
			Naming.rebind("rmi://localhost/ABC", A);
			
			System.out.println("ready!");
		} catch (Exception e) {
			System.out.println("failure");
			e.printStackTrace();
		}
	}
}
