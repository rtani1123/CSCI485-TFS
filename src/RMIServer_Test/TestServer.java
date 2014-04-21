package RMIServer_Test;
import java.rmi.*;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.*;


public class TestServer {

	public TestServer() {
		
	}
	
	public void setupServer() {
		
		
	}
	
	public static void main(String[] argv) {
		try {
			System.setSecurityManager(new RMISecurityManager());
			
			Registry registry = LocateRegistry.createRegistry(1099);
			Test A = new Test();
			Naming.rebind("rmi://dblab-43.vlab.usc.edu/ABC", A);
			
			System.out.println("ready!");
		} catch (Exception e) {
			System.out.println("failure");
			e.printStackTrace();
		}
	}
}
