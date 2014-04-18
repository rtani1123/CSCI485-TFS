package RMIClient_Test;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RMISecurityManager;
import java.rmi.RemoteException;
import java.util.Scanner;

import Interfaces.TestInterface;

public class TestClient{

	TestInterface ai;
	public TestClient() {
		setupServerConnection();
		Scanner in = new Scanner(System.in);
		String input = in.nextLine();
		while(!input.equals("q")) {
			if(input.equals("send hi"))
				try {
					ai.createFile();
				} catch (RemoteException e) {
					// TODO Auto-generated catch block
					System.out.println("message sent to offline server");
				}
			else
				System.out.println("bad input");
			input = in.nextLine();
		}
	}
	private void setupServerConnection() {
		// TODO Auto-generated method stub
		try {
			System.setSecurityManager(new RMISecurityManager());
			ai = (TestInterface)Naming.lookup("rmi://localhost:1099/ABC");
			
		} catch(RemoteException e) {
			System.out.println("server unavailable");
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			System.out.println("server unavailable 2");
		} catch (NotBoundException e) {
			// TODO Auto-generated catch block
			System.out.println("server unavailable 3");
		}

	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		TestClient ac = new TestClient();
	}

}
