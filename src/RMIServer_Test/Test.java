package RMIServer_Test;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

import Interfaces.TestInterface;


public class Test extends UnicastRemoteObject 
		implements TestInterface {

	public Test() throws RemoteException {	}

	public int Add(int a, int b) throws RemoteException {
		return a+b;
	}

	@Override
	public void createFile() throws RemoteException {
		// TODO Auto-generated method stub
		System.out.println("hi");
		try {
			File f = new File("text.txt");
			PrintWriter pw = new PrintWriter(f);
			pw.close();

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
