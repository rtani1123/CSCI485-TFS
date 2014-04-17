package RMIClient_Test;
import java.rmi.Naming;
import java.rmi.RMISecurityManager;
import java.rmi.RemoteException;
import java.util.Scanner;

public class AdditionClient{

	AdditionInterface ai;
	public AdditionClient() {
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
			ai = (AdditionInterface)Naming.lookup("rmi://localhost/ABC");
			
		} catch(Exception e) {
			System.out.println("server unavailable");
		}

	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		AdditionClient ac = new AdditionClient();
	}

}
