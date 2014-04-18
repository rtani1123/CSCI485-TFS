package RMIClient_Test;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface AdditionInterface extends Remote{
	public int Add(int a, int b) throws RemoteException;
	public void createFile() throws RemoteException;
}
