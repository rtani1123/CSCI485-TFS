package Interfaces;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface TestInterface extends Remote{
	public int Add(int a, int b) throws RemoteException;
	public void createFile() throws RemoteException;
}
