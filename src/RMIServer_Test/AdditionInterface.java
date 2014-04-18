package RMIServer_Test;
import java.rmi.*;

public interface AdditionInterface extends Remote{
	public int Add(int a, int b) throws RemoteException;
	public void createFile() throws RemoteException;
}

