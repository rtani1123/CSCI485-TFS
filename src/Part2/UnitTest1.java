package Part2;

import java.rmi.RemoteException;
import java.util.ArrayList;

import Client.Client;
/*
 * Unit1:  Create a hierarchical directory structure with a specified fanout.
	This is an extended version of Test 1 that creates more than two sub-directories per directory.
	The exact number is the input value fanout.  The input values must be greater than or equal to zero.
	Input:  An integer denoting the number of directories, Fanout
	Example1, Unit1 7 0
	Creates one directory with no subdirectories.  It produces 7 directories numbered 1, 2, 3, 4, 5, 6, 7
	Unit1 7 1
	Creates one directory with 6 subdirectories.  The resulting directory structure is:
	1/
	1/2
	1/2/3
	1/2/3/4
	1/2/3/4/5
	1/2/3/4/5/6
	1/2/3/4/5/6/7  
	Example 2, Unit1 7 5
	Creates the following directory structure
	1
	1/2
	1/3
	1/4
	1/5
	1/6
	1/2/7
 */
public class UnitTest1 {
	public static void main(String[] args) throws RemoteException {

		unitTest1Func(4, 0, null);
		//test1(7, 0);

	}
	public static void unitTest1Func(int numFolders, int fanOut, Client myClient){
		if(fanOut == 0){
			for(int i = 1; i <= numFolders; i++){
				String pathFlat = "C:/" + i;
				System.out.println(pathFlat);
			}
		}
		else{
			for(int i = 1; i <= numFolders; i++){
				int k = i;
				StringBuffer path = new StringBuffer(String.valueOf(k));
				k = (int)Math.floor((double)(k-1)/fanOut);
				while (k > 0)
				{
					String addMe = k + "/";
					path.insert(0, addMe);
					k = (int)Math.floor((double)(k-1)/fanOut);
				}
				path.insert(0, "C:/");
				System.out.println(path.toString());
			}
		}
		System.out.println("Existing tree structure: ");
	}
	public static void test1(int numFolders, int fanOut) {
		ArrayList<String> current = new ArrayList<String>();
		int count = 0;
		String base = "C:";
		System.out.println("C:/1");
		current.add(new String("C:/1"));
		while(count < numFolders) {
			System.out.println("current = " + current);
			int size = current.size();
			for(int i = 0; i < fanOut; i++) {
				for(String path : current) {

					i++;
				}
				String newPath = current.get(i) + "/" + count;
				current.add(newPath);
				count++;
				System.out.println(newPath);
				/*String newPath = current.get(i)+"/" + count;
				System.out.println(newPath);
				current.add(newPath);
				count++;
				 */
			}
			for(int i = 0; i < size; i++) {
				current.remove(i);
			}

		}
	}

}
