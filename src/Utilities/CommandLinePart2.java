package Utilities;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.rmi.RemoteException;

import Client.Client;
import Part1.Test1;
import Part1.Test2;
import Part1.Test3;
import Part1.Test4;
import Part1.Test5;
import Part1.Test6;
import Part1.Test7;

public class CommandLinePart2 {
	public static void main(String[] args) throws RemoteException {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		System.out.println(" Welcome to our Tiny File System!");
		System.out.println(" If you don't know what to do, ask us! Simply type help ");
		for (;;) {
			// get user input
			System.out.print(" > ");
			String input = null;

			try {
				input = br.readLine();
			} catch (IOException e) {
				e.printStackTrace(System.out);
				System.exit(1);
			}
			if (input.equals("help")) {
				System.out.println(" CreateDirectory chunkHandle ");
				System.out.println(" DeleteFile chunkHandle ");
				System.out.println(" DeleteDirectory chunkHandle ");
				System.out.println(" CreateFile path filename numOfReplicas ");
				System.out.println(" Append chunkHandle source withSize ");
				System.out.println(" AtomicAppend chunkHandle source withSize ");
				System.out.println(" Read chunkHandle offset length ");
			} else if (input.contains("CreateDirectory")) {
				String [] actuallArgs = getArgs(input);
				if (actuallArgs == null || actuallArgs.length!=1){
					System.out.println(" Please enter appropriate number of arguments. ");
				}else
				Client.myClient.createDirectory(actuallArgs[0]);
			} else if (input.equals("DeleteFile")) {
				String [] actuallArgs = getArgs(input);
				if (actuallArgs == null || actuallArgs.length!=1){
					System.out.println(" Please enter appropriate number of arguments. ");
					
				}else
				Client.myClient.deleteFileMaster(actuallArgs[0]);

			} else if (input.equals("DeleteDirectory")) {
				String [] actuallArgs = getArgs(input);
				if (actuallArgs == null || actuallArgs.length!=1){
					System.out.println(" Please enter appropriate number of arguments. ");
					
				}else
				Client.myClient.deleteDirectory(actuallArgs[0]);

			} else if (input.equals("CreateFile")) {
				String [] actuallArgs = getArgs(input);
				if (actuallArgs == null || actuallArgs.length!=3){
					System.out.println(" Please enter appropriate number of arguments. ");
					
				}else
				Client.myClient.createFile(actuallArgs[0], actuallArgs[1],Integer.parseInt(actuallArgs[2]));
			} else if (input.equals("Append")) {
				String [] actuallArgs = getArgs(input);
				if (actuallArgs == null || actuallArgs.length!=3){
					System.out.println(" Please enter appropriate number of arguments. ");
					
				}else{
					File inputFile = new File(actuallArgs[1]);
					byte[] b = new byte[(int)inputFile.length()];
					//read in the input file
					try{
						FileInputStream fis = new FileInputStream(inputFile);
						fis.read(b);
						fis.close();
					}catch(Exception e){
						e.printStackTrace();
					}
					Client.myClient.append(actuallArgs[0],0, b.length, b, Boolean.parseBoolean(actuallArgs[3]));
				}

			} else if (input.equals("AtomicAppend")) {
				String [] actuallArgs = getArgs(input);
				if (actuallArgs == null || actuallArgs.length!=3){
					System.out.println(" Please enter appropriate number of arguments. ");
					
				}else{
					File inputFile = new File(actuallArgs[1]);
					byte[] b = new byte[(int)inputFile.length()];
					//read in the input file
					try{
						FileInputStream fis = new FileInputStream(inputFile);
						fis.read(b);
						fis.close();
					}catch(Exception e){
						e.printStackTrace();
					}
					Client.myClient.atomicAppend(actuallArgs[0], b.length, b, Boolean.parseBoolean(actuallArgs[3]));
				}

			} else if (input.equals("Read")) {
				String [] actuallArgs = getArgs(input);
				if (actuallArgs == null || actuallArgs.length!=3){
					System.out.println(" Please enter appropriate number of arguments. ");
				}else
				Client.myClient.read(actuallArgs[0], Integer.parseInt(actuallArgs[1]),Integer.parseInt(actuallArgs[1]));

			}
		}

	}

	public static String[] getArgs(String in) {
		String allArgs[] = in.split(" ");
		String actuallArgs[] = new String[allArgs.length - 1];
		for (int i = 0; i < allArgs.length - 1; i++) {
			actuallArgs[i] = allArgs[i + 1];
		}

		return actuallArgs;
	}
}
