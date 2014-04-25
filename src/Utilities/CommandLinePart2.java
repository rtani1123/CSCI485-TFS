package Utilities;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.rmi.RemoteException;

import Client.Client;

public class CommandLinePart2 {

	public static void main(String[] args) throws RemoteException {

		Client myClient = null;
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		System.out.println(" Welcome to our Tiny File System!");
		System.out.println(" Please enter Client ID (Start at '11'): ");
		try {
			Integer id = Integer.parseInt(br.readLine());
			myClient = new Client(id);
		} catch (NumberFormatException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		System.out
				.println(" If you don't know what to do, ask us! Simply type help ");
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
				System.out
						.println(" Append chunkHandle offset source withSize ");
				System.out.println(" Atomic chunkHandle source withSize ");
				System.out
						.println(" Read chunkHandle offset length destination ");
				System.out.println(" Read chunkHandle destination");
			} else if (input.contains("CreateDirectory")) {
				String[] actuallArgs = getArgs(input);
				if (actuallArgs == null || actuallArgs.length != 1) {
					System.out
							.println(" Please enter appropriate number of arguments. ");
				} else
					myClient.createDirectory(actuallArgs[0]);
			} else if (input.contains("DeleteFile")) {
				String[] actuallArgs = getArgs(input);
				if (actuallArgs == null || actuallArgs.length != 1) {
					System.out
							.println(" Please enter appropriate number of arguments. ");

				} else
					myClient.deleteFileMaster(actuallArgs[0]);

			} else if (input.contains("DeleteDirectory")) {
				System.out.println("delete directory called");
				String[] actuallArgs = getArgs(input);
				if (actuallArgs == null || actuallArgs.length != 1) {
					System.out
							.println(" Please enter appropriate number of arguments. ");

				} else
					myClient.deleteDirectory(actuallArgs[0]);

			} else if (input.contains("CreateFile")) {
				String[] actuallArgs = getArgs(input);
				if (actuallArgs == null || actuallArgs.length != 3) {
					System.out
							.println(" Please enter appropriate number of arguments. ");

				} else
					myClient.createFile(actuallArgs[0], actuallArgs[1],
							Integer.parseInt(actuallArgs[2]));
			} else if (input.contains("Append")) {
				String[] actuallArgs = getArgs(input);
				if (actuallArgs == null || actuallArgs.length != 4) {
					System.out
							.println(" Please enter appropriate number of arguments. ");

				} else {
					File inputFile = new File(actuallArgs[2]);
					byte[] b = new byte[(int) inputFile.length()];
					// read in the input file
					try {
						FileInputStream fis = new FileInputStream(inputFile);
						fis.read(b);
						fis.close();
					} catch (Exception e) {
						e.printStackTrace();
					}
					myClient.append(actuallArgs[0],
							Integer.parseInt(actuallArgs[1]), b.length, b,
							Boolean.parseBoolean(actuallArgs[3]));
				}

			} else if (input.contains("Atomic")) {
				String[] actuallArgs = getArgs(input);
				if (actuallArgs == null || actuallArgs.length != 3) {
					System.out
							.println(" Please enter appropriate number of arguments. ");

				} else {
					File inputFile = new File(actuallArgs[1]);
					byte[] b = new byte[(int) inputFile.length()];
					// read in the input file
					try {
						FileInputStream fis = new FileInputStream(inputFile);
						fis.read(b);
						fis.close();
					} catch (Exception e) {
						e.printStackTrace();
					}
					myClient.atomicAppend(actuallArgs[0], b.length, b,
							Boolean.parseBoolean(actuallArgs[2]));
				}

			} else if (input.contains("Read")) {
				String[] actuallArgs = getArgs(input);
				if (actuallArgs == null || actuallArgs.length != 4) {
					System.out
							.println(" Please enter appropriate number of arguments. ");
				} else {
					System.out.println(actuallArgs[3]);
					myClient.read(actuallArgs[0],
							Integer.parseInt(actuallArgs[1]),
							Integer.parseInt(actuallArgs[2]), actuallArgs[3]);
				}

			} else if(input.contains("Completely")) {
				String[] actualArgs = getArgs(input);
				if(actualArgs == null || actualArgs.length != 2) {
					System.out.println(" Please enter appropriate number of arguments. ");
				} else {
					myClient.readCompletely(actualArgs[0], actualArgs[1]);
				}
			}
			else if (input.contains("Unit1")) {
				String[] actualArgs = getArgs(input);
				if (actualArgs == null || actualArgs.length != 2) {
					System.out
							.println(" Please enter appropriate number of arguments. ");
				} else {
					int count = 2;
					System.out.println("C:/1");
					while (count < Integer.parseInt(actualArgs[0])) {

					}
				}
			} else if (input.contains("Unit2")) {
				String[] actualArgs = getArgs(input);

			} else if (input.contains("Unit3")) {
				String[] actualArgs = getArgs(input);

			} else if (input.contains("Unit4")) {
				String[] actualArgs = getArgs(input);

			} else if (input.contains("Unit5")) {
				String[] actualArgs = getArgs(input);

			} else if (input.contains("Unit6")) {
				String[] actualArgs = getArgs(input);

			} else if (input.contains("Unit7")) {
				String[] actualArgs = getArgs(input);

			} else
				System.out.println("Not a command");
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
