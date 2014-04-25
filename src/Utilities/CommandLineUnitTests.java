package Utilities;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.rmi.RemoteException;

import Client.Client;
import Part2.UnitTest1;
import Part2.UnitTest2;
import Part2.UnitTest3;
import Part2.UnitTest4;
import Part2.UnitTest5;
import Part2.UnitTest6;
import Part2.UnitTest7;

public class CommandLineUnitTests {

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
				System.out.println(" UnitTest1 numFolders fanOut ");
				System.out.println(" UnitTest2 startingPath numFiles ");
				System.out.println(" UnitTest3 path ");
				System.out.println(" UnitTest4 src dest numOfReplicas ");
				System.out.println(" UnitTest5 src dest ");
				System.out.println(" UnitTest6 src dest ");
				System.out.println(" UnitTest7 src dest ");
				System.out.println(" UnitTest8 ");
			} else if (input.contains("UnitTest1")) {
				String[] actuallArgs = getArgs(input);
				if (actuallArgs == null || actuallArgs.length != 2)
					System.out.println("Wrong number of arguments");
				else
					UnitTest1.unitTest1Func(Integer.parseInt(actuallArgs[0]),
							Integer.parseInt(actuallArgs[1]), myClient);
			} else if (input.contains("UnitTest2")) {
				String[] actuallArgs = getArgs(input);
				if (actuallArgs == null || actuallArgs.length != 2)
					System.out.println("Wrong number of arguments");
				else
					UnitTest2.unitTest2Func(actuallArgs[0],
							Integer.parseInt(actuallArgs[1]), myClient);

			} else if (input.contains("UnitTest3")) {
				String[] actuallArgs = getArgs(input);
				if (actuallArgs == null || actuallArgs.length != 1)
					System.out.println("Wrong number of arguments");
				else
					UnitTest3.unitTest3Func(actuallArgs[0], myClient);

			} else if (input.contains("UnitTest4")) {
				String[] actuallArgs = getArgs(input);
				if (actuallArgs == null || actuallArgs.length != 3)
					System.out.println("Wrong number of arguments");
				else
					UnitTest4.unitTest4Func(actuallArgs[0], actuallArgs[1],
							Integer.parseInt(actuallArgs[2]), myClient);
			} else if (input.contains("UnitTest5")) {
				String[] actuallArgs = getArgs(input);
				if (actuallArgs == null || actuallArgs.length != 2)
					System.out.println("Wrong number of arguments");
				else
					UnitTest5.unitTest5Func(actuallArgs[0], actuallArgs[1],
							myClient);
			} else if (input.contains("UnitTest6")) {
				String[] actuallArgs = getArgs(input);
				if (actuallArgs == null || actuallArgs.length != 2)
					System.out.println("Wrong number of arguments");
				else
					UnitTest6.unitTest6Func(actuallArgs[0], actuallArgs[1],
							myClient);
			} else if (input.contains("UnitTest7")) {
				String[] actuallArgs = getArgs(input);
				if (actuallArgs == null || actuallArgs.length != 2)
					System.out.println("Wrong number of arguments");
				else
					UnitTest7.unitTest7Func(actuallArgs[0], actuallArgs[1],
							myClient);
			} else if (input.contains("UnitTest8")) {
				// String[] actuallArgs = getArgs(input);
				// if (actuallArgs == null || actuallArgs.length != 2)
				// System.out.println("Wrong number of arguments");
				// else
				// UnitTest8.unitTest8Func(actuallArgs[0], actuallArgs[1],
				// myClient);
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
