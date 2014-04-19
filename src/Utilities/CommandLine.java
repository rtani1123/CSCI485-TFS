package Utilities;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import Part1.Test1;
import Part1.Test2;
import Part1.Test3;
import Part1.Test4;
import Part1.Test5;
import Part1.Test6;
import Part1.Test7;

public class CommandLine {
	public static void main(String[] args) {
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
				System.out.println(" Test1 numberOfFolders ");
				System.out.println(" Test2 path numberOfFiles ");
				System.out.println(" Test3 path ");
				System.out.println(" Test4 source destination ");
				System.out.println(" Test5 source destination ");
				System.out.println(" Test6 source destination ");
				System.out.println(" Test7 fullPath ");
			} else if (input.contains("Test1")) {
				String [] actuallArgs = getArgs(input);
				if (actuallArgs == null || actuallArgs.length!=1){
					System.out.println(" Please enter appropriate number of arguments. ");
				}else
				Test1.main(actuallArgs);
			} else if (input.equals("Test2")) {
				String [] actuallArgs = getArgs(input);
				if (actuallArgs == null || actuallArgs.length!=2){
					System.out.println(" Please enter appropriate number of arguments. ");
					
				}else
				Test2.main(actuallArgs);

			} else if (input.equals("Test3")) {
				String [] actuallArgs = getArgs(input);
				if (actuallArgs == null || actuallArgs.length!=1){
					System.out.println(" Please enter appropriate number of arguments. ");
					
				}else
				Test3.main(actuallArgs);

			} else if (input.equals("Test4")) {
				String [] actuallArgs = getArgs(input);
				if (actuallArgs == null || actuallArgs.length!=2){
					System.out.println(" Please enter appropriate number of arguments. ");
					
				}else
				Test4.main(actuallArgs);
			} else if (input.equals("Test5")) {
				String [] actuallArgs = getArgs(input);
				if (actuallArgs == null || actuallArgs.length!=2){
					System.out.println(" Please enter appropriate number of arguments. ");
					
				}else
				Test5.main(actuallArgs);

			} else if (input.equals("Test6")) {
				String [] actuallArgs = getArgs(input);
				if (actuallArgs == null || actuallArgs.length!=2){
					System.out.println(" Please enter appropriate number of arguments. ");
					
				}else
				Test6.main(actuallArgs);

			} else if (input.equals("Test7")) {
				String [] actuallArgs = getArgs(input);
				if (actuallArgs == null || actuallArgs.length!=1){
					System.out.println(" Please enter appropriate number of arguments. ");
					
				}else
				Test7.main(actuallArgs);

			}
		}

	}
	public static String[] getArgs (String in){
		String allArgs[] = in.split(" ");
		String actuallArgs [] = new String[allArgs.length-1];
		for (int i = 0; i < allArgs.length-1 ; i++){
			actuallArgs[i] = allArgs[i+1];
		}
		
		return actuallArgs;
	}
}
