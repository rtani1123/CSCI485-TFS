package Utilities;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class CommandLine {
	public static void main(String[] args) {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

		for (;;) {
			// get user input
			System.out.print("Please choose a function: ");

			String input = null;

			try {
				input = br.readLine();
			} catch (IOException e) {
				e.printStackTrace(System.out);
				System.exit(1);
			}
			System.out.println("you have entered: " + input);
		}

	}
}
