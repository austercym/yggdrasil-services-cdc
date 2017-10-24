package com.orwellg.yggdrasil.h2;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;

import org.apache.commons.lang3.StringUtils;

public class H2DbHelper {

	public H2DbHelper() {
	}

	public void createDbSchema(Connection con, String sqlFile, String delimiter) throws Exception {
		Statement s = con.createStatement();

//		ClassLoader classLoader = getClass().getClassLoader();

		Scanner sc = new Scanner(this.getClass().getResourceAsStream(sqlFile));

		sc.useDelimiter(delimiter);

		while (sc.hasNext()){

			String line = sc.next();
			if (StringUtils.isNotBlank(line)) {
				try {
					s.execute(this.prepareSentence(line));
				} catch(SQLException ex) {
					System.out.println("Line error -> " + this.prepareSentence(line) + "\n");
				}
			}
		}

		sc.close();
	}

	protected String prepareSentence (String sentence) {
		
		String result = "";
		
		result = StringUtils.strip(sentence).toUpperCase();
		
		if (StringUtils.contains(result, "UTF16")) {
			result = StringUtils.replace(result, "UTF16", "UTF8");
		}
		
		return result;
		
	}
}
