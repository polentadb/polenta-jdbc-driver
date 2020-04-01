package com.polenta.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.function.Consumer;

import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;

public class PolentaIntegrationTest {

	private static Integer port = 3110;

	private static Consumer<CreateContainerCmd> cmd =
			e -> e.withPortBindings(new PortBinding(Ports.Binding.bindPort(port), new ExposedPort(port)));


	@ClassRule
	public static GenericContainer simpleWebServer =
			new GenericContainer("polenta/polenta-db:latest")
					.withExposedPorts(3110)
					.withCreateContainerCmdModifier(cmd);;

	@Test
	public void test() throws ClassNotFoundException, SQLException {

		Class.forName("com.polenta.jdbc.PolentaDriver");

		Properties connectionProps = new Properties();
		connectionProps.put("user", "");
		connectionProps.put("password", "");

		String containerIp = simpleWebServer.getContainerIpAddress();

		Connection conn = DriverManager.getConnection("jdbc:polenta://" + containerIp  + ":" + port + "/", connectionProps);

		if (conn != null) {
			Statement stmt = conn.createStatement();
			//stmt.executeQuery("CREATE TABLE");
			stmt.execute("CREATE BAG PERSON (NAME STRING)");
			
			ResultSet rs = stmt.executeQuery("SELECT * FROM PERSON");
			
			//rs.first();
			
		}

	}

}
