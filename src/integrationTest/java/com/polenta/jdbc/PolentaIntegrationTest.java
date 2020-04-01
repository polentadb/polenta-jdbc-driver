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
import org.junit.Ignore;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.ImageFromDockerfile;

import static com.polenta.jdbc.PolentaDbInstanceFactory.PORT;

public class PolentaIntegrationTest {

	@Test
	public void testConnection() throws ClassNotFoundException, SQLException {
		Class.forName("com.polenta.jdbc.PolentaDriver");

		Properties connectionProps = new Properties();
		connectionProps.put("user", "");
		connectionProps.put("password", "");

		GenericContainer polentaContainer = PolentaDbInstanceFactory.getInstance().getPolentaContainer();

		String containerIp = polentaContainer.getContainerIpAddress();

		String connectionURL = "jdbc:polenta://" + containerIp  + ":" + PORT + "/";

		Connection conn = DriverManager.getConnection(connectionURL, connectionProps);

		//assert conn != null
	}

	@Ignore
	@Test
	public void testCreateBag() throws ClassNotFoundException, SQLException {
		Class.forName("com.polenta.jdbc.PolentaDriver");

		Properties connectionProps = new Properties();
		connectionProps.put("user", "");
		connectionProps.put("password", "");

		GenericContainer polentaContainer = PolentaDbInstanceFactory.getInstance().getPolentaContainer();

		String containerIp = polentaContainer.getContainerIpAddress();

		String connectionURL = "jdbc:polenta://" + containerIp  + ":" + PORT + "/";

		Connection conn = DriverManager.getConnection(connectionURL, connectionProps);

		if (conn != null) {
			Statement stmt = conn.createStatement();
			//stmt.executeQuery("CREATE TABLE");
			stmt.execute("CREATE BAG PERSON (NAME STRING)");

			ResultSet rs = stmt.executeQuery("SELECT * FROM PERSON");

			//rs.first();

		}

	}

}
