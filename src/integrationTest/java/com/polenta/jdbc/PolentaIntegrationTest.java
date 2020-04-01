package com.polenta.jdbc;

import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.ImageFromDockerfile;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.function.Consumer;

import static java.lang.String.format;
import static org.junit.Assert.assertNotNull;

//import static com.polenta.jdbc.PolentaDbInstanceFactory.PORT;

public class PolentaIntegrationTest {

	public static Integer PORT = 3110;

	public static Consumer<CreateContainerCmd> cmd =
			e -> e.withPortBindings(new PortBinding(Ports.Binding.bindPort(PORT), new ExposedPort(PORT)));

	@ClassRule
	public static GenericContainer polentaContainer =
			new GenericContainer(new ImageFromDockerfile()
					.withFileFromClasspath("polenta-db-0.0.3.jar", "polenta-db/polenta-db-0.0.3.jar")
					.withFileFromClasspath("Dockerfile", "polenta-db/Dockerfile"))
					.withExposedPorts(PORT)
					.withCreateContainerCmdModifier(cmd);

	@Test
	public void testConnection() throws ClassNotFoundException, SQLException {
		Class.forName("com.polenta.jdbc.PolentaDriver");

		Properties connectionProps = new Properties();
		connectionProps.put("user", "");
		connectionProps.put("password", "");

		String containerIp = polentaContainer.getContainerIpAddress();
		String connectionURL = format("jdbc:polenta://%s:%s/", containerIp, PORT);

		Connection conn = DriverManager.getConnection(connectionURL, connectionProps);
		assertNotNull(conn);
	}

//	@Ignore
//	@Test
//	public void testCreateBag() throws ClassNotFoundException, SQLException {
//		Class.forName("com.polenta.jdbc.PolentaDriver");
//
//		Properties connectionProps = new Properties();
//		connectionProps.put("user", "");
//		connectionProps.put("password", "");
//
//		GenericContainer polentaContainer = PolentaDbInstanceFactory.getInstance().getPolentaContainer();
//
//		String containerIp = polentaContainer.getContainerIpAddress();
//
//		String connectionURL = "jdbc:polenta://" + containerIp  + ":" + PORT + "/";
//
//		Connection conn = DriverManager.getConnection(connectionURL, connectionProps);
//
//		if (conn != null) {
//			Statement stmt = conn.createStatement();
//			//stmt.executeQuery("CREATE TABLE");
//			stmt.execute("CREATE BAG PERSON (NAME STRING)");
//
//			ResultSet rs = stmt.executeQuery("SELECT * FROM PERSON");
//
//			//rs.first();
//
//		}
//
//	}

}
