package com.polenta.jdbc;

import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import org.junit.ClassRule;
import org.junit.Rule;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.ImageFromDockerfile;

import java.util.function.Consumer;

public class PolentaDbInstanceFactory {

    public static Integer PORT = 3110;

    private  Consumer<CreateContainerCmd> cmd =
            e -> e.withPortBindings(new PortBinding(Ports.Binding.bindPort(PORT), new ExposedPort(PORT)));

//	@ClassRule
//	public static GenericContainer simpleWebServer =
//			new GenericContainer("polenta/polenta-db:latest")
//					.withExposedPorts(3110)
//					.withCreateContainerCmdModifier(cmd);

    @Rule
    private   GenericContainer polentaContainer =
            new GenericContainer(new ImageFromDockerfile()
                    //.withFileFromString("folder/someFile.txt", "hello")
                    //.withFileFromClasspath("test.txt", "mappable-resource/test-resource.txt")
                    .withFileFromClasspath("polenta-db-0.0.3.jar", "polenta-db/polenta-db-0.0.3.jar")
                    .withFileFromClasspath("Dockerfile", "polenta-db/Dockerfile"))
                    .withExposedPorts(PORT)
                    .withCreateContainerCmdModifier(cmd);

    private PolentaDbInstanceFactory() {

    }

    public static PolentaDbInstanceFactory getInstance() {
        return new PolentaDbInstanceFactory();
    }

    public GenericContainer getPolentaContainer() {
        return this.polentaContainer;
    }

}
