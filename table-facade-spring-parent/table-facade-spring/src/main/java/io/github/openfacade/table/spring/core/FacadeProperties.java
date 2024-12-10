package io.github.openfacade.table.spring.core;

import io.github.openfacade.table.api.DriverType;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter
@Setter
@ConfigurationProperties("spring.table.facade")
public class FacadeProperties {
    private DriverType driverType;
}
