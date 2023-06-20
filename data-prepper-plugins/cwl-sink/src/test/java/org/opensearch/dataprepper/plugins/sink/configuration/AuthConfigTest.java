package org.opensearch.dataprepper.plugins.sink.configuration;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.plugins.sink.config.AwsConfig;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class AuthConfigTest {
    @Test
    void check_default_region_test() {
        assertThat(new AwsConfig().getRegion(), equalTo("default"));
    }

    @Test
    void check_default_role_test() {
        assertThat(new AwsConfig().getRole_arn(), equalTo("default"));
    }
}
