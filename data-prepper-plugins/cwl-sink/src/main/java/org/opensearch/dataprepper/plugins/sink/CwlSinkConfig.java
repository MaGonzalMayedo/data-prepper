package org.opensearch.dataprepper.plugins.sink;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotEmpty;

public class CwlSinkConfig {

    @JsonProperty("path")
    @NotEmpty
    private String path = "src/resources/file-test-sample-output.txt";

    @JsonProperty("arn_file")
    private String arnFile = "";



    @JsonProperty("aws-user")
    @NotEmpty
    private String awsUser = "testUser";

    @JsonProperty("aws-cred")
    private int awsCredential = 45;

    public String getPath() {
        return path;
    }

    public String getAwsUser() { return awsUser; }

    public int getAwsCredential() { return awsCredential; }
}
