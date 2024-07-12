package org.mifos.integrationtest.cucumber.stepdef;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.cucumber.java.en.And;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import io.restassured.RestAssured;
import io.restassured.builder.MultiPartSpecBuilder;
import io.restassured.builder.ResponseSpecBuilder;
import io.restassured.specification.MultiPartSpecification;
import io.restassured.specification.RequestSpecification;
import java.io.IOException;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.IOUtils;
import org.mifos.integrationtest.common.UniqueNumberGenerator;
import org.mifos.integrationtest.common.Utils;
import org.mifos.integrationtest.common.dto.CollectionResponse;
import org.mifos.integrationtest.config.ZeebeOperationsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import static com.google.common.truth.Truth.assertThat;

public class ZeebeStepDef extends BaseStepDef {

    @Autowired
    ZeebeOperationsConfig zeebeOperationsConfig;

    private Set<String> processInstanceKeySet = new HashSet<>();

    private static final String BPMN_FILE_URL = "https://raw.githubusercontent.com/arkadasfynarfin/ph-ee-env-labs/zeebe-upgrade-2/orchestration/feel/zeebetest.bpmn";

    Logger logger = LoggerFactory.getLogger(this.getClass());

    @When("I upload the BPMN file to zeebe")
    public void uploadBpmnFileToZeebe() throws MalformedURLException {
        String fileContent = getFileContent(BPMN_FILE_URL);
        String response = uploadBPMNFile(fileContent);
        logger.info("BPMN file upload response: {}", response);
    }

    @And("I can start test workflow n times with message {string} and listen on kafka topic")
    public void iCanStartTestWorkflowNTimesWithMessage(String message) {
        logger.info("Test workflow started");
        String endpoint = zeebeOperationsConfig.workflowEndpoint + "zeebetest";
        ExecutorService apiExecutorService = Executors.newFixedThreadPool(zeebeOperationsConfig.threadCount);

        for (int i = 0; i < zeebeOperationsConfig.noOfWorkflows; i++) {
            final int workflowNumber = i;
            apiExecutorService.execute(() -> {
                String requestBody = String.format("{ \"message\": \"%s\" , \"workflowStartTime\": \"%s\" }", message,
                        new Date().getTime());
                String response = sendWorkflowRequest(endpoint, requestBody);
                JsonObject payload = JsonParser.parseString(response).getAsJsonObject();
                String processInstanceKey = payload.get("processInstanceKey").getAsString();
                processInstanceKeySet.add(processInstanceKey);
                logger.info("Workflow Response {}: {}", workflowNumber, processInstanceKey);
            });
        }

        apiExecutorService.shutdown();
        try {
            apiExecutorService.awaitTermination(Integer.MAX_VALUE, TimeUnit.MICROSECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        logger.info("Test workflow ended");
    }

    @And("I clear the timestamps table")
    public void clearTimestampsTable() {
        RequestSpecification requestSpec = Utils.getDefaultSpec(scenarioScopeState.tenant);
        RestAssured.given(requestSpec).baseUri(operationsAppConfig.operationAppContactPoint).expect()
                .spec(new ResponseSpecBuilder().expectStatusCode(200).build()).when().delete(operationsAppConfig.delayEndpoint).andReturn()
                .asString();
    }

    @And("I hit transfer api {int} times")
    public void makeApiCallNTimes(int n) throws InterruptedException {
        ArrayList<String> transactionIds = new ArrayList<>();
        ScheduledExecutorService apiExecutorService = Executors.newScheduledThreadPool(zeebeOperationsConfig.threadCount);
        String endpoint = channelConnectorConfig.channelConnectorContactPoint + channelConnectorConfig.transferEndpoint;

        for (int i = 0; i < n; i++) {
            int delay = i * 25;
            apiExecutorService.schedule(() -> {
                String response = sendTransferRequest();
                CollectionResponse cr = (new Gson()).fromJson(response, CollectionResponse.class);
                transactionIds.add(cr.getTransactionId());
                logger.info(cr.getTransactionId().toString());
            }, delay, TimeUnit.MILLISECONDS);
        }
        apiExecutorService.shutdown();
        apiExecutorService.awaitTermination(1, TimeUnit.HOURS);
        scenarioScopeState.transactionIds = transactionIds;
    }

    @And("I fetch timestamps data")
    public void getTimestampsData() {
        RequestSpecification requestSpec = Utils.getDefaultSpec(scenarioScopeState.tenant);
        scenarioScopeState.response = RestAssured.given(requestSpec).baseUri(operationsAppConfig.operationAppContactPoint).expect()
                .spec(new ResponseSpecBuilder().expectStatusCode(200).build()).when().get(operationsAppConfig.delayEndpoint).andReturn()
                .asString();
    }

    @Then("I assert the event count with the expected event count for {int} transfers")
    public void checkEventCount(int n) {
        JsonObject jsonObject = JsonParser.parseString(scenarioScopeState.response).getAsJsonObject();
        int eventsCount = jsonObject.getAsJsonObject().get("eventsCount").getAsInt();
        assertThat(eventsCount).isEqualTo(n * 37);
    }

    @And("I log average delays")
    public void logAverageDelays() {
        JsonObject jsonObject = JsonParser.parseString(scenarioScopeState.response).getAsJsonObject();
        logger.info("Average time delay between Exporting and Importing {}",
                String.valueOf(jsonObject.get("averageExportImportTime").getAsLong()));
        logger.info("Average time delay between Event producing and Exporting {}",
                String.valueOf(jsonObject.get("averageZeebeExportTime").getAsLong()));
    }

    @When("I call the count transfer status API with {} status in ops app")
    public void getStatusCount(String status) {
        RequestSpecification requestSpec = Utils.getDefaultSpec(scenarioScopeState.tenant);
        scenarioScopeState.response = RestAssured.given(requestSpec).baseUri(operationsAppConfig.operationAppContactPoint)
                .body(scenarioScopeState.transactionIds).expect().spec(new ResponseSpecBuilder().expectStatusCode(200).build()).when()
                .post(operationsAppConfig.countEndpoint + "/" + status).andReturn().asString();
    }

    @Then("I assert the count to be {}")
    public void assertCount(long transferCount) {
        Long responseCount = JsonParser.parseString(scenarioScopeState.response).getAsLong();
        assertThat(responseCount).isEqualTo(transferCount);
    }

    private String getFileContent(String fileUrl) {
        try {
            return IOUtils.toString(URI.create(fileUrl), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String uploadBPMNFile(String fileContent) throws MalformedURLException {
        RequestSpecification requestSpec = Utils.getDefaultSpec();
        return RestAssured.given(requestSpec).baseUri(zeebeOperationsConfig.zeebeOperationContactPoint).multiPart(getMultiPart(fileContent))
                .expect().spec(new ResponseSpecBuilder().expectStatusCode(200).build()).when()
                .post(zeebeOperationsConfig.uploadBpmnEndpoint).andReturn().asString();
    }

    private MultiPartSpecification getMultiPart(String fileContent) {
        return new MultiPartSpecBuilder(fileContent.getBytes()).fileName("zeebe-test.bpmn").controlName("file").mimeType("text/plain")
                .build();
    }

    private String sendWorkflowRequest(String endpoint, String requestBody) {
        RequestSpecification requestSpec = Utils.getDefaultSpec();
        return RestAssured.given(requestSpec).baseUri(zeebeOperationsConfig.zeebeOperationContactPoint).body(requestBody).expect()
                .spec(new ResponseSpecBuilder().expectStatusCode(200).build()).when().post(endpoint).andReturn().asString();
    }

    private String sendTransferRequest() {
        RequestSpecification requestSpec = Utils.getDefaultSpec(scenarioScopeState.tenant);
        String requestBody = "{\n" + "    \"payer\": {\n" + "        \"partyIdInfo\": {\n" + "            \"partyIdType\": \"MSISDN\",\n"
                + "            \"partyIdentifier\": \"27713803914\"\n" + "        }\n" + "    },\n" + "    \"payee\": {\n"
                + "        \"partyIdInfo\": {\n" + "            \"partyIdType\": \"MSISDN\",\n"
                + "            \"partyIdentifier\": \"27713803915\"\n" + "        }\n" + "    },\n" + "    \"amount\": {\n"
                + "        \"amount\": 1,\n" + "        \"currency\": \"USD\"\n" + "    }\n" + "}";
        return RestAssured.given(requestSpec).baseUri(channelConnectorConfig.channelConnectorContactPoint)
                .header("X-CorrelationID", UniqueNumberGenerator.generateUniqueNumber(12)).body(requestBody).when()
                .post(channelConnectorConfig.transferEndpoint).andReturn().asString();
    }
}
