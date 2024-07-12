@ZeebeExport
Feature: Zeebe exporter test

  Scenario: Test zeebe-export kafka topic
    Given I have tenant as "paymentbb2"
    When I upload the BPMN file to zeebe
    And I can start test workflow n times with message "Hello World" and listen on kafka topic

  Scenario: Test importer-rdbms flow for mmft
    Given I have tenant as "paymentbb2"
    And I clear the timestamps table
    Then I hit transfer api 500 times
    Then I wait for 10 seconds
    And I fetch timestamps data
    Then I assert the event count with the expected event count for 500 transfers
    And I log average delays
    When I call the count transfer status API with failed status in ops app
    Then I assert the count to be 0
    When I call the count transfer status API with completed status in ops app
    Then I assert the count to be 500
    Then I clear the timestamps table
