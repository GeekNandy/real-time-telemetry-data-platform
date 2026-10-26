package dev.sharad.telemetry.core;

import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

class CoreContractTest {
  @TestFactory
  Stream<DynamicTest> coreContracts() {
    return CoreContractCases.cases().stream()
        .map(scenario -> DynamicTest.dynamicTest(scenario.name(), () -> scenario.body().run()));
  }
}
