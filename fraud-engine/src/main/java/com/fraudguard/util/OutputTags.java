package com.fraudguard.util;

import com.fraudguard.model.Alert;
import org.apache.flink.util.OutputTag;

public class OutputTags {
    public static OutputTag<Alert> alertOutputTag = new OutputTag<Alert>("fraud-alert");
}
