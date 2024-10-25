package org.example;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.SendMessageRequest;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;

public class EmployeeProcessor implements RequestHandler<S3Event, String> {

    private final AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
    private final AmazonSNS sns = AmazonSNSClientBuilder.defaultClient();
    private final AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();

    private static final String DEPARTMENT_A_TOPIC = "arn:aws:sns:region:account-id:departmentA";
    private static final String DEPARTMENT_B_TOPIC = "arn:aws:sns:region:account-id:departmentB";
    private static final String SQS_QUEUE_URL = "https://sqs.region.amazonaws.com/account-id/employee-integration-queue";

    @Override
    public String handleRequest(S3Event event, Context context) {
        String bucket = event.getRecords().get(0).getS3().getBucket().getName();
        String key = event.getRecords().get(0).getS3().getObject().getKey();

        try {
            S3Object s3Object = s3.getObject(bucket, key);
            BufferedReader reader = new BufferedReader(new InputStreamReader(s3Object.getObjectContent()));
            StringBuilder jsonContent = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                jsonContent.append(line);
            }

            JSONObject jsonData = new JSONObject(jsonContent.toString());
            JSONArray employees = jsonData.getJSONArray("employees");

            for (int i = 0; i < employees.length(); i++) {
                JSONObject employee = employees.getJSONObject(i);
                int id = employee.getInt("id");
                String name = employee.getString("name");
                int salary = employee.getInt("salary");

                if (salary == 20000) {
                    sendNotification(DEPARTMENT_A_TOPIC, id, name, salary);
                } else if (salary > 30000) {
                    sendNotification(DEPARTMENT_B_TOPIC, id, name, salary);
                }

                sendToSQS(id, name, salary);
            }

            return "Success";

        } catch (Exception e) {
            context.getLogger().log("Error processing employee data: " + e.getMessage());
            return "Failure";
        }
    }

    private void sendNotification(String topicArn, int id, String name, int salary) {
        String message = String.format("Employee ID: %d, Name: %s, Salary: %d", id, name, salary);
        sns.publish(topicArn, message);
    }

    private void sendToSQS(int id, String name, int salary) {
        JSONObject messageBody = new JSONObject();
        messageBody.put("id", id);
        messageBody.put("name", name);
        messageBody.put("salary", salary);

        SendMessageRequest sendMsgRequest = new SendMessageRequest()
                .withQueueUrl(SQS_QUEUE_URL)
                .withMessageBody(messageBody.toString());
        sqs.sendMessage(sendMsgRequest);
    }
}
