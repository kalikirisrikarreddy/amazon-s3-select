package amazon.s3.select;

import java.io.File;
import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CSVInput;
import com.amazonaws.services.s3.model.CSVOutput;
import com.amazonaws.services.s3.model.ExpressionType;
import com.amazonaws.services.s3.model.InputSerialization;
import com.amazonaws.services.s3.model.JSONInput;
import com.amazonaws.services.s3.model.JSONType;
import com.amazonaws.services.s3.model.OutputSerialization;
import com.amazonaws.services.s3.model.ParquetInput;
import com.amazonaws.services.s3.model.SelectObjectContentEvent;
import com.amazonaws.services.s3.model.SelectObjectContentEvent.RecordsEvent;
import com.amazonaws.services.s3.model.SelectObjectContentEventStream;
import com.amazonaws.services.s3.model.SelectObjectContentRequest;
import com.amazonaws.services.s3.model.SelectObjectContentResult;
import com.fasterxml.jackson.core.exc.StreamWriteException;
import com.fasterxml.jackson.databind.DatabindException;
import com.github.javafaker.Faker;

public class AmazonS3Select {

	private static final int N = 5;
	private static final String BUCKET_NAME = "amazon-s3-select-"
			+ LocalDate.now().format(DateTimeFormatter.BASIC_ISO_DATE);
	private static final String EMPLOYEES_JSON_FILE_NAME = "employees.json";
	private static final String EMPLOYEES_CSV_FILE_NAME = "employees.csv";
	private static final String EMPLOYEES_PARQUET_FILE_NAME = "employees.parquet";
	private static final String NEW_LINE = "\n";
	private static final String COMMA = "\n";

	public static void main(String[] args) throws IOException {

		AmazonS3 amazonS3 = getAmazonS3Client();

		createBucketIfNotExists(amazonS3);

		Employees employees = generateEmployeeData();

		exportEmployeesInJsonFormat(employees, amazonS3);
		exportEmployeesInCSVFormat(employees, amazonS3);
		exportEmployeesInParquetFormat(employees, amazonS3);

		long t1 = System.currentTimeMillis();
		getAny5EmployeesWhoseAgeIsGreaterThan50FromEmployeesJsonObject(amazonS3);
		long t2 = System.currentTimeMillis();
		System.out.println("Time to read json : " + (t2 - t1) + " ms");
		System.out.println("------------------------------------------------------------");

		t1 = System.currentTimeMillis();
		getAny5EmployeesWhoseAgeIsGreaterThan50FromEmployeesCsvObject(amazonS3);
		t2 = System.currentTimeMillis();
		System.out.println("Time to read csv : " + (t2 - t1) + " ms");
		System.out.println("------------------------------------------------------------");

		t1 = System.currentTimeMillis();
		getAny5EmployeesWhoseAgeIsGreaterThan50FromEmployeesParquetObject(amazonS3);
		t2 = System.currentTimeMillis();
		System.out.println("Time to read parquet : " + (t2 - t1) + " ms");

	}

	private static void getAny5EmployeesWhoseAgeIsGreaterThan50FromEmployeesJsonObject(AmazonS3 amazonS3)
			throws IOException {
		SelectObjectContentRequest selectObjectContentRequest = new SelectObjectContentRequest();
		selectObjectContentRequest.setBucketName(BUCKET_NAME);
		selectObjectContentRequest.setKey(EMPLOYEES_JSON_FILE_NAME);
		selectObjectContentRequest.setExpressionType(ExpressionType.SQL);
		selectObjectContentRequest.setExpression("select s.* from S3Object[*].employees[*] s where s.age > 50 limit 5");
		selectObjectContentRequest
				.setInputSerialization(new InputSerialization().withJson(new JSONInput().withType(JSONType.DOCUMENT)));
		selectObjectContentRequest.setOutputSerialization(new OutputSerialization().withCsv(new CSVOutput()));
		SelectObjectContentResult selectObjectContentResult = amazonS3.selectObjectContent(selectObjectContentRequest);
		SelectObjectContentEventStream selectObjectContentEventStream = selectObjectContentResult.getPayload();
		Iterator<SelectObjectContentEvent> eventsIterator = selectObjectContentEventStream.getEventsIterator();
		while (eventsIterator.hasNext()) {
			SelectObjectContentEvent selectObjectContentEvent = eventsIterator.next();
			if (selectObjectContentEvent instanceof RecordsEvent) {
				RecordsEvent recordsEvent = (RecordsEvent) selectObjectContentEvent;
				System.out.println(new String(recordsEvent.getPayload().array()));
			}
		}
		selectObjectContentEventStream.close();
		selectObjectContentResult.close();
	}

	private static void getAny5EmployeesWhoseAgeIsGreaterThan50FromEmployeesCsvObject(AmazonS3 amazonS3)
			throws IOException {
		SelectObjectContentRequest selectObjectContentRequest = new SelectObjectContentRequest();
		selectObjectContentRequest.setBucketName(BUCKET_NAME);
		selectObjectContentRequest.setKey(EMPLOYEES_CSV_FILE_NAME);
		selectObjectContentRequest.setExpressionType(ExpressionType.SQL);
		selectObjectContentRequest.setExpression("select s.* from S3Object s where cast(s._3 as int) > 50 limit 5");
		selectObjectContentRequest.setInputSerialization(new InputSerialization().withCsv(new CSVInput()));
		selectObjectContentRequest.setOutputSerialization(new OutputSerialization().withCsv(new CSVOutput()));
		SelectObjectContentResult selectObjectContentResult = amazonS3.selectObjectContent(selectObjectContentRequest);
		SelectObjectContentEventStream selectObjectContentEventStream = selectObjectContentResult.getPayload();
		Iterator<SelectObjectContentEvent> eventsIterator = selectObjectContentEventStream.getEventsIterator();
		while (eventsIterator.hasNext()) {
			SelectObjectContentEvent selectObjectContentEvent = eventsIterator.next();
			if (selectObjectContentEvent instanceof RecordsEvent) {
				RecordsEvent recordsEvent = (RecordsEvent) selectObjectContentEvent;
				System.out.println(new String(recordsEvent.getPayload().array()));
			}
		}
		selectObjectContentEventStream.close();
		selectObjectContentResult.close();
	}

	private static void getAny5EmployeesWhoseAgeIsGreaterThan50FromEmployeesParquetObject(AmazonS3 amazonS3)
			throws IOException {
		SelectObjectContentRequest selectObjectContentRequest = new SelectObjectContentRequest();
		selectObjectContentRequest.setBucketName(BUCKET_NAME);
		selectObjectContentRequest.setKey(EMPLOYEES_PARQUET_FILE_NAME);
		selectObjectContentRequest.setExpressionType(ExpressionType.SQL);
		selectObjectContentRequest.setExpression("select * from S3Object where column2 > 50 limit 5");
		selectObjectContentRequest.setInputSerialization(new InputSerialization().withParquet(new ParquetInput()));
		selectObjectContentRequest.setOutputSerialization(new OutputSerialization().withCsv(new CSVOutput()));
		SelectObjectContentResult selectObjectContentResult = amazonS3.selectObjectContent(selectObjectContentRequest);
		SelectObjectContentEventStream selectObjectContentEventStream = selectObjectContentResult.getPayload();
		Iterator<SelectObjectContentEvent> eventsIterator = selectObjectContentEventStream.getEventsIterator();
		while (eventsIterator.hasNext()) {
			SelectObjectContentEvent selectObjectContentEvent = eventsIterator.next();
			if (selectObjectContentEvent instanceof RecordsEvent) {
				RecordsEvent recordsEvent = (RecordsEvent) selectObjectContentEvent;
				System.out.println(new String(recordsEvent.getPayload().array()));
			}
		}
		selectObjectContentEventStream.close();
		selectObjectContentResult.close();
	}

	private static void createBucketIfNotExists(AmazonS3 amazonS3) {
		if (!amazonS3.doesBucketExistV2(BUCKET_NAME)) {
			amazonS3.createBucket(BUCKET_NAME);
		}
	}

	private static AmazonS3 getAmazonS3Client() {
		return AmazonS3ClientBuilder.standard()
				.withCredentials(new AWSStaticCredentialsProvider(
						new BasicAWSCredentials("AKIA5DV4LDPDPQXDR6JO", "pQV8JRNWu62nT17qTAok8x5c97E4p53AfWDZvwNK")))
				.withRegion("us-east-1").build();
	}

	private static Employees generateEmployeeData() {
		Faker faker = new Faker();

		List<Employee> listOfEmployees = new LinkedList<>();
		for (int i = 1; i <= N; i++) {
			listOfEmployees.add(new Employee(i, faker.name().fullName(), faker.random().nextInt(21, 58)));
		}
		Employees employees = new Employees(listOfEmployees);
		return employees;
	}

	private static void exportEmployeesInJsonFormat(Employees employees, AmazonS3 amazonS3)
			throws IOException, StreamWriteException, DatabindException {
		File employeesJsonFile = new File(EMPLOYEES_JSON_FILE_NAME);
		amazonS3.putObject(BUCKET_NAME, EMPLOYEES_JSON_FILE_NAME, employeesJsonFile);
	}

	private static void exportEmployeesInCSVFormat(Employees employees, AmazonS3 amazonS3)
			throws IOException, StreamWriteException, DatabindException {
		File employeesCsvFile = new File(EMPLOYEES_CSV_FILE_NAME);
		amazonS3.putObject(BUCKET_NAME, EMPLOYEES_CSV_FILE_NAME, employeesCsvFile);
	}

	private static void exportEmployeesInParquetFormat(Employees employees, AmazonS3 amazonS3)
			throws StreamWriteException, DatabindException, IOException {
		File employeesParquetFile = new File(EMPLOYEES_PARQUET_FILE_NAME);
		amazonS3.putObject(BUCKET_NAME, EMPLOYEES_PARQUET_FILE_NAME, employeesParquetFile);
	}

}
