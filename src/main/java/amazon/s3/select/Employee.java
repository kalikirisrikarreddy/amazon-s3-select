package amazon.s3.select;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonPropertyOrder({ "id", "name", "age" })
public record Employee(Integer id, String name, Integer age) {

}
