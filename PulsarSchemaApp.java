/**
Write a Spring Boot App that calls Pulsar Schema 
 REST end point /admin/v2/schemas/public/default/thermalsensors/schema and iterates the list of results
 

Here's a sample Spring Boot application that calls the Pulsar Schema REST endpoint and iterates through the list of results:



This code creates a RestTemplate to call the Pulsar Schema REST endpoint, and then calls the /admin/v2/schemas/public/default/thermalsensors/schema endpoint to get the list of schemas. It then iterates through the list of schemas and prints each schema to the console.

Note that this code is just a sample and may not work as-is. You may need to adjust the code to fit your specific use case and environment.

*/
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.client.RestTemplate;

@SpringBootApplication
public class PulsarSchemaApp {

    public static void main(String[] args) {
        SpringApplication.run(PulsarSchemaApp.class, args);

        // Create a RestTemplate to call the Pulsar Schema REST endpoint
        RestTemplate restTemplate = new RestTemplate();

        // Call the REST endpoint to get the list of schemas
        String schemasUrl = "http://localhost:8080/admin/v2/schemas/public/default/thermalsensors/schema";
        String[] schemas = restTemplate.getForObject(schemasUrl, String[].class);

        // Iterate through the list of schemas
        for (String schema : schemas) {
            System.out.println(schema);
        }
    }

}

