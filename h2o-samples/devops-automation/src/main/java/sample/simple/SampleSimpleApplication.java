/*
 * Copyright 2012-2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package sample.simple;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import sample.simple.service.HelloWorldService;
import sample.simple.service.h2oService;

@SpringBootApplication
public class SampleSimpleApplication implements CommandLineRunner {

	static final String IMPORT_FILE = "/Users/paragsanghavi/Documents/h2o/smalldata/prostate/prostate.csv.zip";

	// Simple example shows how a command line spring application can execute an
	// injected bean service. Also demonstrates how you can use @Value to inject
	// command line args ('--name=whatever') or application properties

	@Autowired
	private HelloWorldService helloWorldService;

	@Autowired
	private h2oService h2oservice;

	private final Logger log = LoggerFactory.getLogger(SampleSimpleApplication.class);

	@Override
	public void run(String... args) {
		System.out.println(this.helloWorldService.getHelloMessage());
		String key_value = h2oservice.ImportCSVFile(IMPORT_FILE);
		if(!key_value.equalsIgnoreCase("error")){
			String destination_key = h2oservice.ParseCSVFile(key_value,"prostate_csv.hex");
			System.out.println(destination_key);
			if(destination_key!=null){
				String gbm_key_value = h2oservice.BuildGBMModel("gbmmodelDestinationKey", destination_key);
				if(gbm_key_value!=null) {
					String predict = h2oservice.PredictGBM(gbm_key_value, destination_key);
					if(predict!=null){
						Double AUC = h2oservice.CalculateAUC(destination_key,"CAPSULE", "1");
						String download_pojo_location = h2oservice.DownloadPOJO("gbmmodelDestinationKey");
						System.out.println(download_pojo_location);
					}else
					{
						System.out.println("Error in prediction");
					}
				}else{
					System.out.println("Error in building GBM model");
				}

			}else{
				System.out.println("Error in parsing data set");
			}

		}else{
			log.debug("Error occurred in Importing File, {}", IMPORT_FILE);
		}
	}

	public static void main(String[] args) throws Exception {
		SpringApplication.run(SampleSimpleApplication.class, args);
		System.exit(0);
	}
}
