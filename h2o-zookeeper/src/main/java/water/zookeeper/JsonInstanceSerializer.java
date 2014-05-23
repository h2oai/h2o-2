/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package water.discovery;

import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.details.InstanceSerializer;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;

/**
 * A serializer that uses GSON to serialize/deserialize as JSON.
 */
public class JsonInstanceSerializer<T> implements InstanceSerializer<T> {
    final Gson gson = new Gson();
    private final Class<T> payloadClass;
    private TypeToken<ServiceInstance<T>> token;

    /**
     * @param payloadClass used to validate payloads when deserializing
     */
    public JsonInstanceSerializer(Class<T> payloadClass) {
        this.payloadClass = payloadClass;
        this.token = new TypeToken<ServiceInstance<T>>(){}.where(new TypeParameter<T>() {}, payloadClass);
    }

    @SuppressWarnings({"unchecked"})
    @Override
    public ServiceInstance<T> deserialize(byte[] bytes) throws Exception {
        ServiceInstance rawServiceInstance = gson.fromJson(
               new InputStreamReader(new ByteArrayInputStream(bytes)), token.getType());
        payloadClass.cast(rawServiceInstance.getPayload()); // just to verify that it's the correct type
        return (ServiceInstance<T>) rawServiceInstance;
    }

    @Override
    public byte[] serialize(ServiceInstance<T> instance) throws Exception {
        String s = gson.toJson(instance, token.getType());
        return s.getBytes(Charset.forName("utf8"));
    }


//    public static void main(String[] args) throws Exception {
//        JsonInstanceSerializer<InstanceDetails> serializer = new JsonInstanceSerializer<InstanceDetails>
//                (InstanceDetails.class);
//        InstanceDetails payload = new InstanceDetails();
//        payload.setDescription("Somethingcool");
//        ServiceInstance<InstanceDetails> serviceInstance = ServiceInstance.<InstanceDetails>builder()
//                .name("test")
//                .address ("192.128.123.12")
//                .port(80)
//                .payload(payload)
//                .build();
//        byte[] serialize = serializer.serialize(serviceInstance);
//        System.out.println("serialize = " + serialize.length);
//        ServiceInstance<InstanceDetails> deserialize = serializer.deserialize(serialize);
//        System.out.println("deserialize = " + deserialize.getPayload().getDescription());
//    }
}
