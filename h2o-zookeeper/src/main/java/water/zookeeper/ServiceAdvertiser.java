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

import com.google.common.collect.Sets;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.x.discovery.*;
import water.H2ONode;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Collection;
import java.util.HashSet;

/**
 * A curator based service registration class.
 * <p/>
 * Adapted from code example at http://curator.apache.org/curator-x-discovery/index.html
 */
public class ServiceAdvertiser implements Closeable {
    private final ServiceDiscovery<InstanceDetails> serviceDiscovery;
    private final ServiceInstance<InstanceDetails> thisInstance;
    private final CuratorFramework client;

    public ServiceAdvertiser(String zkConnectionString, String path, String serviceName, String uriSpec,
                             String myAddress, int port, String description, boolean autoStart) {

        client = CuratorFrameworkFactory.newClient(zkConnectionString,
                new ExponentialBackoffRetry(1000, 3));
        client.start();

        ServiceInstanceBuilder<InstanceDetails> serviceInstanceBuilder;
        try {
            serviceInstanceBuilder = ServiceInstance.<InstanceDetails>builder()
                    .name(serviceName)
                    .payload(new InstanceDetails(description))
                    .address(myAddress)
                    .port(port);

        } catch (Exception e) {
            throw new RuntimeException("Exception while joining zookeeper", e);
        }

        //{scheme}://foo.com:{port}
        if (uriSpec != null) {
            serviceInstanceBuilder.uriSpec(new UriSpec(uriSpec));
        }

        thisInstance = serviceInstanceBuilder.build();

        // if you mark your payload class with @JsonRootName the provided JsonInstanceSerializer will work
        JsonInstanceSerializer<InstanceDetails> serializer = new JsonInstanceSerializer<InstanceDetails>(InstanceDetails.class);

        serviceDiscovery = ServiceDiscoveryBuilder.builder(InstanceDetails.class)
                .client(client)
                .basePath(path)
                .serializer(serializer)
                .thisInstance(thisInstance)
                .build();

        if (autoStart) {
           start();
        }
    }

    public HashSet<H2ONode> getNodes() {

        try {
            Collection<String> serviceNames = serviceDiscovery.queryForNames();
            if (serviceNames.isEmpty()) {
                //impossible as the current node is expected to be present
                throw new IllegalStateException("no nodes found ");
            }
            HashSet<H2ONode> nodes = Sets.newHashSet();
            for (String serviceName : serviceNames) {
                Collection<ServiceInstance<InstanceDetails>> instances = serviceDiscovery
                        .queryForInstances(serviceName);
                for (ServiceInstance<InstanceDetails> instance : instances) {
                    nodes.add(H2ONode.intern(InetAddress.getByName(instance.getAddress()), instance.getPort()));
                }
            }
            return nodes;
        } catch (Exception e) {
            throw new RuntimeException("exception getting nodes list for this cloud", e);
        }
    }

    public ServiceInstance<InstanceDetails> getThisInstance() {
        return thisInstance;
    }

    public void start() {
        try {
            serviceDiscovery.start();
        } catch (Exception e) {
            throw new RuntimeException("exception getting nodes list for this cloud", e);
        }
    }

    @Override
    public void close() throws IOException {
        CloseableUtils.closeQuietly(serviceDiscovery);
        CloseableUtils.closeQuietly(client);
    }

}