/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs.server.datanode;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSCluster.DataNodeProperties;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.namenode.FSImageTestUtil;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.util.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestDataNodeMultipleRegistrations {
  private static final Log LOG = 
    LogFactory.getLog(TestDataNodeMultipleRegistrations.class);
  Configuration conf;

  @Before
  public void setUp() throws Exception {
    conf = new HdfsConfiguration();
  }

  @Test(timeout = 20000)
  public void testClusterIdMismatchAtStartupWithHA() throws Exception {
    MiniDFSNNTopology top = new MiniDFSNNTopology()
      .addNameservice(new MiniDFSNNTopology.NSConf("ns1")
        .addNN(new MiniDFSNNTopology.NNConf("nn0"))
        .addNN(new MiniDFSNNTopology.NNConf("nn1")))
      .addNameservice(new MiniDFSNNTopology.NSConf("ns2")
        .addNN(new MiniDFSNNTopology.NNConf("nn2").setClusterId("bad-cid"))
        .addNN(new MiniDFSNNTopology.NNConf("nn3").setClusterId("bad-cid")));

    top.setFederation(true);

    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).nnTopology(top)
        .numDataNodes(0).build();

    try {
      cluster.startDataNodes(conf, 1, true, null, null);
      // let the initialization be complete
      Thread.sleep(10000);
      DataNode dn = cluster.getDataNodes().get(0);
      assertTrue("Datanode should be running", dn.isDatanodeUp());
      assertEquals("Only one BPOfferService should be running", 1,
          dn.getAllBpOs().length);
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testDNWithInvalidStorageWithHA() throws Exception {
    MiniDFSNNTopology top = new MiniDFSNNTopology()
      .addNameservice(new MiniDFSNNTopology.NSConf("ns1")
        .addNN(new MiniDFSNNTopology.NNConf("nn0").setClusterId("cluster-1"))
        .addNN(new MiniDFSNNTopology.NNConf("nn1").setClusterId("cluster-1")));

    top.setFederation(true);

    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).nnTopology(top)
        .numDataNodes(0).build();
    try {
      cluster.startDataNodes(conf, 1, true, null, null);
      // let the initialization be complete
      Thread.sleep(10000);
      DataNode dn = cluster.getDataNodes().get(0);
      assertTrue("Datanode should be running", dn.isDatanodeUp());
      assertEquals("BPOfferService should be running", 1,
          dn.getAllBpOs().length);
      DataNodeProperties dnProp = cluster.stopDataNode(0);

      cluster.getNameNode(0).stop();
      cluster.getNameNode(1).stop();
      Configuration nn1 = cluster.getConfiguration(0);
      Configuration nn2 = cluster.getConfiguration(1);
      // setting up invalid cluster
      StartupOption.FORMAT.setClusterId("cluster-2");
      DFSTestUtil.formatNameNode(nn1);
      MiniDFSCluster.copyNameDirs(FSNamesystem.getNamespaceDirs(nn1),
          FSNamesystem.getNamespaceDirs(nn2), nn2);
      cluster.restartNameNode(0, false);
      cluster.restartNameNode(1, false);
      cluster.restartDataNode(dnProp);

      // let the initialization be complete
      Thread.sleep(10000);
      dn = cluster.getDataNodes().get(0);
      assertFalse("Datanode should have shutdown as only service failed",
          dn.isDatanodeUp());
    } finally {
      cluster.shutdown();
    }
  }
}
