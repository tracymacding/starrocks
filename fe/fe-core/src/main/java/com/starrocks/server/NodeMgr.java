// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/qe/ConnectProcessor.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.server;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.google.gson.stream.JsonReader;
import com.starrocks.catalog.BrokerMgr;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.Pair;
import com.starrocks.common.util.NetUtils;
import com.starrocks.ha.BDBHA;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.ha.HAProtocol;
import com.starrocks.ha.LeaderInfo;
import com.starrocks.http.meta.MetaBaseAction;
import com.starrocks.leader.MetaHelper;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.Storage;
import com.starrocks.persist.StorageInfo;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.qe.QueryStatisticsInfo;
import com.starrocks.rpc.ThriftConnectionPool;
import com.starrocks.rpc.ThriftRPCRequestExecutor;
import com.starrocks.service.FrontendOptions;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.ModifyFrontendAddressClause;
import com.starrocks.staros.StarMgrServer;
import com.starrocks.system.Frontend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TGetQueryStatisticsRequest;
import com.starrocks.thrift.TGetQueryStatisticsResponse;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TStatusCode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class NodeMgr {
    private static final Logger LOG = LogManager.getLogger(NodeMgr.class);
    private static final int HTTP_TIMEOUT_SECOND = 5;

    /**
     * LeaderInfo
     */
    @SerializedName(value = "r")
    private volatile int leaderRpcPort;
    @SerializedName(value = "h")
    private volatile int leaderHttpPort;
    @SerializedName(value = "ip")
    private volatile String leaderIp;

    /**
     * Frontends
     * <p>
     * frontends : name -> Frontend
     * removedFrontends: removed frontends' name. used for checking if name is duplicated in bdbje
     */
    @SerializedName(value = "f")
    private ConcurrentHashMap<String, Frontend> frontends = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, Frontend> frontendIds = new ConcurrentHashMap<>();

    @SerializedName(value = "rf")
    private ConcurrentLinkedQueue<String> removedFrontends = new ConcurrentLinkedQueue<>();

    /**
     * Backends and Compute Node
     */
    @SerializedName(value = "s")
    private SystemInfoService systemInfo;

    /**
     * Broker
     */
    @SerializedName(value = "b")
    private BrokerMgr brokerMgr;

    private boolean isFirstTimeStartUp = false;
    private boolean isElectable;

    // node name is used for bdbje NodeName.
    private String nodeName;
    private FrontendNodeType role;

    private int clusterId;
    private String token;
    private String runMode;

    private final List<Pair<String, Integer>> helperNodes = Lists.newArrayList();
    private Pair<String, Integer> selfNode = null;

    private final AtomicInteger leaderChangeListenerIndex = new AtomicInteger();
    private final Map<Integer, Consumer<LeaderInfo>> leaderChangeListeners = new ConcurrentHashMap<>();

    public NodeMgr() {
        this.role = FrontendNodeType.UNKNOWN;
        this.leaderRpcPort = 0;
        this.leaderHttpPort = 0;
        this.leaderIp = "";
        this.systemInfo = new SystemInfoService();

        this.brokerMgr = new BrokerMgr();
    }

    // For test
    protected NodeMgr(FrontendNodeType role, String nodeName, Pair<String, Integer> selfNode) {
        this.role = role;
        this.nodeName = nodeName;
        this.selfNode = selfNode;
    }

    public void initialize(String helpers) throws Exception {
        getCheckedSelfHostPort();
        getHelperNodes(helpers);
    }

    private boolean tryLock(boolean mustLock) {
        return GlobalStateMgr.getCurrentState().tryLock(mustLock);
    }

    private void unlock() {
        GlobalStateMgr.getCurrentState().unlock();
    }

    public void registerLeaderChangeListener(Consumer<LeaderInfo> listener) {
        Integer index = leaderChangeListenerIndex.getAndIncrement();
        leaderChangeListeners.put(index, listener);
    }

    public List<Frontend> getAllFrontends() {
        return Lists.newArrayList(frontends.values());
    }

    // All frontends except self
    public List<Frontend> getOtherFrontends() {
        return frontends
                .values()
                .stream()
                .filter(frontend -> !frontend.getNodeName().equals(nodeName))
                .collect(Collectors.toList());
    }

    public List<Frontend> getFrontends(FrontendNodeType nodeType) {
        if (nodeType == null) {
            // get all
            return Lists.newArrayList(frontends.values());
        }

        List<Frontend> result = Lists.newArrayList();
        for (Frontend frontend : frontends.values()) {
            if (frontend.getRole() == nodeType) {
                result.add(frontend);
            }
        }

        return result;
    }

    public long getAliveFrontendsCnt() {
        return frontends.values().stream().filter(Frontend::isAlive).count();
    }

    public Frontend getFrontend(Integer frontendId) {
        if (frontendId == 0) {
            return getMySelf();
        }
        return frontendIds.get(frontendId);
    }

    public List<String> getRemovedFrontendNames() {
        return Lists.newArrayList(removedFrontends);
    }

    public SystemInfoService getClusterInfo() {
        return this.systemInfo;
    }

    public BrokerMgr getBrokerMgr() {
        return brokerMgr;
    }

    public boolean isVersionAndRoleFilesNotExist() {
        File roleFile = new File(GlobalStateMgr.getImageDirPath(), Storage.ROLE_FILE);
        File versionFile = new File(GlobalStateMgr.getImageDirPath(), Storage.VERSION_FILE);
        return !roleFile.exists() && !versionFile.exists();
    }

    private void removeMetaFileIfExist(String fileName) {
        try {
            File file = new File(GlobalStateMgr.getImageDirPath(), fileName);
            if (file.exists()) {
                if (file.delete()) {
                    LOG.warn("Deleted file {}, maybe because the firstly startup failed.", file.getAbsolutePath());
                } else {
                    LOG.warn("Failed to delete role file {}.", file.getAbsolutePath());
                }
            }
        } catch (Exception e) {
            LOG.warn("Exception occurs while deleting file {}, reason: {}", fileName, e.getMessage());
        }
    }

    public void removeClusterIdAndRole() {
        removeMetaFileIfExist(Storage.ROLE_FILE);
        removeMetaFileIfExist(Storage.VERSION_FILE);
    }

    public void getClusterIdAndRoleOnStartup() throws IOException {
        String imageDir = GlobalStateMgr.getImageDirPath();
        File roleFile = new File(imageDir, Storage.ROLE_FILE);
        File versionFile = new File(imageDir, Storage.VERSION_FILE);

        boolean isVersionFileChanged = false;

        Storage storage = new Storage(imageDir);

        // prepare starmgr dir
        if (RunMode.isSharedDataMode()) {
            String subDir = imageDir + StarMgrServer.IMAGE_SUBDIR;
            File dir = new File(subDir);
            if (!dir.exists()) { // subDir might not exist
                LOG.info("create image dir for star mgr, {}.", dir.getAbsolutePath());
                if (!dir.mkdir()) {
                    LOG.error("create image dir for star mgr failed! exit now.");
                    System.exit(-1);
                }
            }
        }

        // if helper node is point to self, or there is ROLE and VERSION file in local.
        // get the node type from local
        if (isMyself() || (roleFile.exists() && versionFile.exists())) {

            if (!isMyself()) {
                LOG.info("find ROLE and VERSION file in local, ignore helper nodes: {}", helperNodes);
            }

            // check file integrity, if has.
            if ((roleFile.exists() && !versionFile.exists())
                    || (!roleFile.exists() && versionFile.exists())) {
                LOG.error("role file and version file must both exist or both not exist. "
                        + "please specific one helper node to recover. will exit.");
                System.exit(-1);
            }

            // ATTN:
            // If the version file and role file does not exist and the helper node is itself,
            // this should be the very beginning startup of the cluster, so we create ROLE and VERSION file,
            // set isFirstTimeStartUp to true, and add itself to frontends list.
            // If ROLE and VERSION file is deleted for some reason, we may arbitrarily start this node as
            // FOLLOWER, which may cause UNDEFINED behavior.
            // Everything may be OK if the origin role is exactly FOLLOWER,
            // but if not, FE process will exit somehow.
            if (!roleFile.exists()) {
                // The very first time to start the first node of the cluster.
                // It should became a Master node (Master node's role is also FOLLOWER, which means electable)

                // For compatibility. Because this is the very first time to start, so we arbitrarily choose
                // a new name for this node
                role = FrontendNodeType.FOLLOWER;
                nodeName = genFeNodeName(selfNode.first, selfNode.second, false /* new style */);
                storage.writeFrontendRoleAndNodeName(role, nodeName);
                LOG.info("very first time to start this node. role: {}, node name: {}", role.name(), nodeName);
            } else {
                role = storage.getRole();
                nodeName = storage.getNodeName();
                if (Strings.isNullOrEmpty(nodeName)) {
                    // In normal case, if ROLE file exist, role and nodeName should both exist.
                    // But we will get a empty nodeName after upgrading.
                    // So for forward compatibility, we use the "old-style" way of naming: "ip_port",
                    // and update the ROLE file.
                    nodeName = genFeNodeName(selfNode.first, selfNode.second, true/* old style */);
                    storage.writeFrontendRoleAndNodeName(role, nodeName);
                    LOG.info("forward compatibility. role: {}, node name: {}", role.name(), nodeName);
                } else if (Config.bdbje_reset_election_group
                        && !isFeNodeNameValid(nodeName, selfNode.first, selfNode.second)) {
                    // Invalid node name, usually happened when the image dir is copied from another node.
                    // Correct the node name
                    String oldNodeName = nodeName;
                    nodeName = genFeNodeName(selfNode.first, selfNode.second, false /* new style */);
                    storage.writeFrontendRoleAndNodeName(role, nodeName);
                    LOG.info("correct the node name {} to new node name: {}, role: {}", oldNodeName, nodeName,
                            role.name());
                }
            }
            Preconditions.checkNotNull(role);
            Preconditions.checkNotNull(nodeName);

            if (!versionFile.exists()) {
                clusterId = Storage.newClusterID();
                token = Strings.isNullOrEmpty(Config.auth_token) ?
                        Storage.newToken() : Config.auth_token;
                storage = new Storage(clusterId, token, imageDir);
                isVersionFileChanged = true;

                isFirstTimeStartUp = true;
                int fid = allocateNextFrontendId();
                Frontend self = new Frontend(fid, role, nodeName, selfNode.first, selfNode.second);
                // We don't need to check if frontends already contains self.
                // frontends must be empty cause no image is loaded and no journal is replayed yet.
                // And this frontend will be persisted later after opening bdbje environment.
                frontends.put(nodeName, self);
                frontendIds.put(fid, self);
            } else {
                clusterId = storage.getClusterID();
                if (storage.getToken() == null) {
                    token = Strings.isNullOrEmpty(Config.auth_token) ?
                            Storage.newToken() : Config.auth_token;
                    LOG.info("new token={}", token);
                    storage.setToken(token);
                    isVersionFileChanged = true;
                } else {
                    token = storage.getToken();
                }
                runMode = storage.getRunMode();
                isFirstTimeStartUp = false;
            }
        } else {
            // try to get role and node name from helper node,
            // this loop will not end until we get certain role type and name
            while (true) {
                if (!getFeNodeTypeAndNameFromHelpers()) {
                    LOG.warn("current node is not added to the group. please add it first. "
                            + "sleep 5 seconds and retry, current helper nodes: {}", helperNodes);
                    try {
                        Thread.sleep(5000);
                        continue;
                    } catch (InterruptedException e) {
                        LOG.warn("Failed to execute sleep", e);
                        System.exit(-1);
                    }
                }

                break;
            }

            Preconditions.checkState(helperNodes.size() == 1);
            Preconditions.checkNotNull(role);
            Preconditions.checkNotNull(nodeName);

            Pair<String, Integer> rightHelperNode = helperNodes.get(0);

            storage = new Storage(imageDir);
            if (roleFile.exists() && (role != storage.getRole() || !nodeName.equals(storage.getNodeName()))
                    || !roleFile.exists()) {
                storage.writeFrontendRoleAndNodeName(role, nodeName);
            }
            if (!versionFile.exists()) {
                // If the version file doesn't exist, download it from helper node
                if (!getVersionFileFromHelper(rightHelperNode)) {
                    System.exit(-1);
                }

                // NOTE: cluster_id will be init when Storage object is constructed,
                //       so we new one.
                storage = new Storage(imageDir);
                clusterId = storage.getClusterID();
                token = storage.getToken();
                runMode = storage.getRunMode();
                if (Strings.isNullOrEmpty(token)) {
                    token = Config.auth_token;
                    isVersionFileChanged = true;
                }
                if (Strings.isNullOrEmpty(runMode)) {
                    // The version of helper node is less than 3.0, run at SAHRED_NOTHING mode and save the run
                    // mode in version file later.
                    runMode = RunMode.SHARED_NOTHING.getName();
                    storage.setRunMode(runMode);
                    isVersionFileChanged = true;
                }
            } else {
                // If the version file exist, read the cluster id and check the
                // id with helper node to make sure they are identical
                clusterId = storage.getClusterID();
                token = storage.getToken();
                runMode = storage.getRunMode();
                if (Strings.isNullOrEmpty(runMode)) {
                    // No run mode saved in the version file, we're upgrading an old cluster of version less than 3.0.
                    runMode = RunMode.SHARED_NOTHING.getName();
                    storage.setRunMode(runMode);
                    isVersionFileChanged = true;
                }
                try {
                    URL idURL = new URL("http://" + NetUtils.getHostPortInAccessibleFormat(
                            rightHelperNode.first, Config.http_port) + "/check");
                    HttpURLConnection conn = null;
                    conn = (HttpURLConnection) idURL.openConnection();
                    conn.setConnectTimeout(2 * 1000);
                    conn.setReadTimeout(2 * 1000);

                    String remoteToken = conn.getHeaderField(MetaBaseAction.TOKEN);
                    if (token == null && remoteToken != null) {
                        LOG.info("get token from helper node. token={}.", remoteToken);
                        token = remoteToken;
                        isVersionFileChanged = true;
                        storage.reload();
                    }
                    if (Config.enable_token_check) {
                        Preconditions.checkNotNull(token);
                        Preconditions.checkNotNull(remoteToken);
                        if (!token.equals(remoteToken)) {
                            LOG.error("token is not equal with helper node {}. will exit.", rightHelperNode.first);
                            System.exit(-1);
                        }
                    }

                    String remoteRunMode = conn.getHeaderField(MetaBaseAction.RUN_MODE);
                    if (Strings.isNullOrEmpty(remoteRunMode)) {
                        // The version of helper node is less than 3.0
                        remoteRunMode = RunMode.SHARED_NOTHING.getName();
                    }

                    if (!runMode.equalsIgnoreCase(remoteRunMode)) {
                        LOG.error("Unmatched run mode with helper node {}: {} vs {}, will exit .",
                                rightHelperNode.first, runMode, remoteRunMode);
                        System.exit(-1);
                    }
                } catch (Exception e) {
                    LOG.warn("fail to check cluster_id and token with helper node.", e);
                    System.exit(-1);
                }
            }
            getNewImageOnStartup(rightHelperNode, "");
            if (RunMode.isSharedDataMode()) { // get star mgr image
                getNewImageOnStartup(rightHelperNode, StarMgrServer.IMAGE_SUBDIR);
            }
        }

        if (Strings.isNullOrEmpty(runMode)) {
            if (isFirstTimeStartUp) {
                runMode = RunMode.name();
                storage.setRunMode(runMode);
                isVersionFileChanged = true;
            } else if (RunMode.isSharedDataMode()) {
                LOG.error("Upgrading from a cluster with version less than 3.0 to a cluster with run mode {} of " +
                        "version 3.0 or above is disallowed. will exit", RunMode.name());
                System.exit(-1);
            }
        } else if (!runMode.equalsIgnoreCase(RunMode.name())) {
            LOG.error("Unmatched run mode between config file and version file: {} vs {}. will exit! ",
                    RunMode.name(), runMode);
            System.exit(-1);
        } // else nothing to do

        if (isVersionFileChanged) {
            storage.writeVersionFile();
        }

        // Tell user current run_mode
        LOG.info("Current run_mode is {}", runMode);

        isElectable = role.equals(FrontendNodeType.FOLLOWER);

        Preconditions.checkState(helperNodes.size() == 1);
        LOG.info("Got role: {}, node name: {} and run_mode: {}", role.name(), nodeName, runMode);
    }

    // Get the role info and node name from helper node.
    // return false if failed.
    private boolean getFeNodeTypeAndNameFromHelpers() {
        // we try to get info from helper nodes, once we get the right helper node,
        // other helper nodes will be ignored and removed.
        Pair<String, Integer> rightHelperNode = null;
        for (Pair<String, Integer> helperNode : helperNodes) {
            try {
                String accessibleHostPort = NetUtils.getHostPortInAccessibleFormat(helperNode.first, Config.http_port);
                String encodedAddress = URLEncoder.encode(selfNode.first,
                        StandardCharsets.UTF_8.toString());
                URL url = new URL("http://" + accessibleHostPort + "/role?host=" + encodedAddress +
                        "&port=" + selfNode.second);
                HttpURLConnection conn = null;
                conn = (HttpURLConnection) url.openConnection();
                if (conn.getResponseCode() != 200) {
                    LOG.warn("failed to get fe node type from helper node: {}. response code: {}",
                            helperNode, conn.getResponseCode());
                    continue;
                }

                String type = conn.getHeaderField("role");
                if (type == null) {
                    LOG.warn("failed to get fe node type from helper node: {}.", helperNode);
                    continue;
                }
                role = FrontendNodeType.valueOf(type);
                nodeName = conn.getHeaderField("name");

                // get role and node name before checking them, because we want to throw any exception
                // as early as we encounter.

                if (role == FrontendNodeType.UNKNOWN) {
                    LOG.warn("frontend {} is not added to cluster yet. role UNKNOWN", selfNode);
                    return false;
                }

                if (Strings.isNullOrEmpty(nodeName)) {
                    // For forward compatibility, we use old-style name: "ip_port"
                    nodeName = genFeNodeName(selfNode.first, selfNode.second, true /* old style */);
                }
            } catch (Exception e) {
                LOG.warn("failed to get fe node type from helper node: {}.", helperNode, e);
                continue;
            }

            LOG.info("get fe node type {}, name {} from {}:{}", role, nodeName, helperNode.first, Config.http_port);
            rightHelperNode = helperNode;
            break;
        }

        if (rightHelperNode == null) {
            return false;
        }

        helperNodes.clear();
        helperNodes.add(rightHelperNode);
        return true;
    }

    private boolean isMyself() {
        Preconditions.checkNotNull(selfNode);
        Preconditions.checkNotNull(helperNodes);
        LOG.debug("self: {}. helpers: {}", selfNode, helperNodes);
        // if helper nodes contain it self, remove other helpers
        boolean containSelf = false;
        for (Pair<String, Integer> helperNode : helperNodes) {
            if (selfNode.equals(helperNode)) {
                containSelf = true;
                break;
            }
        }
        if (containSelf) {
            helperNodes.clear();
            helperNodes.add(selfNode);
        }

        return containSelf;
    }

    private StorageInfo getStorageInfo(URL url) throws IOException {
        HttpURLConnection connection = null;
        try {
            connection = (HttpURLConnection) url.openConnection();
            connection.setConnectTimeout(HTTP_TIMEOUT_SECOND * 1000);
            connection.setReadTimeout(HTTP_TIMEOUT_SECOND * 1000);

            InputStreamReader inputStreamReader = new InputStreamReader(connection.getInputStream());
            JsonReader jsonReader = new JsonReader(inputStreamReader);
            return GsonUtils.GSON.fromJson(jsonReader, StorageInfo.class);
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    private void getHelperNodes(String helpers) {
        if (!Strings.isNullOrEmpty(helpers)) {
            String[] splittedHelpers = helpers.split(",");
            for (String helper : splittedHelpers) {
                Pair<String, Integer> helperHostPort = SystemInfoService.validateHostAndPort(helper, false);
                if (helperHostPort.equals(selfNode)) {
                    /*
                     * If user specified the helper node to this FE itself,
                     * we will stop the starting FE process and report an error.
                     * First, it is meaningless to point the helper to itself.
                     * Secondly, when some users add FE for the first time, they will mistakenly
                     * point the helper that should have pointed to the Master to themselves.
                     * In this case, some errors have caused users to be troubled.
                     * So here directly exit the program and inform the user to avoid unnecessary trouble.
                     */
                    throw new SemanticException(
                            "Do not specify the helper node to FE itself. "
                                    + "Please specify it to the existing running Leader or Follower FE");
                }
                helperNodes.add(helperHostPort);
            }
        } else {
            // If helper node is not designated, use local node as helper node.
            helperNodes.add(Pair.create(selfNode.first, Config.edit_log_port));
        }

        LOG.info("get helper nodes: {}", helperNodes);
    }

    private void getCheckedSelfHostPort() {
        selfNode = new Pair<>(FrontendOptions.getLocalHostAddress(), Config.edit_log_port);
        /*
         * For the first time, if the master start up failed, it will also fail to restart.
         * Check port using before create meta files to avoid this problem.
         */
        try {
            if (NetUtils.isPortUsing(selfNode.first, selfNode.second)) {
                LOG.error("edit_log_port {} is already in use. will exit.", selfNode.second);
                System.exit(-1);
            }
        } catch (UnknownHostException e) {
            LOG.error(e.getMessage(), e);
            System.exit(-1);
        }
        LOG.debug("get self node: {}", selfNode);
    }

    public Pair<String, Integer> getHelperNode() {
        Preconditions.checkState(helperNodes.size() >= 1);
        return this.helperNodes.get(0);
    }

    public List<Pair<String, Integer>> getHelperNodes() {
        return Lists.newArrayList(helperNodes);
    }

    /*
     * If the current node is not in the frontend list, then exit. This may
     * happen when this node is removed from frontend list, and the drop
     * frontend log is deleted because of checkpoint.
     */
    public void checkCurrentNodeExist() {
        if (Config.bdbje_reset_election_group) {
            return;
        }

        Frontend fe = checkFeExist(selfNode.first, selfNode.second);
        if (fe == null) {
            LOG.error("current node is not added to the cluster, will exit");
            System.exit(-1);
        } else if (fe.getRole() != role) {
            LOG.error("current node role is {} not match with frontend recorded role {}. will exit", role,
                    fe.getRole());
            System.exit(-1);
        }
    }

    private boolean getVersionFileFromHelper(Pair<String, Integer> helperNode) throws IOException {
        String url = "http://" + NetUtils.getHostPortInAccessibleFormat(
                helperNode.first, Config.http_port) + "/version";
        LOG.info("Downloading version file from {}", url);
        try {
            File dir = new File(GlobalStateMgr.getImageDirPath());
            MetaHelper.getRemoteFile(url, HTTP_TIMEOUT_SECOND * 1000,
                    MetaHelper.getOutputStream(Storage.VERSION_FILE, dir));
            MetaHelper.complete(Storage.VERSION_FILE, dir);
            return true;
        } catch (Exception e) {
            LOG.warn("Fail to download version file from {}:{}", url, e.getMessage());
        }

        return false;
    }

    /**
     * When a new node joins in the cluster for the first time, it will download image from the helper at the very beginning
     * Exception are free to raise on initialized phase
     */
    private void getNewImageOnStartup(Pair<String, Integer> helperNode, String subDir) throws IOException {
        String imageFileDir = MetaHelper.getImageFileDir(Strings.isNullOrEmpty(subDir));
        Storage storage = new Storage(imageFileDir);
        long localImageJournalId = storage.getImageJournalId();

        String accessibleHostPort = NetUtils.getHostPortInAccessibleFormat(helperNode.first, Config.http_port);
        URL infoUrl = new URL("http://" + accessibleHostPort + "/info?subdir=" + subDir);
        StorageInfo remoteStorageInfo = getStorageInfo(infoUrl);
        long remoteImageJournalId = remoteStorageInfo.getImageJournalId();
        if (remoteImageJournalId > localImageJournalId) {
            String url = "http://" + accessibleHostPort + "/image?"
                    + "version=" + remoteImageJournalId
                    + "&subdir=" + subDir
                    + "&image_format_version=" + remoteStorageInfo.getImageFormatVersion();
            LOG.info("start to download image.{} version:{}, from {}", remoteImageJournalId,
                    remoteStorageInfo.getImageFormatVersion(), url);
            MetaHelper.downloadImageFile(url, HTTP_TIMEOUT_SECOND * 1000,
                    Long.toString(remoteImageJournalId), new File(imageFileDir));
        } else {
            LOG.info("skip download image for {}, current version {} >= version {} from {}",
                    imageFileDir, localImageJournalId, remoteImageJournalId, helperNode);
        }
    }

    public void addFrontend(FrontendNodeType role, String host, int editLogPort) throws DdlException {
        if (!tryLock(false)) {
            throw new DdlException("Failed to acquire globalStateMgr lock. Try again");
        }
        try {
            try {
                if (checkFeExistByIpOrFqdn(host)) {
                    throw new DdlException("FE with the same host: " + host + " already exists");
                }
            } catch (UnknownHostException e) {
                LOG.warn("failed to get right ip by fqdn {}", host, e);
                throw new DdlException("unknown fqdn host: " + host);
            }

            String nodeName = genFeNodeName(host, editLogPort, false /* new name style */);

            if (removedFrontends.contains(nodeName)) {
                throw new DdlException("frontend name already exists " + nodeName + ". Try again");
            }

            int fid = allocateNextFrontendId();
            if (fid == 0) {
                throw new DdlException("No available frontend ID can allocate to new frontend");
            }
            Frontend fe = new Frontend(fid, role, nodeName, host, editLogPort);
            frontends.put(nodeName, fe);
            frontendIds.put(fid, fe);
            if (role == FrontendNodeType.FOLLOWER) {
                helperNodes.add(Pair.create(host, editLogPort));
            }
            if (GlobalStateMgr.getCurrentState().getHaProtocol() instanceof BDBHA) {
                BDBHA bdbha = (BDBHA) GlobalStateMgr.getCurrentState().getHaProtocol();
                if (role == FrontendNodeType.FOLLOWER) {
                    bdbha.addUnstableNode(host, getFollowerCnt());
                }

                // In some cases, for example, fe starts with the outdated meta, the node name that has been dropped
                // will remain in bdb.
                // So we should remove those nodes before joining the group,
                // or it will throws NodeConflictException (New or moved node:xxxx, is configured with the socket address:
                // xxx. It conflicts with the socket already used by the member: xxxx)
                bdbha.removeNodeIfExist(host, editLogPort, nodeName);
            }

            GlobalStateMgr.getCurrentState().getEditLog().logJsonObject(OperationType.OP_ADD_FRONTEND_V2, fe);
        } finally {
            unlock();
        }
    }

    /**
     * Allocates the next available ID, ensuring it stays within the range [1, 255].
     * If the ID reaches 255, it wraps around and searches from 1.
     *
     * @return the allocated ID, or 0 if all IDs are occupied.
     */
    public int allocateNextFrontendId() {
        if (frontendIds.isEmpty()) {
            return 1; // Start from 0 if no IDs are assigned yet
        }

        // Find the max key in the current map
        int maxKey = Collections.max(frontendIds.keySet());

        // Try to allocate the next available ID
        for (int i = 0; i <= 255; i++) {
            int candidateId = ((maxKey + i) % 255) + 1;
            if (!frontendIds.containsKey(candidateId)) {
                return candidateId; // Found an available ID
            }
        }

        return 0; //No available frontend ID can allocate to new frontend
    }

    public void modifyFrontendHost(ModifyFrontendAddressClause modifyFrontendAddressClause) throws DdlException {
        String toBeModifyHost = modifyFrontendAddressClause.getSrcHost();
        String fqdn = modifyFrontendAddressClause.getDestHost();
        if (toBeModifyHost.equals(selfNode.first) && role == FrontendNodeType.LEADER) {
            throw new DdlException("can not modify current master node.");
        }
        if (!tryLock(false)) {
            throw new DdlException("Failed to acquire globalStateMgr lock. Try again");
        }
        try {
            Frontend preUpdateFe = getFeByHost(toBeModifyHost);
            if (preUpdateFe == null) {
                throw new DdlException(String.format("frontend [%s] not found", toBeModifyHost));
            }

            Frontend existFe = null;
            for (Frontend fe : frontends.values()) {
                if (fe.getHost().equals(fqdn)) {
                    existFe = fe;
                }
            }

            if (null != existFe) {
                throw new DdlException("frontend with host [" + fqdn + "] already exists ");
            }

            // step 1 update the fe information stored in bdb
            BDBHA bdbha = (BDBHA) GlobalStateMgr.getCurrentState().getHaProtocol();
            bdbha.updateFrontendHostAndPort(preUpdateFe.getNodeName(), fqdn, preUpdateFe.getEditLogPort());
            // step 2 update the fe information stored in memory
            preUpdateFe.updateHostAndEditLogPort(fqdn, preUpdateFe.getEditLogPort());
            frontends.put(preUpdateFe.getNodeName(), preUpdateFe);

            // editLog
            GlobalStateMgr.getCurrentState().getEditLog().logUpdateFrontend(preUpdateFe);
            LOG.info("send update fe editlog success, fe info is [{}]", preUpdateFe.toString());
        } finally {
            unlock();
        }
    }

    public void dropFrontend(FrontendNodeType role, String host, int port) throws DdlException {
        if (NetUtils.isSameIP(host, selfNode.first) && port == selfNode.second &&
                GlobalStateMgr.getCurrentState().getFeType() == FrontendNodeType.LEADER) {
            throw new DdlException("can not drop current master node.");
        }
        if (!tryLock(false)) {
            throw new DdlException("Failed to acquire globalStateMgr lock. Try again");
        }
        Frontend fe = null;
        try {
            fe = unprotectCheckFeExist(host, port);
            if (fe == null) {
                throw new DdlException("frontend does not exist[" +
                        NetUtils.getHostPortInAccessibleFormat(host, port) + "]");
            }
            if (fe.getRole() != role) {
                throw new DdlException(role.toString() + " does not exist[" +
                        NetUtils.getHostPortInAccessibleFormat(host, port) + "]");
            }
            frontends.remove(fe.getNodeName());
            frontendIds.remove(fe.getFid());
            removedFrontends.add(fe.getNodeName());

            if (fe.getRole() == FrontendNodeType.FOLLOWER) {
                GlobalStateMgr.getCurrentState().getHaProtocol().removeElectableNode(fe.getNodeName());
                helperNodes.remove(Pair.create(host, port));

                HAProtocol ha = GlobalStateMgr.getCurrentState().getHaProtocol();
                ha.removeUnstableNode(host, getFollowerCnt());
            }
            GlobalStateMgr.getCurrentState().getEditLog().logRemoveFrontend(fe);
        } finally {
            unlock();

            if (fe != null) {
                dropFrontendHook(fe);
            }
        }
    }

    private void dropFrontendHook(Frontend fe) {
        GlobalStateMgr.getCurrentState().getSlotManager().notifyFrontendDeadAsync(fe.getNodeName());

        GlobalStateMgr.getCurrentState().getCheckpointController().cancelCheckpoint(fe.getNodeName(), "FE is dropped");
        if (RunMode.isSharedDataMode()) {
            StarMgrServer.getCurrentState().getCheckpointController().cancelCheckpoint(fe.getNodeName(), "FE is dropped");
        }
    }

    public void replayAddFrontend(Frontend fe) {
        tryLock(true);
        try {
            Frontend existFe = unprotectCheckFeExist(fe.getHost(), fe.getEditLogPort());
            if (existFe != null) {
                LOG.warn("fe {} already exist.", existFe);
                if (existFe.getRole() != fe.getRole()) {
                    /*
                     * This may happen if:
                     * 1. first, add a FE as OBSERVER.
                     * 2. This OBSERVER is restarted with ROLE and VERSION file being DELETED.
                     *    In this case, this OBSERVER will be started as a FOLLOWER, and add itself to the frontends.
                     * 3. this "FOLLOWER" begin to load image or replay journal,
                     *    then find the origin OBSERVER in image or journal.
                     * This will cause UNDEFINED behavior, so it is better to exit and fix it manually.
                     */
                    System.err.println("Try to add an already exist FE with different role" + fe.getRole());
                    System.exit(-1);
                }
                return;
            }
            frontends.put(fe.getNodeName(), fe);
            frontendIds.put(fe.getFid(), fe);
            if (fe.getRole() == FrontendNodeType.FOLLOWER) {
                helperNodes.add(Pair.create(fe.getHost(), fe.getEditLogPort()));
            }
        } finally {
            unlock();
        }
    }

    public void replayUpdateFrontend(Frontend frontend) {
        tryLock(true);
        try {
            Frontend fe = frontends.get(frontend.getNodeName());
            if (fe == null) {
                LOG.error("try to update frontend, but " + frontend.toString() + " does not exist.");
                return;
            }
            fe.updateHostAndEditLogPort(frontend.getHost(), frontend.getEditLogPort());
            frontends.put(fe.getNodeName(), fe);
            LOG.info("update fe successfully, fe info is [{}]", frontend.toString());
        } finally {
            unlock();
        }
    }

    public void replayDropFrontend(Frontend frontend) {
        tryLock(true);
        Frontend removedFe = null;
        try {
            removedFe = frontends.remove(frontend.getNodeName());
            if (removedFe == null) {
                LOG.error(frontend.toString() + " does not exist.");
                return;
            }
            if (removedFe.getRole() == FrontendNodeType.FOLLOWER) {
                helperNodes.remove(Pair.create(removedFe.getHost(), removedFe.getEditLogPort()));
            }

            frontendIds.remove(removedFe.getFid());

            removedFrontends.add(removedFe.getNodeName());
        } finally {
            unlock();

            if (removedFe != null) {
                GlobalStateMgr.getCurrentState().getSlotManager().notifyFrontendDeadAsync(removedFe.getNodeName());
            }
        }
    }

    public boolean checkFeExistByRPCPort(String host, int rpcPort) {
        try {
            tryLock(true);
            return frontends
                    .values()
                    .stream()
                    .anyMatch(fe -> fe.getHost().equals(host) && fe.getRpcPort() == rpcPort);
        } finally {
            unlock();
        }
    }

    public Frontend checkFeExist(String host, int port) {
        tryLock(true);
        try {
            return unprotectCheckFeExist(host, port);
        } finally {
            unlock();
        }
    }

    public Frontend unprotectCheckFeExist(String host, int port) {
        for (Frontend fe : frontends.values()) {
            if (fe.getEditLogPort() == port && NetUtils.isSameIP(fe.getHost(), host)) {
                return fe;
            }
        }
        return null;
    }

    protected boolean checkFeExistByIpOrFqdn(String ipOrFqdn) throws UnknownHostException {
        Pair<String, String> targetIpAndFqdn = NetUtils.getIpAndFqdnByHost(ipOrFqdn);

        for (Frontend fe : frontends.values()) {
            Pair<String, String> curIpAndFqdn;
            try {
                curIpAndFqdn = NetUtils.getIpAndFqdnByHost(fe.getHost());
            } catch (UnknownHostException e) {
                LOG.warn("failed to get right ip by fqdn {}", fe.getHost(), e);
                if (targetIpAndFqdn.second.equals(fe.getHost())
                        && !Strings.isNullOrEmpty(targetIpAndFqdn.second)) {
                    return true;
                }
                continue;
            }
            // target, cur has same ip
            if (targetIpAndFqdn.first.equals(curIpAndFqdn.first)) {
                return true;
            }
            // target, cur has same fqdn and both of them are not equal ""
            if (targetIpAndFqdn.second.equals(curIpAndFqdn.second)
                    && !Strings.isNullOrEmpty(targetIpAndFqdn.second)) {
                return true;
            }
        }

        return false;
    }

    public Frontend getFeByHost(String ipOrFqdn) {
        // This host could be Ip, or fqdn
        Pair<String, String> targetPair;
        try {
            targetPair = NetUtils.getIpAndFqdnByHost(ipOrFqdn);
        } catch (UnknownHostException e) {
            LOG.warn("failed to get right ip by fqdn {}", e.getMessage());
            return null;
        }
        for (Frontend fe : frontends.values()) {
            Pair<String, String> curPair;
            try {
                curPair = NetUtils.getIpAndFqdnByHost(fe.getHost());
            } catch (UnknownHostException e) {
                LOG.warn("failed to get right ip by fqdn {}", e.getMessage());
                continue;
            }
            // target, cur has same ip
            if (NetUtils.isSameIP(targetPair.first, curPair.first)) {
                return fe;
            }
            // target, cur has same fqdn and both of them are not equal ""
            if (targetPair.second.equals(curPair.second) && !curPair.second.equals("")) {
                return fe;
            }
        }
        return null;
    }

    public Frontend getFeByName(String name) {
        return frontends.get(name);
    }

    public int getFollowerCnt() {
        int cnt = 0;
        for (Frontend fe : frontends.values()) {
            if (fe.getRole() == FrontendNodeType.FOLLOWER) {
                cnt++;
            }
        }
        return cnt;
    }

    public int getClusterId() {
        return this.clusterId;
    }

    public String getToken() {
        return token;
    }

    public FrontendNodeType getRole() {
        return this.role;
    }

    public Pair<String, Integer> getSelfNode() {
        return this.selfNode;
    }

    public Pair<String, Integer> getSelfIpAndRpcPort() {
        return Pair.create(FrontendOptions.getLocalHostAddress(), Config.rpc_port);
    }

    public String getNodeName() {
        return this.nodeName;
    }

    public Pair<String, Integer> getLeaderIpAndRpcPort() {
        if (GlobalStateMgr.getServingState().isReady()) {
            return new Pair<>(this.leaderIp, this.leaderRpcPort);
        } else {
            String leaderNodeName = GlobalStateMgr.getServingState().getHaProtocol().getLeaderNodeName();
            Frontend frontend = frontends.get(leaderNodeName);
            return new Pair<>(frontend.getHost(), frontend.getRpcPort());
        }
    }

    public TNetworkAddress getLeaderRpcEndpoint() {
        Pair<String, Integer> ipAndRpcPort = getLeaderIpAndRpcPort();
        return new TNetworkAddress(ipAndRpcPort.first, ipAndRpcPort.second);
    }

    public Pair<String, Integer> getLeaderIpAndHttpPort() {
        if (GlobalStateMgr.getServingState().isReady()) {
            return new Pair<>(this.leaderIp, this.leaderHttpPort);
        } else {
            String leaderNodeName = GlobalStateMgr.getServingState().getHaProtocol().getLeaderNodeName();
            Frontend frontend = frontends.get(leaderNodeName);
            return new Pair<>(frontend.getHost(), Config.http_port);
        }
    }

    public String getLeaderIp() {
        if (GlobalStateMgr.getServingState().isReady()) {
            return this.leaderIp;
        } else {
            String leaderNodeName = GlobalStateMgr.getServingState().getHaProtocol().getLeaderNodeName();
            return frontends.get(leaderNodeName).getHost();
        }
    }

    public void setLeader(LeaderInfo info) {
        this.leaderIp = info.getIp();
        this.leaderHttpPort = info.getHttpPort();
        this.leaderRpcPort = info.getRpcPort();

        leaderChangeListeners.values().forEach(listener -> listener.accept(info));
    }

    public List<QueryStatisticsInfo> getQueryStatisticsInfoFromOtherFEs() {
        List<QueryStatisticsInfo> statisticsItems = Lists.newArrayList();
        TGetQueryStatisticsRequest request = new TGetQueryStatisticsRequest();

        List<Frontend> allFrontends = getAllFrontends();
        for (Frontend fe : allFrontends) {
            if (fe.getHost().equals(getSelfNode().first)) {
                continue;
            }

            try {
                TGetQueryStatisticsResponse response = ThriftRPCRequestExecutor.call(
                        ThriftConnectionPool.frontendPool,
                        new TNetworkAddress(fe.getHost(), fe.getRpcPort()),
                        Config.thrift_rpc_timeout_ms,
                        Config.thrift_rpc_retry_times,
                        client -> client.getQueryStatistics(request));
                if (response.getStatus().getStatus_code() != TStatusCode.OK) {
                    LOG.warn("getQueryStatisticsInfo to remote fe: {} failed", fe.getHost());
                } else if (response.isSetQueryStatistics_infos()) {
                    response.getQueryStatistics_infos().stream()
                            .map(QueryStatisticsInfo::fromThrift)
                            .forEach(statisticsItems::add);
                }
            } catch (Exception e) {
                LOG.warn("getQueryStatisticsInfo to remote fe: {} failed", fe.getHost(), e);
            }
        }

        return statisticsItems;
    }

    public Frontend getMySelf() {
        return frontends.get(nodeName);
    }

    @VisibleForTesting
    public void setMySelf(Frontend frontend) {
        selfNode = Pair.create(frontend.getHost(), frontend.getRpcPort());
    }

    public ConcurrentHashMap<String, Frontend> getFrontends() {
        return frontends;
    }

    public void resetFrontends() {
        frontends.clear();
        frontendIds.clear();

        int fid = allocateNextFrontendId();
        Frontend self = new Frontend(fid, role, nodeName, selfNode.first, selfNode.second);
        frontends.put(self.getNodeName(), self);
        frontendIds.put(fid, self);
        // reset helper nodes
        helperNodes.clear();
        helperNodes.add(selfNode);

        GlobalStateMgr.getCurrentState().getEditLog().logResetFrontends(self);
    }

    public void replayResetFrontends(Frontend frontend) {
        frontends.clear();
        frontendIds.clear();

        frontends.put(frontend.getNodeName(), frontend);
        frontendIds.put(frontend.getFid(), frontend);
        // reset helper nodes
        helperNodes.clear();
        helperNodes.add(Pair.create(frontend.getHost(), frontend.getEditLogPort()));
    }

    public void save(ImageWriter imageWriter) throws IOException, SRMetaBlockException {
        SRMetaBlockWriter writer = imageWriter.getBlockWriter(SRMetaBlockID.NODE_MGR, 1);
        writer.writeJson(this);
        writer.close();
    }

    public void load(SRMetaBlockReader reader) throws IOException, SRMetaBlockException, SRMetaBlockEOFException {
        NodeMgr nodeMgr = reader.readJson(NodeMgr.class);

        leaderRpcPort = nodeMgr.leaderRpcPort;
        leaderHttpPort = nodeMgr.leaderHttpPort;
        leaderIp = nodeMgr.leaderIp;

        frontends = nodeMgr.frontends;
        removedFrontends = nodeMgr.removedFrontends;

        for (Frontend fe : frontends.values()) {
            if (fe.getRole() == FrontendNodeType.FOLLOWER) {
                helperNodes.add(Pair.create(fe.getHost(), fe.getEditLogPort()));
            }
        }

        systemInfo = nodeMgr.systemInfo;
        brokerMgr = nodeMgr.brokerMgr;

        //Sort by frontend name to ensure the order of assigned IDs is consistent
        List<Frontend> sortedList = frontends.entrySet().stream()
                .sorted(Map.Entry.comparingByKey()).map(Map.Entry::getValue).collect(Collectors.toList());
        for (Frontend fe : sortedList) {
            if (fe.getFid() == 0) {
                int frontendId = allocateNextFrontendId();
                LOG.info("Frontend has no ID assigned, newly assigned ID {} to {}", frontendId, fe.getNodeName());
                fe.setFid(frontendId);
            }
            frontendIds.put(fe.getFid(), fe);
        }
    }

    public void setLeaderInfo() {
        this.leaderIp = FrontendOptions.getLocalHostAddress();
        this.leaderRpcPort = Config.rpc_port;
        this.leaderHttpPort = Config.http_port;
        LeaderInfo info = new LeaderInfo(this.leaderIp, this.leaderHttpPort, this.leaderRpcPort);
        GlobalStateMgr.getCurrentState().getEditLog().logLeaderInfo(info);

        leaderChangeListeners.values().forEach(listener -> listener.accept(info));
    }

    public boolean isFirstTimeStartUp() {
        return isFirstTimeStartUp;
    }

    public boolean isElectable() {
        return isElectable;
    }

    public static String genFeNodeName(String host, int port, boolean isOldStyle) {
        String name = host + "_" + port;
        if (isOldStyle) {
            return name;
        } else {
            return name + "_" + System.currentTimeMillis();
        }
    }

    public static boolean isFeNodeNameValid(String nodeName, String host, int port) {
        return nodeName.startsWith(host + "_" + port);
    }
}
