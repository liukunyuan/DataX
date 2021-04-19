/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.datax.plugin.reader.elasticsearchreader.util;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;


/**
 * Utilities for ElasticSearch
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class EsUtil {

    public static RestHighLevelClient getClient(String address, String username, String password, Map<String,Object> config) {
        List<HttpHost> httpHostList = new ArrayList<>();
        String[] addr = address.split(",");
        for(String add : addr) {
            String[] pair = add.split(":");
            httpHostList.add(new HttpHost(pair[0], Integer.parseInt(pair[1]), "http"));
        }
        final Integer timeout = MapUtils.getInteger(config, EsConfigKeys.KEY_TIMEOUT,30);


//        RestClientBuilder builder = RestClient.builder(httpHostList.toArray(new HttpHost[0]));
        RestClientBuilder builder = RestClient.builder(httpHostList.toArray(new HttpHost[0])).setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
            // 该方法接收一个RequestConfig.Builder对象，对该对象进行修改后然后返回。
            @Override
            public RequestConfig.Builder customizeRequestConfig(
                    RequestConfig.Builder requestConfigBuilder) {
                return requestConfigBuilder.setConnectTimeout(timeout * 1000) // 连接超时（默认为1秒）
                        .setSocketTimeout(timeout * 1000*10);// 套接字超时（默认为30秒）//更改客户端的超时限制默认30秒现在改为100*1000分钟
            }
        }).setMaxRetryTimeoutMillis(timeout * 1000);// 调整最大重试超时时间（默认为30秒）.setMaxRetryTimeoutMillis(60000);;

        RequestConfig.Builder requestConfigBuilder = null;


        String pathPrefix = MapUtils.getString(config, EsConfigKeys.KEY_PATH_PREFIX);
        if (StringUtils.isNotEmpty(pathPrefix)){
            builder.setPathPrefix(pathPrefix);
        }
        if(StringUtils.isNotBlank(username)){
            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
            builder.setHttpClientConfigCallback(httpClientBuilder -> {
                httpClientBuilder.disableAuthCaching();
                return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            });
        }

        return new RestHighLevelClient(builder);
    }



    private static Object readMapValue(Map<String,Object> jsonMap, String[] fieldParts) {
        Map<String,Object> current = jsonMap;
        int i = 0;
        for(; i < fieldParts.length - 1; ++i) {
            if(current.containsKey(fieldParts[i])) {
                current = (Map<String, Object>) current.get(fieldParts[i]);
            } else {
                return null;
            }
        }
        return  current.get(fieldParts[i]);
    }

    private static Object convertValueToAssignType(String columnType, String constantValue) {
        Object column  = null;
        if(StringUtils.isEmpty(constantValue)) {
            return column;
        }

        switch (columnType.toUpperCase()) {
            case "BOOLEAN":
                column = Boolean.valueOf(constantValue);
                break;
            case "SHORT":
            case "INT":
            case "LONG":
                column = NumberUtils.createBigDecimal(constantValue).toBigInteger();
                break;
            case "FLOAT":
            case "DOUBLE":
                column = new BigDecimal(constantValue);
                break;
            case "STRING":
                column = constantValue;
                break;
            case "DATE":
                column = DateUtil.stringToDate(constantValue,null);
                break;
            default:
                throw new IllegalArgumentException("Unsupported column type: " + columnType);
        }
        return column;
    }

    public static String[] getStringArray(Object value){
        if(value == null){
            return null;
        }

        if(value instanceof String){
            String stringValue = value.toString();
            return stringValue.split(",");
        } else if(value instanceof List){
            List list = (List)value;
            String[] array = new String[list.size()];
            for (int i = 0; i < list.size(); i++) {
                array[i] = list.get(i).toString();
            }

            return array;
        } else {
            return new String[]{value.toString()};
        }
    }
}
