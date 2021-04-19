package com.alibaba.datax.plugin.reader.elasticsearchreader;


import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.slice.SliceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.element.BoolColumn;
import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.datax.common.element.DoubleColumn;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.elasticsearchreader.util.EsConfigKeys;
import com.alibaba.datax.plugin.reader.elasticsearchreader.util.EsUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;


public class ESReader extends Reader {
    private static final Logger logger = LoggerFactory
            .getLogger(ESReader.class);

    public static class Job extends Reader.Job {


        private String esClusterName = null;

        private String esClusterIP = null;

        private String esUsername = null;
        private String esPassword = null;

        private String esIndex = null;
        private JSONArray esColumnMeta = null;
        private String esType = null;


        private String query = null;

//        private TransportClient client = null;

        private Integer batchSize = 1000;


        private Configuration readerSplitConfiguration = null;

        @Override
        public void preCheck() {
            super.preCheck();
        }

        @Override
        public void preHandler(Configuration jobConfiguration) {
            super.preHandler(jobConfiguration);
        }

        @Override
        public void init() {
            this.readerSplitConfiguration = super.getPluginJobConf();


            this.esColumnMeta = JSON.parseArray(readerSplitConfiguration.getString(Key.esColumn));
            this.esClusterName = this.readerSplitConfiguration.getString(Key.esClusterName);
            this.esClusterIP = readerSplitConfiguration.getString(Key.esClusterIP);
            this.esIndex = readerSplitConfiguration.getString(Key.esIndex);

            this.esType = readerSplitConfiguration.getString(Key.esType);
            this.batchSize = readerSplitConfiguration.getInt(Key.batchSize, 1000);
            this.esUsername = readerSplitConfiguration.getString(Key.esUsername);
            this.esPassword = readerSplitConfiguration.getString(Key.esPassword);
            this.query = readerSplitConfiguration.getString(Key.query);
        }

        @Override
        public void prepare() {
            super.prepare();

        }

        @Override
        public void post() {
            super.post();
        }

        @Override
        public void postHandler(Configuration jobConfiguration) {
            super.postHandler(jobConfiguration);
        }

        @Override
        public void destroy() {
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            List<Configuration> readerSplitConfigurations = new ArrayList<Configuration>();
            for (int i = 0; i < adviceNumber; i++) {
                Configuration readerSplitConfiguration = this.readerSplitConfiguration.clone();
                readerSplitConfiguration.set(EsConfigKeys.SPLIT_INDEX_KEY, i);
                readerSplitConfiguration.set(EsConfigKeys.SPLIT_ADVICE_NUMBER, adviceNumber);
                readerSplitConfigurations.add(readerSplitConfiguration);
            }




            logger.info("切片个数为:{}", readerSplitConfigurations.size());

            return readerSplitConfigurations;
        }

    }

    public static class Task extends Reader.Task {

        private Configuration readerSplitConfiguration = null;

        private String esClusterName = null;

        private String esClusterIP = null;

        private String esUsername = null;
        private String esPassword = null;

        private String esIndex = null;
        private JSONArray esColumnMeta = null;
        private String esType = null;





        // 以下是新增





        protected String query;




        protected int batchSize = 1000;

        protected Map<String, Object> clientConfig;

        protected long keepAlive = 1;

        private transient RestHighLevelClient client;

        private Iterator<Map<String,Object>>  iterator;

        private transient SearchRequest searchRequest;

        private transient Scroll scroll;

        private String scrollId;


        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        @Override
        public void preCheck() {
            super.preCheck();
        }

        @Override
        public void preHandler(Configuration jobConfiguration) {
            super.preHandler(jobConfiguration);
        }

        @Override
        public void init() {
            this.readerSplitConfiguration = super.getPluginJobConf();


            this.esColumnMeta = JSON.parseArray(readerSplitConfiguration.getString(Key.esColumn));
            this.esClusterName = this.readerSplitConfiguration.getString(Key.esClusterName);
            this.esClusterIP = readerSplitConfiguration.getString(Key.esClusterIP);
            this.esIndex = readerSplitConfiguration.getString(Key.esIndex);
            this.esType = readerSplitConfiguration.getString(Key.esType);
            this.batchSize = readerSplitConfiguration.getInt(Key.batchSize, 100);
            this.esUsername = readerSplitConfiguration.getString(Key.esUsername);
            this.esPassword = readerSplitConfiguration.getString(Key.esPassword);
            this.query = readerSplitConfiguration.getString(Key.query);

        }

        @Override
        public void prepare() {
            super.prepare();
            Integer splitIndex = readerSplitConfiguration.getInt(EsConfigKeys.SPLIT_INDEX_KEY);
            Integer adviceNumber = readerSplitConfiguration.getInt(EsConfigKeys.SPLIT_ADVICE_NUMBER);
            String jsonConfigs = readerSplitConfiguration.toJSON();
            clientConfig = JSONObject.parseObject(jsonConfigs, Map.class);


            client = EsUtil.getClient(this.esClusterIP, this.esUsername, this.esPassword, clientConfig);

            scroll = new Scroll(TimeValue.timeValueMinutes(keepAlive));

            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.size(batchSize);

            JSONObject jsonObject=null;
            if(StringUtils.isNotBlank(query)){
               searchSourceBuilder.query(QueryBuilders.wrapperQuery(query));
            }




            if (adviceNumber > 1) {
                searchSourceBuilder.slice(new SliceBuilder(splitIndex, adviceNumber));
            }

            searchRequest = new SearchRequest(this.esIndex.split(","));
            searchRequest.types(this.esType);
            searchRequest.scroll(scroll);
            searchRequest.source(searchSourceBuilder);
        }

        private boolean searchScroll() throws IOException {
            SearchHit[] searchHits;
            if(scrollId == null){
                SearchResponse searchResponse = client.search(searchRequest);
                scrollId = searchResponse.getScrollId();
                searchHits = searchResponse.getHits().getHits();
            } else {
                SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
                scrollRequest.scroll(scroll);
                SearchResponse searchResponse = client.searchScroll(scrollRequest);
                scrollId = searchResponse.getScrollId();
                searchHits = searchResponse.getHits().getHits();
            }
            LinkedList<Map<String,Object>> linkedList = new LinkedList<Map<String,Object>>();
            for(SearchHit searchHit: searchHits ) {
                Map<String,Object> source = searchHit.getSourceAsMap();
                linkedList.add(source);
            }
            iterator = linkedList.iterator();
            if(null==searchHits || searchHits.length==0) {
                return false;
            }else {
                return true;
            }




        }


        @Override
        public void post() {
            super.post();
        }

        @Override
        public void postHandler(Configuration jobConfiguration) {
            super.postHandler(jobConfiguration);
        }

        @Override
        public void destroy() {
            //这里可以将游标进行销毁
//            if(client != null) {
//                if(scrollId != null){
//                    ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
//                    clearScrollRequest.addScrollId(scrollId);
//                    ClearScrollResponse clearScrollResponse = null;
//                    try {
//                        clearScrollResponse = client.clearScroll(clearScrollRequest);
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    }
//                    boolean succeeded = clearScrollResponse.isSucceeded();
//                    LOG.info("Clear scroll response:{}", succeeded);
//                }
//
//
//                try {
//                    client.close();
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//                client = null;
//            }
        }





        @Override
        public void startRead(RecordSender recordSender) {

            try {
                while(searchScroll()) {
                    while(iterator.hasNext()) {

                        Record record = recordSender.createRecord();
                        Map<String, Object> line = iterator.next();
                        logger.debug(line.toString());
                        Iterator columnItera = esColumnMeta.iterator();
                        while (columnItera.hasNext()) {
                            JSONObject column = (JSONObject) columnItera.next();

                            Object tempCol = line.get(column.getString(Key.COLUMN_NAME));
                            if (tempCol == null) {
                                record.addColumn(new StringColumn(""));
                            } else if (tempCol instanceof JSONObject) {
                                JSONObject jsonObject = (JSONObject) tempCol;
                                record.addColumn(new StringColumn(jsonObject.toJSONString()));
                            } else if (tempCol instanceof JSONArray) {
                                JSONArray jsonArray = (JSONArray) tempCol;
                                record.addColumn(new StringColumn(jsonArray.toJSONString()));
                            }  else if (tempCol instanceof ArrayList) {
                                ArrayList array = (ArrayList) tempCol;
                                record.addColumn(new StringColumn(JSON.toJSONString(array)));
                            } else if (tempCol instanceof Double) {
                                record.addColumn(new DoubleColumn((Double) tempCol));
                            } else if (tempCol instanceof Boolean) {
                                record.addColumn(new BoolColumn((Boolean) tempCol));
                            } else if (tempCol instanceof Date) {
                                record.addColumn(new DateColumn((Date) tempCol));
                            } else if (tempCol instanceof Integer) {
                                record.addColumn(new LongColumn((Integer) tempCol));
                            } else if (tempCol instanceof Long) {
                                record.addColumn(new LongColumn((Long) tempCol));
                            } else if (tempCol instanceof BigDecimal) {
                                record.addColumn(new DoubleColumn((BigDecimal) tempCol));
                            } else {
                                record.addColumn(new StringColumn((String) tempCol));
                            }

                        }
                        recordSender.sendToWriter(record);
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }


        }




    }


}

