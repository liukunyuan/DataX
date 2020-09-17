package com.alibaba.datax.core.transport.transformer;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.transformer.Transformer;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

/**
 * no comments.
 * Created by liqiang on 16/3/4.
 */
public class ReplaceNewLineSymbolTransformer extends Transformer {
    public ReplaceNewLineSymbolTransformer() {
        setTransformerName("dx_replaceNewLineSymbol");
    }

    @Override
    public Record evaluate(Record record, Object... paras) {

        int columnIndex;


        try {
            if (paras.length != 2) {
                throw new RuntimeException("dx_substr paras must be 3");
            }

            columnIndex = (Integer) paras[0];

        } catch (Exception e) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER, "paras:" + Arrays.asList(paras).toString() + " => " + e.getMessage());
        }

        Column column = record.getColumn(columnIndex);


        String columnStr = column.asString();
        if (null != columnStr) {

            columnStr=columnStr.replaceAll("\\\\N","").replaceAll("\r","").replaceAll("\n","");
            record.setColumn(columnIndex, new StringColumn(columnStr));
        };
        return record;
    }
}
