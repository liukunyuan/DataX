package com.alibaba.datax.core.transport.transformer;

import static com.alibaba.datax.core.transport.transformer.GroovyTransformerStaticUtil.*;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.transformer.Transformer;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class RULE extends Transformer {

    private int i;

    public Record evaluate(Record record, Object... paras) {
        i = 1;
        Column column = record.getColumn(i);
        String md5str = column.asString();
        if (null != md5str) {
            try {
                MessageDigest md5 = MessageDigest.getInstance("MD5");
                byte[] bytes = md5.digest(md5str.getBytes());
                StringBuffer stringBuffer = new StringBuffer();
                for (byte b : bytes) {
                    int bt = b & 0xff;
                    if (bt < 16) {
                        stringBuffer.append(0);
                    }
                    stringBuffer.append(Integer.toHexString(bt));
                }
                md5str = stringBuffer.toString();
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            record.setColumn(i, new StringColumn(md5str));
        }
        ;
        return record;


    }


    public static void main(String[] args) {
//        String str="CT3651819925515743232\n姓名：张雷\n手机号";
        String str = "CT3651819925515743232\\n姓名：张雷\\n手机号";
        str = str.replaceAll("\n", "");
        str = str.replaceAll("\\\\n", "");
        System.out.println(str);
        String md5str="3424";
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            byte[] bytes = md5.digest(md5str.getBytes());
            StringBuffer stringBuffer = new StringBuffer();
            for (byte b : bytes) {
                int bt = b & 0xff;
                if (bt < 16) {
                    stringBuffer.append(0);
                }
                stringBuffer.append(Integer.toHexString(bt));
            }
            md5str = stringBuffer.toString();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        System.out.println(md5str);



    }
}

