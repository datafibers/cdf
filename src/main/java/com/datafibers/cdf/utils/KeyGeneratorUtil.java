package com.datafibers.cdf.utils;

import com.sun.crypto.provider.SunJCE;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Array;
import java.security.SecureRandom;
import java.security.Security;
import java.util.Base64;
import java.util.logging.Level;
import java.util.logging.Logger;

public class KeyGeneratorUtil {

    private final static Logger logger = Logger.getLogger(KeyGeneratorUtil.class.getName());

    static {
        System.setProperty("java.util.logging.SimpleFormatter.format", "%5$s %6$s %n");
    }

    private static byte[] concatenate(final byte[] a, final byte[] b){
        int aLen = a.length;
        int bLen = b.length;
        byte[] c = (byte[]) Array.newInstance(byte.class, aLen + bLen);
        System.arraycopy(a, 0, c, 0, aLen);
        System.arraycopy(b, 0, c, aLen, bLen);
        return c;
    }

    public static void main(final String[] args) {
        if(args.length < 1) {
            KeyGeneratorUtil.logger.info("Usage: java keyGenerator <output file name> <seed - optional>");
            System.exit(0);
        }

        try {
            String algorithm = PasswordUtilConstant.DEFAULT_KEY_ALGORITHM;
            if(algorithm == null) {
                KeyGeneratorUtil.logger.info("Cipher Algorithm is not defined.");
                System.exit(1);
            }

            Security.addProvider(new SunJCE());
            KeyGenerator localKeyGenerator = KeyGenerator.getInstance(algorithm);
            if(args.length == 2)
                localKeyGenerator.init(PasswordUtilConstant.DEFAULT_AES_KEY_LENGTH, new SecureRandom(args[1].getBytes()));
            else
                localKeyGenerator.init(PasswordUtilConstant.DEFAULT_AES_KEY_LENGTH, new SecureRandom());

            SecretKey localSecretKey = localKeyGenerator.generateKey();
            byte[] arrayOfByte = localSecretKey.getEncoded();
            // generate vi
            byte[] iv = new byte[Cipher.getInstance(PasswordUtilConstant.DEFAULT_CIPHER_ALGORITHM).getBlockSize()];
            SecureRandom secRandom = new SecureRandom();
            secRandom.nextBytes(iv);

            byte[] keyByte = concatenate(arrayOfByte, iv);

            Base64.Encoder localBASE64Encoder = Base64.getEncoder();
            try {
                PrintWriter localPrintWriter = new PrintWriter(new BufferedWriter(new FileWriter(args[1])));
                localPrintWriter.println(new String(localBASE64Encoder.encode(keyByte)));
                localPrintWriter.close();
            } catch (IOException ioe) {
                KeyGeneratorUtil.logger.log(Level.SEVERE,"Error: writing password file.", ioe);
                System.exit(1);
            }
        } catch (Exception e) {
            KeyGeneratorUtil.logger.log(Level.SEVERE,"Error: cannot generate secret key", e);
            System.exit(2);
        }
    }
}
