package com.datafibers.utils;


import com.sun.crypto.provider.SunJCE;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.apache.hadoop.conf.Configuration;

import java.io.BufferedReader;
import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.InputStreamReader;
import java.security.Security;
import java.util.Arrays;
import java.util.Base64;

public class DecryptPasswordUtil {

    public static byte[] decrypt(final byte[] paramArrayOfByte, final byte[] keyInArrayOfByte) {
        try {
            Security.addProvider(new SunJCE());
            SecretKeySpec keySpec = new SecretKeySpec(
                    keyInArrayOfByte, 0, PasswordUtilConstant.DEFAULT_AES_KEY_LENGTH / 8,
                    PasswordUtilConstant.DEFAULT_KEY_ALGORITHM);
            // get iv
            byte[] iv = Arrays.copyOfRange(
                    keyInArrayOfByte, PasswordUtilConstant.DEFAULT_AES_KEY_LENGTH / 8,
                    PasswordUtilConstant.DEFAULT_AES_KEY_LENGTH / 8 + Cipher.getInstance(PasswordUtilConstant.DEFAULT_KEY_ALGORITHM).getBlockSize()
            );
            Cipher localCipher = Cipher.getInstance(PasswordUtilConstant.DEFAULT_CIPHER_ALGORITHM);
            localCipher.init(Cipher.ENCRYPT_MODE, keySpec, new IvParameterSpec(iv));
            return localCipher.doFinal(paramArrayOfByte);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error: cannot retrieving secret key.");
            System.exit(1);
        }
        return null;
    }

    public static byte[] decryptPwdFile(final String pwdFilePath, final String keyFilePath) throws IOException {
        byte[] encryptedPasswordByte = new byte[128];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        try {
            Base64.Decoder localBASE64Decoder = Base64.getDecoder();
            BufferedReader localBufferedReader = new BufferedReader(new InputStreamReader(fs.open(new Path(pwdFilePath))));
            encryptedPasswordByte = localBASE64Decoder.decode(localBufferedReader.readLine());
            localBufferedReader.close();
        } catch (IOException ioe) {
            System.out.println("Error: cannot read from password file.");
        }

        byte[] arrayOfKeyByte = new byte[1024];
        try {
            Base64.Decoder localBASE64Decoder = Base64.getDecoder();
            BufferedReader localBufferedReader = new BufferedReader(new InputStreamReader(fs.open(new Path(pwdFilePath))));
            byte[] keyArrayOfByte = localBASE64Decoder.decode(localBufferedReader.readLine());
            arrayOfKeyByte = Arrays.copyOf(keyArrayOfByte, 1024);
            localBufferedReader.close();
        } catch (IOException ioe) {
            System.out.println("Error: cannot read from password file.");
        }
        return decrypt(encryptedPasswordByte, arrayOfKeyByte);
    }

    public static void main(final String[] paramArrayOfString) throws IOException {
        if(paramArrayOfString.length != 2) {
            System.out.println("Usage: java decryptpwd <key filename> <password filename>");
            System.exit(0);
        }

        String str = new String(decryptPwdFile(paramArrayOfString[1], paramArrayOfString[0]));
        System.out.println(str);
    }

}
