package com.datafibers.utils;

import com.sun.crypto.provider.SunJCE;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.*;
import java.security.NoSuchAlgorithmException;
import java.security.Security;
import java.util.Arrays;
import java.util.Base64;
import java.util.logging.Level;
import java.util.logging.Logger;

public class EncryptPassword {

    private final static Logger logger = Logger.getLogger(EncryptPassword.class.getName());

    private static boolean stopThread = false;

    static {
        System.setProperty("java.util.logging.SimpleFormatter.format", "%5$s %6$s %n");
    }

    public static byte[] encrypt(final byte[] paramArrayOfByte, final String paramString) {
        byte[] arrayOfByte = new byte[1024];
        try {
            Security.addProvider(new SunJCE());
            Base64.Decoder localBASE64Decoder = Base64.getDecoder();
            try {
                BufferedReader localBufferedReader = new BufferedReader(new FileReader(paramString));
                byte[] keyArrayOfByte = localBASE64Decoder.decode(localBufferedReader.readLine());
                arrayOfByte = Arrays.copyOf(keyArrayOfByte, 1024);
                localBufferedReader.close();
            } catch (IOException ioe) {
                System.out.println("Error: cannot read from key file.");
            }
            // get the iv
            byte[] iv = Arrays.copyOfRange(
                    arrayOfByte,
                    PasswordUtilConstant.DEFAULT_AES_KEY_LENGTH / 8,
                    PasswordUtilConstant.DEFAULT_AES_KEY_LENGTH / 8 + Cipher.getInstance(PasswordUtilConstant.DEFAULT_CIPHER_ALGORITHM).getBlockSize()
            );

            EncryptPassword.logger.info("IV bytes length:" + iv.length);
            SecretKeySpec keySpec = new SecretKeySpec(
                    arrayOfByte, 0, PasswordUtilConstant.DEFAULT_AES_KEY_LENGTH / 8,
                    PasswordUtilConstant.DEFAULT_KEY_ALGORITHM
            );
            Cipher localCipher = Cipher.getInstance(PasswordUtilConstant.DEFAULT_CIPHER_ALGORITHM);
            localCipher.init(Cipher.ENCRYPT_MODE, keySpec, new IvParameterSpec(iv));
            return localCipher.doFinal(paramArrayOfByte);
        } catch (Exception e) {
            EncryptPassword.logger.log(Level.SEVERE, "Error: cannot retrieve secret key.");
            System.exit(1);
        }
        return null;
    }

    public static void main(final String[] args) throws NoSuchAlgorithmException, NoSuchPaddingException {
        if (args.length != 2) {
            System.out.println("Usage: java encryptpwd <key filename> <output filename>");
            System.exit(0);
        }

        String algorithm = PasswordUtilConstant.DEFAULT_CIPHER_ALGORITHM;
        if (algorithm == null) {
            System.out.println("UCipher Algorithm is not defined.");
            System.exit(0);
        }

        String str1 = "";
        String str2 = "";

        BufferedReader localBufferedReader = new BufferedReader(new InputStreamReader(System.in));

        while (true) {
            try {
                EncryptPassword.logger.info("Enter password: ");
                str1 = localBufferedReader.readLine();
                EncryptPassword.logger.info("Re-enter password:");
                str2 = localBufferedReader.readLine();
            } catch (IOException ioe) {
                System.out.println("Error getting standard input.");
                break;
            }

            if (!str1.equals(str2)) {
                EncryptPassword.logger.info("The two passwords do not match.");
                continue;
            }
            if (str1.length() >= 6) break;
            EncryptPassword.logger.info("Minimum password length should be 6 characters");
        }

        EncryptPassword.stopThread = true;

        byte[] arrayOfByte = encrypt(str1.getBytes(), args[0]);
        Base64.Encoder localBASE64Encoder = Base64.getEncoder();
        try {
            PrintWriter localPrintWriter = new PrintWriter(new BufferedWriter(new FileWriter(args[1])));
            localPrintWriter.println(new String(localBASE64Encoder.encode(arrayOfByte)));
            localPrintWriter.close();
        } catch (IOException ioe) {
            EncryptPassword.logger.info("Password encrypted in file: " + args[1]);
        }
    }
}
