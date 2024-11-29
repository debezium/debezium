package io.debezium.transforms;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.util.Base64;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.kafka.connect.components.Versioned;

public class AESEncryption implements Versioned {

    private static final String ALGORITHM = "AES/CBC/PKCS5Padding";
    private static final String VERSION = "v1";

    final String secretKey;
    private final String salt;
    final int ivLength = 16;

    public AESEncryption(String secretKey) {
        this(secretKey, "salt");
    }

    public AESEncryption(String secretKey, String salt) {
        this.secretKey = secretKey;
        this.salt = salt;
    }

    @Override
    public String version() {
        return VERSION;
    }

    private SecretKey getKeyFromPassword() throws NoSuchAlgorithmException, InvalidKeySpecException {
        PBEKeySpec pbeKeySpec = new PBEKeySpec(secretKey.toCharArray(), salt.getBytes(), 1000, 256);
        SecretKey pbeKey = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256").generateSecret(pbeKeySpec);
        return new SecretKeySpec(pbeKey.getEncoded(), "AES");
    }

    public String encrypt(String input) throws Exception {
        if (input == null || input.isEmpty()) {
            return input;
        }

        // Generate a random initialization vector (IV)
        byte[] iv = new byte[ivLength];
        SecureRandom random = SecureRandom.getInstanceStrong();
        random.nextBytes(iv);

        // Create a SecretKeySpec object from the secret key
        SecretKey secretKeySpec = getKeyFromPassword();

        // Create a Cipher object and initialize it for encryption
        Cipher cipher = Cipher.getInstance(ALGORITHM);
        cipher.init(Cipher.ENCRYPT_MODE, secretKeySpec, new IvParameterSpec(iv));

        // Encrypt the input data
        byte[] encryptedData = cipher.doFinal(input.getBytes());

        // Combine the IV and encrypted data into a single byte array
        byte[] encryptedDataWithIV = new byte[ivLength + encryptedData.length];
        System.arraycopy(iv, 0, encryptedDataWithIV, 0, ivLength);
        System.arraycopy(encryptedData, 0, encryptedDataWithIV, ivLength, encryptedData.length);

        // Encode the combined data using Base64 for transmission
        return Base64.getEncoder().encodeToString(encryptedDataWithIV);
    }

    public String decrypt(String encryptedDataWithIV) throws Exception {
        if (encryptedDataWithIV == null || encryptedDataWithIV.isEmpty()) {
            return encryptedDataWithIV;
        }
        // Decode the Base64-encoded data
        byte[] encryptedDataWithIVDecoded = Base64.getDecoder().decode(encryptedDataWithIV);

        // Extract the IV and encrypted data
        byte[] iv = new byte[ivLength];
        System.arraycopy(encryptedDataWithIVDecoded, 0, iv, 0, ivLength);
        byte[] encryptedData = new byte[encryptedDataWithIVDecoded.length - ivLength];
        System.arraycopy(encryptedDataWithIVDecoded, ivLength, encryptedData, 0, encryptedData.length);

        // Create a SecretKeySpec object from the secret key
        SecretKey secretKeySpec = getKeyFromPassword();

        // Create a Cipher object and initialize it for decryption
        Cipher cipher = Cipher.getInstance(ALGORITHM);
        cipher.init(Cipher.DECRYPT_MODE, secretKeySpec, new IvParameterSpec(iv));

        // Decrypt the data
        byte[] decryptedData = cipher.doFinal(encryptedData);

        // Return the decrypted string
        return new String(decryptedData);
    }
}
