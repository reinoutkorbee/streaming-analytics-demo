package com.snowice23;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.openssl.jcajce.JceOpenSSLPKCS8DecryptorProviderBuilder;
import org.bouncycastle.operator.InputDecryptorProvider;
import org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfo;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.security.PrivateKey;
import java.security.Security;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

public class ConnectionHelper {

    public static final String ACCOUNT_NAME = "<<ACCOUNT_NAME>>";

    public static final String REGION = "eu-central-1";

    public static final String USER_NAME = "<<USERNAME>>";

    private static final String PRIVATE_KEY_FILE = "ssh_keys/rsa_key.p8";

    public static final String KAFKA_TOPIC = "streaming-demo";

    public static final String KAFKA_BOOTSTRAP_SERVERS = "127.0.0.1:9092";;


    public static Connection getConnection() throws Exception {
        Properties props = getProperties();
        return DriverManager.getConnection(props.getProperty("connect_string"), props);
    }

    public static Properties getKafkaConsumerProperties(String groupName) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupName);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    public static Properties getKafkaProducerProperties() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }

    public static Properties getProperties() throws Exception {

        Properties props = new Properties();

        // Snowflake connection properties
        props.put("user", USER_NAME);
        //props.put("url", "https://account_name.snowflakecomputing.com:443");
        props.put("account", ACCOUNT_NAME);

        // JDBC needs the PrivateKey object
        props.put("privateKey", PrivateKeyReader.getPrivateKey(PRIVATE_KEY_FILE));

        // SnowPipe needs the private key as string to parse it internally
        props.put("private_key", PrivateKeyReader.getPrivateKeyAsString(PRIVATE_KEY_FILE));

        props.put("port", "443");
        props.put("host", ACCOUNT_NAME + "."+REGION+".snowflakecomputing.com");
        props.put("schema", "public");
        props.put("scheme", "https");
        props.put("database", "streaming_demo");
        props.put("connect_string", "jdbc:snowflake://" + ACCOUNT_NAME + "."+REGION+".snowflakecomputing.com");
        props.put("ssl", "on");
        props.put("warehouse", "compute_wh");
        props.put("role", "accountadmin");
        return props;
    }

    // https://docs.snowflake.com/en/developer-guide/jdbc/jdbc-configure
    public static class PrivateKeyReader {

        // If you generated an encrypted private key, implement this method to return
        // the passphrase for decrypting your private key.
        private static String getPrivateKeyPassphrase() {
            return "";
        }

        public static PrivateKey getPrivateKey(String filename) throws Exception {
            PrivateKeyInfo privateKeyInfo = null;
            Security.addProvider(new BouncyCastleProvider());
            // Read an object from the private key file.
            File privateKeyFile = loadFileFromClasspath(filename);
            PEMParser pemParser = new PEMParser(new FileReader(privateKeyFile));
            Object pemObject = pemParser.readObject();
            if (pemObject instanceof PKCS8EncryptedPrivateKeyInfo encryptedPrivateKeyInfo) {
                // Handle the case where the private key is encrypted.
                String passphrase = getPrivateKeyPassphrase();
                InputDecryptorProvider pkcs8Prov = new JceOpenSSLPKCS8DecryptorProviderBuilder().build(passphrase.toCharArray());
                privateKeyInfo = encryptedPrivateKeyInfo.decryptPrivateKeyInfo(pkcs8Prov);
            } else if (pemObject instanceof PrivateKeyInfo) {
                // Handle the case where the private key is unencrypted.
                privateKeyInfo = (PrivateKeyInfo) pemObject;
            }
            pemParser.close();
            JcaPEMKeyConverter converter = new JcaPEMKeyConverter().setProvider(BouncyCastleProvider.PROVIDER_NAME);
            return converter.getPrivateKey(privateKeyInfo);
        }

        static File loadFileFromClasspath(String filePath) {
            // Use the class loader to load the file from the classpath.
            ClassLoader classLoader = ConnectionHelper.class.getClassLoader();

            // Get the file as a URL.
            URL url = classLoader.getResource(filePath);

            if (url == null) {
                throw new IllegalArgumentException("File not found on classpath");
            }

            try {
                // Convert the URL to a URI, then to a File.
                URI uri = url.toURI();
                return new File(uri);
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException("URL could not be converted to a URI", e);
            }
        }

        public static String getPrivateKeyAsString(String filePath) throws IOException {
            File privateKeyFile = loadFileFromClasspath(filePath);
            return new String(Files.readAllBytes(privateKeyFile.toPath()));
        }

    }
}
